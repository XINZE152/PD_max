"""15 天仓库发货预测服务（豆包方案）。

流程：验证请求 → 构建 prompt → 调用 AI → 解析 JSON → 缓存 → 返回。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.models import DeliveryRecord, LeadMarketPrice, PredictionResult as PredictionResultRow
from app.intelligent_prediction.schemas.doubao_prediction import (
    DailyTonnageItem,
    DoubaoBatchRequest,
    DoubaoHistoryItem,
    DoubaoPredictionRequest,
    DoubaoPredictionResult,
    SmelterPriceItem,
    SMMPricingItem,
)
from app.intelligent_prediction.services.ai_client import AIModelClient
from app.intelligent_prediction.services.cache_manager import CacheManager
from app.intelligent_prediction.services.doubao_prompt_builder import DoubaoPromptBuilder
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.utils.json_extract import extract_json_object

logger = get_logger(__name__)

# 预测天数（day0 ~ day15）
HORIZON = 16

# 缓存 TTL（秒）
CACHE_TTL_SECONDS = 600  # 10 分钟


class DoubaoPredictionService:
    """15 天发货预测编排服务。"""

    def __init__(
        self,
        ai_client: AIModelClient,
        cache: CacheManager,
        prompt_builder: DoubaoPromptBuilder,
    ) -> None:
        self._ai = ai_client
        self._cache = cache
        self._prompt = prompt_builder
        self._batch_semaphore = asyncio.Semaphore(10)

    # ------------------------------------------------------------------
    # 公开方法
    # ------------------------------------------------------------------

    async def predict_single(
        self,
        session: AsyncSession,
        req: DoubaoPredictionRequest,
    ) -> DoubaoPredictionResult:
        """单笔 15 天预测。"""
        start = req.prediction_start_date or date.today()
        forecast_dates = [start + timedelta(days=i) for i in range(HORIZON)]

        # 自动从 DB 加载缺失数据
        req = await self._auto_load_missing_data(session, req)

        logger.info(
            "doubao_prediction_request warehouse=%s variety=%s history_len=%d",
            req.warehouse,
            req.product_variety or "(全部)",
            len(req.history),
        )

        # 1. 缓存检查（校验脏数据：空报告 + 全零预测 = 旧版 local_rule 污染，拒绝命中）
        cache_key = self._build_cache_key(req)
        if req.use_cache:
            cached = await self._cache.redis.get_json(cache_key)
            if cached is not None:
                if self._is_cache_data_valid(cached):
                    logger.info("doubao_prediction_cache_hit warehouse=%s", req.warehouse)
                    return self._result_from_cache(cached)
                else:
                    logger.warning(
                        "doubao_prediction_cache_rejected warehouse=%s — stale/bad data, refetching",
                        req.warehouse,
                    )

        # 2. 构建 prompt
        system_prompt, user_prompt = self._prompt.build_messages(req, forecast_dates)

        # 3. 调用 AI（历史重量按日期聚合后传入，用于本地 fallback 的日发货量均值计算）
        t0 = time.monotonic()
        daily_agg: dict[date, float] = defaultdict(float)
        for h in (req.history or []):
            daily_agg[h.送货日期] += float(h.重量吨)
        history_weights = [Decimal(str(w)) for w in daily_agg.values()]
        parsed_json, provider, latency_ms, cost_usd, raw_excerpt, errors = (
            await self._ai.complete_with_fallback(
                system_prompt,
                user_prompt,
                history_weights=history_weights,
                horizon_days=HORIZON,
                warehouse=req.warehouse,
                product_variety=req.product_variety or "",
                start_date=start,
            )
        )
        elapsed_ms = (time.monotonic() - t0) * 1000

        if errors:
            logger.warning("doubao_prediction_ai_errors: %s", errors)

        # 4. 解析 LLM 输出
        items, report_text, parse_error = self._parse_llm_response(
            parsed_json, forecast_dates
        )

        # 推断冶炼厂（从历史数据中取最常见的）
        smelter = self._infer_smelter(req)

        result = DoubaoPredictionResult(
            warehouse=req.warehouse,
            product_variety=req.product_variety,
            analysis_report=report_text,
            items=items,
            provider_used=provider,
            latency_ms=round(elapsed_ms, 2),
            cost_usd=cost_usd,
            cache_hit=False,
            parse_error=parse_error or (raw_excerpt if errors else None),
        )

        # 5. 写缓存（本地规则推算的结果不缓存，避免污染）
        if req.use_cache and provider != "local_rule":
            await self._cache.redis.set_json(
                cache_key,
                result.model_dump(mode="json"),
                CACHE_TTL_SECONDS,
            )

        return result

    async def predict_batch(
        self,
        batch: DoubaoBatchRequest,
    ) -> list[DoubaoPredictionResult]:
        """批量预测（并发控制）。"""
        SessionFactory = get_prediction_session_factory()

        async def one(r: DoubaoPredictionRequest) -> DoubaoPredictionResult:
            async with self._batch_semaphore:
                async with SessionFactory() as session:
                    try:
                        out = await self.predict_single(session, r)
                        await session.commit()
                        return out
                    except Exception:
                        await session.rollback()
                        raise

        tasks = [one(r) for r in batch.items]
        return await asyncio.gather(*tasks)

    async def persist_sync_results(
        self,
        session: AsyncSession,
        rows: list[DoubaoPredictionResult],
        batch_id: Optional[str] = None,
        history_map: Optional[dict[str, list[DoubaoHistoryItem]]] = None,
    ) -> None:
        """将预测结果写入 pd_ip_prediction_results 表。

        history_map: 仓库名 → 历史记录列表，用于推断 regional_manager 和 smelter。
        """
        for pr in rows:
            # 从 history_map 推断 regional_manager 和 smelter
            regional_manager = None
            smelter = None
            if history_map and pr.warehouse in history_map:
                hist = history_map[pr.warehouse]
                if hist:
                    # 取最新记录的大区经理
                    regional_manager = hist[0].大区经理
                    # 取出现次数最多的冶炼厂
                    from collections import Counter
                    smelter_counter = Counter(h.冶炼厂 for h in hist if h.冶炼厂)
                    if smelter_counter:
                        smelter = smelter_counter.most_common(1)[0][0]

            for it in pr.items:
                session.add(
                    PredictionResultRow(
                        batch_id=batch_id,
                        regional_manager=regional_manager,
                        warehouse=pr.warehouse,
                        product_variety=pr.product_variety or "",
                        smelter=smelter,
                        target_date=it.target_date,
                        predicted_weight=it.predicted_weight,
                        confidence=it.confidence_level,
                        ship_probability=it.ship_probability,
                        expected_ship_date=it.target_date,
                        expected_shipment=it.predicted_weight,
                        confidence_level=it.confidence_level,
                        main_factors=it.main_factors,
                        history_analysis="详见 comprehensive_analysis",
                        price_sensitivity_analysis="详见 comprehensive_analysis",
                        price_competitiveness_analysis="详见 comprehensive_analysis",
                        holiday_analysis="详见 comprehensive_analysis",
                        weather_analysis="详见 comprehensive_analysis",
                        comprehensive_analysis=pr.analysis_report[:10000] if pr.analysis_report else None,
                        provider_used=pr.provider_used,
                        latency_ms=Decimal(str(pr.latency_ms)),
                        cost_usd=Decimal(str(pr.cost_usd)) if pr.cost_usd is not None else None,
                        raw_response_excerpt=(pr.parse_error or "")[:2000] if pr.parse_error else None,
                    )
                )

    # ------------------------------------------------------------------
    # 自动从 DB 加载数据
    # ------------------------------------------------------------------

    async def _auto_load_missing_data(
        self,
        session: AsyncSession,
        req: DoubaoPredictionRequest,
    ) -> DoubaoPredictionRequest:
        """当 history / smelter_prices / smm_prices 为空时，自动从数据库加载。"""
        updates: dict[str, Any] = {}

        if not req.history:
            history = await self._load_history_from_db(
                session, req.warehouse, req.product_variety,
            )
            if history:
                updates["history"] = history

        if not req.smelter_prices:
            smelter_prices = await self._load_smelter_prices_from_db(
                session, req.warehouse, req.product_variety,
            )
            if smelter_prices:
                updates["smelter_prices"] = smelter_prices

        if not req.smm_prices:
            smm_prices = await self._load_smm_prices_from_db(session)
            if smm_prices:
                updates["smm_prices"] = smm_prices

        if updates:
            req = req.model_copy(update=updates)
        return req

    async def _load_history_from_db(
        self,
        session: AsyncSession,
        warehouse: str,
        product_variety: Optional[str],
        limit: int = 200,
    ) -> list[DoubaoHistoryItem]:
        """从 pd_ip_delivery_records 加载历史送货数据。"""
        conds = [DeliveryRecord.warehouse == warehouse]
        if product_variety:
            conds.append(DeliveryRecord.product_variety == product_variety)

        stmt = (
            select(DeliveryRecord)
            .where(and_(*conds))
            .order_by(desc(DeliveryRecord.delivery_date))
            .limit(limit)
        )
        res = await session.execute(stmt)
        rows = list(res.scalars().all())

        if not rows and product_variety:
            # 指定品种无数据时，尝试不带品种筛选
            stmt_fallback = (
                select(DeliveryRecord)
                .where(DeliveryRecord.warehouse == warehouse)
                .order_by(desc(DeliveryRecord.delivery_date))
                .limit(limit)
            )
            res = await session.execute(stmt_fallback)
            rows = list(res.scalars().all())

        items = []
        for r in rows:
            weather = None
            if r.weather_json and isinstance(r.weather_json, dict):
                weather = r.weather_json.get("summary") or r.weather_json.get("description")
            if not weather:
                weather = r.import_weather

            items.append(DoubaoHistoryItem(
                **{
                    "送货日期": r.delivery_date,
                    "大区经理": r.regional_manager,
                    "冶炼厂": r.smelter,
                    "仓库": r.warehouse,
                    "品类": r.product_variety,
                    "天气": weather,
                    "重量(吨)": r.weight,
                }
            ))

        logger.info(
            "doubao_loaded_history warehouse=%s variety=%s count=%d",
            warehouse, product_variety or "(全部)", len(items),
        )
        return items

    async def _load_smelter_prices_from_db(
        self,
        session: AsyncSession,
        warehouse: str,
        product_variety: Optional[str],
        days: int = 30,
    ) -> list[SmelterPriceItem]:
        """从 quote_details 加载冶炼厂报价数据。

        通过 quote_details → dict_factories 关联获取冶炼厂名称和基准价。
        """
        from app.intelligent_prediction.models import Base as IPBase
        # 使用原始 SQL 查询（因为 quote_details 在主库，不在 async engine）
        # 这里通过 async session 执行子查询
        cutoff = date.today() - timedelta(days=days)

        # 直接用 async session 的 execute
        from sqlalchemy import text
        sql = text("""
            SELECT
                qd.quote_date AS price_date,
                df.name AS factory_name,
                qd.category_name,
                qd.unit_price
            FROM quote_details qd
            JOIN dict_factories df ON df.id = qd.factory_id
            WHERE qd.quote_date >= :cutoff
              AND qd.unit_price IS NOT NULL
            ORDER BY qd.quote_date DESC
            LIMIT 30
        """)
        res = await session.execute(sql, {"cutoff": cutoff})
        rows = res.fetchall()

        items = []
        for r in rows:
            items.append(SmelterPriceItem(**{
                "日期": r.price_date,
                "冶炼厂": r.factory_name,
                "品种": r.category_name,
                "基准价": Decimal(str(r.unit_price)),
            }))

        logger.info("doubao_loaded_smelter_prices count=%d", len(items))
        return items

    async def _load_smm_prices_from_db(
        self,
        session: AsyncSession,
        days: int = 30,
    ) -> list[SMMPricingItem]:
        """从 pd_smm_lead_reference_prices 加载 SMM 铅价。"""
        from sqlalchemy import text
        cutoff = date.today() - timedelta(days=days)
        sql = text("""
            SELECT quote_date, price_low, price_high, average_price
            FROM pd_smm_lead_reference_prices
            WHERE quote_date >= :cutoff
            ORDER BY quote_date DESC
            LIMIT 30
        """)
        res = await session.execute(sql, {"cutoff": cutoff})
        rows = res.fetchall()

        items = []
        for r in rows:
            items.append(SMMPricingItem(**{
                "定价日期": r.quote_date,
                "最低价": Decimal(str(r.price_low)),
                "最高价": Decimal(str(r.price_high)),
                "均价": Decimal(str(r.average_price)),
            }))

        logger.info("doubao_loaded_smm_prices count=%d", len(items))
        return items

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    @staticmethod
    def _is_cache_data_valid(cached: dict[str, Any]) -> bool:
        """校验缓存数据是否合法，拒绝已知的脏数据模式。

        拒绝条件（任一满足即拒绝）：
        - analysis_report 为空 且 全部 predicted_weight == 0
          （旧版 _local_rule_json 污染的典型特征）
        - items 为空列表
        """
        items = cached.get("items")
        if not isinstance(items, list) or len(items) == 0:
            return False

        report = cached.get("analysis_report")
        report_empty = not report or not str(report).strip()
        all_zero = all(
            float(it.get("predicted_weight", 0) or 0) == 0.0
            for it in items
            if isinstance(it, dict)
        )
        if report_empty and all_zero:
            return False

        return True

    def _build_cache_key(self, req: DoubaoPredictionRequest) -> str:
        """基于请求数据指纹生成 Redis 缓存键。"""
        fingerprint_data = {
            "warehouse": req.warehouse,
            "variety": req.product_variety or "",
            "start": (req.prediction_start_date or date.today()).isoformat(),
            "history_hash": self._hash_list(req.history),
            "smelter_hash": self._hash_list(req.smelter_prices),
            "smm_hash": self._hash_list(req.smm_prices),
        }
        fp = CacheManager.stats_fingerprint(fingerprint_data)
        return f"pred:doubao:v2:{fp}"

    @staticmethod
    def _hash_list(items: list[Any]) -> str:
        """将列表序列化后取 SHA256 短指纹。"""
        if not items:
            return "empty"
        try:
            s = json.dumps(
                [item.model_dump(mode="json") for item in items],
                sort_keys=True,
                ensure_ascii=False,
                default=str,
            )
        except Exception:
            s = str(items)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _result_from_cache(cached: dict[str, Any]) -> DoubaoPredictionResult:
        """从缓存字典恢复 DoubaoPredictionResult。"""
        items = []
        for it in cached.get("items", []):
            items.append(
                DailyTonnageItem(
                    target_date=date.fromisoformat(it["target_date"]) if isinstance(it.get("target_date"), str) else it.get("target_date"),
                    predicted_weight=Decimal(str(it.get("predicted_weight", 0))),
                    ship_probability=it.get("ship_probability", "中"),
                    confidence_level=it.get("confidence_level", "中"),
                    main_factors=it.get("main_factors", ""),
                )
            )
        return DoubaoPredictionResult(
            warehouse=cached.get("warehouse", ""),
            product_variety=cached.get("product_variety"),
            analysis_report=cached.get("analysis_report", ""),
            items=items,
            provider_used=cached.get("provider_used", "cached"),
            latency_ms=cached.get("latency_ms", 0),
            cost_usd=cached.get("cost_usd"),
            cache_hit=True,
            parse_error=cached.get("parse_error"),
        )

    @staticmethod
    def _parse_llm_response(
        parsed: dict[str, Any] | None,
        forecast_dates: list[date],
    ) -> tuple[list[DailyTonnageItem], str, Optional[str]]:
        """解析 LLM 返回的 JSON，返回 (items, report_text, parse_error)。"""
        if parsed is None:
            return (
                [DailyTonnageItem(target_date=d, predicted_weight=Decimal("0"),
                                  ship_probability="低", confidence_level="低",
                                  main_factors="模型未返回有效数据")
                 for d in forecast_dates],
                "",
                "模型未返回有效 JSON",
            )

        # 提取分析报告
        report_text = str(parsed.get("analysis_report", ""))[:10000]

        # 提取逐日预测
        raw_items = parsed.get("items")
        if not isinstance(raw_items, list):
            raw_items = []

        items: list[DailyTonnageItem] = []
        used_dates: set[str] = set()

        for i, ed in enumerate(forecast_dates):
            entry: dict[str, Any] | None = None

            # 优先按索引匹配
            if i < len(raw_items) and isinstance(raw_items[i], dict):
                entry = raw_items[i]

            # 其次按日期匹配
            if entry is None:
                for cand in raw_items:
                    if not isinstance(cand, dict):
                        continue
                    td = cand.get("target_date")
                    if td == ed.isoformat() or td == str(ed):
                        entry = cand
                        break

            if entry is None:
                items.append(
                    DailyTonnageItem(
                        target_date=ed,
                        predicted_weight=Decimal("0"),
                        ship_probability="低",
                        confidence_level="低",
                        main_factors="模型输出缺失该日期数据",
                    )
                )
                continue

            # 解析字段
            weight_raw = entry.get("predicted_weight", 0)
            try:
                weight = Decimal(str(weight_raw))
                if weight < 0:
                    weight = Decimal("0")
            except Exception:
                weight = Decimal("0")

            ship_prob = str(entry.get("ship_probability", "中")).strip()
            if ship_prob not in ("高", "中", "低"):
                ship_prob = "中"

            conf_level = str(entry.get("confidence_level", "中")).strip()
            if conf_level not in ("高", "中", "低"):
                conf_level = "中"

            main_factors = str(entry.get("main_factors", ""))[:500]

            items.append(
                DailyTonnageItem(
                    target_date=ed,
                    predicted_weight=weight,
                    ship_probability=ship_prob,
                    confidence_level=conf_level,
                    main_factors=main_factors,
                )
            )

        return items, report_text, None

    @staticmethod
    def _infer_smelter(req: DoubaoPredictionRequest) -> Optional[str]:
        """从冶炼厂价格数据中推断目标冶炼厂名称。"""
        if req.smelter_prices:
            # 取出现次数最多的冶炼厂
            from collections import Counter
            counter = Counter(p.冶炼厂 for p in req.smelter_prices)
            if counter:
                return counter.most_common(1)[0][0]
        if req.history:
            from collections import Counter
            counter = Counter(h.冶炼厂 for h in req.history if h.冶炼厂)
            if counter:
                return counter.most_common(1)[0][0]
        return None

    @staticmethod
    def _infer_smelter_from_history(warehouse: str, items: list[DailyTonnageItem]) -> Optional[str]:
        """从结果中推断冶炼厂（简化版，实际应从请求上下文获取）。"""
        return None


def get_doubao_prediction_service(
    ai_client: AIModelClient,
    cache: CacheManager,
    prompt_builder: DoubaoPromptBuilder,
) -> DoubaoPredictionService:
    """组装 DoubaoPredictionService。"""
    return DoubaoPredictionService(ai_client, cache, prompt_builder)
