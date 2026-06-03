"""综合预测 v2 — 核心服务。

整合：历史深度分析 + 价格竞争力 + 价格敏感度 + 节假日 + 天气 + LLM 推理。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.settings import settings
from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.models import DeliveryRecord
from app.intelligent_prediction.models import PredictionResult as PredictionResultRow
from app.intelligent_prediction.schemas.prediction import (
    BatchPredictionRequest,
    ComprehensiveBatchRequest,
    ComprehensivePredictionItem,
    ComprehensivePredictionResult,
    PredictionHistoryPoint,
    PredictionItem,
    PredictionRequest,
)
from app.intelligent_prediction.services.ai_client import AIModelClient
from app.intelligent_prediction.services.cache_manager import CacheManager
from app.intelligent_prediction.services.comprehensive_history_analysis import (
    analyze_delivery_pattern,
    format_history_analysis_text,
)
from app.intelligent_prediction.services.price_competitiveness_service import (
    analyze_price_competitiveness,
)
from app.intelligent_prediction.services.holiday_analysis_service import (
    analyze_holiday_impact,
)
from app.intelligent_prediction.services.comprehensive_prompt_builder import (
    ComprehensivePromptBuilder,
)
from app.intelligent_prediction.services.price_context_service import (
    estimate_warehouse_price_profile,
    resolve_own_factory_id,
)
from app.intelligent_prediction.services.weather_client import (
    fetch_forecast_weather_by_dates,
    summary_from_weather_json,
)

logger = get_logger(__name__)


class ComprehensivePredictionService:
    """综合预测 v2 服务。"""

    _batch_semaphore = asyncio.Semaphore(10)

    def __init__(
        self,
        ai_client: AIModelClient,
        cache: CacheManager,
        prompt_builder: ComprehensivePromptBuilder,
    ) -> None:
        self._ai = ai_client
        self._cache = cache
        self._prompt = prompt_builder

    def _utc_today(self) -> date:
        return datetime.now(timezone.utc).date()

    async def _load_history_from_db(
        self,
        session: AsyncSession,
        warehouse: str,
        variety: str,
        smelter: Optional[str] = None,
        limit: int = 120,
    ) -> list[PredictionHistoryPoint]:
        """从数据库加载最近历史记录。

        若指定品种无数据，自动推断该仓库（+冶炼厂）出现次数最多的品种。
        """
        conds = [
            DeliveryRecord.warehouse == warehouse,
            DeliveryRecord.product_variety == variety,
        ]
        if smelter:
            conds.append(DeliveryRecord.smelter == smelter)
        stmt = (
            select(DeliveryRecord)
            .where(and_(*conds))
            .order_by(DeliveryRecord.delivery_date.desc())
            .limit(limit)
        )
        res = await session.execute(stmt)
        rows = list(res.scalars().all())

        # 指定品种无数据时，自动推断最常见品种
        if not rows:
            variety_conds = [DeliveryRecord.warehouse == warehouse]
            if smelter:
                variety_conds.append(DeliveryRecord.smelter == smelter)
            variety_stmt = (
                select(DeliveryRecord.product_variety, func.count().label("cnt"))
                .where(and_(*variety_conds))
                .group_by(DeliveryRecord.product_variety)
                .order_by(func.count().desc())
                .limit(1)
            )
            vrow = (await session.execute(variety_stmt)).first()
            if vrow and vrow[0]:
                inferred_variety = str(vrow[0]).strip()
                logger.info(
                    "infer product_variety for warehouse=%s smelter=%s: %r -> %r",
                    warehouse, smelter, variety, inferred_variety,
                )
                conds = [
                    DeliveryRecord.warehouse == warehouse,
                    DeliveryRecord.product_variety == inferred_variety,
                ]
                if smelter:
                    conds.append(DeliveryRecord.smelter == smelter)
                stmt = (
                    select(DeliveryRecord)
                    .where(and_(*conds))
                    .order_by(DeliveryRecord.delivery_date.desc())
                    .limit(limit)
                )
                res = await session.execute(stmt)
                rows = list(res.scalars().all())

        rows.reverse()
        return [
            PredictionHistoryPoint(
                delivery_date=r.delivery_date,
                weight=Decimal(r.weight),
                cn_calendar_label=getattr(r, "cn_calendar_label", None),
                weather_summary=self._merged_history_weather(r),
            )
            for r in rows
        ]

    @staticmethod
    def _merged_history_weather(r: DeliveryRecord) -> str:
        imp = (getattr(r, "import_weather", None) or "").strip()
        if imp:
            return imp[:200]
        from app.intelligent_prediction.services.weather_client import summary_from_weather_json
        api = summary_from_weather_json(getattr(r, "weather_json", None))
        if api:
            return api[:500]
        return "晴"

    async def _ensure_request_history(
        self,
        session: AsyncSession,
        req: PredictionRequest,
    ) -> PredictionRequest:
        if req.history:
            return req
        hist = await self._load_history_from_db(
            session, req.warehouse, req.product_variety, smelter=req.smelter,
        )
        return req.model_copy(update={"history": hist})

    async def _resolve_result_smelter(
        self,
        session: AsyncSession,
        req: PredictionRequest,
    ) -> Optional[str]:
        if req.smelter and str(req.smelter).strip():
            return str(req.smelter).strip()
        stmt = (
            select(DeliveryRecord.smelter, func.count().label("cnt"))
            .where(
                DeliveryRecord.warehouse == req.warehouse,
                DeliveryRecord.product_variety == req.product_variety,
                DeliveryRecord.smelter.isnot(None),
                DeliveryRecord.smelter != "",
            )
            .group_by(DeliveryRecord.smelter)
            .order_by(func.count().desc())
            .limit(1)
        )
        row = (await session.execute(stmt)).first()
        if row is None or row[0] is None:
            return None
        s = str(row[0]).strip()
        return s or None

    def _items_from_parsed(
        self,
        parsed: dict[str, Any],
        start_date: date,
        horizon: int,
    ) -> list[ComprehensivePredictionItem]:
        """将模型 JSON 转为 ComprehensivePredictionItem 列表。"""
        raw_items = parsed.get("items")
        if not isinstance(raw_items, list):
            return []

        items: list[ComprehensivePredictionItem] = []
        expected_dates = [start_date + timedelta(days=i) for i in range(horizon)]

        for i, ed in enumerate(expected_dates):
            entry: dict[str, Any] | None = None
            if i < len(raw_items) and isinstance(raw_items[i], dict):
                entry = raw_items[i]
            elif isinstance(raw_items, list):
                for cand in raw_items:
                    if not isinstance(cand, dict):
                        continue
                    td = cand.get("target_date")
                    if td == ed.isoformat() or td == str(ed):
                        entry = cand
                        break

            if entry is None:
                # 兜底条目
                items.append(
                    ComprehensivePredictionItem(
                        target_date=ed,
                        ship_probability="中",
                        expected_ship_date=None,
                        expected_shipment=Decimal("0"),
                        confidence_level="低",
                        main_factors="模型输出缺失该日期数据",
                        history_analysis="无",
                        price_sensitivity_analysis="无",
                        price_competitiveness_analysis="无",
                        holiday_analysis="无",
                        weather_analysis="无",
                        comprehensive_analysis="模型输出缺失该日期数据。",
                    )
                )
                continue

            # 解析各字段
            td_raw = entry.get("target_date")
            try:
                td = date.fromisoformat(str(td_raw)[:10]) if td_raw else ed
            except (ValueError, TypeError):
                td = ed

            ship_prob = str(entry.get("ship_probability", "中")).strip()
            if ship_prob not in ("高", "中", "低"):
                ship_prob = "中"

            exp_date_raw = entry.get("expected_ship_date")
            try:
                exp_date = date.fromisoformat(str(exp_date_raw)[:10]) if exp_date_raw else None
            except (ValueError, TypeError):
                exp_date = None

            exp_shipment = entry.get("expected_shipment", 0)
            try:
                exp_shipment_dec = Decimal(str(exp_shipment))
                if exp_shipment_dec <= 0:
                    exp_shipment_dec = Decimal("0")
            except Exception:
                exp_shipment_dec = Decimal("0")

            conf_level = str(entry.get("confidence_level", "中")).strip()
            if conf_level not in ("高", "中", "低"):
                conf_level = "中"

            main_factors = str(entry.get("main_factors", ""))[:500]
            history_analysis = str(entry.get("history_analysis", ""))[:2000]
            price_sensitivity = str(entry.get("price_sensitivity_analysis", ""))[:2000]
            price_competitiveness = str(entry.get("price_competitiveness_analysis", ""))[:2000]
            holiday = str(entry.get("holiday_analysis", ""))[:1000]
            weather = str(entry.get("weather_analysis", ""))[:1000]
            comprehensive = str(entry.get("comprehensive_analysis", ""))[:3000]

            items.append(
                ComprehensivePredictionItem(
                    target_date=td,
                    ship_probability=ship_prob,
                    expected_ship_date=exp_date,
                    expected_shipment=exp_shipment_dec,
                    confidence_level=conf_level,
                    main_factors=main_factors,
                    history_analysis=history_analysis,
                    price_sensitivity_analysis=price_sensitivity,
                    price_competitiveness_analysis=price_competitiveness,
                    holiday_analysis=holiday,
                    weather_analysis=weather,
                    comprehensive_analysis=comprehensive,
                )
            )

        return items

    async def predict_single(
        self,
        session: AsyncSession,
        req: PredictionRequest,
    ) -> ComprehensivePredictionResult:
        """单笔综合预测。"""
        req = await self._ensure_request_history(session, req)
        resolved_smelter = await self._resolve_result_smelter(session, req)
        start = req.prediction_start_date or self._utc_today()
        forecast_dates = [start + timedelta(days=i) for i in range(req.horizon_days)]

        logger.info(
            "comprehensive_prediction_request warehouse=%s smelter=%s variety=%s horizon=%s",
            req.warehouse, req.smelter, req.product_variety, req.horizon_days,
        )

        # 1. 历史深度分析
        history_pattern = analyze_delivery_pattern(req.history, as_of_date=start)
        history_analysis_text = format_history_analysis_text(history_pattern)
        history_analysis = {
            "analysis_text": history_analysis_text,
            "pattern": history_pattern,
        }

        # 2. 价格竞争力分析
        fid = await resolve_own_factory_id(session)
        price_competitiveness = await analyze_price_competitiveness(
            session,
            as_of=start,
            product_variety=req.product_variety,
            own_factory_id=fid,
        )

        # 3. 价格敏感度分析
        profile = await estimate_warehouse_price_profile(
            session,
            warehouse=req.warehouse,
            product_variety=req.product_variety,
            own_factory_id=fid,
        )
        sens_label = {"sensitive": "高价格敏感", "medium": "中价格敏感", "stable": "低价格敏感"}.get(
            profile.sensitivity, profile.sensitivity
        )
        price_sensitivity_info = {
            "analysis_text": (
                f"该仓库价格敏感度等级：{sens_label}。"
                f"历史发货量与价格优势的相关系数为 {profile.correlation or 'N/A'}。"
                f"过去单日发货：最多 {profile.capacity_max} 吨、最少 {profile.capacity_min} 吨、"
                f"平均 {profile.capacity_avg} 吨。"
                f"{'价格无优势时经常暂停发货，属于高价格敏感仓库。' if profile.sensitivity == 'sensitive' else ''}"
                f"{'价格无优势时减少发货，属于中价格敏感仓库。' if profile.sensitivity == 'medium' else ''}"
                f"{'价格变化对发货影响较小，属于低价格敏感仓库。' if profile.sensitivity == 'stable' else ''}"
            ),
        }

        # 4. 节假日分析
        holiday_impact = analyze_holiday_impact(forecast_dates)

        # 5. 天气数据
        wh_ctx, sm_ctx, loc_fb = self._resolve_weather_location(req)
        weather_by_date = await fetch_forecast_weather_by_dates(
            forecast_dates, wh_ctx, sm_ctx, loc_fb, default_when_missing="晴"
        )

        # 缓存键
        hist_fp = self._cache.stats_fingerprint(history_pattern)
        price_fp = self._cache.stats_fingerprint(price_competitiveness)
        weather_fp = self._cache.forecast_weather_fingerprint(weather_by_date)
        redis_key = f"pred:v2:{self._cache.prediction_cache_key(req.warehouse, req.product_variety, req.horizon_days, hist_fp, smelter=req.smelter, forecast_fp=f'{weather_fp}:{price_fp}')}"

        if req.use_cache:
            cached = await self._cache.redis.get_json(redis_key)
            if isinstance(cached, dict):
                try:
                    hit = ComprehensivePredictionResult.model_validate(cached)
                    return hit.model_copy(update={"cache_hit": True})
                except Exception:
                    logger.warning("redis v2 cache schema mismatch, ignoring")

        # 构建 Prompt
        system, user = self._prompt.build_messages(
            req=req,
            history_analysis=history_analysis,
            price_competitiveness=price_competitiveness,
            holiday_impact=holiday_impact,
            weather_by_date=weather_by_date,
            price_sensitivity_info=price_sensitivity_info,
            forecast_dates=forecast_dates,
        )

        hist_weights = [h.weight for h in req.history]
        parsed, provider, lat, cost, raw_excerpt, errs = await self._ai.complete_with_fallback(
            system,
            user,
            history_weights=hist_weights,
            horizon_days=req.horizon_days,
            warehouse=req.warehouse,
            product_variety=req.product_variety,
            start_date=start,
        )

        items = self._items_from_parsed(parsed, start, req.horizon_days)

        parse_note = ";".join(errs)[:500] if errs else None
        if parse_note:
            for it in items:
                it = ComprehensivePredictionItem(
                    **{**it.model_dump(mode="json"), "main_factors": f"{it.main_factors}（解析备注：{parse_note}）"}
                )

        result = ComprehensivePredictionResult(
            warehouse=req.warehouse,
            product_variety=req.product_variety,
            smelter=resolved_smelter,
            regional_manager=req.regional_manager,
            items=items,
            provider_used=provider,
            latency_ms=lat,
            cost_usd=float(cost) if cost is not None else None,
            cache_hit=False,
            parse_error=parse_note,
        )

        if req.use_cache:
            await self._cache.redis.set_json(
                redis_key, result.model_dump(mode="json"), settings.prediction_redis_ttl_seconds,
            )

        return result

    async def predict_batch(
        self,
        batch: ComprehensiveBatchRequest,
    ) -> list[ComprehensivePredictionResult]:
        """批量综合预测。"""
        SessionFactory = get_prediction_session_factory()

        async def one(r: PredictionRequest) -> ComprehensivePredictionResult:
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
        rows: list[ComprehensivePredictionResult],
        batch_id: Optional[str] = None,
    ) -> None:
        """将综合预测结果写入数据库。"""
        for pr in rows:
            for it in pr.items:
                session.add(
                    PredictionResultRow(
                        batch_id=batch_id,
                        regional_manager=pr.regional_manager,
                        warehouse=pr.warehouse,
                        product_variety=pr.product_variety,
                        smelter=pr.smelter,
                        target_date=it.target_date,
                        predicted_weight=it.expected_shipment,
                        confidence=it.confidence_level,
                        ship_probability=it.ship_probability,
                        expected_ship_date=it.expected_ship_date,
                        expected_shipment=it.expected_shipment,
                        confidence_level=it.confidence_level,
                        main_factors=it.main_factors,
                        history_analysis=it.history_analysis,
                        price_sensitivity_analysis=it.price_sensitivity_analysis,
                        price_competitiveness_analysis=it.price_competitiveness_analysis,
                        holiday_analysis=it.holiday_analysis,
                        weather_analysis=it.weather_analysis,
                        comprehensive_analysis=it.comprehensive_analysis,
                        provider_used=pr.provider_used,
                        latency_ms=Decimal(str(pr.latency_ms)),
                        cost_usd=Decimal(str(pr.cost_usd)) if pr.cost_usd is not None else None,
                        raw_response_excerpt=pr.parse_error,
                    )
                )

    def _resolve_weather_location(self, req: PredictionRequest) -> tuple[str, Optional[str], str]:
        """简化版天气位置解析（从请求信息推断）。"""
        wh = str(req.warehouse).strip()
        sm: Optional[str] = str(req.smelter).strip() if req.smelter else None
        return wh, sm, wh


def get_comprehensive_prediction_service(
    ai_client: AIModelClient,
    cache: CacheManager,
    prompt_builder: ComprehensivePromptBuilder,
) -> ComprehensivePredictionService:
    """组装 ComprehensivePredictionService。"""
    return ComprehensivePredictionService(ai_client, cache, prompt_builder)
