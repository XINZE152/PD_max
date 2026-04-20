"""核心预测逻辑：缓存、并发限制、结果校验。"""

from __future__ import annotations

import asyncio
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
    PredictionHistoryPoint,
    PredictionItem,
    PredictionRequest,
    PredictionResultSchema,
)
from app.intelligent_prediction.services.ai_client import AIModelClient
from app.intelligent_prediction.services.cache_manager import CacheManager
from app.intelligent_prediction.services.prompt_builder import PromptBuilder
from app.intelligent_prediction.services.weather_client import (
    fetch_forecast_weather_by_dates,
    summary_from_weather_json,
)

logger = get_logger(__name__)


class PredictionService:
    """送货量预测服务。"""

    _batch_semaphore = asyncio.Semaphore(10)

    def __init__(
        self,
        ai_client: AIModelClient,
        cache: CacheManager,
        prompt_builder: PromptBuilder,
    ) -> None:
        self._ai = ai_client
        self._cache = cache
        self._prompt = prompt_builder

    def _utc_today(self) -> date:
        """取得 UTC 当日日期。"""
        return datetime.now(timezone.utc).date()

    async def _load_history_from_db(
        self,
        session: AsyncSession,
        warehouse: str,
        variety: str,
        smelter: Optional[str] = None,
        limit: int = 120,
    ) -> list[PredictionHistoryPoint]:
        """从数据库加载最近历史记录。"""
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
        api = summary_from_weather_json(getattr(r, "weather_json", None))
        if api:
            return api[:500]
        return "晴"

    async def _resolve_weather_context(
        self,
        session: AsyncSession,
        req: PredictionRequest,
    ) -> tuple[str, Optional[str], str]:
        """用于预测日天气拉取：取同条件最近一条历史的仓/厂名与地址拼接兜底。"""
        conds = [
            DeliveryRecord.warehouse == req.warehouse,
            DeliveryRecord.product_variety == req.product_variety,
        ]
        if req.smelter and str(req.smelter).strip():
            conds.append(DeliveryRecord.smelter == str(req.smelter).strip())
        stmt = (
            select(DeliveryRecord)
            .where(and_(*conds))
            .order_by(DeliveryRecord.delivery_date.desc(), DeliveryRecord.id.desc())
            .limit(1)
        )
        row = (await session.execute(stmt)).scalar_one_or_none()
        wh = (str(row.warehouse).strip() if row else "") or str(req.warehouse).strip()
        sm: Optional[str] = None
        if row and row.smelter and str(row.smelter).strip():
            sm = str(row.smelter).strip()
        elif req.smelter and str(req.smelter).strip():
            sm = str(req.smelter).strip()
        wa = (str(row.warehouse_address).strip() if row and row.warehouse_address else "")
        sa = (str(row.smelter_address).strip() if row and row.smelter_address else "")
        loc = " ".join(x for x in [wa, sa, wh] if x) or wh
        return wh, sm, loc

    async def _ensure_request_history(
        self,
        session: AsyncSession,
        req: PredictionRequest,
    ) -> PredictionRequest:
        """若请求未带 history，则由数据库补齐。"""
        if req.history:
            return req
        hist = await self._load_history_from_db(
            session,
            req.warehouse,
            req.product_variety,
            smelter=req.smelter,
        )
        return req.model_copy(update={"history": hist})

    async def _resolve_result_smelter(
        self,
        session: AsyncSession,
        req: PredictionRequest,
    ) -> Optional[str]:
        """返回展示用冶炼厂：请求已填优先；否则按仓+品种从历史表取出现次数最多的非空冶炼厂。"""
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

    def _post_process_items(
        self,
        items: list[PredictionItem],
        parse_error: Optional[str],
    ) -> list[PredictionItem]:
        """负数修正为 0 并标记低信心；相邻日波动>50% 加警告。"""
        out: list[PredictionItem] = []
        prev_w: float | None = None
        for it in sorted(items, key=lambda x: x.target_date):
            w = float(it.predicted_weight)
            warnings = list(it.warnings or [])
            conf = it.confidence if isinstance(it.confidence, str) else str(it.confidence)
            if w < 0:
                w = 0.0
                conf = "low"
                warnings.append("negative_corrected_to_zero")
            new_it = PredictionItem(
                target_date=it.target_date,
                predicted_weight=Decimal(str(w)),
                confidence=conf,
                warnings=warnings,
            )
            if parse_error:
                nw = list(new_it.warnings)
                nw.append(f"parse_assist_used:{parse_error}")
                new_it = PredictionItem(
                    target_date=new_it.target_date,
                    predicted_weight=new_it.predicted_weight,
                    confidence="low",
                    warnings=nw,
                )
            if prev_w is not None and prev_w > 0:
                ratio = abs(w - prev_w) / prev_w
                if ratio > 0.5:
                    nw = list(new_it.warnings)
                    nw.append(f"adjacent_day_swing_gt_50pct:{ratio:.2f}")
                    new_it = PredictionItem(
                        target_date=new_it.target_date,
                        predicted_weight=new_it.predicted_weight,
                        confidence=new_it.confidence,
                        warnings=nw,
                    )
            prev_w = w
            out.append(new_it)
        return out

    def _items_from_parsed(
        self,
        parsed: dict[str, Any],
        start_date: date,
        horizon: int,
    ) -> tuple[list[PredictionItem], Optional[str]]:
        """将模型 JSON 转为 PredictionItem 列表。"""
        parse_err: Optional[str] = None
        raw_items = parsed.get("items")
        if not isinstance(raw_items, list):
            return [], "items_not_list"
        items: list[PredictionItem] = []
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
                if entry is None and i < len(raw_items) and isinstance(raw_items[i], dict):
                    entry = raw_items[i]
            if entry is None:
                items.append(
                    PredictionItem(
                        target_date=ed,
                        predicted_weight=Decimal("0"),
                        confidence="low",
                        warnings=["missing_model_output_for_date"],
                    )
                )
                continue
            td_raw = entry.get("target_date")
            try:
                if td_raw:
                    td = date.fromisoformat(str(td_raw)[:10])
                else:
                    td = ed
            except ValueError:
                td = ed
                parse_err = parse_err or "bad_target_date"
            pw = entry.get("predicted_weight", 0)
            try:
                wdec = Decimal(str(pw))
            except Exception:
                wdec = Decimal("0")
                parse_err = parse_err or "bad_weight"
            conf = str(entry.get("confidence", "medium"))
            warns = entry.get("warnings")
            if warns is None:
                wl: list[str] = []
            elif isinstance(warns, list):
                wl = [str(x) for x in warns]
            else:
                wl = [str(warns)]
            items.append(
                PredictionItem(
                    target_date=td,
                    predicted_weight=wdec,
                    confidence=conf,
                    warnings=wl,
                )
            )
        return items, parse_err

    async def predict_single(
        self,
        session: AsyncSession,
        req: PredictionRequest,
    ) -> PredictionResultSchema:
        """单笔预测：L1 + L2 Redis。"""
        req = await self._ensure_request_history(session, req)
        resolved_smelter = await self._resolve_result_smelter(session, req)
        start = req.prediction_start_date or self._utc_today()
        logger.info(
            "prediction_request warehouse=%s smelter=%s variety=%s horizon=%s hist_count=%s client_request_id=%s",
            req.warehouse,
            req.smelter,
            req.product_variety,
            req.horizon_days,
            len(req.history),
            req.client_request_id,
        )
        stats = self._prompt.analyze_history(req.history)
        fp = self._cache.stats_fingerprint(stats)
        forecast_dates = [start + timedelta(days=i) for i in range(req.horizon_days)]
        wh_ctx, sm_ctx, loc_fb = await self._resolve_weather_context(session, req)
        forecast_map = await fetch_forecast_weather_by_dates(
            forecast_dates, wh_ctx, sm_ctx, loc_fb, default_when_missing="晴"
        )
        forecast_fp = self._cache.forecast_weather_fingerprint(forecast_map)
        sm_part = req.smelter or ""
        mem_key = f"prompt:{req.warehouse}:{sm_part}:{req.product_variety}:{fp}:{forecast_fp}"
        cached_prompt = await self._cache.memory.get(mem_key)
        if cached_prompt is None:
            system, user = self._prompt.build_messages(
                req, stats, start, forecast_weather_by_date=forecast_map
            )
            await self._cache.memory.set(mem_key, (system, user))
        else:
            system, user = cached_prompt

        redis_key = self._cache.prediction_cache_key(
            req.warehouse,
            req.product_variety,
            req.horizon_days,
            fp,
            smelter=req.smelter,
            forecast_fp=forecast_fp,
        )
        if req.use_cache:
            cached = await self._cache.redis.get_json(redis_key)
            if isinstance(cached, dict):
                try:
                    return PredictionResultSchema.model_validate(cached)
                except Exception:
                    logger.warning("redis prediction cache schema mismatch, ignoring")

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
        parse_note: Optional[str] = None
        if errs:
            parse_note = ";".join(errs)[:500]

        items, perr = self._items_from_parsed(parsed, start, req.horizon_days)
        if perr:
            parse_note = (parse_note + "|" if parse_note else "") + perr
        items = self._post_process_items(items, parse_note)

        result = PredictionResultSchema(
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
                redis_key,
                result.model_dump(mode="json"),
                settings.prediction_redis_ttl_seconds,
            )
        return result

    async def predict_batch(self, batch: BatchPredictionRequest) -> list[PredictionResultSchema]:
        """批量预测：Semaphore(10)，每笔独立 Session。"""
        SessionFactory = get_prediction_session_factory()

        async def one(r: PredictionRequest) -> PredictionResultSchema:
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
        rows: list[PredictionResultSchema],
        batch_id: Optional[str] = None,
    ) -> None:
        """将预测结果写入数据库。"""
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
                        predicted_weight=it.predicted_weight,
                        confidence=str(it.confidence),
                        warnings=list(it.warnings),
                        provider_used=pr.provider_used,
                        latency_ms=Decimal(str(pr.latency_ms)),
                        cost_usd=Decimal(str(pr.cost_usd)) if pr.cost_usd is not None else None,
                        raw_response_excerpt=pr.parse_error,
                    )
                )


def get_prediction_service(
    ai_client: AIModelClient,
    cache: CacheManager,
    prompt_builder: PromptBuilder,
) -> PredictionService:
    """组装 PredictionService。"""
    return PredictionService(ai_client, cache, prompt_builder)
