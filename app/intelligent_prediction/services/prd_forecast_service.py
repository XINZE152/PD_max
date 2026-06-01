"""PRD V2：近30天线性加权移动平均 + 仓库周规律系数 + 价格因素（80%）与历史规律（20%）。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.intelligent_prediction.models import DeliveryRecord
from app.intelligent_prediction.schemas.forecast import (
    PrdForecastByRmSeries,
    PrdForecastChartResponse,
    PrdForecastDetailResponse,
    PrdForecastDetailRow,
    PrdForecastQuery,
    PrdForecastWarehouseProfile,
)
from app.intelligent_prediction.services.price_context_service import (
    blend_history_and_price,
    compute_price_factor,
    estimate_warehouse_price_profile,
    explain_prediction,
    load_price_context_for_horizon,
    resolve_own_factory_id,
)


def _daterange_inclusive(a: date, b: date) -> Iterable[date]:
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def _linear_wma(
    daily_amounts: dict[date, Decimal],
    forecast_day: date,
    window_days: int = 30,
) -> Decimal:
    """以 forecast_day 为预测日，使用 [D-window, D-1] 日历日上的有数据日做线性加权平均。"""
    end = forecast_day - timedelta(days=1)
    start = forecast_day - timedelta(days=window_days)
    num = Decimal("0")
    den = Decimal("0")
    for t in _daterange_inclusive(start, end):
        if t not in daily_amounts:
            continue
        w = Decimal((t - start).days + 1)
        num += daily_amounts[t] * w
        den += w
    if den == 0:
        return Decimal("0")
    return num / den


def _mean(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values) / Decimal(len(values))


def _series_positive_baseline(series: dict[date, Decimal]) -> Decimal:
    """序列在窗口内若有正送货量，取其均值作下限；否则用 1 占位（避免预测日为 0）。"""
    vals = [v for v in series.values() if v > 0]
    if not vals:
        return Decimal("1")
    m = sum(vals) / Decimal(len(vals))
    q = m.quantize(Decimal("0.01"))
    return q if q > 0 else Decimal("1")


def _weekday_coefs(
    daily_wh_totals: dict[tuple[str, date], Decimal],
    warehouses: set[str],
    ref_start: date,
    ref_end: date,
) -> dict[tuple[str, int], Decimal]:
    """各仓库 × 星期几(0=周一) 相对全局日均的系数。"""
    flat: list[Decimal] = []
    for (_, _d), v in daily_wh_totals.items():
        if ref_start <= _d <= ref_end:
            flat.append(v)
    g = _mean(flat)
    if g == 0:
        return {(wh, wd): Decimal("1") for wh in warehouses for wd in range(7)}

    sums: dict[tuple[str, int], list[Decimal]] = defaultdict(list)
    for (wh, d), tot in daily_wh_totals.items():
        if wh not in warehouses:
            continue
        if ref_start <= d <= ref_end:
            sums[(wh, d.weekday())].append(tot)

    out: dict[tuple[str, int], Decimal] = {}
    for wh in warehouses:
        for wd in range(7):
            m = _mean(sums.get((wh, wd), []))
            out[(wh, wd)] = (m / g) if g > 0 else Decimal("1")
    return out


class PrdForecastService:
    """从送货历史聚合后计算 PRD 规则预测。"""

    async def _load_filtered_daily(
        self,
        session: AsyncSession,
        *,
        load_from: date,
        load_to: date,
        q: PrdForecastQuery,
    ) -> tuple[
        dict[tuple[str, str, str, Optional[str], date], Decimal],
        dict[tuple[str, str, str], str],
    ]:
        """返回 ( (rm,wh,v,smelter,d)->sum , (wh,v,sm_key)->最近一日的大区经理 )；sm_key 空串表示历史无冶炼厂。"""
        stmt = (
            select(
                DeliveryRecord.regional_manager,
                DeliveryRecord.warehouse,
                DeliveryRecord.product_variety,
                DeliveryRecord.smelter,
                DeliveryRecord.delivery_date,
                func.sum(DeliveryRecord.weight).label("tw"),
            )
            .where(
                and_(
                    DeliveryRecord.delivery_date >= load_from,
                    DeliveryRecord.delivery_date <= load_to,
                )
            )
            .group_by(
                DeliveryRecord.regional_manager,
                DeliveryRecord.warehouse,
                DeliveryRecord.product_variety,
                DeliveryRecord.smelter,
                DeliveryRecord.delivery_date,
            )
        )
        if q.regional_managers:
            stmt = stmt.where(DeliveryRecord.regional_manager.in_(q.regional_managers))
        if q.warehouses:
            stmt = stmt.where(DeliveryRecord.warehouse.in_(q.warehouses))
        if q.product_varieties:
            stmt = stmt.where(DeliveryRecord.product_variety.in_(q.product_varieties))
        if q.smelters:
            stmt = stmt.where(DeliveryRecord.smelter.in_(q.smelters))

        res = await session.execute(stmt)
        cell: dict[tuple[str, str, str, Optional[str], date], Decimal] = {}
        latest: dict[tuple[str, str, str], tuple[date, str]] = {}
        for rm, wh, v, sm, d, tw in res.all():
            key5 = (str(rm), str(wh), str(v), sm, d)
            cell[key5] = Decimal(tw)
            sm_k = str(sm).strip() if sm is not None and str(sm).strip() else ""
            k3 = (str(wh), str(v), sm_k)
            prev = latest.get(k3)
            if prev is None or d > prev[0]:
                latest[k3] = (d, str(rm))

        rm_map = {k: v[1] for k, v in latest.items()}
        return cell, rm_map

    @staticmethod
    def _smelter_key(smelter: Any) -> str:
        if smelter is None:
            return ""
        s = str(smelter).strip()
        return s

    def _build_structures(
        self,
        cell: dict[tuple[str, str, str, Optional[str], date], Decimal],
        rm_map: dict[tuple[str, str, str], str],
    ) -> tuple[
        dict[tuple[str, str, str], dict[date, Decimal]],
        dict[tuple[str, date], Decimal],
    ]:
        daily_wv: dict[tuple[str, str, str], dict[date, Decimal]] = defaultdict(
            lambda: defaultdict(Decimal)
        )
        daily_wh: dict[tuple[str, date], Decimal] = defaultdict(Decimal)
        for (_rm, wh, v, sm, d), w in cell.items():
            sm_k = self._smelter_key(sm)
            daily_wv[(wh, v, sm_k)][d] += w
            daily_wh[(wh, d)] += w
        return daily_wv, daily_wh

    async def compute(
        self,
        session: AsyncSession,
        q: PrdForecastQuery,
    ) -> tuple[list[PrdForecastDetailRow], PrdForecastChartResponse]:
        ref_end = q.date_from - timedelta(days=1)
        load_from = min(ref_end - timedelta(days=149), q.date_from - timedelta(days=40))
        load_to = ref_end
        cell, rm_map = await self._load_filtered_daily(session, load_from=load_from, load_to=load_to, q=q)
        daily_wv, daily_wh = self._build_structures(cell, rm_map)

        if not daily_wv:
            dates = list(_daterange_inclusive(q.date_from, q.date_to))
            z = [Decimal("0").quantize(Decimal("0.01"))] * len(dates)
            from app.intelligent_prediction.services.forecast_analysis_service import (
                explain_chart_summary,
            )

            empty_summary = explain_chart_summary(
                date_from=q.date_from,
                date_to=q.date_to,
                dates=dates,
                total_by_date=z,
                detail_rows=[],
            )
            return [], PrdForecastChartResponse(
                dates=dates,
                total_by_date=z,
                by_regional_manager=[],
                warehouse_profiles=[],
                summary_analysis=empty_summary,
            )

        wh_set = {wh for (wh, _, _) in daily_wv.keys()} or {wh for (wh, _, _) in rm_map.keys()}
        forecast_dates = list(_daterange_inclusive(q.date_from, q.date_to))
        own_fid = await resolve_own_factory_id(session)

        coef_ref_start = ref_end - timedelta(days=119)
        coefs = _weekday_coefs(dict(daily_wh), wh_set, coef_ref_start, ref_end)

        profile_cache: dict[tuple[str, str], Any] = {}
        price_ctx_cache: dict[str, dict[date, Any]] = {}

        detail_rows: list[PrdForecastDetailRow] = []
        wv_keys = sorted(daily_wv.keys(), key=lambda x: (x[0], x[1], x[2]))
        for wh, v, sm_k in wv_keys:
            rm = rm_map.get((wh, v, sm_k)) or "未分配"
            series = dict(daily_wv.get((wh, v, sm_k), {}))
            baseline_floor = _series_positive_baseline(series)

            pk = (wh, v)
            if pk not in profile_cache:
                profile_cache[pk] = await estimate_warehouse_price_profile(
                    session, warehouse=wh, product_variety=v, own_factory_id=own_fid
                )
            profile = profile_cache[pk]

            if v not in price_ctx_cache:
                price_ctx_cache[v] = await load_price_context_for_horizon(
                    session, dates=forecast_dates, product_variety=v, own_factory_id=own_fid
                )
            ctx_by_date = price_ctx_cache[v]

            for d in forecast_dates:
                wma = _linear_wma(series, d, 30)
                wd = d.weekday()
                c = coefs.get((wh, wd), Decimal("1"))
                history_baseline = (wma * c).quantize(Decimal("0.01"))
                if history_baseline <= 0:
                    history_baseline = max(baseline_floor, Decimal("0.01")).quantize(Decimal("0.01"))

                ctx = ctx_by_date[d]
                pf = compute_price_factor(ctx, profile.sensitivity)
                pred = blend_history_and_price(history_baseline, pf).quantize(Decimal("0.01"))
                if pred <= 0:
                    pred = max(baseline_floor, Decimal("0.01")).quantize(Decimal("0.01"))

                analysis = explain_prediction(
                    target_date=d,
                    history_baseline=history_baseline,
                    price_factor=pf,
                    predicted=pred,
                    profile=profile,
                    ctx=ctx,
                )
                detail_rows.append(
                    PrdForecastDetailRow(
                        target_date=d,
                        regional_manager=rm,
                        warehouse=wh,
                        product_variety=v,
                        smelter=sm_k if sm_k else None,
                        wma_base=wma.quantize(Decimal("0.01")),
                        week_coef=c.quantize(Decimal("0.01")),
                        history_baseline=history_baseline,
                        price_factor=pf,
                        lead_market_price=ctx.lead_market_price,
                        own_calibration_price=ctx.own_calibration_price,
                        competitor_price_max=ctx.competitor_price_max,
                        price_sensitivity=profile.sensitivity,
                        analysis=analysis,
                        predicted_weight=pred,
                    )
                )

        dates = forecast_dates
        by_d_total: dict[date, Decimal] = defaultdict(Decimal)
        by_d_rm: dict[tuple[date, str], Decimal] = defaultdict(Decimal)
        for row in detail_rows:
            by_d_total[row.target_date] += row.predicted_weight
            by_d_rm[(row.target_date, row.regional_manager)] += row.predicted_weight

        rms_sorted = sorted({r for (_, r) in by_d_rm.keys()})
        by_rm_series = [
            PrdForecastByRmSeries(
                regional_manager=rm,
                totals=[by_d_rm.get((dt, rm), Decimal("0")).quantize(Decimal("0.01")) for dt in dates],
            )
            for rm in rms_sorted
        ]
        total_by_date = [
            by_d_total.get(dt, Decimal("0")).quantize(Decimal("0.01")) for dt in dates
        ]
        from app.intelligent_prediction.services.forecast_analysis_service import (
            explain_chart_summary,
        )

        summary = explain_chart_summary(
            date_from=q.date_from,
            date_to=q.date_to,
            dates=dates,
            total_by_date=total_by_date,
            detail_rows=detail_rows,
        )
        chart = PrdForecastChartResponse(
            dates=dates,
            total_by_date=total_by_date,
            by_regional_manager=by_rm_series,
            warehouse_profiles=[
                PrdForecastWarehouseProfile(
                    warehouse=wh,
                    product_variety=v,
                    price_sensitivity=prof.sensitivity,
                    price_correlation=prof.correlation,
                    capacity_max=prof.capacity_max,
                    capacity_min=prof.capacity_min,
                    capacity_avg=prof.capacity_avg,
                )
                for (wh, v), prof in sorted(profile_cache.items())
            ],
            summary_analysis=summary,
        )
        return detail_rows, chart

    async def detail_page(
        self,
        session: AsyncSession,
        q: PrdForecastQuery,
    ) -> PrdForecastDetailResponse:
        rows, _chart = await self.compute(session, q)
        total = len(rows)
        offset = (q.page - 1) * q.page_size
        page_rows = rows[offset : offset + q.page_size]
        return PrdForecastDetailResponse(
            total=total,
            page=q.page,
            page_size=q.page_size,
            items=page_rows,
        )

    async def chart_only(
        self,
        session: AsyncSession,
        q: PrdForecastQuery,
    ) -> PrdForecastChartResponse:
        _rows, chart = await self.compute(session, q)
        return chart


def get_prd_forecast_service() -> PrdForecastService:
    return PrdForecastService()
