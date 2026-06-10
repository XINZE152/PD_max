import asyncio
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

from app.intelligent_prediction.api.v1 import predict as predict_api
from app.intelligent_prediction.schemas.doubao_prediction import DoubaoPredictionRequest


class _ScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _ExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _ScalarResult(self._rows)


class _FakeSession:
    def __init__(self, row_sets):
        self._row_sets = list(row_sets)
        self.execute_count = 0

    async def execute(self, _stmt):
        self.execute_count += 1
        return _ExecuteResult(self._row_sets.pop(0))


def _daily_rows(start: date, product_variety: str = ""):
    return [
        SimpleNamespace(
            id=index,
            batch_id="batch-1",
            warehouse="W1",
            product_variety=product_variety,
            target_date=start + timedelta(days=index),
            predicted_weight=Decimal(str(index + 1)),
            ship_probability="medium",
            confidence_level="high",
            confidence="high",
            main_factors=f"factor-{index}",
            comprehensive_analysis="daily analysis",
            analysis=None,
            created_at=None,
        )
        for index in range(predict_api._DAILY_PREDICTION_HORIZON_DAYS)
    ]


def test_daily_cache_returns_warehouse_level_prediction(monkeypatch):
    async def latest_batch_id(_session):
        return "batch-1"

    monkeypatch.setattr(predict_api, "_latest_daily_prediction_batch_id", latest_batch_id)
    start = date(2026, 6, 10)
    session = _FakeSession([_daily_rows(start)])
    req = DoubaoPredictionRequest(warehouse="W1", prediction_start_date=start)

    result = asyncio.run(predict_api._daily_cache_result_for_request(session, req))

    assert result is not None
    assert result.cache_hit is True
    assert result.provider_used == "daily_cache"
    assert len(result.items) == predict_api._DAILY_PREDICTION_HORIZON_DAYS
    assert result.items[0].predicted_weight == Decimal("1")
    assert session.execute_count == 1


def test_daily_cache_falls_back_to_warehouse_level_when_variety_missing(monkeypatch):
    async def latest_batch_id(_session):
        return "batch-1"

    monkeypatch.setattr(predict_api, "_latest_daily_prediction_batch_id", latest_batch_id)
    start = date(2026, 6, 10)
    session = _FakeSession([[], _daily_rows(start)])
    req = DoubaoPredictionRequest(
        warehouse="W1",
        product_variety="battery",
        prediction_start_date=start,
    )

    result = asyncio.run(predict_api._daily_cache_result_for_request(session, req))

    assert result is not None
    assert result.cache_hit is True
    assert result.product_variety is None
    assert session.execute_count == 2


def test_daily_cache_respects_use_cache_false(monkeypatch):
    async def latest_batch_id(_session):
        raise AssertionError("daily cache should not query latest batch")

    monkeypatch.setattr(predict_api, "_latest_daily_prediction_batch_id", latest_batch_id)
    req = DoubaoPredictionRequest(warehouse="W1", use_cache=False)

    result = asyncio.run(predict_api._daily_cache_result_for_request(_FakeSession([]), req))

    assert result is None