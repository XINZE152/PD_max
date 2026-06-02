"""综合预测 v2 — 历史分析单元测试。"""

from datetime import date, timedelta
from decimal import Decimal

from app.intelligent_prediction.schemas.prediction import PredictionHistoryPoint
from app.intelligent_prediction.services.comprehensive_history_analysis import (
    analyze_delivery_pattern,
    format_history_analysis_text,
    _judge_cycle_proximity,
    _std,
)


def _make_points(dates_and_weights: list[tuple[str, float]]) -> list[PredictionHistoryPoint]:
    return [
        PredictionHistoryPoint(
            delivery_date=date.fromisoformat(d),
            weight=Decimal(str(w)),
        )
        for d, w in dates_and_weights
    ]


def test_empty_history():
    result = analyze_delivery_pattern([])
    assert result["delivery_frequency"] == 0
    assert result["cycle_judgment"] == "无历史数据，无法判断周期"


def test_single_point():
    points = _make_points([("2026-05-01", 10.5)])
    result = analyze_delivery_pattern(points, as_of_date=date(2026, 5, 5))
    assert result["delivery_frequency"] == 1
    assert result["days_since_last_delivery"] == 4
    assert result["avg_interval_days"] == 0


def test_regular_intervals():
    # 每 7 天发货一次，稳定
    dates = [("2026-01-01", 10), ("2026-01-08", 12), ("2026-01-15", 11), ("2026-01-22", 10)]
    points = _make_points(dates)
    result = analyze_delivery_pattern(points, as_of_date=date(2026, 1, 22))
    assert result["avg_interval_days"] == 7.0
    assert result["interval_std_days"] == 0.0
    assert result["period_stable"] is True


def test_irregular_intervals():
    dates = [("2026-01-01", 10), ("2026-01-05", 12), ("2026-01-20", 11), ("2026-01-22", 10)]
    points = _make_points(dates)
    result = analyze_delivery_pattern(points, as_of_date=date(2026, 1, 22))
    assert result["intervals"] == [4, 15, 2]
    assert result["avg_interval_days"] > 0
    assert result["period_stable"] is False


def test_cycle_proximity_exceeded():
    result = _judge_cycle_proximity(days_since=15, avg_interval=10)
    assert "已超过" in result


def test_cycle_proximity_approaching():
    result = _judge_cycle_proximity(days_since=9, avg_interval=10)
    assert "已接近" in result


def test_cycle_proximity_far():
    result = _judge_cycle_proximity(days_since=2, avg_interval=10)
    assert "远未达到" in result


def test_format_text_empty():
    pattern = analyze_delivery_pattern([])
    text = format_history_analysis_text(pattern)
    assert "无历史" in text


def test_format_text_with_data():
    dates = [("2026-01-01", 10), ("2026-01-08", 12), ("2026-01-15", 11)]
    points = _make_points(dates)
    pattern = analyze_delivery_pattern(points, as_of_date=date(2026, 1, 15))
    text = format_history_analysis_text(pattern)
    assert "发货" in text
    assert "吨" in text


def test_std():
    assert _std([2, 2, 2]) == 0.0
    assert abs(_std([1, 2, 3, 4, 5]) - 1.414) < 0.01
