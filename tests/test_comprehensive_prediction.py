"""综合预测 v2 — 集成测试（schema + prompt builder）。"""

from datetime import date
from decimal import Decimal

from app.intelligent_prediction.schemas.prediction import (
    ComprehensivePredictionItem,
    ComprehensivePredictionResult,
    ShipProbability,
    ConfidenceLevelV2,
    PredictionHistoryPoint,
    PredictionRequest,
)
from app.intelligent_prediction.services.comprehensive_prompt_builder import (
    ComprehensivePromptBuilder,
    SYSTEM_PROMPT_V2,
)
from app.intelligent_prediction.services.holiday_analysis_service import analyze_holiday_impact


def test_ship_probability_enum():
    assert ShipProbability.HIGH.value == "高"
    assert ShipProbability.MEDIUM.value == "中"
    assert ShipProbability.LOW.value == "低"


def test_confidence_level_v2_enum():
    assert ConfidenceLevelV2.HIGH.value == "高"
    assert ConfidenceLevelV2.MEDIUM.value == "中"
    assert ConfidenceLevelV2.LOW.value == "低"


def test_comprehensive_prediction_item_creation():
    item = ComprehensivePredictionItem(
        target_date=date(2026, 6, 1),
        ship_probability="高",
        expected_ship_date=date(2026, 6, 1),
        expected_shipment=Decimal("15.5"),
        confidence_level="中",
        main_factors="历史周期已到",
        history_analysis="历史分析文本",
        price_sensitivity_analysis="敏感度分析",
        price_competitiveness_analysis="竞争力分析",
        holiday_analysis="节假日分析",
        weather_analysis="天气分析",
        comprehensive_analysis="综合分析",
    )
    assert item.ship_probability == ShipProbability.HIGH
    assert item.confidence_level == ConfidenceLevelV2.MEDIUM
    assert item.expected_shipment == Decimal("15.5")


def test_comprehensive_prediction_item_chinese_normalization():
    item = ComprehensivePredictionItem(
        target_date=date(2026, 6, 1),
        ship_probability="high",
        expected_ship_date=None,
        expected_shipment=Decimal("10"),
        confidence_level="LOW",
        main_factors="",
        history_analysis="",
        price_sensitivity_analysis="",
        price_competitiveness_analysis="",
        holiday_analysis="",
        weather_analysis="",
        comprehensive_analysis="",
    )
    assert item.ship_probability == ShipProbability.HIGH
    assert item.confidence_level == ConfidenceLevelV2.LOW


def test_comprehensive_prediction_result():
    item = ComprehensivePredictionItem(
        target_date=date(2026, 6, 1),
        ship_probability="高",
        expected_ship_date=date(2026, 6, 1),
        expected_shipment=Decimal("15.5"),
        confidence_level="高",
        main_factors="原因",
        history_analysis="",
        price_sensitivity_analysis="",
        price_competitiveness_analysis="",
        holiday_analysis="",
        weather_analysis="",
        comprehensive_analysis="",
    )
    result = ComprehensivePredictionResult(
        warehouse="测试仓库",
        product_variety="铅锭",
        items=[item],
        provider_used="openai",
        latency_ms=1500.0,
    )
    assert len(result.items) == 1
    assert result.cache_hit is False


def test_prompt_builder_system_prompt():
    assert "历史发货规律" in SYSTEM_PROMPT_V2
    assert "价格只是修正因素" in SYSTEM_PROMPT_V2
    assert "40%" in SYSTEM_PROMPT_V2


def test_prompt_builder_build_messages():
    builder = ComprehensivePromptBuilder()
    req = PredictionRequest(
        warehouse="测试仓",
        product_variety="铅锭",
        smelter="金利",
        horizon_days=7,
    )
    history_analysis = {"analysis_text": "历史分析"}
    price_competitiveness = {"analysis_text": "价格分析"}
    holiday_impact = {"analysis_text": "节假日分析"}
    weather = {date(2026, 6, 1): "晴"}
    price_sensitivity = {"analysis_text": "敏感度分析"}
    forecast_dates = [date(2026, 6, i) for i in range(1, 8)]

    system, user = builder.build_messages(
        req=req,
        history_analysis=history_analysis,
        price_competitiveness=price_competitiveness,
        holiday_impact=holiday_impact,
        weather_by_date=weather,
        price_sensitivity_info=price_sensitivity,
        forecast_dates=forecast_dates,
    )
    assert system == SYSTEM_PROMPT_V2
    assert "测试仓" in user
    assert "铅锭" in user
    assert "历史分析" in user
    assert "价格分析" in user
    assert "2026-06-01" in user


def test_holiday_impact_no_holiday():
    dates = [date(2026, 3, 1), date(2026, 3, 2)]
    result = analyze_holiday_impact(dates)
    assert result["impact_summary"] == "无显著影响"
