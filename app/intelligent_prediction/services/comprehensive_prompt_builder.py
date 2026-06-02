"""综合预测 v2 — Prompt 构建器。

与旧版 PromptBuilder 不同，v2 强调：
1. 历史发货规律 > 发货周期 > 库存周转 > 价格 > 节假日 > 天气
2. 价格只是修正因素，不是决定因素
3. 输出结构化中文分析报告 + JSON 预测结果
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

from app.intelligent_prediction.schemas.prediction import PredictionHistoryPoint, PredictionRequest
from app.intelligent_prediction.settings import settings

SYSTEM_PROMPT_V2: str = (
    "你是生产计划与送货量预测专家。请根据以下六大维度分析仓库未来是否会向目标冶炼厂发货，"
    "并输出 JSON 格式的预测结果。\n\n"
    "## 核心原则\n"
    "仓库发货的本质逻辑是：\n"
    "历史发货规律 > 发货周期 > 库存周转需求 > 价格因素 > 节假日因素 > 天气因素\n\n"
    "价格只是修正因素，而不是决定因素。\n"
    "很多仓库即使价格有优势，如果未到历史发货周期，也未必发货；\n"
    "很多仓库即使价格没有优势，如果已经到了库存周转周期，仍然可能发货。\n"
    "因此应优先分析仓库历史行为规律。\n\n"
    "## 分析维度与权重\n"
    f"1. 历史发货周期与复购规律（{settings.prediction_v2_history_weight:.0%}）\n"
    f"2. 价格竞争力（{settings.prediction_v2_price_weight:.0%}）\n"
    f"3. 仓库价格敏感度（{settings.prediction_v2_sensitivity_weight:.0%}）\n"
    f"4. 节假日因素（{settings.prediction_v2_holiday_weight:.0%}）\n"
    f"5. 天气物流因素（{settings.prediction_v2_weather_weight:.0%}）\n\n"
    "## 输出格式\n"
    "仅输出一个 JSON 对象，不要 Markdown、不要代码块。\n"
    "JSON 必须可被 json.loads 解析，结构如下：\n"
    '{\n'
    '  "items": [\n'
    '    {\n'
    '      "target_date": "YYYY-MM-DD",\n'
    '      "ship_probability": "高|中|低",\n'
    '      "expected_ship_date": "YYYY-MM-DD|null",\n'
    '      "expected_shipment": 数字,\n'
    '      "confidence_level": "高|中|低",\n'
    '      "main_factors": "影响判断的主要原因",\n'
    '      "history_analysis": "第一部分：历史发货规律分析文本",\n'
    '      "price_sensitivity_analysis": "第二部分：价格敏感度分析文本",\n'
    '      "price_competitiveness_analysis": "第三部分：价格竞争力分析文本",\n'
    '      "holiday_analysis": "第四部分：节假日影响文本",\n'
    '      "weather_analysis": "第五部分：天气物流影响文本",\n'
    '      "comprehensive_analysis": "第六部分：综合判断完整报告"\n'
    '    }\n'
    '  ]\n'
    '}\n\n'
    "每个预测日的 target_date 必须与输入的目标日期列表一致。\n"
    "expected_shipment 必须为大于 0 的正数。\n"
    "ship_probability 表示该仓库在该目标日是否可能发货的概率判断。\n"
    "天气因素主要影响发货时间，不一定影响发货意愿。\n"
)


class ComprehensivePromptBuilder:
    """综合预测 v2 Prompt 构建器。"""

    def build_messages(
        self,
        req: PredictionRequest,
        history_analysis: dict[str, Any],
        price_competitiveness: dict[str, Any],
        holiday_impact: dict[str, Any],
        weather_by_date: dict[date, str],
        price_sensitivity_info: dict[str, Any],
        forecast_dates: list[date],
    ) -> tuple[str, str]:
        """返回 (system_prompt, user_prompt)。"""
        user = self._build_user_prompt(
            req=req,
            history_analysis=history_analysis,
            price_competitiveness=price_competitiveness,
            holiday_impact=holiday_impact,
            weather_by_date=weather_by_date,
            price_sensitivity_info=price_sensitivity_info,
            forecast_dates=forecast_dates,
        )
        return SYSTEM_PROMPT_V2, user

    def _build_user_prompt(
        self,
        req: PredictionRequest,
        history_analysis: dict[str, Any],
        price_competitiveness: dict[str, Any],
        holiday_impact: dict[str, Any],
        weather_by_date: dict[date, str],
        price_sensitivity_info: dict[str, Any],
        forecast_dates: list[date],
    ) -> str:
        lines: list[str] = []

        # 基本信息
        lines.append(f"仓库: {req.warehouse}")
        lines.append(f"冶炼厂: {req.smelter or '未指定（历史含全部冶炼厂）'}")
        lines.append(f"品种: {req.product_variety}")
        lines.append(f"大区经理: {req.regional_manager or '未提供'}")

        # 预测目标日期
        date_lines = ", ".join(d.isoformat() for d in forecast_dates)
        lines.append(f"需要预测的目标日期（依次）: {date_lines}")

        # 第一部分：历史发货规律分析
        lines.append("\n## 第一部分：仓库历史发货规律分析")
        lines.append(history_analysis.get("analysis_text", "无历史数据"))

        # 第二部分：价格敏感度分析
        lines.append("\n## 第二部分：仓库价格敏感度分析")
        lines.append(price_sensitivity_info.get("analysis_text", "无敏感度数据"))

        # 第三部分：目标冶炼厂价格竞争力
        lines.append("\n## 第三部分：目标冶炼厂价格竞争力分析")
        lines.append(price_competitiveness.get("analysis_text", "无价格竞争力数据"))

        # 第四部分：节假日因素
        lines.append("\n## 第四部分：节假日因素")
        lines.append(holiday_impact.get("analysis_text", "无节假日影响"))

        # 第五部分：天气与运输因素
        lines.append("\n## 第五部分：天气与运输因素")
        weather_lines: list[str] = []
        for d in forecast_dates:
            ws = weather_by_date.get(d, "晴")
            weather_lines.append(f"- {d.isoformat()}: {ws}")
        lines.append("预测日天气:\n" + "\n".join(weather_lines))
        lines.append("注意：天气因素主要影响发货时间，不一定影响发货意愿。")

        # 输出要求
        lines.append("\n## 输出要求")
        lines.append("请为每个目标日期输出一条 items。")
        lines.append("每个目标日期必须包含六大段分析文字和综合预测结论。")
        lines.append("综合分析时应严格按照权重：历史规律40%、价格竞争力30%、价格敏感度15%、节假日10%、天气5%。")

        return "\n".join(lines)
