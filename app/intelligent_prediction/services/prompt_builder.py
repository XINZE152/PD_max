"""Prompt 构建：系统提示、历史统计、强制 JSON 输出格式。"""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from typing import Optional
from decimal import Decimal
from statistics import mean, pstdev
from typing import Any

from app.intelligent_prediction.schemas.prediction import PredictionHistoryPoint, PredictionRequest


class PromptBuilder:
    """根据历史数据构建 System + User Prompt。"""

    SYSTEM_PROMPT: str = (
        "你是生产计划与送货量预测助理。仅输出一个 JSON 对象，不要 Markdown、不要代码块。"
        " JSON 必须可被 json.loads 解析，键名使用英文小写与下划线。"
        " 字段结构：{\"items\":[{\"target_date\":\"YYYY-MM-DD\",\"predicted_weight\":数字,"
        "\"confidence\":\"high|medium|low\",\"warnings\":[\"可选字符串\"]}]}。"
        " predicted_weight 必须为非负数；若无把握请降低 confidence 并在 warnings 说明。"
    )

    def analyze_history(self, points: list[PredictionHistoryPoint]) -> dict[str, Any]:
        """计算趋势与周期相关统计（用于 Prompt 与缓存键）。"""
        if not points:
            return {
                "count": 0,
                "mean_weight": None,
                "std_weight": None,
                "last_date": None,
                "last_weight": None,
                "weekday_counter": {},
                "trend_note": "无历史数据",
            }
        weights = [float(p.weight) for p in points]
        sorted_pts = sorted(points, key=lambda x: x.delivery_date)
        wd_counter: Counter[int] = Counter(p.delivery_date.weekday() for p in points)
        m = mean(weights) if weights else 0.0
        sd = pstdev(weights) if len(weights) > 1 else 0.0
        first_half = weights[: max(1, len(weights) // 2)]
        second_half = weights[max(1, len(weights) // 2) :]
        trend = "flat"
        if first_half and second_half:
            a = mean(first_half)
            b = mean(second_half)
            if b > a * 1.1:
                trend = "up"
            elif b < a * 0.9:
                trend = "down"
        return {
            "count": len(points),
            "mean_weight": round(m, 4),
            "std_weight": round(sd, 4),
            "last_date": sorted_pts[-1].delivery_date.isoformat(),
            "last_weight": float(sorted_pts[-1].weight),
            "weekday_counter": {str(k): v for k, v in wd_counter.items()},
            "trend_note": f"recent_trend={trend}",
        }

    def build_user_prompt(
        self,
        req: PredictionRequest,
        stats: dict[str, Any],
        start_date: date,
        forecast_weather_by_date: Optional[dict[date, str]] = None,
    ) -> str:
        """组装 User Prompt。"""
        dates = [start_date + timedelta(days=i) for i in range(req.horizon_days)]
        date_lines = ", ".join(d.isoformat() for d in dates)
        def _hist_line(p: PredictionHistoryPoint) -> str:
            base = f"- {p.delivery_date.isoformat()}: {float(p.weight)}"
            bits: list[str] = []
            if getattr(p, "cn_calendar_label", None):
                lab = str(p.cn_calendar_label).strip()
                if lab in ("是", "否"):
                    bits.append(f"节假日:{lab}")
                else:
                    bits.append(lab)
            ws = getattr(p, "weather_summary", None)
            if ws and str(ws).strip():
                bits.append(f"天气:{str(ws).strip()}")
            else:
                bits.append("天气:晴")
            if bits:
                return f"{base} ({' | '.join(bits)})"
            return base

        hist_lines = "\n".join(
            _hist_line(p) for p in sorted(req.history, key=lambda x: x.delivery_date)[-30:]
        )
        fw = forecast_weather_by_date or {}
        forecast_block = ""
        if dates:
            lines = [f"- {d.isoformat()}: {fw.get(d, '晴')}" for d in dates]
            forecast_block = (
                "预测日当日参考天气（按目标日期拉取，未配置天气服务或失败时为「晴」）:\n"
                + "\n".join(lines)
                + "\n"
            )
        return (
            f"仓库: {req.warehouse}\n"
            f"冶炼厂: {req.smelter or '未指定（历史含全部冶炼厂）'}\n"
            f"品种: {req.product_variety}\n"
            f"大区经理: {req.regional_manager or '未提供'}\n"
            f"需要预测的目标日期（依次）: {date_lines}\n"
            f"{forecast_block}"
            f"历史统计: {stats}\n"
            f"最近历史记录（最多30笔）:\n{hist_lines or '（无）'}\n"
            "请为每个目标日期输出一条 items，target_date 必须与上述日期一致且为 YYYY-MM-DD。"
            " 若相邻日期预测波动可能很大，请在 warnings 标注可能原因。"
        )

    def build_messages(
        self,
        req: PredictionRequest,
        stats: dict[str, Any],
        start_date: date,
        forecast_weather_by_date: Optional[dict[date, str]] = None,
    ) -> tuple[str, str]:
        """返回 (system, user) 双字符串。"""
        return self.SYSTEM_PROMPT, self.build_user_prompt(
            req, stats, start_date, forecast_weather_by_date=forecast_weather_by_date
        )
