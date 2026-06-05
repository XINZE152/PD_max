"""15 天发货预测提示词构建器（豆包方案）。

将仓库历史、冶炼厂价格、SMM 铅价三组原始数据拼装为
System + User prompt，交给 LLM 完成六大维度分析与逐日预测。
"""

from __future__ import annotations

import statistics
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from app.intelligent_prediction.schemas.doubao_prediction import (
    DoubaoHistoryItem,
    DoubaoPredictionRequest,
    SMMPricingItem,
    SmelterPriceItem,
)

# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT: str = """\
你是专业的仓储品类发货预测专家。你的任务是根据提供的历史送货数据、冶炼厂价格和SMM铅价，预测指定仓库发往冶炼厂未来15天的逐日发货吨数。

【核心原则 · 优先级从高到低】
历史整月发货规律（最重要） > 品类发货周期 > 库存周转 > 品类价格 > 节假日 > 天气

【重要规则 · 必须严格遵守】
1. 仓库发货 **按品类区分**，冶炼厂收货价格 **按品类区分**。
2. 价格优势评估 **只针对当前发货品类**，不混用其他品类。
3. 分析历史发货规律时，**必须优先看整月总发货次数、整月总货量、月度整体发货频率**，不能只看短期连续两次的间隔。
4. 如果一个月只发几次，即使短期间隔很近，也判定为 **月度低频仓库**，未来15天不会高频发货。
5. 严禁连续3天以上 predicted_weight 完全相同；必须模拟历史波动特征。

【发货重量规则 · 必须严格遵守】
货车是仓库发货的唯一运输方式，以下规则至关重要：
6. 单车最大载重约 35 吨。仓库有货时一般会装满发车（成本最优），因此
   - 大部分历史发货量是 35 吨的整数倍（1车=35t，2车=70t，3车=105t）
   - 库存不足时单次发货量可能低于 35 吨
7. 多个品类可以拼一车：一天内的发货量就是当天所有品类的总重量，通常 ≤ 一整车。
8. 预测的单次发货量必须优先取自历史中出现过的数值（如 30t、35t、70t、90t、140t），
   不要随意创造一个历史上从未出现过的吨数。允许合理取整（如 68t→70t、142t→140t）。
9. **同日多记录=多品种/多车次明细**：历史数据中同一天可能出现多条记录，这代表当天有多个品种或多车次发货。分析时必须将同日所有记录的重量**累加**作为该日总发货量，预测输出的 predicted_weight 也按**日总发货量**给出（而非单品种或单车次）。例如：6月1日有"电动80t"+"电轿50t"共2条记录 → 该日日总发货量=130吨。

==================================================
你必须按以下六个部分输出分析报告（纯文本），然后输出 JSON 格式的逐日预测。

【第一部分：品类整月发货规律分析】
必须分析：
1. 上个月总发货天数
2. 上个月总发货量:上个月发货量总和（吨）
3. 月度发货频率（高频 / 中频 / 低频）
4. 单次平均发货量
5. 历史最大、最小发货量
6. 平均发货间隔（但必须结合整月次数判断是否为常态周期）
7. 周期判断（依据整月表现）
8.本月已发货天数和总发货量（如果有），并分析是否与上个月规律一致。
9.本月总发货品类

【第二部分：品类价格敏感度分析】
根据历史发货与品类价格关系判断：
- A 高敏感：价格差 → 停发
- B 中敏感：价格差 → 减量
- C 低敏感：价格几乎不影响发货
必须写依据。

【第三部分：目标冶炼厂品类价格竞争力】
第一优先级：目标厂品类价格 vs 竞品同品类价格
第二优先级：目标厂近3天、7天品类价格走势
第三优先级：SMM铅价（仅辅助）
等级：A 优势高 / B 优势中 / C 优势低 / D 劣势低 / E 劣势中 / F 劣势高

【第四部分：节假日影响】
分析预测期内是否有中国法定节假日，对发货的影响。

【第五部分：天气物流因素】
根据历史天气记录推断天气对发货的可能影响。

【第六部分：综合预测结论】

	⚠️ 必须先输出「预测发货汇总表」，再写分析文本。表格是后续所有结论和 JSON 的唯一数据来源。

	--- 预测发货汇总表（必须严格按此格式输出）---
	| 序号 | 预测日期 | 日发货量(吨) |
	|------|----------|-------------|
	| 1 | YYYY-MM-DD | XXX |
	| 2 | YYYY-MM-DD | XXX |
	（只列出 predicted_weight > 0 的日期，按日期升序排列）
	--- 汇总表结束 ---

	然后基于上表输出以下分析：
	整体发货概率：高 / 中 / 低
	预计发货次数：（必须 = 上表行数）
	预计发货时间：（必须 = 上表中的全部日期）
	预计日发货量范围：（必须取自上表中的日发货量最小值 ~ 最大值，如"预计日发货量在 70-130 吨之间"）
	预测置信度
	主要原因
	最终结论

	⚠️ 重要约束：
	1.「预测发货汇总表」是唯一的权威数据源。第六部分的分析数字和 JSON items 都必须**严格从上表读取**，不允许出现表中没有的数据。
	2. 日发货量按日期汇总：同一天可能有多品种/多车次，日发货量 = 该日所有记录的重量之和。
	3. JSON items 与汇总表的对应规则：
	   - 汇总表中有 N 行 → items 中恰好有 N 条 predicted_weight > 0 的记录
	   - 每条 items 的 target_date 和 predicted_weight 必须与汇总表完全一致
	   - 汇总表中未列出的日期 → items 中 predicted_weight 必须为 0
	4. 不允许分析文本与 JSON 数据不一致。例如分析说"70-130吨"但 JSON 中出现 140 吨即为错误。

==================================================
【输出格式 · 必须严格遵守】

只输出一个 JSON 对象（不要 Markdown、不要代码块、不要纯文本前言）。
JSON 结构如下：
{
    "analysis_report": "完整的六部分分析报告文本（包含第一到第六部分的所有分析内容）",
    "items": [
        {"target_date": "YYYY-MM-DD", "predicted_weight": 0, "ship_probability": "低", "confidence_level": "低", "main_factors": ""},
        ...
    ]
}

其中：
- analysis_report 必须包含完整的六部分分析报告（第一到第六部分），这是给用户阅读的分析文本
- items 必须包含从 day0 到 day15 共 16 条记录
- predicted_weight 为发货吨数（不发货则为 0）
- ship_probability 为发货概率（高/中/低）
- confidence_level 为置信度（高/中/低）
- main_factors 为该日预测的主要影响因素
"""


# ---------------------------------------------------------------------------
# Prompt Builder
# ---------------------------------------------------------------------------

class DoubaoPromptBuilder:
    """将三组原始数据拼装为 (system_prompt, user_prompt) 二元组。"""

    # 数据量限制（防止超 token）
    MAX_HISTORY_RECORDS = 200
    MAX_SMELTER_PRICE_RECORDS = 30
    MAX_SMM_PRICE_RECORDS = 30

    def build_messages(
        self,
        req: DoubaoPredictionRequest,
        forecast_dates: list[date],
    ) -> tuple[str, str]:
        """返回 (system_prompt, user_prompt)。"""
        user_prompt = self._build_user_prompt(req, forecast_dates)
        return SYSTEM_PROMPT, user_prompt

    # ------------------------------------------------------------------
    # 内部：拼装 User Prompt
    # ------------------------------------------------------------------

    def _build_user_prompt(
        self,
        req: DoubaoPredictionRequest,
        forecast_dates: list[date],
    ) -> str:
        sections: list[str] = []

        # 1. 基础信息
        sections.append(self._section_basic(req, forecast_dates))

        # 2. 历史发货数据
        sections.append(self._section_history(req))

        # 3. 冶炼厂价格
        sections.append(self._section_smelter_prices(req))

        # 4. SMM 铅价
        sections.append(self._section_smm_prices(req))

        # 5. 预测目标日期
        sections.append(self._section_forecast_dates(forecast_dates))

        return "\n\n".join(sections)

    # ------------------------------------------------------------------
    # 各段落构建
    # ------------------------------------------------------------------

    @staticmethod
    def _section_basic(req: DoubaoPredictionRequest, forecast_dates: list[date]) -> str:
        lines = [
            "==================================================",
            "【基础信息】",
            f"仓库名称：{req.warehouse}",
        ]
        if req.product_variety:
            lines.append(f"目标品类：{req.product_variety}")
        if forecast_dates:
            lines.append(f"预测起始日：{forecast_dates[0].isoformat()}")
            lines.append(f"预测结束日：{forecast_dates[-1].isoformat()}")
            lines.append(f"预测天数：{len(forecast_dates)} 天")
        lines.append("==================================================")
        return "\n".join(lines)

    def _section_history(self, req: DoubaoPredictionRequest) -> str:
        history = req.history
        lines = [
            "==================================================",
            "【仓库品类历史发货数据分析】",
        ]

        if not history:
            lines.append("（无历史送货数据）")
            lines.append("==================================================")
            return "\n".join(lines)

        # 按品类筛选
        filtered = history
        if req.product_variety:
            filtered = [h for h in history if h.品类 == req.product_variety]
            if not filtered:
                filtered = history  # 没有匹配则用全部
                lines.append(f"注意：未找到品类「{req.product_variety}」的历史数据，使用全部品类数据。")

        # 按日期排序
        filtered = sorted(filtered, key=lambda x: x.送货日期)

        # 统计摘要
        weights = [float(h.重量吨) for h in filtered]
        delivery_dates = [h.送货日期 for h in filtered]
        total_days = len(filtered)
        total_weight = sum(weights)
        avg_weight = statistics.mean(weights) if weights else 0
        std_weight = statistics.stdev(weights) if len(weights) > 1 else 0
        max_weight = max(weights) if weights else 0
        min_weight = min(weights) if weights else 0

        # 月度频率
        if delivery_dates:
            date_range_days = (delivery_dates[-1] - delivery_dates[0]).days + 1
            months = max(date_range_days / 30, 1)
            monthly_freq = total_days / months
        else:
            monthly_freq = 0

        # 发货间隔
        intervals = []
        for i in range(1, len(delivery_dates)):
            delta = (delivery_dates[i] - delivery_dates[i - 1]).days
            intervals.append(delta)
        avg_interval = statistics.mean(intervals) if intervals else 0

        # 星期分布
        weekday_counter = Counter(d.weekday() for d in delivery_dates)
        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday_dist = " / ".join(
            f"{weekday_names[w]}:{c}次" for w, c in sorted(weekday_counter.items())
        )

        lines.append(f"历史记录总数：{total_days} 条")
        lines.append(f"整月总发货量：{total_weight:.2f} 吨")
        lines.append(f"单次平均发货量：{avg_weight:.2f} 吨")
        lines.append(f"历史最大发货量：{max_weight:.2f} 吨")
        lines.append(f"历史最小发货量：{min_weight:.2f} 吨")
        lines.append(f"发货量标准差：{std_weight:.2f}")
        lines.append(f"月度发货频率：约 {monthly_freq:.1f} 次/月")
        if avg_interval:
            lines.append(f"平均发货间隔：{avg_interval:.1f} 天")
        lines.append(f"星期分布：{weekday_dist}")

        # 月度频率判定
        if monthly_freq >= 8:
            freq_label = "高频（≥8次/月）"
        elif monthly_freq >= 3:
            freq_label = "中频（3~7次/月）"
        else:
            freq_label = "低频（<3次/月）"
        lines.append(f"月度频率判定：{freq_label}")

        # 原始记录（最近 50 条）
        recent = filtered[-50:]
        lines.append("")
        lines.append("近期送货记录（日期 | 品类 | 冶炼厂 | 天气 | 重量吨）：")
        for h in recent:
            weather = h.天气 or "未知"
            smelter = h.冶炼厂 or "未知"
            lines.append(f"  {h.送货日期.isoformat()} | {h.品类} | {smelter} | {weather} | {h.重量吨}")

        # 截断提示
        if len(filtered) > self.MAX_HISTORY_RECORDS:
            lines.append(f"\n（仅展示最近 {self.MAX_HISTORY_RECORDS} 条，共 {total_days} 条）")

        lines.append("==================================================")
        return "\n".join(lines)

    def _section_smelter_prices(self, req: DoubaoPredictionRequest) -> str:
        prices = req.smelter_prices
        lines = [
            "==================================================",
            "【冶炼厂品类收货价格（含竞品）】",
        ]

        if not prices:
            lines.append("（无冶炼厂价格数据）")
            lines.append("==================================================")
            return "\n".join(lines)

        # 按日期排序，取最近 N 条
        prices = sorted(prices, key=lambda x: x.日期, reverse=True)
        recent = prices[: self.MAX_SMELTER_PRICE_RECORDS]
        recent = list(reversed(recent))  # 恢复正序

        lines.append(f"共 {len(prices)} 条价格记录，展示最近 {len(recent)} 条：")
        lines.append("日期 | 冶炼厂 | 品种 | 基准价")
        for p in recent:
            lines.append(f"  {p.日期.isoformat()} | {p.冶炼厂} | {p.品种} | {p.基准价}")

        # 趋势摘要
        if len(recent) >= 2:
            first_price = float(recent[0].基准价)
            last_price = float(recent[-1].基准价)
            avg_price = statistics.mean(float(p.基准价) for p in recent)
            change_pct = ((last_price - first_price) / first_price * 100) if first_price else 0
            trend = "上涨" if change_pct > 0.5 else ("下跌" if change_pct < -0.5 else "持平")
            lines.append("")
            lines.append(f"价格趋势：{trend}（{change_pct:+.2f}%）")
            lines.append(f"近期均价：{avg_price:.0f}")
            lines.append(f"最新价：{last_price:.0f}")

        lines.append("==================================================")
        return "\n".join(lines)

    def _section_smm_prices(self, req: DoubaoPredictionRequest) -> str:
        prices = req.smm_prices
        lines = [
            "==================================================",
            "【SMM 1# 铅锭价格走势】",
        ]

        if not prices:
            lines.append("（无 SMM 铅价数据）")
            lines.append("==================================================")
            return "\n".join(lines)

        prices = sorted(prices, key=lambda x: x.定价日期, reverse=True)
        recent = prices[: self.MAX_SMM_PRICE_RECORDS]
        recent = list(reversed(recent))

        lines.append(f"共 {len(prices)} 条铅价记录，展示最近 {len(recent)} 条：")
        lines.append("定价日期 | 最低价 | 最高价 | 均价")
        for p in recent:
            lines.append(f"  {p.定价日期.isoformat()} | {p.最低价} | {p.最高价} | {p.均价}")

        # 趋势摘要
        if len(recent) >= 2:
            first_avg = float(recent[0].均价)
            last_avg = float(recent[-1].均价)
            overall_avg = statistics.mean(float(p.均价) for p in recent)
            change_pct = ((last_avg - first_avg) / first_avg * 100) if first_avg else 0
            trend = "上涨" if change_pct > 0.5 else ("下跌" if change_pct < -0.5 else "持平")
            lines.append("")
            lines.append(f"铅价趋势：{trend}（{change_pct:+.2f}%）")
            lines.append(f"近期均价：{overall_avg:.0f}")
            lines.append(f"最新均价：{last_avg:.0f}")

        lines.append("==================================================")
        return "\n".join(lines)

    @staticmethod
    def _section_forecast_dates(forecast_dates: list[date]) -> str:
        lines = [
            "==================================================",
            "【预测目标日期】",
            f"请预测以下 {len(forecast_dates)} 天的逐日发货吨数：",
        ]
        for i, d in enumerate(forecast_dates):
            lines.append(f"  day{i}: {d.isoformat()} ({_weekday_name(d)})")
        lines.append("")
        lines.append("请严格按照上述日期输出 items 数组，不要遗漏任何一天。")
        lines.append("==================================================")
        return "\n".join(lines)


def _weekday_name(d: date) -> str:
    names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    return names[d.weekday()]
