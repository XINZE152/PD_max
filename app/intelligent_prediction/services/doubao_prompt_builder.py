"""15 天发货预测提示词构建器（豆包方案）。

将仓库历史、冶炼厂价格、SMM 铅价三组原始数据拼装为
System + User prompt，交给 LLM 完成六大维度分析与逐日预测。
"""

from __future__ import annotations

import statistics
from collections import Counter, defaultdict
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

【时间锚定规则 · 必须严格遵守】
※ User Prompt 末尾的【时间锚点】表定义了本次预测的全部目标日期（day0 ~ day15），这是所有日期引用的唯一标准。
※ 分析文本中提到任意发货日期时，必须同时写出其 day 索引，格式为"dayN（YYYY-MM-DD 周X）"。禁止只写裸日期：如"6月9日发货"是错误的，必须写为"day3（6月9日 周一）发货"。
※ 第六部分的「预测发货汇总表」中，每行的索引列和日期列必须取自【时间锚点】表，禁止自编日期。
※ JSON items 中每条 target_date 必须等于【时间锚点】中对应 day 索引的日期值。

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
以下统计数据已由系统预计算，**直接引用即可，禁止自行重新计算**。
1. 上个月总发货天数 → 见 User Prompt【按月拆分统计】中的"上个月"
2. 上个月总发货量 → 见 User Prompt【按月拆分统计】中的"上个月"
3. 月度发货频率 → 见 User Prompt 整体统计中的"月度频率判定"
4. 日发货量均值/最大/最小/标准差 → 见 User Prompt 整体统计
5. 平均发货间隔 → 见 User Prompt 整体统计
6. 周期判断 → 依据上述预计算统计数据和月度频率判定得出结论
7. 本月已发货天数和总发货量 → 见 User Prompt【按月拆分统计】中的"本月"
8. 对比上个月与本月规律是否一致，如有变化分析原因

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
⚠️ 以下数据中未提供法定节假日/调休信息，你**不得凭记忆臆测**哪天是节假日。
分析范围仅限于时间锚点表中已标注的"周末/工作日"规律：
- 周末（周六、周日）：部分仓库可能减少或不发货
- 工作日（周一~周五）：正常发货
若时间锚点表中未标注特殊节日，直接写明"预测期内未提供法定节假日数据，仅按周末/工作日规律分析"。

【第五部分：天气物流因素】
⚠️ 预测期未来天气未知，你**不得编造**具体天气预报（如"预计6月8日有暴雨"）。
仅能根据历史记录中同季节/同月份的经验规律做推断：
- 历史记录中是否有因天气导致的发货中断或减量
- 该仓库所在地的季节性天气特征（从历史数据中间接反映）
若历史天气与发货无明显关联，直接写明"历史天气数据未显示对发货的显著影响，天气因素未纳入本次预测"，不得强行编造。

【第六部分：综合预测结论】

	⚠️ 必须先输出「预测发货汇总表」，再写分析文本。表格是后续所有结论和 JSON 的唯一数据来源。

	--- 预测发货汇总表（必须严格按此格式输出）---
	| 序号 | 索引 | 日期 | 日发货量(吨) |
	|------|------|------|-------------|
	| 1 | dayX | YYYY-MM-DD | XXX |
	| 2 | dayY | YYYY-MM-DD | XXX |
	（只列出 predicted_weight > 0 的日期，按日期升序排列；索引列和日期列必须取自 User Prompt 末尾的【时间锚点】表）
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
	2. 汇总表中的日期和索引必须与 User Prompt 末尾【时间锚点】表中的对应值完全一致，禁止自编不在锚点表中的日期。
	3. 日发货量按日期汇总：同一天可能有多品种/多车次，日发货量 = 该日所有记录的重量之和。
	4. JSON items 与汇总表的对应规则：
	   - 汇总表中有 N 行 → items 中恰好有 N 条 predicted_weight > 0 的记录
	   - 每条 items 的 target_date 和 predicted_weight 必须与汇总表完全一致
	   - 汇总表中未列出的日期 → items 中 predicted_weight 必须为 0
	5. 不允许分析文本与 JSON 数据不一致。例如分析说"70-130吨"但 JSON 中出现 140 吨即为错误。

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
- items 必须包含从 day0 到 day15 共 16 条记录，target_date 分别对应 User Prompt【时间锚点】表中的日期
- predicted_weight 为发货吨数（不发货则为 0）
- ship_probability 为发货概率（高/中/低）
- confidence_level 为置信度（高/中/低）
- 每条 target_date 必须与【时间锚点】中对应 day 索引的日期严格一致
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
        from datetime import date as _date

        today = _date.today()
        # 计算上个月和本月（用于后续分析中的相对时间参照）
        if today.month == 1:
            last_month = _date(today.year - 1, 12, 1)
        else:
            last_month = _date(today.year, today.month - 1, 1)
        this_month = _date(today.year, today.month, 1)

        lines = [
            "==================================================",
            "【基础信息 · 绝对时间锚点】",
            f"当前日期（今天）：{today.isoformat()}（{_weekday_name(today)}）",
            f"  → 后续分析中'上个月'指 {last_month.year}年{last_month.month}月",
            f"  → 后续分析中'本月'指 {this_month.year}年{this_month.month}月",
            f"  → '近3天'指 {today.isoformat()} 往前3天；'近7天'指往前7天",
            f"仓库名称：{req.warehouse}",
        ]
        if req.product_variety:
            lines.append(f"目标品类：{req.product_variety}")
        if forecast_dates:
            lines.append(f"预测起始日：{forecast_dates[0].isoformat()}")
            lines.append(f"预测结束日：{forecast_dates[-1].isoformat()}")
            lines.append(f"预测天数：{len(forecast_dates)} 天")
            # 跨月检测与说明
            start_month = forecast_dates[0].month
            end_month = forecast_dates[-1].month
            if start_month != end_month:
                split_day = None
                for i, d in enumerate(forecast_dates):
                    if d.month != start_month:
                        split_day = i
                        break
                if split_day is not None:
                    lines.append(f"⚠️ 预测期跨月：day0~day{split_day-1} 属于 {forecast_dates[0].year}年{start_month}月，day{split_day}~day{len(forecast_dates)-1} 属于 {forecast_dates[split_day].year}年{end_month}月。分析时请分段讨论。")
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

        # ── 先按日期聚合，同日多条记录累加为日总发货量 ──
        daily_aggregated: dict[date, float] = defaultdict(float)
        daily_details: dict[date, list[str]] = defaultdict(list)  # 品类明细文字
        for h in filtered:
            daily_aggregated[h.送货日期] += float(h.重量吨)
            daily_details[h.送货日期].append(f"{h.品类}{float(h.重量吨):.0f}t")

        distinct_dates = sorted(daily_aggregated.keys())
        daily_weights = [daily_aggregated[d] for d in distinct_dates]
        total_records = len(filtered)       # 原始明细条数
        total_days = len(distinct_dates)    # 实际发货天数
        total_weight = sum(daily_weights)   # 总发货量（与明细累加一致）
        avg_weight = statistics.mean(daily_weights) if daily_weights else 0
        std_weight = statistics.stdev(daily_weights) if len(daily_weights) > 1 else 0
        max_weight = max(daily_weights) if daily_weights else 0
        min_weight = min(daily_weights) if daily_weights else 0

        # 月度频率（基于实际发货天数）
        if distinct_dates:
            date_range_days = (distinct_dates[-1] - distinct_dates[0]).days + 1
            months = max(date_range_days / 30, 1)
            monthly_freq = total_days / months
        else:
            monthly_freq = 0

        # 发货间隔（基于去重后的日期序列）
        intervals = []
        for i in range(1, len(distinct_dates)):
            delta = (distinct_dates[i] - distinct_dates[i - 1]).days
            intervals.append(delta)
        avg_interval = statistics.mean(intervals) if intervals else 0

        # 星期分布（基于去重日期）
        weekday_counter = Counter(d.weekday() for d in distinct_dates)
        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday_dist = " / ".join(
            f"{weekday_names[w]}:{c}次" for w, c in sorted(weekday_counter.items())
        )

        # 月度频率判定
        if monthly_freq >= 8:
            freq_label = "高频（≥8次/月）"
        elif monthly_freq >= 3:
            freq_label = "中频（3~7次/月）"
        else:
            freq_label = "低频（<3次/月）"

        # ── 输出：先输出按日汇总的统计，再输出原始明细 ──
        lines.append(f"原始明细条数：{total_records} 条（同日多品种/多车次会拆分为多条明细）")
        lines.append(f"实际发货天数：{total_days} 天（同一天的多条明细已合并为一个发货日）")
        lines.append(f"历史总发货量：{total_weight:.2f} 吨")
        lines.append(f"日均发货量（按发货日）：{avg_weight:.2f} 吨")
        lines.append(f"日发货量最大值：{max_weight:.2f} 吨")
        lines.append(f"日发货量最小值：{min_weight:.2f} 吨")
        lines.append(f"日发货量标准差：{std_weight:.2f}")
        lines.append(f"月度发货频率：约 {monthly_freq:.1f} 次/月")
        lines.append(f"月度频率判定：{freq_label}")
        if avg_interval:
            lines.append(f"平均发货间隔：{avg_interval:.1f} 天（仅计不同日期）")
        lines.append(f"星期分布：{weekday_dist}")

        # ── 按月拆分统计（系统预计算，LLM 直接引用，禁止自行计算）──
        from datetime import date as _date_section
        _today = _date_section.today()
        if _today.month == 1:
            _last_month_start = _date_section(_today.year - 1, 12, 1)
            _last_month_end = _date_section(_today.year - 1, 12, 31)
        else:
            _last_month_start = _date_section(_today.year, _today.month - 1, 1)
            _last_month_end = _date_section(_today.year, _today.month, 1) - timedelta(days=1)
        _this_month_start = _date_section(_today.year, _today.month, 1)

        _last_month_dates = [d for d in distinct_dates if _last_month_start <= d <= _last_month_end]
        _this_month_dates = [d for d in distinct_dates if d >= _this_month_start]

        _last_month_days = len(_last_month_dates)
        _last_month_weight = sum(daily_aggregated[d] for d in _last_month_dates)
        _last_month_avg = (_last_month_weight / _last_month_days) if _last_month_days else 0

        _this_month_days = len(_this_month_dates)
        _this_month_weight = sum(daily_aggregated[d] for d in _this_month_dates)

        _this_month_varieties: set[str] = set()
        for d in _this_month_dates:
            for detail in daily_details[d]:
                _this_month_varieties.add(detail.rstrip("0123456789.t"))

        lines.append("")
        lines.append("──【按月拆分统计 · 系统预计算 · 直接引用】──")
        lines.append(f"上个月（{_last_month_start.year}年{_last_month_start.month}月）：")
        lines.append(f"  发货天数：{_last_month_days} 天")
        lines.append(f"  总发货量：{_last_month_weight:.2f} 吨")
        if _last_month_days > 0:
            lines.append(f"  日均发货量：{_last_month_avg:.2f} 吨")
            lines.append(f"  发货日期：{', '.join(d.isoformat() for d in _last_month_dates)}")
        else:
            lines.append(f"  上个月无发货记录")
        lines.append(f"本月（{_this_month_start.year}年{_this_month_start.month}月，截至今天）：")
        lines.append(f"  已发货天数：{_this_month_days} 天")
        lines.append(f"  已发货总量：{_this_month_weight:.2f} 吨")
        if _this_month_days > 0:
            lines.append(f"  发货日期：{', '.join(d.isoformat() for d in _this_month_dates)}")
        lines.append(f"  已发货品类：{', '.join(sorted(_this_month_varieties)) if _this_month_varieties else '无'}")

        # 按日汇总表（最近 30 个发货日）
        recent_dates = distinct_dates[-30:]
        lines.append("")
        lines.append("近期按日汇总（日期 | 日总发货量 | 当日品类明细）：")
        for d in recent_dates:
            detail_str = " + ".join(daily_details[d])
            lines.append(f"  {d.isoformat()} | {daily_aggregated[d]:.0f}吨 | {detail_str}")

        # 原始明细记录（最近 50 条，供参考）
        recent = filtered[-50:]
        lines.append("")
        lines.append("原始明细记录（日期 | 品类 | 冶炼厂 | 天气 | 重量吨）【仅供参考，统计以上方按日汇总为准】：")
        for h in recent:
            weather = h.天气 or "未知"
            smelter = h.冶炼厂 or "未知"
            lines.append(f"  {h.送货日期.isoformat()} | {h.品类} | {smelter} | {weather} | {h.重量吨}")

        # 截断提示
        if len(filtered) > self.MAX_HISTORY_RECORDS:
            lines.append(f"\n（仅展示最近 {self.MAX_HISTORY_RECORDS} 条明细，共 {total_records} 条）")

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
        """构建时间锚点表 — 贯穿分析文本、汇总表和 JSON 的统一日期引用标准。"""
        from datetime import date as _date

        def _wk_type(d: _date) -> str:
            return "周末" if d.weekday() >= 5 else "工作日"

        lines = [
            "==================================================",
            "╔══════════════════════════════════════════════════╗",
            "║  【时间锚点】本次预测全部目标日期（不可变）      ║",
            "║  后续所有分析和输出中的日期必须严格取自本表，    ║",
            "║  必须同时使用 day 索引 + 日期引用，禁止自编日期。 ║",
            "╚══════════════════════════════════════════════════╝",
            "",
            "┌───────┬──────────────┬────────┬──────────┐",
            "│ 索引  │ 日期         │ 星期   │ 类型   │ 所属月份 │",
            "├───────┼──────────────┼────────┼────────┼──────────┤",
        ]
        for i, d in enumerate(forecast_dates):
            wk = _weekday_name(d)
            wt = _wk_type(d)
            month_label = f"{d.month}月"
            lines.append(f"│ day{i:<2} │ {d.isoformat()}   │ {wk}   │ {wt}   │ {month_label}       │")
        lines.append("└───────┴──────────────┴────────┴──────────┘")
        lines.append("")
        lines.append(f"共 {len(forecast_dates)} 天。请在后续第六部分的汇总表和 JSON items 中严格使用上表中的索引和日期。")
        lines.append("==================================================")
        return "\n".join(lines)

def _weekday_name(d: date) -> str:
    names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    return names[d.weekday()]
