"""库房送货统计缓存服务 — T+1 定时汇总，供电子地图展示当月发货量/年度累计发货量。

每日凌晨自动执行一次：
- 当月发货量 = SUM(weight) WHERE YEAR/MONTH 匹配当月 AND delivery_date <= 统计日
- 年度累计发货量 = SUM(weight) WHERE YEAR 匹配当年 AND delivery_date <= 统计日

聚合结果 UPSERT 至 pd_warehouse_delivery_stats，前端直接从仓库列表 API 读取。
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import List, Tuple

from app.database import get_conn

logger = logging.getLogger(__name__)


def aggregate_delivery_stats(target_date: date | None = None) -> int:
    """执行一次全量库房送货统计，写入缓存表。

    Args:
        target_date: 统计截止日期。None 则默认取昨天（T+1）。

    Returns:
        写入的记录数。
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    stat_year = target_date.year
    stat_month = target_date.month

    logger.info(
        "开始聚合库房送货统计：截止日期=%s, 年=%d, 月=%d",
        target_date.isoformat(),
        stat_year,
        stat_month,
    )

    rows: List[Tuple[str, float, float]] = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        warehouse,
                        SUM(CASE
                            WHEN YEAR(delivery_date) = %s AND MONTH(delivery_date) = %s
                                 AND delivery_date <= %s
                            THEN weight ELSE 0
                        END) AS monthly_ton,
                        SUM(CASE
                            WHEN YEAR(delivery_date) = %s AND delivery_date <= %s
                            THEN weight ELSE 0
                        END) AS yearly_ton
                    FROM pd_ip_delivery_records
                    WHERE delivery_date <= %s
                      AND weight > 0
                    GROUP BY warehouse
                    """,
                    (stat_year, stat_month, target_date,
                     stat_year, target_date,
                     target_date),
                )
                for wh, monthly, yearly in cur.fetchall():
                    rows.append((str(wh), float(monthly), float(yearly)))
    except Exception:
        logger.exception("聚合库房送货统计查询失败")
        return 0

    if not rows:
        logger.info("库房送货统计聚合完成：0 条记录（无送货数据）")
        return 0

    written = 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for wh, monthly, yearly in rows:
                    cur.execute(
                        """
                        INSERT INTO pd_warehouse_delivery_stats
                            (warehouse, stat_year, stat_month,
                             monthly_delivery_ton, yearly_delivery_ton, stat_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            monthly_delivery_ton = VALUES(monthly_delivery_ton),
                            yearly_delivery_ton = VALUES(yearly_delivery_ton),
                            stat_date = VALUES(stat_date)
                        """,
                        (wh, stat_year, stat_month, monthly, yearly, target_date),
                    )
                    written += 1
        logger.info(
            "库房送货统计聚合完成：%d 条记录已写入 pd_warehouse_delivery_stats",
            written,
        )
    except Exception:
        logger.exception("写入 pd_warehouse_delivery_stats 失败")
    return written


def run_scheduled_delivery_stats_aggregation() -> None:
    """定时任务入口（供 APScheduler 调用）。"""
    try:
        written = aggregate_delivery_stats()
        logger.info("定时送货统计聚合任务完成，写入 %d 条", written)
    except Exception:
        logger.exception("定时送货统计聚合任务执行失败")
