#!/usr/bin/env python
"""测试数据生成器：往 pd_ip_delivery_records 表插入多组有代表性的历史送货数据。

使用方式：
    uv run scripts/seed_test_prediction_data.py [--dry-run] [--truncate] [--seed 42]

场景说明：
  1. 稳定周期仓（上海宝钢库 × 铅锭 × 金利）—— 每 7 天左右发一次，月均 ~30 吨
  2. 不规律仓（广州天河库 × 铅锭 × 金利）—— 间隔 3-20 天不等
  3. 高敏感仓（深圳南山库 × 铅锭 × 金利）—— 前期规律发货，后期中断
  4. 低频仓（成都武侯库 × 废铅膏 × 金利）—— 每月仅发 1-2 次，量大
  5. 新仓（杭州余杭库 × 铅锭 × 金利）—— 近期才开始有数据
"""

from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import pymysql


def get_conn() -> pymysql.Connection:
    import os
    import sys
    from pathlib import Path
    from dotenv import load_dotenv
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
        autocommit=True,
    )


def truncate_existing(conn: pymysql.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE pd_ip_delivery_records")
    print("已清空 pd_ip_delivery_records")


def _insert(
    conn: pymysql.Connection,
    regional_manager: str,
    warehouse: str,
    smelter: str,
    warehouse_address: Optional[str],
    smelter_address: Optional[str],
    delivery_date: date,
    product_variety: str,
    weight: float,
    cn_is_workday: Optional[bool] = None,
    cn_calendar_label: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pd_ip_delivery_records (
                regional_manager, warehouse, smelter,
                warehouse_address, smelter_address,
                delivery_date, product_variety, weight,
                cn_is_workday, cn_calendar_label
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                regional_manager,
                warehouse,
                smelter,
                warehouse_address,
                smelter_address,
                delivery_date,
                product_variety,
                weight,
                cn_is_workday,
                cn_calendar_label,
            ),
        )


# ---------------------------------------------------------------------------
# 场景 1：稳定周期仓 — 上海宝钢库 × 铅锭 × 金利
# 每 7±1 天发一次，重量 10~15 吨，近 6 个月数据
# ---------------------------------------------------------------------------
def seed_stable_warehouse(conn: pymysql.Connection, rng: random.Random) -> int:
    count = 0
    start = date.today() - timedelta(days=180)
    d = start + timedelta(days=rng.randint(0, 6))
    while d < date.today():
        weight = round(rng.uniform(10, 15), 2)
        _insert(
            conn,
            regional_manager="张明",
            warehouse="上海宝钢库",
            smelter="河南金利金铅集团有限公司",
            warehouse_address="上海市宝山区铁力路2500号",
            smelter_address="河南省济源市梨林工业区",
            delivery_date=d,
            product_variety="铅锭",
            weight=weight,
            cn_is_workday=True,
            cn_calendar_label="否",
        )
        count += 1
        interval = max(5, min(9, rng.gauss(7, 1)))
        d += timedelta(days=int(round(interval)))
    return count


# ---------------------------------------------------------------------------
# 场景 2：不规律仓 — 广州天河库 × 铅锭 × 金利
# 间隔 3~20 天不等，重量波动大（5~25 吨）
# ---------------------------------------------------------------------------
def seed_irregular_warehouse(conn: pymysql.Connection, rng: random.Random) -> int:
    count = 0
    start = date.today() - timedelta(days=200)
    d = start
    while d < date.today() - timedelta(days=3):
        weight = round(rng.uniform(5, 25), 2)
        _insert(
            conn,
            regional_manager="张明",
            warehouse="广州天河库",
            smelter="河南金利金铅集团有限公司",
            warehouse_address="广州市天河区科韵路16号",
            smelter_address="河南省济源市梨林工业区",
            delivery_date=d,
            product_variety="铅锭",
            weight=weight,
        )
        count += 1
        interval = rng.randint(3, 20)
        d += timedelta(days=interval)
    return count


# ---------------------------------------------------------------------------
# 场景 3：高敏感仓 — 深圳南山库 × 铅锭 × 金利
# 前 3 个月每 10 天发一次（15~20 吨），后 3 个月大幅减少（仅 3 次，量也小）
# 模拟"价格劣势导致发货意愿减弱"
# ---------------------------------------------------------------------------
def seed_sensitive_warehouse(conn: pymysql.Connection, rng: random.Random) -> int:
    count = 0
    # 前期：规律发货
    d = date.today() - timedelta(days=180)
    phase1_end = date.today() - timedelta(days=90)
    while d < phase1_end:
        weight = round(rng.uniform(15, 20), 2)
        _insert(
            conn,
            regional_manager="李华",
            warehouse="深圳南山库",
            smelter="河南金利金铅集团有限公司",
            warehouse_address="深圳市南山区科技园南路8号",
            smelter_address="河南省济源市梨林工业区",
            delivery_date=d,
            product_variety="铅锭",
            weight=weight,
        )
        count += 1
        d += timedelta(days=max(8, min(12, rng.gauss(10, 1))))

    # 后期：大幅减少
    phase2_dates = [
        date.today() - timedelta(days=75),
        date.today() - timedelta(days=40),
        date.today() - timedelta(days=10),
    ]
    for pd in phase2_dates:
        if pd < date.today():
            weight = round(rng.uniform(3, 8), 2)
            _insert(
                conn,
                regional_manager="李华",
                warehouse="深圳南山库",
                smelter="河南金利金铅集团有限公司",
                warehouse_address="深圳市南山区科技园南路8号",
                smelter_address="河南省济源市梨林工业区",
                delivery_date=pd,
                product_variety="铅锭",
                weight=weight,
            )
            count += 1
    return count


# ---------------------------------------------------------------------------
# 场景 4：低频仓 — 成都武侯库 × 废铅膏 × 金利
# 每月 1-2 次，量大（20~40 吨）
# ---------------------------------------------------------------------------
def seed_low_frequency_warehouse(conn: pymysql.Connection, rng: random.Random) -> int:
    count = 0
    d = date.today() - timedelta(days=210)
    while d < date.today():
        # 每月 1-2 次
        n_shipments = rng.choice([1, 2])
        for _ in range(n_shipments):
            weight = round(rng.uniform(20, 40), 2)
            ship_day = d + timedelta(days=rng.randint(5, 25))
            if ship_day < date.today():
                _insert(
                    conn,
                    regional_manager="王强",
                    warehouse="成都武侯库",
                    smelter="河南金利金铅集团有限公司",
                    warehouse_address="成都市武侯区人民南路四段1号",
                    smelter_address="河南省济源市梨林工业区",
                    delivery_date=ship_day,
                    product_variety="废铅膏",
                    weight=weight,
                )
                count += 1
        d += timedelta(days=rng.randint(25, 35))
    return count


# ---------------------------------------------------------------------------
# 场景 5：新仓 — 杭州余杭库 × 铅锭 × 金利
# 最近 30 天才开始有数据，约 5 条记录
# ---------------------------------------------------------------------------
def seed_new_warehouse(conn: pymysql.Connection, rng: random.Random) -> int:
    count = 0
    start = date.today() - timedelta(days=30)
    dates = [start + timedelta(days=x) for x in [2, 7, 13, 20, 27]]
    for d in dates:
        if d < date.today():
            weight = round(rng.uniform(8, 14), 2)
            _insert(
                conn,
                regional_manager="赵敏",
                warehouse="杭州余杭库",
                smelter="河南金利金铅集团有限公司",
                warehouse_address="杭州市余杭区文一西路969号",
                smelter_address="河南省济源市梨林工业区",
                delivery_date=d,
                product_variety="铅锭",
                weight=weight,
            )
            count += 1
    return count


# ---------------------------------------------------------------------------
# 场景 6：多品种仓 — 武汉洪山库 × 铅锭/废铅膏 × 金利
# 同一仓库有两个品种，用于测试品种维度隔离
# ---------------------------------------------------------------------------
def seed_multi_variety_warehouse(conn: pymysql.Connection, rng: random.Random) -> int:
    count = 0
    start = date.today() - timedelta(days=150)
    d = start
    while d < date.today():
        # 铅锭：每 8 天一次
        weight_ingot = round(rng.uniform(12, 18), 2)
        _insert(
            conn,
            regional_manager="赵敏",
            warehouse="武汉洪山库",
            smelter="河南金利金铅集团有限公司",
            warehouse_address="武汉市洪山区光谷大道100号",
            smelter_address="河南省济源市梨林工业区",
            delivery_date=d,
            product_variety="铅锭",
            weight=weight_ingot,
        )
        count += 1

        # 废铅膏：每 15 天一次（错开）
        if (d - start).days % 15 < 3:
            weight_waste = round(rng.uniform(5, 10), 2)
            _insert(
                conn,
                regional_manager="赵敏",
                warehouse="武汉洪山库",
                smelter="河南金利金铅集团有限公司",
                warehouse_address="武汉市洪山区光谷大道100号",
                smelter_address="河南省济源市梨林工业区",
                delivery_date=d,
                product_variety="废铅膏",
                weight=weight_waste,
            )
            count += 1

        d += timedelta(days=max(6, min(10, rng.gauss(8, 1))))
    return count


# ---------------------------------------------------------------------------
# 场景 7：春节影响仓 — 北京大兴库 × 铅锭 × 金利
# 正常发货，但春节期间有明显断层
# ---------------------------------------------------------------------------
def seed_spring_festival_warehouse(conn: pymysql.Connection, rng: random.Random) -> int:
    count = 0
    d = date(2025, 12, 1)
    # 2026 春节大约在 2 月 17 日，1 月 25 日~2 月 20 日设为春节窗口
    spring_start = date(2026, 1, 20)
    spring_end = date(2026, 2, 25)
    while d < date.today():
        # 春节期间不发货
        if spring_start <= d <= spring_end:
            d += timedelta(days=1)
            continue

        weight = round(rng.uniform(10, 16), 2)
        is_spring_nearby = (d >= date(2026, 1, 10) and d < spring_start) or \
                           (d > spring_end and d <= date(2026, 3, 5))
        _insert(
            conn,
            regional_manager="张明",
            warehouse="北京大兴库",
            smelter="河南金利金铅集团有限公司",
            warehouse_address="北京市大兴区亦庄经济技术开发区",
            smelter_address="河南省济源市梨林工业区",
            delivery_date=d,
            product_variety="铅锭",
            weight=weight,
            cn_is_workday=True,
            cn_calendar_label="否",
        )
        count += 1
        interval = max(5, min(10, rng.gauss(7, 1.5)))
        d += timedelta(days=int(round(interval)))
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="生成测试预测数据")
    parser.add_argument("--dry-run", action="store_true", help="仅打印计划，不写库")
    parser.add_argument("--truncate", action="store_true", help="先清空现有数据")
    parser.add_argument("--seed", type=int, default=42, help="随机种子")
    args = parser.parse_args()

    rng = random.Random(args.seed)

    if args.dry_run:
        print("=== 计划生成的测试数据 ===")
        scenarios = [
            ("稳定周期仓", "上海宝钢库 × 铅锭 × 金利", "每7天一次，10-15吨，近180天"),
            ("不规律仓", "广州天河库 × 铅锭 × 金利", "间隔3-20天，5-25吨，近200天"),
            ("高敏感仓", "深圳南山库 × 铅锭 × 金利", "前期15-20吨，后期锐减至3-8吨"),
            ("低频仓", "成都武侯库 × 废铅膏 × 金利", "每月1-2次，20-40吨，近210天"),
            ("新仓", "杭州余杭库 × 铅锭 × 金利", "仅近30天有5条数据"),
            ("多品种仓", "武汉洪山库 × 铅锭/废铅膏 × 金利", "同一仓库两个品种"),
            ("春节影响仓", "北京大兴库 × 铅锭 × 金利", "春节期间有断层"),
        ]
        for name, combo, desc in scenarios:
            print(f"  {name}: {combo} — {desc}")
        return

    conn = get_conn()
    try:
        if args.truncate:
            truncate_existing(conn)

        seeders = [
            ("稳定周期仓", seed_stable_warehouse),
            ("不规律仓", seed_irregular_warehouse),
            ("高敏感仓", seed_sensitive_warehouse),
            ("低频仓", seed_low_frequency_warehouse),
            ("新仓", seed_new_warehouse),
            ("多品种仓", seed_multi_variety_warehouse),
            ("春节影响仓", seed_spring_festival_warehouse),
        ]

        total = 0
        for name, fn in seeders:
            n = fn(conn, rng)
            print(f"  [{name}] 已插入 {n} 条记录")
            total += n

        # 统计
        with conn.cursor() as cur:
            cur.execute(
                "SELECT warehouse, product_variety, smelter, COUNT(*) as cnt "
                "FROM pd_ip_delivery_records GROUP BY warehouse, product_variety, smelter "
                "ORDER BY cnt DESC"
            )
            print("\n=== 当前库中的仓库 × 品种 × 冶炼厂组合 ===")
            for row in cur.fetchall():
                print(f"  {row[0]} × {row[1]} × {row[2]}: {row[3]} 条")

        print(f"\n共插入 {total} 条测试记录")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
