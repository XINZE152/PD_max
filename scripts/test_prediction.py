#!/usr/bin/env python3
"""预测功能测试脚本 — 在服务器上运行。

用法:
    uv run scripts/test_prediction.py                    # 完整测试（插入数据+预测+清理）
    uv run scripts/test_prediction.py --no-cleanup       # 保留测试数据
    uv run scripts/test_prediction.py --only-predict     # 仅预测（假设数据已存在）
"""

import argparse
import asyncio
import json
import sys
from datetime import date
from decimal import Decimal

import httpx
import pymysql


# ── 配置 ──────────────────────────────────────────────────────────────
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "Aa1968535800",
    "database": "pd_max_db",
    "charset": "utf8mb4",
}

BASE_URL = "http://127.0.0.1:8002"

# 测试数据
TEST_WAREHOUSE = "陕西龙盛金属物资回收有限公司"
TEST_SMELTER = "河南金利金铅集团有限公司"
TEST_MANAGER = "李浩"
TEST_VARIETY = "废铅酸电池"

TEST_RECORDS = [
    ("2026-05-26", 35.36),
    ("2026-05-27", 71.44),
    ("2026-05-28", 212.539),
    ("2026-05-29", 72.40),
    ("2026-05-30", 34.78),
    ("2026-05-31", 142.845),
]


# ── 工具函数 ──────────────────────────────────────────────────────────

def print_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def insert_test_data(conn: pymysql.Connection) -> int:
    """插入测试送货历史，返回插入行数。"""
    with conn.cursor() as cursor:
        # 清理旧数据
        cursor.execute(
            "DELETE FROM pd_ip_delivery_records WHERE warehouse = %s AND smelter = %s",
            (TEST_WAREHOUSE, TEST_SMELTER),
        )
        cleaned = cursor.rowcount

        # 插入新数据
        sql = """
            INSERT INTO pd_ip_delivery_records
            (regional_manager, warehouse, smelter, delivery_date, product_variety, weight,
             cn_is_workday, cn_calendar_label, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, 1, '是', NOW(), NOW())
        """
        rows = [
            (TEST_MANAGER, TEST_WAREHOUSE, TEST_SMELTER, d, TEST_VARIETY, w)
            for d, w in TEST_RECORDS
        ]
        cursor.executemany(sql, rows)
        conn.commit()
        return cleaned, cursor.rowcount


def query_test_data(conn: pymysql.Connection) -> list[dict]:
    """查询刚插入的测试数据。"""
    with conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            "SELECT delivery_date, weight FROM pd_ip_delivery_records "
            "WHERE warehouse = %s AND smelter = %s ORDER BY delivery_date",
            (TEST_WAREHOUSE, TEST_SMELTER),
        )
        return cursor.fetchall()


async def call_predict_api() -> dict:
    """调用同步预测接口。"""
    payload = {
        "items": [
            {
                "warehouse": TEST_WAREHOUSE,
                "product_variety": TEST_VARIETY,
                "smelter": TEST_SMELTER,
                "horizon_days": 15,
            }
        ]
    }
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(f"{BASE_URL}/predict", json=payload)
        resp.raise_for_status()
        return resp.json()


def cleanup_test_data(conn: pymysql.Connection) -> int:
    """清理测试数据。"""
    with conn.cursor() as cursor:
        cursor.execute(
            "DELETE FROM pd_ip_delivery_records WHERE warehouse = %s AND smelter = %s",
            (TEST_WAREHOUSE, TEST_SMELTER),
        )
        conn.commit()
        return cursor.rowcount


def format_prediction_result(result: dict):
    """格式化输出预测结果。"""
    print(f"\n  仓库: {result['warehouse']}")
    print(f"  品种: {result['product_variety']}")
    print(f"  冶炼厂: {result['smelter']}")
    print(f"  供应商: {result['provider_used']}")
    print(f"  耗时: {result['latency_ms']:.1f}ms")
    print(f"  缓存命中: {result['cache_hit']}")
    if result.get("parse_error"):
        print(f"  解析备注: {result['parse_error']}")

    print(f"\n  {'日期':<12} {'发货概率':<8} {'预计发货量':<12} {'置信度':<8}")
    print(f"  {'─'*42}")
    for item in result["items"]:
        td = item.get("targetDate", "?")
        prob = item.get("shipProbability", "?")
        ship = item.get("expectedShipment", "?")
        conf = item.get("confidenceLevel", "?")
        print(f"  {td:<12} {prob:<8} {ship:<12} {conf:<8}")

    # 展示第一条的六大分析
    if result["items"]:
        first = result["items"][0]
        print(f"\n  ── {first['targetDate']} 分析摘要 ──")
        for key in [
            "historyAnalysis",
            "priceSensitivityAnalysis",
            "priceCompetitivenessAnalysis",
            "holidayAnalysis",
            "weatherAnalysis",
            "comprehensiveAnalysis",
        ]:
            val = first.get(key, "")
            if val:
                label = {
                    "historyAnalysis": "历史分析",
                    "priceSensitivityAnalysis": "价格敏感度",
                    "priceCompetitivenessAnalysis": "价格竞争力",
                    "holidayAnalysis": "节假日",
                    "weatherAnalysis": "天气",
                    "comprehensiveAnalysis": "综合判断",
                }.get(key, key)
                preview = val[:120] + "..." if len(val) > 120 else val
                print(f"  [{label}] {preview}")


# ── 主流程 ────────────────────────────────────────────────────────────

async def main(args):
    conn = pymysql.connect(**MYSQL_CONFIG)

    try:
        # ── Step 1: 插入数据 ──
        if not args.only_predict:
            print_section("Step 1: 插入测试送货历史")
            cleaned, inserted = insert_test_data(conn)
            print(f"  清理旧记录: {cleaned} 条")
            print(f"  插入新记录: {inserted} 条")

            # 验证插入
            rows = query_test_data(conn)
            print(f"\n  已插入 {len(rows)} 条送货记录:")
            for r in rows:
                print(f"    {r['delivery_date']}: {r['weight']} 吨")

            if len(rows) == 0:
                print("\n  ❌ 数据插入失败，无法继续测试")
                return 1

        # ── Step 2: 调用预测 ──
        print_section("Step 2: 调用预测接口 (POST /predict)")
        result = await call_predict_api()

        if isinstance(result, list) and len(result) > 0:
            format_prediction_result(result[0])
        else:
            print(f"  ❌ 返回格式异常: {json.dumps(result, ensure_ascii=False)[:200]}")
            return 1

        # ─ Step 3: 检查结果 ──
        print_section("Step 3: 结果检查")
        first = result[0]["items"][0] if result[0].get("items") else {}
        provider = result[0].get("provider_used", "")

        if provider == "local_rule":
            print("  ⚠️  使用了本地规则兜底（LLM 不可用或未配置）")
            print("  检查项: VLM_API_KEY / LLM_API_KEY 是否已配置")
        else:
            print(f"  ✅ 使用了 LLM 供应商: {provider}")

        shipment = first.get("expectedShipment", "0")
        if shipment == "0" or shipment == 0:
            print("  ️  预计发货量为 0（可能历史数据不足或 LLM 判断不发货）")
        else:
            print(f"  ✅ 预计发货量: {shipment} 吨")

        analysis = first.get("comprehensiveAnalysis", "")
        if analysis:
            print(f"  ✅ 综合分析文案长度: {len(analysis)} 字符")
        else:
            print("  ️  综合分析文案为空")

        # ─ Step 4: 查询落库结果 ──
        print_section("Step 4: 查询落库预测结果 (GET /predict/results)")
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{BASE_URL}/predict/results",
                params={"warehouse": TEST_WAREHOUSE, "page": 1, "page_size": 5},
            )
            resp.raise_for_status()
            stored = resp.json()

        total = stored.get("total", 0)
        print(f"  落库记录总数: {total}")
        if total > 0:
            print(f"  ✅ 预测结果已成功写入数据库")
            # 检查 v2 字段
            item = stored["items"][0] if stored.get("items") else {}
            v2_fields = [
                "ship_probability", "expected_shipment", "confidence_level",
                "history_analysis", "comprehensive_analysis",
            ]
            present = [f for f in v2_fields if item.get(f)]
            print(f"  v2 字段填充: {len(present)}/{len(v2_fields)} 个")
        else:
            print("  ❌ 落库记录为空")

        # ── Step 5: 清理 ──
        if not args.no_cleanup and not args.only_predict:
            print_section("Step 5: 清理测试数据")
            deleted = cleanup_test_data(conn)
            print(f"  删除了 {deleted} 条记录")

        print_section("测试完成")
        return 0

    except httpx.HTTPStatusError as e:
        print(f"\n  ❌ HTTP 错误: {e.response.status_code}")
        print(f"     {e.response.text[:500]}")
        return 1
    except httpx.ConnectError:
        print(f"\n   无法连接后端 {BASE_URL}")
        print("     请确认后端服务是否运行: uv run app.py")
        return 1
    except Exception as e:
        print(f"\n  ❌ 异常: {type(e).__name__}: {e}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="预测功能测试脚本")
    parser.add_argument("--no-cleanup", action="store_true", help="保留测试数据，不删除")
    parser.add_argument("--only-predict", action="store_true", help="仅执行预测，不插入/清理数据")
    args = parser.parse_args()

    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
