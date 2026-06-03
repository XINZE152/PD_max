#!/usr/bin/env python
"""15 天发货预测接口测试脚本。

使用方式：
    uv run scripts/test_doubao_prediction.py
    uv run scripts/test_doubao_prediction.py --url http://localhost:8001
    uv run scripts/test_doubao_prediction.py --scenario stable
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from typing import Any

try:
    import requests
except ImportError:
    print("请先安装 requests: uv add requests")
    sys.exit(1)


# ---------------------------------------------------------------------------
# 测试场景数据构造
# ---------------------------------------------------------------------------

def _history(
    dates_weights: list[tuple[str, float]],
    weather: str = "晴",
) -> list[dict[str, Any]]:
    """构造历史送货数据（匹配 PredictionHistoryPoint Schema）。"""
    return [
        {
            "deliveryDate": d,
            "weight": w,
            "weatherSummary": weather,
            "cnCalendarLabel": "否",
        }
        for d, w in dates_weights
    ]


# ---------------------------------------------------------------------------
# 场景 1：稳定周期仓（上海宝钢库 × 铅锭）
# ---------------------------------------------------------------------------

def scenario_stable() -> dict[str, Any]:
    """每 7 天发一次，10~15 吨，近 5 个月数据。"""
    today = date(2026, 6, 3)
    history_dates = []
    d = today - timedelta(days=140)
    while d < today:
        history_dates.append(d)
        d += timedelta(days=7)

    history = _history(
        dates_weights=[
            (d.isoformat(), round(10 + (i % 5) * 1.2, 2))
            for i, d in enumerate(history_dates)
        ],
    )

    return {
        "name": "稳定周期仓（上海宝钢库 × 铅锭）",
        "body": {
            "items": [{
                "warehouse": "上海宝钢库",
                "productVariety": "铅锭",
                "horizonDays": 14,
                "history": history,
            }]
        },
    }


# ---------------------------------------------------------------------------
# 场景 2：不规律仓（广州天河库 × 铅锭）
# ---------------------------------------------------------------------------

def scenario_irregular() -> dict[str, Any]:
    """间隔 3~20 天不等，5~25 吨。"""
    today = date(2026, 6, 3)
    intervals = [5, 12, 3, 18, 8, 20, 6, 15, 4, 10, 14, 7, 11, 16, 9, 5, 13, 8]
    history_dates = []
    d = today - timedelta(days=150)
    for iv in intervals:
        if d < today:
            history_dates.append(d)
        d += timedelta(days=iv)

    history = _history(
        dates_weights=[
            (d.isoformat(), round(5 + (i % 6) * 3.5, 2))
            for i, d in enumerate(history_dates)
        ],
    )

    return {
        "name": "不规律仓（广州天河库 × 铅锭）",
        "body": {
            "items": [{
                "warehouse": "广州天河库",
                "productVariety": "铅锭",
                "horizonDays": 14,
                "history": history,
            }]
        },
    }


# ---------------------------------------------------------------------------
# 场景 3：高敏感仓（深圳南山库 × 铅锭）— 前期规律后期中断
# ---------------------------------------------------------------------------

def scenario_sensitive() -> dict[str, Any]:
    """前期每 10 天发一次 15~20 吨，后期大幅减少。"""
    today = date(2026, 6, 3)
    history_points: list[tuple[str, float]] = []

    # 前 90 天：规律发货
    d = today - timedelta(days=180)
    phase1_end = today - timedelta(days=90)
    i = 0
    while d < phase1_end:
        history_points.append((
            d.isoformat(),
            round(15 + (i % 4) * 1.5, 2),
        ))
        d += timedelta(days=max(8, min(12, 10 + (i % 3) - 1)))
        i += 1

    # 后 90 天：仅 3 次少量
    for offset in [75, 40, 10]:
        pd = today - timedelta(days=offset)
        history_points.append((
            pd.isoformat(),
            round(3 + (offset % 5), 2),
        ))

    history_points.sort(key=lambda x: x[0])
    history = _history(dates_weights=history_points)

    return {
        "name": "高敏感仓（深圳南山库 × 铅锭）— 后期中断",
        "body": {
            "items": [{
                "warehouse": "深圳南山库",
                "productVariety": "铅锭",
                "horizonDays": 14,
                "history": history,
            }]
        },
    }


# ---------------------------------------------------------------------------
# 场景 4：低频仓（成都武侯库 × 废铅膏）
# ---------------------------------------------------------------------------

def scenario_low_frequency() -> dict[str, Any]:
    """每月仅 1~2 次，量大 20~40 吨。"""
    today = date(2026, 6, 3)
    history_points: list[tuple[str, float]] = []
    d = today - timedelta(days=210)
    i = 0
    while d < today:
        n = 1 if i % 2 == 0 else 2
        for j in range(n):
            ship_day = d + timedelta(days=5 + j * 12)
            if ship_day < today:
                history_points.append((
                    ship_day.isoformat(),
                    round(20 + (i % 5) * 4, 2),
                ))
        d += timedelta(days=30)
        i += 1

    history_points.sort(key=lambda x: x[0])
    history = _history(dates_weights=history_points, weather="多云")

    return {
        "name": "低频仓（成都武侯库 × 废铅膏）",
        "body": {
            "items": [{
                "warehouse": "成都武侯库",
                "productVariety": "废铅膏",
                "horizonDays": 14,
                "history": history,
            }]
        },
    }


# ---------------------------------------------------------------------------
# 场景 5：多品类仓（武汉洪山库 × 铅锭/废铅膏）
# ---------------------------------------------------------------------------

def scenario_multi_variety() -> dict[str, Any]:
    """同一仓库两个品种混合发货。"""
    today = date(2026, 6, 3)
    history_points: list[tuple[str, float]] = []
    d = today - timedelta(days=150)
    i = 0
    while d < today:
        history_points.append((
            d.isoformat(),
            round(12 + (i % 4) * 1.5, 2),
        ))
        d += timedelta(days=max(6, min(10, 8 + (i % 3) - 1)))
        i += 1

    history = _history(dates_weights=history_points)

    return {
        "name": "多品类仓（武汉洪山库 × 铅锭）",
        "body": {
            "items": [{
                "warehouse": "武汉洪山库",
                "productVariety": "铅锭",
                "horizonDays": 14,
                "history": history,
            }]
        },
    }


# ---------------------------------------------------------------------------
# 场景 6：新仓（杭州余杭库 × 铅锭）— 数据很少
# ---------------------------------------------------------------------------

def scenario_new_warehouse() -> dict[str, Any]:
    """仅近 30 天有 5 条数据。"""
    today = date(2026, 6, 3)
    new_dates = [today - timedelta(days=x) for x in [28, 22, 15, 8, 3]]

    history = _history(
        dates_weights=[
            (d.isoformat(), round(8 + (i % 3) * 2, 2))
            for i, d in enumerate(new_dates)
        ],
    )

    return {
        "name": "新仓（杭州余杭库 × 铅锭）— 数据稀疏",
        "body": {
            "items": [{
                "warehouse": "杭州余杭库",
                "productVariety": "铅锭",
                "horizonDays": 14,
                "history": history,
            }]
        },
    }


# ---------------------------------------------------------------------------
# 场景 7：空数据测试
# ---------------------------------------------------------------------------

def scenario_empty() -> dict[str, Any]:
    """无历史数据的极端情况。"""
    return {
        "name": "空数据测试（无历史）",
        "body": {
            "items": [{
                "warehouse": "未知仓库",
                "productVariety": "铅锭",
                "horizonDays": 14,
                "history": [],
            }]
        },
    }


# ---------------------------------------------------------------------------
# 所有场景
# ---------------------------------------------------------------------------

SCENARIOS = {
    "stable": scenario_stable,
    "irregular": scenario_irregular,
    "sensitive": scenario_sensitive,
    "low_freq": scenario_low_frequency,
    "multi_variety": scenario_multi_variety,
    "new": scenario_new_warehouse,
    "empty": scenario_empty,
}


# ---------------------------------------------------------------------------
# 发送请求
# ---------------------------------------------------------------------------

def send_request(url: str, body: dict[str, Any], timeout: int = 180) -> Any:
    """发送预测请求并返回结果。"""
    headers = {"Content-Type": "application/json"}
    print(f"  发送请求到 {url} ...")
    resp = requests.post(url, headers=headers, json=body, timeout=timeout)
    print(f"  状态码: {resp.status_code}")
    return resp.json()


def print_result(result: dict[str, Any]) -> None:
    """格式化打印预测结果。"""
    if isinstance(result, list):
        for i, r in enumerate(result):
            print_result(r)
        return

    print(f"\n{'='*60}")
    print(f"  仓库: {result.get('warehouse', '?')}")
    print(f"  品类: {result.get('productVariety', result.get('product_variety', '?'))}")
    print(f"  供应商: {result.get('providerUsed', result.get('provider_used', '?'))}")
    print(f"  耗时: {result.get('latencyMs', result.get('latency_ms', '?'))} ms")
    print(f"  缓存命中: {result.get('cacheHit', result.get('cache_hit', '?'))}")
    parse_err = result.get('parseError', result.get('parse_error'))
    if parse_err:
        print(f"  ⚠️  解析错误: {parse_err}")
    print(f"{'='*60}")

    # 分析报告（截取前 800 字）
    report = result.get("analysisReport", result.get("analysis_report", ""))
    if report:
        print(f"\n【分析报告（前 800 字）】")
        print(report[:800])
        if len(report) > 800:
            print(f"... (共 {len(report)} 字)")

    # 逐日预测
    items = result.get("items", [])
    if items:
        print(f"\n【逐日预测（共 {len(items)} 天）】")
        print(f"  {'日期':<12} {'发货吨数':>8} {'发货概率':>6} {'置信度':>6}  主要因素")
        print(f"  {'-'*70}")
        for it in items:
            td = it.get("targetDate", it.get("target_date", "?"))
            pw = it.get("predictedWeight", it.get("predicted_weight",
                         it.get("expectedShipment", it.get("expected_shipment", 0))))
            sp = it.get("shipProbability", it.get("ship_probability", "?"))
            cl = it.get("confidenceLevel", it.get("confidence_level", "?"))
            mf = it.get("mainFactors", it.get("main_factors", ""))
            if isinstance(pw, str):
                pw = float(pw)
            print(f"  {td:<12} {pw:>8.2f} {sp:>6} {cl:>6}  {mf[:50]}")

    print()


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="15 天发货预测接口测试")
    parser.add_argument("--url", default="http://localhost:8001/predict", help="接口地址")
    parser.add_argument("--scenario", default="all", choices=list(SCENARIOS.keys()), help="测试场景")
    parser.add_argument("--save", action="store_true", help="将请求和响应保存到文件")
    parser.add_argument("--timeout", type=int, default=180, help="请求超时秒数")
    args = parser.parse_args()

    print(f"测试接口: {args.url}")
    print()

    scenarios_to_run = list(SCENARIOS.keys())
    if args.scenario != "all":
        scenarios_to_run = [args.scenario]

    all_results = []

    for name in scenarios_to_run:
        factory = SCENARIOS[name]
        scenario = factory()

        print(f"{'='*60}")
        print(f"场景: {scenario['name']}")
        print(f"{'='*60}")
        print(f"  历史数据: {len(scenario['body']['items'][0].get('history', []))} 条")

        try:
            result = send_request(args.url, scenario["body"], timeout=args.timeout)
            all_results.append({"scenario": scenario["name"], "request": scenario["body"], "response": result})

            if isinstance(result, dict) and "detail" in result:
                print(f"  ❌ 错误: {result['detail']}")
            else:
                print_result(result)
        except requests.exceptions.ConnectionError:
            print(f"  ❌ 连接失败: 请确认服务已启动 ({args.url})")
            break
        except requests.exceptions.Timeout:
            print(f"  ❌ 请求超时（{args.timeout}s）")
        except Exception as e:
            print(f"  ❌ 异常: {e}")

    # 保存结果
    if args.save and all_results:
        out_path = "test_doubao_results.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n结果已保存到: {out_path}")

    print("\n测试完成！")


if __name__ == "__main__":
    main()
