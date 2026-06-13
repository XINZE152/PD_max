# -*- coding: utf-8 -*-
"""规则检测结果对外展示摘要（供 v3 结果聚合与历史列表）。"""
from __future__ import annotations

from typing import Any, Dict, Optional


def derive_rule_check_status(data: Optional[Dict[str, Any]]) -> str:
    """从 rule-checks 原始 data 推导展示状态：正常 / 可疑 / 篡改。"""
    if not data:
        return "正常"

    flags = data.get("hard_tamper_flags") or {}
    if any(bool(v) for v in flags.values()):
        return "篡改"

    pixel = data.get("pixel_overlap") or {}
    timestamp = data.get("timestamp") or {}

    if pixel.get("alert"):
        return "篡改"
    if bool(timestamp.get("hard_tamper")):
        return "篡改"

    risk_values = [
        float(timestamp.get("risk") or 0.0),
        float(pixel.get("pixel_overlap_score") or 0.0),
    ]
    if max(risk_values) >= 0.55:
        return "可疑"

    reason = str(data.get("reason") or "")
    if reason and reason != "未检出明显规则类异常":
        return "可疑"

    return "正常"


def build_rule_check_public_summary(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """生成前端「辅助核查 / 规则检测」可直接使用的摘要结构。"""
    if not data:
        return {
            "status": "正常",
            "reason": "未发现明显的拼接痕迹或时间矛盾。",
            "pixel_overlap": None,
            "timestamp": None,
            "available": False,
        }

    pixel = data.get("pixel_overlap")
    timestamp = data.get("timestamp")
    status = derive_rule_check_status(data)

    suggested = data.get("suggested_rois")
    pixel_message = "未在检查区域内发现明显拼接/贴图痕迹"
    if pixel:
        if pixel.get("alert"):
            pixel_message = "检测到疑似像素重叠/拼接痕迹"
        elif data.get("pixel_overlap_source") in (None, ""):
            pixel_message = (
                "本次未指定关注区域，未做局部拼接检查。"
                "如需排查某一块是否被改过，可在左侧开启「仅分析框选区域」并框选后重新检测。"
            )

    if pixel is not None:
        pixel_item = {
            "passed": not bool(pixel.get("alert")),
            "message": pixel_message,
        }
    elif suggested:
        pixel_item = {
            "passed": True,
            "message": f"未指定检测区域，已自动识别 {len(suggested)} 个建议检测区域（金额/账号/时间/单号等），请勾选后重新检测",
            "suggested_rois": suggested,
        }
    else:
        pixel_item = None

    ts_check = (timestamp or {}).get("timestamp_check") or {}
    ts_anomalies = list((timestamp or {}).get("anomalies") or [])
    timestamp_item = {
        "passed": not ts_anomalies,
        "message": (
            "；".join((timestamp or {}).get("reasons") or []) or "时间与单据信息存在异常"
            if ts_anomalies
            else "图片中的时间与单据信息未发现明显矛盾"
        ),
        "transaction_time": ts_check.get("transaction_datetime") or ts_check.get("transaction_time"),
    } if timestamp is not None else None

    return {
        "status": status,
        "reason": data.get("reason") or "未检出明显规则类异常",
        "hard_tamper_flags": data.get("hard_tamper_flags") or {},
        "pixel_overlap": pixel_item,
        "timestamp": timestamp_item,
        "pixel_overlap_source": data.get("pixel_overlap_source"),
        "available": True,
    }
