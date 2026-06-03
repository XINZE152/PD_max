"""天地图 keyWord 限长与缩短逻辑单元测试。"""

from __future__ import annotations

import logging

import pytest

from app.services import tianditu_geocoder as tg


def test_build_keyword_concat():
    kw = tg._build_keyword("广东省", "深圳市", "南山区", "科技园南路1号")
    assert kw == "广东省深圳市南山区科技园南路1号"


def test_fit_keyword_within_limit():
    kw = tg._fit_keyword(
        "广东省", "深圳市", "南山区", "科技园南路1号", max_len=50
    )
    assert len(kw) <= 50
    assert kw == "广东省深圳市南山区科技园南路1号"


def test_fit_keyword_shortens_by_dropping_province():
    long_address = "A" * 42
    kw = tg._fit_keyword(
        "广东省", "深圳市", "南山区", long_address, max_len=50
    )
    assert len(kw) <= 50
    assert kw.startswith("深圳市")


def test_fit_keyword_truncates_tail_when_all_candidates_too_long():
    long_address = "B" * 60
    kw = tg._fit_keyword(
        "广东省", "深圳市", "南山区", long_address, max_len=50
    )
    assert len(kw) == 50
    assert kw == long_address[-50:]


def test_fit_keyword_logs_when_shortened(caplog):
    long_address = "C" * 42
    with caplog.at_level(logging.INFO):
        tg._fit_keyword("广东省", "深圳市", "南山区", long_address, max_len=50)
    assert any("天地图 keyWord 已缩短" in r.message for r in caplog.records)


def test_fit_keyword_logs_when_truncated(caplog):
    long_address = "D" * 60
    with caplog.at_level(logging.INFO):
        tg._fit_keyword("广东省", "深圳市", "南山区", long_address, max_len=50)
    assert any("天地图 keyWord 已截断" in r.message for r in caplog.records)


def test_fit_keyword_empty_returns_empty():
    assert tg._fit_keyword("", "", "", "", max_len=50) == ""
