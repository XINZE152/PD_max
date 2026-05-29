"""
SMM 1#铅锭参考价抓取：优先 hq.smm.cn 公开 Ajax 历史接口，失败时再尝试 HTML 解析。

均价 = (最低价 + 最高价) / 2。非 SMM 官方授权 API，仅供内部参考。
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# SMM 1#铅锭（仓库自提指导价）品类 id，见 hq.smm.cn/lead/category/201102250211
_SMM_LEAD_PRODUCT_ID = "201102250211"
_HQ_CATEGORY_URL = f"https://hq.smm.cn/lead/category/{_SMM_LEAD_PRODUCT_ID}"
_HQ_AJAX_HISTORY_TMPL = (
    "https://hq.smm.cn/ajax/spot/history/{product_id}/{start}/{end}"
)

_HTML_FALLBACK_URLS: Tuple[str, ...] = ("https://hq.smm.cn/lead",)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_ROW_RE = re.compile(
    r"SMM\s*1#铅锭"
    r"(?:(?!SMM\s*1#铅锭).){0,200}?"
    r"(\d{4,6})\s*[~～\-－—–]\s*(\d{4,6})"
    r"(?:(?!SMM\s*1#铅锭).){0,120}?"
    r"(\d{2}-\d{2})?",
    re.IGNORECASE | re.DOTALL,
)


class SmmLeadScraperError(ValueError):
    """抓取或解析失败。"""


@dataclass(frozen=True)
class SmmLeadReferenceQuote:
    product_name: str
    price_low: Decimal
    price_high: Decimal
    average_price: Decimal
    unit: str
    quote_date: date
    source_url: str

    def to_api_dict(self) -> dict:
        return {
            "产品": self.product_name,
            "最低价": float(self.price_low),
            "最高价": float(self.price_high),
            "均价": float(self.average_price),
            "单位": self.unit,
            "定价日期": self.quote_date.isoformat(),
            "数据来源": self.source_url,
            "备注": "公开数据按 (最低价+最高价)/2 折算均价，非 SMM 官方授权数据",
        }


def _http_get(
    url: str,
    *,
    timeout: float = 20.0,
    extra_headers: Optional[dict] = None,
) -> bytes:
    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "application/json, text/html, */*",
    }
    if extra_headers:
        headers.update(extra_headers)
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except HTTPError as e:
        raise SmmLeadScraperError(f"请求失败 HTTP {e.code}: {url}") from e
    except URLError as e:
        raise SmmLeadScraperError(f"无法访问 {url}: {e.reason}") from e


def _decode_html(raw: bytes) -> str:
    for enc in ("utf-8", "gbk", "gb2312"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _parse_mmdd(raw: Optional[str], *, default_year: int) -> date:
    if not raw:
        return date.today()
    mm, dd = raw.split("-", 1)
    return date(default_year, int(mm), int(dd))


def _quote_from_low_high(
    *,
    price_low: Decimal,
    price_high: Decimal,
    quote_day: date,
    source_url: str,
) -> SmmLeadReferenceQuote:
    low, high = price_low, price_high
    if low <= 0 or high <= 0:
        raise SmmLeadScraperError("解析到的价格无效")
    if low > high:
        low, high = high, low
    avg = ((low + high) / Decimal(2)).quantize(Decimal("0.01"))
    return SmmLeadReferenceQuote(
        product_name="SMM 1#铅锭",
        price_low=low,
        price_high=high,
        average_price=avg,
        unit="元/吨",
        quote_date=quote_day,
        source_url=source_url,
    )


def _fetch_from_hq_ajax(
    *,
    product_id: str = _SMM_LEAD_PRODUCT_ID,
    lookback_days: int = 30,
) -> SmmLeadReferenceQuote:
    """hq.smm.cn 公开 Ajax：无需登录即可取历史区间价。"""
    today = date.today()
    start = (today - timedelta(days=lookback_days)).isoformat()
    end = today.isoformat()
    url = _HQ_AJAX_HISTORY_TMPL.format(
        product_id=product_id,
        start=start,
        end=end,
    )
    raw = _http_get(
        url,
        extra_headers={
            "Referer": _HQ_CATEGORY_URL,
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    try:
        body = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise SmmLeadScraperError(f"Ajax 响应非 JSON: {url}") from e

    if body.get("code") != 0:
        raise SmmLeadScraperError(
            f"Ajax 返回异常 code={body.get('code')} msg={body.get('msg')}"
        )
    rows = body.get("data") or []
    if not rows:
        raise SmmLeadScraperError("Ajax 未返回任何历史价格")

    latest = max(rows, key=lambda r: str(r.get("renew_date") or ""))
    renew = str(latest.get("renew_date") or "").strip()
    if not renew:
        raise SmmLeadScraperError("Ajax 记录缺少 renew_date")

    try:
        low_raw = latest.get("low")
        high_raw = latest.get("highs", latest.get("high"))
        if low_raw is None or high_raw is None:
            raise SmmLeadScraperError("Ajax 记录缺少 low/highs")
        price_low = Decimal(str(low_raw))
        price_high = Decimal(str(high_raw))
        quote_day = date.fromisoformat(renew[:10])
    except (TypeError, ValueError) as e:
        raise SmmLeadScraperError(f"Ajax 记录字段无效: {latest}") from e

    return _quote_from_low_high(
        price_low=price_low,
        price_high=price_high,
        quote_day=quote_day,
        source_url=url,
    )


def _parse_quote_from_html(html: str, *, source_url: str) -> SmmLeadReferenceQuote:
    match = _ROW_RE.search(html)
    if not match:
        raise SmmLeadScraperError("页面中未找到「SMM 1#铅锭」价格区间")

    today = date.today()
    quote_day = _parse_mmdd(match.group(3), default_year=today.year)
    if quote_day > today:
        quote_day = _parse_mmdd(match.group(3), default_year=today.year - 1)

    return _quote_from_low_high(
        price_low=Decimal(match.group(1)),
        price_high=Decimal(match.group(2)),
        quote_day=quote_day,
        source_url=source_url,
    )


def fetch_smm_lead_reference_price(
    sources: Optional[List[str]] = None,
) -> SmmLeadReferenceQuote:
    """
    获取 SMM 1#铅锭最新参考价：优先 Ajax，失败再尝试 HTML 公开页。
    """
    errors: List[str] = []

    try:
        quote = _fetch_from_hq_ajax()
        logger.info(
            "SMM 1#铅锭参考价(Ajax): %s~%s 均价=%s 日期=%s",
            quote.price_low,
            quote.price_high,
            quote.average_price,
            quote.quote_date,
        )
        return quote
    except SmmLeadScraperError as e:
        errors.append(f"hq ajax: {e}")
        logger.warning("SMM 铅价 Ajax 抓取失败: %s", e)

    for url in list(sources or _HTML_FALLBACK_URLS):
        try:
            html = _decode_html(_http_get(url))
            quote = _parse_quote_from_html(html, source_url=url)
            logger.info(
                "SMM 1#铅锭参考价(HTML): %s~%s 均价=%s 日期=%s 来源=%s",
                quote.price_low,
                quote.price_high,
                quote.average_price,
                quote.quote_date,
                url,
            )
            return quote
        except SmmLeadScraperError as e:
            errors.append(f"{url}: {e}")
            logger.warning("SMM 铅价 HTML 抓取失败 %s", errors[-1])

    raise SmmLeadScraperError("；".join(errors) or "所有来源均失败")
