"""
EasyOCR 默认用 urllib 无超时地从 GitHub releases 拉 zip，在国内/弱网易 RemoteDisconnected。
在 import Reader 之前 monkeypatch easyocr.utils.download_and_unzip。

环境变量（均可选）：
- EASYOCR_GITHUB_MIRROR_CANDIDATES：多个镜像前缀，英文逗号分隔，按顺序尝试（慢的往后挪或删掉）。
  示例：https://ghfast.top,https://mirror.ghproxy.com,https://ghproxy.net
- EASYOCR_GITHUB_MIRROR：仅单个前缀时可用（未设 CANDIDATES 时生效）
- EASYOCR_NO_BUILTIN_MIRROR：设为 1 时不用内置镜像列表，只试直连（或仅你配置的镜像）
- EASYOCR_DOWNLOAD_TIMEOUT：单次请求超时秒数（默认 120，便于快速换下一个镜像）
- EASYOCR_PER_MIRROR_RETRIES：每个镜像 URL 重试次数（默认 2）
- EASYOCR_MIRROR_SWITCH_DELAY：换下一个镜像前等待秒数（默认 0.5）
"""
from __future__ import annotations

import http.client
import logging
import os
import shutil
import ssl
import time
import urllib.error
import urllib.request
from zipfile import ZipFile

logger = logging.getLogger(__name__)

_ORIGINAL = None

# 内置顺序可随网络环境调整；优先尝试通常较快的入口，失败自动换下一个
_DEFAULT_MIRROR_PREFIXES: tuple[str, ...] = (
    "https://ghfast.top",
    "https://mirror.ghproxy.com",
    "https://ghproxy.net",
)


def _is_github_release_url(url: str) -> bool:
    u = url.strip()
    return u.startswith("https://github.com/") or u.startswith("http://github.com/")


def _mirror_prefixes() -> list[str]:
    raw = os.getenv("EASYOCR_GITHUB_MIRROR_CANDIDATES", "").strip()
    if raw:
        return [p.strip().rstrip("/") for p in raw.split(",") if p.strip().rstrip("/")]

    single = os.getenv("EASYOCR_GITHUB_MIRROR", "").strip().rstrip("/")
    if single:
        return [single]

    if os.getenv("EASYOCR_NO_BUILTIN_MIRROR", "").strip().lower() in ("1", "true", "yes", "on"):
        return []

    return list(_DEFAULT_MIRROR_PREFIXES)


def _candidate_download_urls(original: str) -> list[str]:
    """返回按顺序尝试的完整 URL 列表，最后一项为直连 GitHub。"""
    u = original.strip()
    if not _is_github_release_url(u):
        return [u]

    prefixes = _mirror_prefixes()
    mirrored = [f"{p}/{u}" for p in prefixes]
    if u not in mirrored:
        mirrored.append(u)
    return mirrored


def _download_to_file(url: str, dest: str, *, timeout: float) -> None:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; PD_max EasyOCR prefetch)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        with open(dest, "wb") as out:
            shutil.copyfileobj(resp, out)


def _patched_download_and_unzip(url: str, filename: str, model_storage_directory: str, verbose: bool = True) -> None:
    candidates = _candidate_download_urls(url)
    timeout = float(os.getenv("EASYOCR_DOWNLOAD_TIMEOUT", "120"))
    per_mirror = max(1, int(os.getenv("EASYOCR_PER_MIRROR_RETRIES", "2")))
    switch_delay = float(os.getenv("EASYOCR_MIRROR_SWITCH_DELAY", "0.5"))

    zip_path = os.path.join(model_storage_directory, "temp.zip")
    last_err: BaseException | None = None

    for idx, real_url in enumerate(candidates):
        if verbose and real_url != url:
            logger.info(
                "EasyOCR 尝试下载 (%s/%s): %s",
                idx + 1,
                len(candidates),
                real_url[:100] + ("…" if len(real_url) > 100 else ""),
            )
        for attempt in range(per_mirror):
            try:
                _download_to_file(real_url, zip_path, timeout=timeout)
                with ZipFile(zip_path, "r") as zip_obj:
                    zip_obj.extract(filename, model_storage_directory)
                os.remove(zip_path)
                return
            except (
                urllib.error.URLError,
                TimeoutError,
                OSError,
                ConnectionError,
                ssl.SSLError,
                http.client.RemoteDisconnected,
            ) as e:
                last_err = e
                logger.warning(
                    "EasyOCR 下载失败 镜像#%s 尝试 %s/%s: %s",
                    idx + 1,
                    attempt + 1,
                    per_mirror,
                    e,
                )
                if os.path.isfile(zip_path):
                    try:
                        os.remove(zip_path)
                    except OSError:
                        pass
                if attempt < per_mirror - 1:
                    time.sleep(min(30.0, 1.5 * (attempt + 1)))
        if idx < len(candidates) - 1:
            time.sleep(switch_delay)

    assert last_err is not None
    raise last_err


def patch_easyocr_download() -> None:
    """对 easyocr 的下载函数打补丁；可重复调用（幂等）。

    easyocr.easyocr 在 import 时用 ``from .utils import download_and_unzip`` 绑定了
    函数对象；只改 ``easyocr.utils.download_and_unzip`` 不会更新 Reader 里用的那份引用，
    Reader 仍会走 urllib 的 urlretrieve（无镜像、易 SSL EOF / 超时）。
    因此需同时替换 utils 与 easyocr 包内已缓存的绑定。
    """
    global _ORIGINAL
    import easyocr.utils as eu

    if _ORIGINAL is None:
        _ORIGINAL = eu.download_and_unzip
    eu.download_and_unzip = _patched_download_and_unzip
    try:
        import easyocr.easyocr as eocr

        eocr.download_and_unzip = _patched_download_and_unzip
    except Exception:  # noqa: BLE001 — 无 easyocr.easyocr 的旧版本等，utils 已修补即可
        logger.debug("未修补 easyocr.easyocr.download_and_unzip", exc_info=True)
