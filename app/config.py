import os
from pathlib import Path
from typing import FrozenSet, Optional, Tuple

from dotenv import load_dotenv

from app.paths import PROJECT_ROOT

# 始终加载项目根 .env，不依赖进程当前工作目录（避免 uvicorn、Docker、IDE 启动路径不一致）
load_dotenv(PROJECT_ROOT / ".env")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    # 去掉首尾空白，避免 .env 里误带空格/换行（尤其 MYSQL_PASSWORD）导致 MySQL 1045
    return value.strip()


# 数据库配置
MYSQL_HOST = _require_env("MYSQL_HOST")
MYSQL_PORT = int(_require_env("MYSQL_PORT"))
MYSQL_USER = _require_env("MYSQL_USER")
MYSQL_PASSWORD = _require_env("MYSQL_PASSWORD")
MYSQL_DATABASE = _require_env("MYSQL_DATABASE")
MYSQL_CHARSET = os.getenv("MYSQL_CHARSET", "utf8mb4")

# 文件上传目录（相对路径相对项目根，避免启动目录不同写到别处）
_raw_upload = (os.getenv("UPLOAD_DIR") or "uploads").strip() or "uploads"
_up_path = Path(_raw_upload)
UPLOAD_DIR = (
    str(_up_path.resolve())
    if _up_path.is_absolute()
    else str(PROJECT_ROOT / _raw_upload)
)


# JWT 认证配置
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change_this_to_a_strong_random_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  # 默认 24 小时

# 百炼 OpenAI 兼容端点（地域：中国内地）
DASHSCOPE_COMPATIBLE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
# Token Plan 团队版专属（sk-sp- key，与 Coding Plan / 通用 key 不互通）
TOKEN_PLAN_COMPATIBLE_BASE_URL = "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1"
# Coding Plan 专属（sk-sp- key，须与 Token Plan 区分，见 BAILIAN_PLAN_TYPE）
DASHSCOPE_CODING_PLAN_BASE_URL = "https://coding.dashscope.aliyuncs.com/v1"


def resolve_bailian_base_url(api_key: str, explicit_base_url: str = "") -> str:
    """按 API Key 与套餐类型选择百炼兼容端点。

    sk-sp- 前缀可能属于 Token Plan 或 Coding Plan，二者 Base URL 不同；
    可通过 BAILIAN_PLAN_TYPE=token_plan|coding_plan 指定，未配置时默认 token_plan。
    若已显式设置 LLM_BASE_URL / VLM_BASE_URL，优先使用显式值。
    """
    explicit = (explicit_base_url or "").strip()
    if explicit:
        return explicit
    key = (api_key or "").strip()
    if not key.startswith("sk-sp-"):
        return DASHSCOPE_COMPATIBLE_BASE_URL
    plan = (
        os.getenv("BAILIAN_PLAN_TYPE", "").strip()
        or os.getenv("DASHSCOPE_PLAN_TYPE", "").strip()
        or "token_plan"
    ).lower()
    if plan in ("coding_plan", "coding"):
        return DASHSCOPE_CODING_PLAN_BASE_URL
    return TOKEN_PLAN_COMPATIBLE_BASE_URL


# 向后兼容旧函数名
resolve_dashscope_base_url = resolve_bailian_base_url


# DeepSeek OpenAI 兼容端点
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_DEFAULT_MODEL = "deepseek-v4-pro"


def _is_deepseek_model(model: str) -> bool:
    return "deepseek" in (model or "").strip().lower()


def resolve_llm_base_url(api_key: str, explicit_base_url: str = "", model: str = "") -> str:
    """按显式 base_url、模型名、API Key 选择 LLM 兼容端点。"""
    explicit = (explicit_base_url or "").strip()
    if explicit:
        return explicit
    model_l = (model or "").strip().lower()
    provider = (os.getenv("LLM_PROVIDER", "") or "").strip().lower()
    if _is_deepseek_model(model) or provider == "deepseek":
        return DEEPSEEK_BASE_URL
    key = (api_key or "").strip()
    if key.startswith("sk-sp-"):
        return resolve_bailian_base_url(key, "")
    return DASHSCOPE_COMPATIBLE_BASE_URL


def resolve_llm_model(explicit_model: str = "", *, has_explicit_key: bool = False) -> str:
    """解析默认 LLM 模型名。"""
    model = (explicit_model or "").strip()
    if model:
        return model
    provider = (os.getenv("LLM_PROVIDER", "") or "").strip().lower()
    if provider == "deepseek":
        return DEEPSEEK_DEFAULT_MODEL
    if has_explicit_key:
        return "qwen-plus"
    return "qwen-plus"


def is_deepseek_llm() -> bool:
    """当前 LLM 配置是否指向 DeepSeek。"""
    if _is_deepseek_model(LLM_MODEL):
        return True
    base = (LLM_BASE_URL or "").strip().lower()
    return "deepseek.com" in base


# LLM API 配置（采购建议等文本接口，OpenAI 兼容协议）
# 未单独配置 LLM_API_KEY 时，按顺序复用 DASHSCOPE_API_KEY / QWEN_API_KEY / VLM_API_KEY（与报价图识别同源 key 时可少配一项）
_explicit_llm_key = os.getenv("LLM_API_KEY", "").strip()
LLM_API_KEY = (
    _explicit_llm_key
    or os.getenv("DASHSCOPE_API_KEY", "").strip()
    or os.getenv("QWEN_API_KEY", "").strip()
    or os.getenv("VLM_API_KEY", "").strip()
)
_llm_base_env = os.getenv("LLM_BASE_URL", "").strip()
_llm_model_env = os.getenv("LLM_MODEL", "").strip()
LLM_BASE_URL = resolve_llm_base_url(LLM_API_KEY, _llm_base_env, _llm_model_env)
LLM_MODEL = resolve_llm_model(_llm_model_env, has_explicit_key=bool(_explicit_llm_key))

_llm_thinking_raw = os.getenv("LLM_THINKING_ENABLED", "1").strip().lower()
LLM_THINKING_ENABLED = _llm_thinking_raw in ("1", "true", "yes", "on")
LLM_REASONING_EFFORT = (os.getenv("LLM_REASONING_EFFORT", "high") or "high").strip() or "high"

# VLM API 配置
VLM_API_KEY = os.getenv("VLM_API_KEY", "")
_vlm_base_env = os.getenv("VLM_BASE_URL", "").strip()
VLM_BASE_URL = resolve_bailian_base_url(VLM_API_KEY, _vlm_base_env)
VLM_MODEL = os.getenv("VLM_MODEL", "qwen-vl-max-latest")

# 天地图地理编码（MAP_API_KEY 即文档中的 tk）
MAP_API_KEY = os.getenv("MAP_API_KEY", "").strip()
MAP_GEOCODER_URL = (
    os.getenv("MAP_GEOCODER_URL", "http://api.tianditu.gov.cn/geocoder").strip()
    or "http://api.tianditu.gov.cn/geocoder"
)

# 天地图不可用（403/网络/无 KEY）时是否仍写入仓库/冶炼厂，经纬度置 NULL（默认允许，避免阻断业务）
_MAP_ALLOW_NULL_RAW = os.getenv("MAP_GEOCODE_ALLOW_NULL", "1").strip().lower()
MAP_GEOCODE_ALLOW_NULL = _MAP_ALLOW_NULL_RAW in ("1", "true", "yes", "on")

try:
    _map_geo_t = float(os.getenv("MAP_GEOCODER_TIMEOUT", "20").strip() or "20")
    MAP_GEOCODER_TIMEOUT = max(3.0, min(120.0, _map_geo_t))
except ValueError:
    MAP_GEOCODER_TIMEOUT = 20.0

try:
    _map_kw_max = int(os.getenv("MAP_GEOCODER_KEYWORD_MAX_LEN", "50").strip() or "50")
    MAP_GEOCODER_KEYWORD_MAX_LEN = max(10, min(200, _map_kw_max))
except ValueError:
    MAP_GEOCODER_KEYWORD_MAX_LEN = 50


def _optional_positive_int(name: str) -> Optional[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        v = int(raw)
        return v if v > 0 else None
    except ValueError:
        return None


# 上传百炼前将图最长边压到此值（像素），可明显缩短传输与视觉编码时间；不设则不缩放
VLM_IMAGE_MAX_EDGE = _optional_positive_int("VLM_IMAGE_MAX_EDGE")
_vlm_mt = os.getenv("VLM_MAX_TOKENS", "8192").strip()
try:
    VLM_MAX_TOKENS = max(1024, min(32768, int(_vlm_mt)))
except ValueError:
    VLM_MAX_TOKENS = 8192
try:
    VLM_JPEG_QUALITY = max(60, min(100, int(os.getenv("VLM_JPEG_QUALITY", "88"))))
except ValueError:
    VLM_JPEG_QUALITY = 88
try:
    # 调用百炼/兼容 OpenAI 接口的单次 HTTP 读超时（秒），应 ≥ 上游最慢一次 VLM 耗时，且不大于 Nginx proxy_read_timeout
    VLM_REQUEST_TIMEOUT = max(60.0, float(os.getenv("VLM_REQUEST_TIMEOUT", "600")))
except ValueError:
    VLM_REQUEST_TIMEOUT = 600.0


def _env_enabled(name: str, *, default: bool = True) -> bool:
    """环境变量开关：未设置时用 default；0/false/off 为关，1/true/on 为开。"""
    raw = os.getenv(name)
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in ("0", "false", "no", "off", "disabled"):
        return False
    if v in ("1", "true", "yes", "on", "enabled"):
        return True
    return default


# 为 0 时不注册 /ai-detection/*、不启动鉴伪 GC/任务表、不预加载模型（省内存）
AI_DETECTION_ENABLED = _env_enabled("AI_DETECTION_ENABLED", default=True)

# 规则检测（像素重叠 / 时间戳）是否写入 ai_detection_history
AI_RULE_CHECK_PERSIST = _env_enabled("AI_RULE_CHECK_PERSIST", default=True)
# 为 0 时规则检测历史仅存 JSON，不复制归档图
AI_RULE_CHECK_STORE_IMAGE = _env_enabled("AI_RULE_CHECK_STORE_IMAGE", default=True)

# 为 0 时不注册智能预测相关路由、不连 Redis 预热线程、不启动定时预测调度
INTELLIGENT_PREDICTION_ENABLED = _env_enabled("INTELLIGENT_PREDICTION_ENABLED", default=True)

# SMM 1#铅锭公开页参考价：每日定时抓取入库（默认 10:35 上海时区）
SMM_LEAD_PRICE_SCHEDULE_ENABLED = _env_enabled("SMM_LEAD_PRICE_SCHEDULE_ENABLED", default=True)
try:
    SMM_LEAD_PRICE_SCHEDULE_HOUR = int(os.getenv("SMM_LEAD_PRICE_SCHEDULE_HOUR", "10"))
except ValueError:
    SMM_LEAD_PRICE_SCHEDULE_HOUR = 10
try:
    SMM_LEAD_PRICE_SCHEDULE_MINUTE = int(os.getenv("SMM_LEAD_PRICE_SCHEDULE_MINUTE", "35"))
except ValueError:
    SMM_LEAD_PRICE_SCHEDULE_MINUTE = 35

# 库房送货统计定时聚合（T+1，每日凌晨汇总当月/年度发货量缓存，供电子地图展示）
WAREHOUSE_DELIVERY_STATS_SCHEDULE_ENABLED = _env_enabled(
    "WAREHOUSE_DELIVERY_STATS_SCHEDULE_ENABLED", default=True
)
try:
    WAREHOUSE_DELIVERY_STATS_SCHEDULE_HOUR = int(
        os.getenv("WAREHOUSE_DELIVERY_STATS_SCHEDULE_HOUR", "2")
    )
except ValueError:
    WAREHOUSE_DELIVERY_STATS_SCHEDULE_HOUR = 2
try:
    WAREHOUSE_DELIVERY_STATS_SCHEDULE_MINUTE = int(
        os.getenv("WAREHOUSE_DELIVERY_STATS_SCHEDULE_MINUTE", "0")
    )
except ValueError:
    WAREHOUSE_DELIVERY_STATS_SCHEDULE_MINUTE = 0

# 启用「循融宝发货」的冶炼厂在比价/采购建议中货物单价加价（元/吨）；默认 80，可用环境变量覆盖
try:
    _xrb = (os.getenv("XUNRONGBAO_SHIPPING_PREMIUM_PER_TON", "") or "80").strip() or "80"
    XUNRONGBAO_SHIPPING_PREMIUM_PER_TON = float(_xrb)
except ValueError:
    XUNRONGBAO_SHIPPING_PREMIUM_PER_TON = 80.0


def _parse_csv_positive_ints(raw: str) -> FrozenSet[int]:
    out: set[int] = set()
    for part in (raw or "").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            v = int(s)
            if v > 0:
                out.add(v)
        except ValueError:
            continue
    return frozenset(out)


def _parse_csv_names(raw: str) -> Tuple[str, ...]:
    return tuple(p.strip() for p in (raw or "").split(",") if p.strip())


# 垂直库房 AI 分析：竞品库房类型（dict_warehouse_types.id）；名称在运行时查库解析
VERTICAL_WAREHOUSE_AI_COMPETITOR_TYPE_IDS: FrozenSet[int] = _parse_csv_positive_ints(
    os.getenv("VERTICAL_WAREHOUSE_AI_COMPETITOR_TYPE_IDS", "")
)
VERTICAL_WAREHOUSE_AI_COMPETITOR_TYPE_NAMES: Tuple[str, ...] = _parse_csv_names(
    os.getenv("VERTICAL_WAREHOUSE_AI_COMPETITOR_TYPE_NAMES", "")
)
