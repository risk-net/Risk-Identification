import asyncio
import configparser
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import re
import time
import queue
import json
import threading

from volcenginesdkarkruntime import AsyncArk
from volcenginesdkarkruntime._exceptions import ArkBadRequestError

BASE_DIR = Path(__file__).resolve().parents[4]
CONFIG_PATH = os.path.join(BASE_DIR, "config/Identification_Method-Chinese_Data-llm_filter-config.ini")
CONFIG = configparser.ConfigParser()
if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"配置文件不存在: {CONFIG_PATH}")
CONFIG.read(CONFIG_PATH, encoding="utf-8")

API_CONFIG = CONFIG["Chinese_Data.API"]
BATCH_CONFIG = CONFIG["Chinese_Data.AsyncBatch"]
ONLINE_CONFIG = CONFIG["Chinese_Data.AsyncOnline"]


def _resolve_path(path_value: str, default: Optional[Path] = None) -> Path:
    value = (path_value or "").strip()
    if not value:
        if default is None:
            raise ValueError("缺少必要的路径配置")
        return default
    candidate = Path(os.path.expanduser(os.path.expandvars(value)))
    if not candidate.is_absolute():
        candidate = (BASE_DIR / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _get_int(cfg: configparser.SectionProxy, option: str, default: int) -> int:
    raw = cfg.get(option, fallback=str(default)).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_bool(cfg: configparser.SectionProxy, option: str, default: bool) -> bool:
    raw = cfg.get(option, fallback=str(default)).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


RUN_MODE = API_CONFIG.get("RUN_MODE", "batch").strip().lower() or "batch"
if RUN_MODE not in {"batch", "online"}:
    raise ValueError(
        f"Chinese_Data.API.RUN_MODE 仅支持 'batch' 或 'online'，当前值: {RUN_MODE}"
    )

MODE_CONFIG = BATCH_CONFIG if RUN_MODE == "batch" else ONLINE_CONFIG
MODE_LABEL = "批量推理" if RUN_MODE == "batch" else "在线推理"
MODE_TARGET_LABEL = "推理点" if RUN_MODE == "batch" else "模型"

INPUT_ROOT = _resolve_path(MODE_CONFIG.get("INPUT_ROOT", "download_dir/cn_risk_news"))
MAX_ARTICLES = _get_int(MODE_CONFIG, "MAX_ARTICLES", 0)

OUTPUT_ROOT_FULL = _resolve_path(
    MODE_CONFIG.get(
        "OUTPUT_DIR_FULL",
        "outputs/Identification_Method-Chinese_Data-llm_filter/batch_full" if RUN_MODE == "batch"
        else "outputs/Identification_Method-Chinese_Data-llm_filter/online_full",
    )
)
OUTPUT_ROOT_TEST = _resolve_path(
    MODE_CONFIG.get("OUTPUT_DIR_TEST", OUTPUT_ROOT_FULL.as_posix() + "_test"),
    OUTPUT_ROOT_FULL.parent / f"{OUTPUT_ROOT_FULL.name}_test",
)
USE_TEST_OUTPUT = _get_bool(MODE_CONFIG, "USE_TEST_OUTPUT_FOR_LIMITED_RUN", True)
OUTPUT_ROOT = OUTPUT_ROOT_TEST if (USE_TEST_OUTPUT and MAX_ARTICLES > 0) else OUTPUT_ROOT_FULL
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

LOG_DIR = _resolve_path(
    MODE_CONFIG.get(
        "LOG_DIR",
        "logs/Identification_Method-Chinese_Data-llm_filter/batch" if RUN_MODE == "batch"
        else "logs/Identification_Method-Chinese_Data-llm_filter/online",
    )
)
LOG_DIR.mkdir(parents=True, exist_ok=True)
PROMPT_PATH = _resolve_path(
    MODE_CONFIG.get("PROMPT_PATH", "prompt/Identification_Method-Chinese_Data-llm_filter-prompt.md")
)

MAX_ARTICLES_PER_FILE = _get_int(MODE_CONFIG, "MAX_ARTICLES_PER_FILE", 0)
THREAD_COUNT = _get_int(MODE_CONFIG, "THREAD_COUNT", 1 if RUN_MODE == "batch" else 2)
PER_THREAD_WORKERS = _get_int(MODE_CONFIG, "PER_THREAD_WORKERS", 50 if RUN_MODE == "batch" else 35)
BATCH_SIZE = _get_int(MODE_CONFIG, "BATCH_SIZE", 5)
CLIENT_TIMEOUT = _get_int(MODE_CONFIG, "CLIENT_TIMEOUT_SECONDS", 3600)
MAX_CONCURRENT_REQUESTS = _get_int(MODE_CONFIG, "MAX_CONCURRENT_REQUESTS", 100)
# 使用threading.Semaphore以支持跨线程的并发控制
_global_concurrency_semaphore: Optional[threading.Semaphore] = None

# 用于区分"超时"和"真正的结束信号"的哨兵对象
_QUEUE_TIMEOUT_SENTINEL = object()
RUN_TS = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
LOG_PATH = LOG_DIR / f"async_{RUN_MODE}_multi_endpoint_cn_news_{RUN_TS}.log"

logger = logging.getLogger(f"chinese_data_async_{RUN_MODE}")
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")

file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# 记录输出目录模式信息
if MAX_ARTICLES > 0:
    logger.info(f"[配置] 测试模式：MAX_ARTICLES={MAX_ARTICLES}，结果将保存到测试文件夹: {OUTPUT_ROOT}")
else:
    logger.info(f"[配置] 全量模式：MAX_ARTICLES=0，结果将保存到全量数据文件夹: {OUTPUT_ROOT}")


def _load_targets() -> List[str]:
    """根据 RUN_MODE 加载批量推理点或在线模型。"""
    if RUN_MODE == "batch":
        disable_multi = not _get_bool(API_CONFIG, "ENABLE_MULTI_ENDPOINTS", True)
        if disable_multi:
            primary = API_CONFIG.get("ARK_BATCH_MODEL_ID", "").strip()
            if not primary:
                raise RuntimeError(
                    "单推理点模式需要在配置文件中设置 ARK_BATCH_MODEL_ID。\n"
                    "请编辑 config/Identification_Method-Chinese_Data-llm_filter-config.ini"
                )
            return [primary]

        ids = API_CONFIG.get("ARK_BATCH_MODEL_IDS", "")
        if ids:
            endpoints = [x.strip() for x in ids.split(",") if x.strip()]
            if endpoints:
                return endpoints

        primary = API_CONFIG.get("ARK_BATCH_MODEL_ID", "").strip()
        ep2 = API_CONFIG.get("ARK_BATCH_MODEL_ID_2", "").strip()
        ep3 = API_CONFIG.get("ARK_BATCH_MODEL_ID_3", "").strip()
        out = [value for value in [primary, ep2, ep3] if value]
        if not out:
            raise RuntimeError(
                "请在配置文件中设置批量推理点：ARK_BATCH_MODEL_IDS 或 ARK_BATCH_MODEL_ID(+_2/_3)，"
                "或禁用多 endpoint 后仅设置 ARK_BATCH_MODEL_ID"
            )
        return out

    disable_multi = not _get_bool(API_CONFIG, "ENABLE_MULTI_MODELS", True)
    if disable_multi:
        primary = API_CONFIG.get("ARK_ONLINE_MODEL_NAME", "").strip() or API_CONFIG.get("ARK_MODEL_NAME", "").strip()
        if not primary:
            raise RuntimeError(
                "单模型模式需要在配置文件中设置 ARK_ONLINE_MODEL_NAME 或 ARK_MODEL_NAME。\n"
                "请编辑 config/Identification_Method-Chinese_Data-llm_filter-config.ini"
            )
        return [primary]

    names = API_CONFIG.get("ARK_ONLINE_MODEL_NAMES", "")
    if names:
        models = [x.strip() for x in names.split(",") if x.strip()]
        if models:
            return models

    primary = API_CONFIG.get("ARK_ONLINE_MODEL_NAME", "").strip() or API_CONFIG.get("ARK_MODEL_NAME", "").strip()
    model2 = API_CONFIG.get("ARK_ONLINE_MODEL_NAME_2", "").strip()
    model3 = API_CONFIG.get("ARK_ONLINE_MODEL_NAME_3", "").strip()
    out = [value for value in [primary, model2, model3] if value]
    if not out:
        raise RuntimeError(
            "请在配置文件中设置在线推理模型：ARK_ONLINE_MODEL_NAMES 或 ARK_ONLINE_MODEL_NAME(+_2/_3)，"
            "或禁用多模型后设置 ARK_MODEL_NAME"
        )
    return out


def _split_files_round_robin(files: List[Path], k: int) -> List[List[Path]]:
    """将文件列表按轮询方式分配到 k 个桶中"""
    if k <= 1:
        return [files]
    buckets: List[List[Path]] = [[] for _ in range(k)]
    for i, f in enumerate(files):
        buckets[i % k].append(f)
    return buckets


def _extract_usage_tokens(usage) -> Tuple[int, int]:
    if usage is None:
        return 0, 0

    def _maybe_attr(obj, name, default=0):
        try:
            value = getattr(obj, name)
        except AttributeError:
            return default
        return value if value is not None else default

    def _maybe_get(obj, name, default=0):
        if isinstance(obj, dict):
            return obj.get(name, default)
        return default

    candidates_prompt = [
        "prompt_tokens",
        "promptTokens",
        "prompt",
        "input_tokens",
        "inputTokens",
    ]
    candidates_completion = [
        "completion_tokens",
        "completionTokens",
        "completion",
        "generation_tokens",
        "output_tokens",
        "outputTokens",
    ]

    prompt_tokens = 0
    completion_tokens = 0

    for key in candidates_prompt:
        prompt_tokens = _maybe_attr(usage, key, prompt_tokens) or _maybe_get(usage, key, prompt_tokens)
        if prompt_tokens:
            break

    for key in candidates_completion:
        completion_tokens = (
            _maybe_attr(usage, key, completion_tokens) or _maybe_get(usage, key, completion_tokens)
        )
        if completion_tokens:
            break

    if not prompt_tokens or not completion_tokens:
        try:
            # 优先使用 Pydantic V2 的 model_dump()，如果没有则回退到 V1 的 dict()
            if hasattr(usage, 'model_dump'):
                usage_dict = usage.model_dump()  # Pydantic V2
            elif hasattr(usage, 'dict'):
                usage_dict = usage.dict()  # Pydantic V1 (deprecated)
            else:
                usage_dict = {}
        except (AttributeError, TypeError):
            usage_dict = {}

        if isinstance(usage_dict, dict):
            if not prompt_tokens:
                for key in candidates_prompt:
                    if key in usage_dict and usage_dict[key]:
                        prompt_tokens = usage_dict[key]
                        break
            if not completion_tokens:
                for key in candidates_completion:
                    if key in usage_dict and usage_dict[key]:
                        completion_tokens = usage_dict[key]
                        break

    return int(prompt_tokens or 0), int(completion_tokens or 0)


def _load_system_prompt() -> str:
    """加载系统提示词文件（使用 UTF-8 编码）"""
    if not PROMPT_PATH.exists():
        raise FileNotFoundError(f"system prompt 文件不存在: {PROMPT_PATH}")
    return PROMPT_PATH.read_text(encoding="utf-8")


def _is_unable_to_answer_response(content: str) -> bool:
    """
    检测LLM返回的内容是否是"无法回答"类型的回复
    
    根据实际日志分析，常见的"无法回答"模式包括：
    - '抱歉，您的问题我无法识别。'
    - '你好，我无法给到相关内容。'
    - '抱歉，我无法回答这个问题。'
    - '您的问题我无法回答。'
    - '你好，这个问题我无法回答，很遗憾不能帮助你。'
    
    Args:
        content: LLM返回的内容
        
    Returns:
        如果是"无法回答"类型的回复，返回True；否则返回False
    """
    if not content:
        return False
    
    content_lower = content.lower().strip()
    content_len = len(content)
    
    # 如果内容为空或只有空白字符，不算"无法回答"
    if not content_lower:
        return False
    
    # 常见的"无法回答"关键词模式（按优先级排序）
    # 强匹配模式：这些模式出现时，基本可以确定是"无法回答"
    strong_patterns = [
        "无法给到",
        "无法回答",
        "无法识别",
        "无法处理",
        "无法理解",
        "无法提供",
        "无法完成",
        "我无法回答",
        "我无法识别",
        "我无法给到",
        "不能帮助你",
        "不能帮助您",
        "很遗憾不能",
    ]
    
    # 弱匹配模式：需要结合其他条件判断
    weak_patterns = [
        "抱歉",
        "对不起",
        "我无法",
        "我不能",
        "无法",
        "unable",
        "cannot",
        "can't",
    ]
    
    # 先检查强匹配模式
    for pattern in strong_patterns:
        if pattern in content_lower:
            # 强匹配模式出现时，如果内容较短（<500字符），直接判定为"无法回答"
            if content_len < 500:
                return True
            # 如果内容较长，但模式出现在开头部分（前200字符），也可能是"无法回答"
            if content_len >= 500 and pattern in content_lower[:200]:
                return True
    
    # 再检查弱匹配模式
    for pattern in weak_patterns:
        if pattern in content_lower:
            # 弱匹配模式需要内容较短才判定为"无法回答"
            if content_len < 300:
                return True
            # 如果内容稍长（300-500字符），但模式出现在开头部分，也可能是"无法回答"
            if 300 <= content_len < 500 and pattern in content_lower[:150]:
                return True
    
    return False


async def _process_single_article(
    client: AsyncArk,
    model: str,
    system_prompt: str,
    article: Dict[str, str],
    temperature: float = 0.0,
) -> Tuple[Optional[Dict[str, Any]], int, int]:
    """
    对单个文章发送请求并返回结构化结果和token统计
    
    Args:
        client: AsyncArk客户端
        model: 模型ID
        system_prompt: 系统提示词
        article: 单个文章数据（包含title, content, release_date）
        temperature: 温度参数
        
    Returns:
        (结构化结果字典, input_tokens, output_tokens)，如果失败返回(None, 0, 0)
    """
    try:
        # 构建单个文章的请求
        article_payload = json.dumps([article], ensure_ascii=False)
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": article_payload},
        ]
        
        if RUN_MODE == "batch":
            completion = await client.batch.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
            )
        else:
            completion = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
            )
        
        # 提取token统计
        usage = getattr(completion, "usage", None)
        input_tokens, output_tokens = _extract_usage_tokens(usage)
        
        # 解析结果
        choices = getattr(completion, "choices", None) or []
        if not choices:
            return None, input_tokens, output_tokens
        
        primary_choice = choices[0]
        message = getattr(primary_choice, "message", None)
        content = getattr(message, "content", "") if message else ""
        
        # 检查是否是"无法回答"
        if _is_unable_to_answer_response(content):
            logger.warning(f"[single] 单个请求也返回'无法回答': {content[:100]}")
            return None, input_tokens, output_tokens
        
        # 提取结构化结果
        structured_results = _extract_structured_results_from_content(content)
        if structured_results and len(structured_results) > 0:
            return structured_results[0], input_tokens, output_tokens
        
        return None, input_tokens, output_tokens
    except Exception as exc:
        logger.error(f"[single] 单个文章请求失败: {exc}", exc_info=True)
        return None, 0, 0


def _parse_date_string(date_str: str) -> Optional[str]:
    """解析日期字符串，返回YYYY-MM-DD格式"""
    date_str = date_str.strip()
    
    # 月份名称映射
    month_map = {
        "january": "01", "jan": "01",
        "february": "02", "feb": "02",
        "march": "03", "mar": "03",
        "april": "04", "apr": "04",
        "may": "05",
        "june": "06", "jun": "06",
        "july": "07", "jul": "07",
        "august": "08", "aug": "08",
        "september": "09", "sep": "09", "sept": "09",
        "october": "10", "oct": "10",
        "november": "11", "nov": "11",
        "december": "12", "dec": "12",
    }
    
    # 格式1: "2023-01-20" 或 "2023-01-20T10:00:08+0000"
    iso_match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    if iso_match:
        return iso_match.group(1)
    
    # 格式2: "January 20, 2023" 或 "Jan 20, 2023"
    month_day_year = re.match(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", date_str, re.IGNORECASE)
    if month_day_year:
        month_name = month_day_year.group(1).lower()
        day = month_day_year.group(2).zfill(2)
        year = month_day_year.group(3)
        
        month_num = month_map.get(month_name)
        if month_num:
            return f"{year}-{month_num}-{day}"
    
    # 格式3: "20 January 2023"
    day_month_year = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", date_str, re.IGNORECASE)
    if day_month_year:
        day = day_month_year.group(1).zfill(2)
        month_name = day_month_year.group(2).lower()
        year = day_month_year.group(3)
        
        month_num = month_map.get(month_name)
        if month_num:
            return f"{year}-{month_num}-{day}"
    
    return None


def _extract_date_from_text(lines: List[str]) -> Optional[str]:
    """从文本文件的前几行提取发布日期"""
    # 检查前5行，寻找日期格式
    date_patterns = [
        r"([A-Za-z]+\s+\d{1,2},\s+\d{4})",  # "January 20, 2023"
        r"(\d{4}-\d{2}-\d{2})",  # "2023-01-20"
        r"(\d{4}-\d{2}-\d{2}T[0-9:+\-]+)",  # "2023-01-20T10:00:08+0000"
        r"([A-Za-z]{3}\s+\d{1,2},\s+\d{4})",  # "Jan 20, 2023"
    ]
    
    for line in lines[:5]:
        line = line.strip()
        if not line:
            continue
        
        for pattern in date_patterns:
            match = re.search(pattern, line)
            if match:
                parsed_date = _parse_date_string(match.group(1))
                if parsed_date:
                    return parsed_date
    
    return None


def _extract_date_from_html_info(html_info: Any) -> Optional[str]:
    """从 html_info 字段（可能是字符串或字典）中提取发布日期"""
    if not html_info:
        return None
    
    # 如果 html_info 是字符串，尝试直接解析
    if isinstance(html_info, str):
        return _extract_date_from_html_content(html_info)
    
    # 如果 html_info 是字典，尝试提取相关字段
    if isinstance(html_info, dict):
        # 尝试常见的日期字段
        for key in ["datePublished", "publish_time", "date", "published_time"]:
            if key in html_info:
                parsed_date = _parse_date_string(html_info[key])
                if parsed_date:
                    return parsed_date
    
    return None


def _extract_date_from_html_content(html_content: str) -> Optional[str]:
    """从 HTML 内容字符串中提取发布日期"""
    if not html_content:
        return None
    
    # 使用与 _extract_date_from_html 相同的模式
    json_ld_patterns = [
        r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})T[^"]*"',
        r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})"',
    ]
    
    for pattern in json_ld_patterns:
        match = re.search(pattern, html_content, re.IGNORECASE)
        if match:
            return match.group(1)
    
    meta_patterns = [
        r'<meta\s+property=["\']article:published_time["\']\s+content=["\'](\d{4}-\d{2}-\d{2})T[^"\']*["\']',
        r'<meta\s+property=["\']article:published_time["\']\s+content=["\'](\d{4}-\d{2}-\d{2})["\']',
        r'<meta\s+name=["\']date["\']\s+content=["\'](\d{4}-\d{2}-\d{2})[^"\']*["\']',
    ]
    
    for pattern in meta_patterns:
        match = re.search(pattern, html_content, re.IGNORECASE)
        if match:
            return match.group(1)
    
    time_pattern = r'<time[^>]*datetime=["\'](\d{4}-\d{2}-\d{2})[^"\']*["\']'
    match = re.search(time_pattern, html_content, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None


def _extract_date_from_html(html_path: Path) -> Optional[str]:
    """
    从HTML文件中提取发布日期（优先从JSON-LD结构化数据提取，格式更统一）
    
    注意：使用 UTF-8 编码读取 HTML 文件
    """
    try:
        with html_path.open("r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read()
        
        # 优先级1: JSON-LD中的datePublished字段（最常见且格式统一）
        json_ld_patterns = [
            r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})T[^"]*"',
            r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})"',
        ]
        
        for pattern in json_ld_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1)
        
        # 优先级2: meta标签中的article:published_time
        meta_patterns = [
            r'<meta\s+property=["\']article:published_time["\']\s+content=["\'](\d{4}-\d{2}-\d{2})T[^"\']*["\']',
            r'<meta\s+property=["\']article:published_time["\']\s+content=["\'](\d{4}-\d{2}-\d{2})["\']',
            r'<meta\s+name=["\']date["\']\s+content=["\'](\d{4}-\d{2}-\d{2})[^"\']*["\']',
        ]
        
        for pattern in meta_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1)
        
        # 优先级3: time标签的datetime属性
        time_pattern = r'<time[^>]*datetime=["\'](\d{4}-\d{2}-\d{2})[^"\']*["\']'
        match = re.search(time_pattern, content, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # 优先级4: 其他格式的datePublished
        fallback_pattern = r'"datePublished"\s*:\s*"([^"]+)"'
        match = re.search(fallback_pattern, content, re.IGNORECASE)
        if match:
            parsed_date = _parse_date_string(match.group(1))
            if parsed_date:
                return parsed_date
                
    except Exception:
        pass
    
    return None


def _extract_release_date(article_data: Dict[str, Any], jsonl_path: Optional[Path] = None) -> str:
    """
    从文章数据中提取发布日期
    
    提取优先级：
    1. 直接使用 publish_time 字段（优先，因为数据集中已有）
    2. 从 html_info 字段中提取（如果存在）
    3. 尝试从对应的 HTML 文件中提取（如果存在）
    4. 使用当前日期作为默认值
    """
    release_date = None
    
    # 方法1: 直接使用 publish_time 字段（优先）
    publish_time = article_data.get("publish_time")
    if publish_time:
        parsed_date = _parse_date_string(publish_time)
        if parsed_date:
            release_date = parsed_date
            logger.debug(f"[collect] 从 publish_time 提取日期: {release_date}")
    
    # 方法2: 从 html_info 字段中提取
    if not release_date:
        html_info = article_data.get("html_info")
        if html_info:
            try:
                release_date = _extract_date_from_html_info(html_info)
                if release_date:
                    logger.debug(f"[collect] 从 html_info 提取日期: {release_date}")
            except Exception as exc:
                logger.debug(f"[collect] 从 html_info 提取日期失败: {exc}")
    
    # 方法3: 尝试从对应的 HTML 文件中提取（如果 JSONL 文件同目录下有对应的 HTML 文件）
    if not release_date and jsonl_path:
        try:
            # 假设 HTML 文件可能在同目录或 htmls 子目录中
            # 根据 JSONL 文件名尝试找到对应的 HTML 文件
            html_candidates = [
                jsonl_path.parent / "htmls" / jsonl_path.name.replace(".jsonl", ".html"),
                jsonl_path.parent / jsonl_path.name.replace(".jsonl", ".html"),
            ]
            
            for html_path in html_candidates:
                if html_path.exists():
                    release_date = _extract_date_from_html(html_path)
                    if release_date:
                        logger.debug(f"[collect] 从 HTML 文件提取日期: {release_date} (from {html_path})")
                        break
        except Exception as exc:
            logger.debug(f"[collect] 从 HTML 文件提取日期失败: {exc}")
    
    # 方法4: 使用当前日期作为默认值
    if not release_date:
        release_date = datetime.now().strftime("%Y-%m-%d")
        logger.debug(f"[collect] 使用默认发布时间: {release_date}")
    
    return release_date


def _load_article_from_jsonl_line(
    line: str, 
    jsonl_path: Path, 
    line_number: int
) -> Optional[Dict[str, str]]:
    """
    从 JSONL 文件的一行中加载文章数据
    
    注意：JSONL 文件读取时必须使用 UTF-8 编码（在调用处已确保）
    
    Args:
        line: JSONL 文件的一行内容（已使用 UTF-8 编码读取）
        jsonl_path: JSONL 文件路径（用于记录和可能的 HTML 文件查找）
        line_number: 行号（用于记录）
    
    Returns:
        包含 title, content, release_date 的字典，失败返回 None
    """
    try:
        line = line.strip()
        if not line:
            return None
        
        article_data = json.loads(line)
        if not isinstance(article_data, dict):
            logger.warning(f"[collect] JSONL 行不是字典格式 {jsonl_path}:{line_number}")
            return None
        
        # 提取标题：尝试多个可能的字段名
        title = ""
        title_candidates = ["title", "Title", "TITLE", "headline", "Headline", "HEADLINE", "subject", "Subject"]
        for key in title_candidates:
            if key in article_data:
                title = str(article_data[key]).strip()
                if title:
                    break
        
        # 如果还没找到，尝试 abstract
        if not title:
            title = str(article_data.get("abstract", "")).strip()
        
        # 提取内容：尝试多个可能的字段名
        content = ""
        content_candidates = ["content", "Content", "CONTENT", "text", "Text", "TEXT", "body", "Body", "BODY", "article", "Article"]
        for key in content_candidates:
            if key in article_data:
                content = str(article_data[key]).strip()
                if content:
                    break
        
        # 如果还没找到，尝试 abstract
        if not content:
            content = str(article_data.get("abstract", "")).strip()
        
        # 如果仍然没有内容，尝试使用 title 作为内容
        if not content and title:
            content = title
        
        # 如果 title 和 content 都为空，记录详细信息并返回 None
        if not title and not content:
            # 只在第一次遇到问题时输出详细调试信息
            if line_number <= 3:
                available_keys = list(article_data.keys())
                logger.warning(
                    f"[collect] 文章缺少标题和内容 {jsonl_path}:{line_number}\n"
                    f"  可用字段: {available_keys}\n"
                    f"  前100个字符: {line[:100]}"
                )
            else:
                logger.debug(f"[collect] 文章缺少标题和内容 {jsonl_path}:{line_number}")
            return None
        
        # 提取发布日期（传入 jsonl_path 以便查找对应的 HTML 文件）
        release_date = _extract_release_date(article_data, jsonl_path)
        
        return {
            "title": title or "无标题",
            "content": content or title or "无内容",
            "release_date": release_date
        }
    except json.JSONDecodeError as exc:
        # 只在前面几行输出详细错误信息
        if line_number <= 3:
            logger.error(
                f"[collect] JSON 解析失败 {jsonl_path}:{line_number}: {exc}\n"
                f"  行内容前200字符: {line[:200]}"
            )
        else:
            logger.warning(f"[collect] JSON 解析失败 {jsonl_path}:{line_number}: {exc}")
        return None
    except Exception as exc:
        logger.error(f"[collect] 处理文章失败 {jsonl_path}:{line_number}: {exc}", exc_info=True)
        return None


def _build_processed_index(output_root: Path) -> Dict[str, set]:
    """
    构建已处理文章的索引（文件路径 -> 已处理的行号集合）
    这样可以快速检查，避免每次都扫描文件系统
    
    Returns:
        Dict[file_stem, set(line_numbers)]
    """
    processed_index: Dict[str, set] = {}
    classifications = ["AIrisk_relevant_event", "AIrisk_relevant_discussion", "AIrisk_Irrelevant"]
    
    logger.info(f"[collect] 开始扫描输出目录，构建已处理文章索引... (输出目录: {output_root})")
    logger.info(f"[collect] 输出目录是否存在: {output_root.exists()}")
    start_time = time.time()
    total_files = 0
    
    for classification in classifications:
        classification_dir = output_root / classification
        logger.debug(f"[collect] 检查分类目录: {classification_dir}, 存在: {classification_dir.exists()}")
        if not classification_dir.exists():
            continue
        
        # 遍历所有 year/month 目录
        year_dirs = list(classification_dir.glob("[0-9][0-9][0-9][0-9]"))
        logger.debug(f"[collect] 分类 {classification} 找到 {len(year_dirs)} 个年份目录")
        for year_dir in year_dirs:
            month_dirs = list(year_dir.glob("[0-9][0-9]"))
            logger.debug(f"[collect] 年份 {year_dir.name} 找到 {len(month_dirs)} 个月份目录")
            for month_dir in month_dirs:
                result_files = list(month_dir.glob("*_line*_result.json"))
                logger.debug(f"[collect] 月份 {month_dir.name} 找到 {len(result_files)} 个结果文件")
                for result_file in result_files:
                    # 解析文件名：{file_stem}_line{line_number:07d}_result.json
                    # 注意：result_file.stem 不包含 .json 扩展名
                    match = re.match(r"(.+)_line(\d+)_result$", result_file.stem)
                    if match:
                        file_stem = match.group(1)
                        line_number = int(match.group(2))
                        
                        if file_stem not in processed_index:
                            processed_index[file_stem] = set()
                        processed_index[file_stem].add(line_number)
                        total_files += 1
                    else:
                        logger.warning(f"[collect] 无法解析文件名: {result_file.name}")
    
    elapsed = time.time() - start_time
    logger.info(
        f"[collect] 索引构建完成，找到 {total_files} 个已处理的结果文件，"
        f"涉及 {len(processed_index)} 个源文件，耗时 {elapsed:.2f} 秒"
    )
    if total_files == 0:
        logger.warning(
            f"[collect] 警告：未找到任何已处理的结果文件。"
            f"请检查输出目录是否正确: {output_root}"
        )
    return processed_index


def _is_article_processed_fast(jsonl_path: Path, line_number: int, processed_index: Dict[str, set]) -> bool:
    """快速检查文章是否已处理（使用索引）"""
    file_stem = jsonl_path.stem
    return file_stem in processed_index and line_number in processed_index[file_stem]


def _collect_requests_streaming(
    endpoint_id: str, 
    system_prompt: str, 
    jsonl_files_iter,
    start_task_id: int = 1,
    processed_index: Optional[Dict[str, set]] = None
):
    """
    流式从 JSONL 文件中收集请求（生成器版本）
    
    Args:
        endpoint_id: 批量推理点 ID
        system_prompt: 系统提示词
        jsonl_files_iter: JSONL 文件迭代器
        start_task_id: 起始任务 ID
        processed_index: 已处理文章的索引（文件stem -> 行号集合），如果为None则不跳过
    
    Yields:
        request: 单个批次请求字典
        task_id: 当前任务ID（用于跟踪）
    """
    current_batch: List[Dict[str, str]] = []
    current_metadata: List[Dict[str, Any]] = []
    task_id = start_task_id
    processed_articles = 0
    failed_articles = 0
    skipped_articles = 0
    file_count = 0
    last_log_time = time.time()
    log_interval = 5.0

    def _flush_batch(batch: List[Dict[str, str]], metadata: List[Dict[str, Any]], tid: int):
        if not batch or not metadata:
            return None
        if len(batch) != len(metadata):
            logger.warning(
                f"[collect] 批次数据数量不一致: texts={len(batch)} metadata={len(metadata)} (task={tid})"
            )
            return None
        try:
            # 验证和清理batch数据
            cleaned_batch = []
            for idx, article in enumerate(batch):
                if not isinstance(article, dict):
                    logger.error(f"[collect] task {tid} batch[{idx}]不是dict: {type(article)}")
                    continue
                
                cleaned_article = {}
                for key, value in article.items():
                    key_str = str(key) if key is not None else "unknown"
                    if value is None:
                        cleaned_article[key_str] = ""
                    elif isinstance(value, str):
                        cleaned_value = ''.join(
                            c for c in value 
                            if ord(c) >= 32 or c in '\n\t\r'
                        )
                        cleaned_article[key_str] = cleaned_value
                    else:
                        cleaned_article[key_str] = str(value)
                
                cleaned_batch.append(cleaned_article)
            
            if not cleaned_batch:
                logger.error(f"[collect] task {tid} 清理后batch为空，跳过")
                return None
            
            # 序列化为JSON并验证
            try:
                payload = json.dumps(cleaned_batch, ensure_ascii=False)
                json.loads(payload)
            except (TypeError, ValueError, json.JSONDecodeError) as e:
                logger.error(
                    f"[collect] task {tid} JSON序列化/验证失败: {e}\n"
                    f"  batch长度: {len(cleaned_batch)}\n"
                    f"  第一条数据: {json.dumps(cleaned_batch[0] if cleaned_batch else {}, ensure_ascii=False)[:500]}"
                )
                return None
            
            # 验证system_prompt
            system_prompt_str = str(system_prompt) if system_prompt is not None else ""
            system_prompt_str = ''.join(
                c for c in system_prompt_str 
                if ord(c) >= 32 or c in '\n\t\r'
            )
            
            # 验证messages结构
            messages = [
                {"role": "system", "content": system_prompt_str},
                {"role": "user", "content": payload},
            ]
            
            for i, msg in enumerate(messages):
                if not isinstance(msg, dict):
                    raise ValueError(f"messages[{i}]必须是dict")
                if "role" not in msg or "content" not in msg:
                    raise ValueError(f"messages[{i}]缺少role或content字段")
                if not isinstance(msg["role"], str):
                    raise ValueError(f"messages[{i}].role必须是str")
                if not isinstance(msg["content"], str):
                    raise ValueError(f"messages[{i}].content必须是str")
            
            return {
                "task_id": tid,
                "article_count": len(batch),
                "model": endpoint_id,
                "metadata": metadata,
                "messages": messages,
                "temperature": 0.0,
            }
        except Exception as exc:
            logger.error(f"[collect] 构建批次请求失败 (task={tid}): {exc}", exc_info=True)
            return None

    logger.info(f"[collect] 开始流式处理 JSONL 文件（endpoint={endpoint_id}）...")
    
    for jsonl_path in jsonl_files_iter:
        file_count += 1
        try:
            # 添加文件读取前的调试信息
            if file_count == 1:
                logger.info(f"[collect] 开始读取第一个文件: {jsonl_path}")
                logger.info(f"[collect] 文件是否存在: {jsonl_path.exists()}")
                if jsonl_path.exists():
                    file_size = jsonl_path.stat().st_size
                    logger.info(f"[collect] 文件大小: {file_size} 字节")
            
            with jsonl_path.open("r", encoding="utf-8", errors="ignore") as fh:
                line_count = 0
                articles_in_file = 0
                file_skipped_start = skipped_articles
                file_failed_start = failed_articles
                
                for line_number, line in enumerate(fh, start=1):
                    # 检查是否已处理
                    if processed_index is not None:
                        if _is_article_processed_fast(jsonl_path, line_number, processed_index):
                            skipped_articles += 1
                            if skipped_articles % 10000 == 0:
                                logger.debug(f"[collect] 已跳过 {skipped_articles} 篇已处理的文章")
                            continue
                    
                    # 应用总文章数限制
                    if MAX_ARTICLES > 0 and processed_articles >= MAX_ARTICLES:
                        logger.info(f"[collect] 达到总文章数限制 {MAX_ARTICLES}，停止处理")
                        return
                    
                    # 应用每文件文章数限制
                    if MAX_ARTICLES_PER_FILE > 0 and articles_in_file >= MAX_ARTICLES_PER_FILE:
                        break
                    
                    article = _load_article_from_jsonl_line(line, jsonl_path, line_number)
                    if not article:
                        failed_articles += 1
                        # 只在前面几行或失败率较高时输出详细信息
                        if line_number <= 5 or (failed_articles - file_failed_start) <= 3:
                            logger.debug(f"[collect] 跳过无效文章 {jsonl_path}:{line_number} (累计失败: {failed_articles})")
                        continue
                    
                    current_batch.append(article)
                    current_metadata.append({
                        "jsonl_path": str(jsonl_path),
                        "line_number": line_number,
                        "file_name": jsonl_path.name,
                        "release_date": article.get("release_date"),
                        # ✅ 优化：在 metadata 中直接包含 title 和 content，避免后续重复读文件
                        "title": article.get("title", ""),
                        "content": article.get("content", ""),
                    })
                    processed_articles += 1
                    articles_in_file += 1
                    line_count += 1
                    
                    # 当批次满了，立即yield出去
                    if len(current_batch) >= BATCH_SIZE:
                        request = _flush_batch(current_batch, current_metadata, task_id)
                        if request:
                            yield request, task_id
                        current_batch = []
                        current_metadata = []
                        task_id += 1
                    
                    # 定期输出进度日志
                    current_time = time.time()
                    if current_time - last_log_time >= log_interval:
                        skip_info = f" | 跳过: {skipped_articles}" if skipped_articles > 0 else ""
                        logger.info(
                            f"[collect] 进度: 文件 {file_count} | "
                            f"扫描行数: {line_number} | "
                            f"实际处理: {line_count} | "
                            f"已处理文章: {processed_articles}" + 
                            (f"/{MAX_ARTICLES}" if MAX_ARTICLES > 0 else "") +
                            f" | 失败: {failed_articles}{skip_info} | 已生成任务: {task_id - start_task_id}"
                        )
                        last_log_time = current_time
                
                # 如果达到总文章数限制，返回
                if MAX_ARTICLES > 0 and processed_articles >= MAX_ARTICLES:
                    break
                
                file_skipped = skipped_articles - file_skipped_start
                file_failed = failed_articles - file_failed_start
                skip_info_file = f"，跳过 {file_skipped} 篇" if file_skipped > 0 else ""
                fail_info_file = f"，失败 {file_failed} 篇" if file_failed > 0 else ""
                logger.info(
                    f"[collect] 完成文件 {jsonl_path.name}: "
                    f"扫描 {line_number} 行 | "
                    f"实际处理 {articles_in_file} 篇文章（{line_count} 行）{skip_info_file}{fail_info_file}"
                )
                
                # 如果文件处理失败率很高，输出警告
                if line_number > 0 and file_failed > line_number * 0.5:
                    logger.warning(
                        f"[collect] 警告：文件 {jsonl_path.name} 失败率较高 "
                        f"({file_failed}/{line_number} = {file_failed/line_number:.1%})，"
                        f"请检查文件格式是否正确"
                    )
                
        except Exception as exc:
            failed_articles += 1
            logger.error(f"[collect] 读取文件失败 {jsonl_path}: {exc}", exc_info=True)
            continue

    # 处理最后一个不完整的批次
    if current_batch:
        request = _flush_batch(current_batch, current_metadata, task_id)
        if request:
            yield request, task_id
        task_id += 1

    skip_info = f" | 跳过文章: {skipped_articles}" if skipped_articles > 0 else ""
    logger.info(
        f"[collect] 完成流式处理 | "
        f"成功文件: {file_count} | 成功文章: {processed_articles} | 失败文章: {failed_articles}{skip_info} | 生成任务: {task_id - start_task_id}"
    )


def _collect_requests(
    endpoint_id: str, 
    system_prompt: str, 
    jsonl_files: List[Path], 
    start_task_id: int = 1,
    processed_index: Optional[Dict[str, set]] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """
    从 JSONL 文件中收集请求
    
    Args:
        endpoint_id: 批量推理点 ID
        system_prompt: 系统提示词
        jsonl_files: JSONL 文件列表
        start_task_id: 起始任务 ID
        processed_index: 已处理文章的索引（文件stem -> 行号集合），如果为None则不跳过
    
    Returns:
        (requests, next_task_id): 请求列表和下一个可用的 task_id
    """
    requests: List[Dict[str, Any]] = []

    if not jsonl_files:
        return requests, start_task_id

    current_batch: List[Dict[str, str]] = []
    current_metadata: List[Dict[str, Any]] = []  # 存储文件路径和行号信息
    task_id = start_task_id
    total_files = len(jsonl_files)
    processed_articles = 0
    failed_articles = 0
    skipped_articles = 0  # 跳过的已处理文章数
    processed_files = 0
    last_log_time = time.time()
    log_interval = 5.0  # 每5秒输出一次进度

    def _flush_batch(batch: List[Dict[str, str]], metadata: List[Dict[str, Any]], tid: int) -> None:
        if not batch or not metadata:
            return
        if len(batch) != len(metadata):
            logger.warning(
                f"[collect] 批次数据数量不一致: texts={len(batch)} metadata={len(metadata)} (task={tid})"
            )
            return
        try:
            # ===== 验证和清理batch数据 =====
            cleaned_batch = []
            for idx, article in enumerate(batch):
                if not isinstance(article, dict):
                    logger.error(f"[collect] task {tid} batch[{idx}]不是dict: {type(article)}")
                    continue
                
                cleaned_article = {}
                for key, value in article.items():
                    # 确保key是字符串
                    key_str = str(key) if key is not None else "unknown"
                    
                    # 处理value
                    if value is None:
                        cleaned_article[key_str] = ""  # None值替换为空字符串
                    elif isinstance(value, str):
                        # 移除控制字符（保留换行符、制表符、回车符）
                        cleaned_value = ''.join(
                            c for c in value 
                            if ord(c) >= 32 or c in '\n\t\r'
                        )
                        cleaned_article[key_str] = cleaned_value
                    else:
                        # 其他类型转为字符串
                        cleaned_article[key_str] = str(value)
                
                cleaned_batch.append(cleaned_article)
            
            if not cleaned_batch:
                logger.error(f"[collect] task {tid} 清理后batch为空，跳过")
                return
            
            # 序列化为JSON并验证
            try:
                payload = json.dumps(cleaned_batch, ensure_ascii=False)
                # 验证JSON可以反序列化
                json.loads(payload)
            except (TypeError, ValueError, json.JSONDecodeError) as e:
                logger.error(
                    f"[collect] task {tid} JSON序列化/验证失败: {e}\n"
                    f"  batch长度: {len(cleaned_batch)}\n"
                    f"  第一条数据: {json.dumps(cleaned_batch[0] if cleaned_batch else {}, ensure_ascii=False)[:500]}"
                )
                return
            
            # 验证system_prompt
            system_prompt_str = str(system_prompt) if system_prompt is not None else ""
            # 清理system_prompt中的控制字符
            system_prompt_str = ''.join(
                c for c in system_prompt_str 
                if ord(c) >= 32 or c in '\n\t\r'
            )
            
            # 验证messages结构
            messages = [
                {"role": "system", "content": system_prompt_str},
                {"role": "user", "content": payload},
            ]
            
            # 最终验证：确保messages结构正确
            for i, msg in enumerate(messages):
                if not isinstance(msg, dict):
                    raise ValueError(f"messages[{i}]必须是dict")
                if "role" not in msg or "content" not in msg:
                    raise ValueError(f"messages[{i}]缺少role或content字段")
                if not isinstance(msg["role"], str):
                    raise ValueError(f"messages[{i}].role必须是str")
                if not isinstance(msg["content"], str):
                    raise ValueError(f"messages[{i}].content必须是str")
            
            requests.append(
                {
                    "task_id": tid,
                    "article_count": len(batch),
                    "model": endpoint_id,
                    "metadata": metadata,
                    "messages": messages,
                    "temperature": 0.0,
                }
            )
        except Exception as exc:
            logger.error(f"[collect] 构建批次请求失败 (task={tid}): {exc}", exc_info=True)

    logger.info(f"[collect] 开始处理 {total_files} 个 JSONL 文件（endpoint={endpoint_id}）...")
    
    for file_idx, jsonl_path in enumerate(jsonl_files, start=1):
        try:
            # 添加文件读取前的调试信息
            if file_idx == 1:
                logger.info(f"[collect] 开始读取第一个文件: {jsonl_path}")
                logger.info(f"[collect] 文件是否存在: {jsonl_path.exists()}")
                if jsonl_path.exists():
                    file_size = jsonl_path.stat().st_size
                    logger.info(f"[collect] 文件大小: {file_size} 字节")
            
            # 使用 UTF-8 编码读取 JSONL 文件
            with jsonl_path.open("r", encoding="utf-8", errors="ignore") as fh:
                line_count = 0
                articles_in_file = 0
                file_skipped_start = skipped_articles  # 记录文件开始前的累计跳过数
                file_failed_start = failed_articles  # 记录文件开始前的累计失败数
                
                for line_number, line in enumerate(fh, start=1):
                    # 检查是否已处理（如果提供了索引）
                    if processed_index is not None:
                        if _is_article_processed_fast(jsonl_path, line_number, processed_index):
                            skipped_articles += 1
                            if skipped_articles % 10000 == 0:  # 每10000条记录一次
                                logger.debug(f"[collect] 已跳过 {skipped_articles} 篇已处理的文章")
                            continue
                    
                    # 应用总文章数限制（用于测试）
                    if MAX_ARTICLES > 0 and processed_articles >= MAX_ARTICLES:
                        logger.info(
                            f"[collect] 达到总文章数限制 {MAX_ARTICLES}，停止处理"
                        )
                        break
                    
                    # 应用每文件文章数限制
                    if MAX_ARTICLES_PER_FILE > 0 and articles_in_file >= MAX_ARTICLES_PER_FILE:
                        break
                    
                    article = _load_article_from_jsonl_line(line, jsonl_path, line_number)
                    if not article:
                        failed_articles += 1
                        continue
                    
                    current_batch.append(article)
                    current_metadata.append({
                        "jsonl_path": str(jsonl_path),
                        "line_number": line_number,
                        "file_name": jsonl_path.name,
                        "release_date": article.get("release_date"),  # 添加发布日期到 metadata
                        # ✅ 优化：在 metadata 中直接包含 title 和 content，避免后续重复读文件
                        "title": article.get("title", ""),
                        "content": article.get("content", ""),
                    })
                    processed_articles += 1
                    articles_in_file += 1
                    line_count += 1
                    
                    if len(current_batch) >= BATCH_SIZE:
                        _flush_batch(current_batch, current_metadata, task_id)
                        current_batch = []
                        current_metadata = []
                        task_id += 1
                    
                    # 定期输出进度日志
                    current_time = time.time()
                    if current_time - last_log_time >= log_interval:
                        skip_info = f" | 跳过: {skipped_articles}" if skipped_articles > 0 else ""
                        logger.info(
                            f"[collect] 进度: 文件 {file_idx}/{total_files} | "
                            f"扫描行数: {line_number} | "
                            f"实际处理: {line_count} | "
                            f"已处理文章: {processed_articles}" + 
                            (f"/{MAX_ARTICLES}" if MAX_ARTICLES > 0 else "") +
                            f" | 失败: {failed_articles}{skip_info} | 已生成批次: {len(requests)}"
                        )
                        last_log_time = current_time
                
                # 如果达到总文章数限制，跳出外层循环
                if MAX_ARTICLES > 0 and processed_articles >= MAX_ARTICLES:
                    break
                
                processed_files += 1
                # 计算该文件跳过的文章数（当前累计跳过 - 文件开始前的累计跳过）
                file_skipped = skipped_articles - file_skipped_start
                file_failed = failed_articles - file_failed_start
                skip_info_file = f"，跳过 {file_skipped} 篇" if file_skipped > 0 else ""
                fail_info_file = f"，失败 {file_failed} 篇" if file_failed > 0 else ""
                logger.info(
                    f"[collect] 完成文件 {jsonl_path.name}: "
                    f"扫描 {line_number} 行 | "
                    f"实际处理 {articles_in_file} 篇文章（{line_count} 行）{skip_info_file}{fail_info_file}"
                )
                
                # 如果文件处理失败率很高，输出警告
                if line_number > 0 and file_failed > line_number * 0.5:
                    logger.warning(
                        f"[collect] 警告：文件 {jsonl_path.name} 失败率较高 "
                        f"({file_failed}/{line_number} = {file_failed/line_number:.1%})，"
                        f"请检查文件格式是否正确"
                    )
                
        except Exception as exc:
            failed_articles += 1
            logger.error(f"[collect] 读取文件失败 {jsonl_path}: {exc}", exc_info=True)
            continue

    # 处理最后一个不完整的批次
    if current_batch:
        _flush_batch(current_batch, current_metadata, task_id)
        task_id += 1

    skip_info = f" | 跳过文章: {skipped_articles}" if skipped_articles > 0 else ""
    fail_info = f" | 失败文章: {failed_articles}" if failed_articles > 0 else ""
    
    logger.info(
        f"[collect] 完成处理 {total_files} 个文件 | "
        f"成功文件: {processed_files} | 成功文章: {processed_articles}{fail_info}{skip_info} | 生成批次: {len(requests)}"
    )
    
    # 如果失败率很高，输出警告
    total_attempted = processed_articles + failed_articles + skipped_articles
    if total_attempted > 0 and failed_articles > total_attempted * 0.3:
        logger.warning(
            f"[collect] 警告：文章处理失败率较高 "
            f"({failed_articles}/{total_attempted} = {failed_articles/total_attempted:.1%})，"
            f"请检查 JSONL 文件格式是否正确（字段名应为 title/content 或类似名称）"
        )
    
    # 如果没有生成任何批次，输出详细信息
    if len(requests) == 0:
        logger.warning(
            f"[collect] 未生成任何批次请求！\n"
            f"  总文件数: {total_files}\n"
            f"  成功处理文章: {processed_articles}\n"
            f"  失败文章: {failed_articles}\n"
            f"  跳过文章: {skipped_articles}\n"
            f"  批次大小: {BATCH_SIZE}\n"
            f"  可能原因：\n"
            f"    1. 所有文章都已处理过（跳过）\n"
            f"    2. 文件格式不匹配（字段名错误）\n"
            f"    3. 文件为空或损坏"
        )

    return requests, task_id


def _normalize_label(value: str) -> str:
    if not value:
        return "AIrisk_Irrelevant"
    v = str(value).strip()
    if v in {"AIrisk_relevant_event", "AIGCrisk_relevant_event"}:
        return "AIrisk_relevant_event"
    if v in {"AIrisk_relevant_discussion", "AIGCrisk_relevant_discussion"}:
        return "AIrisk_relevant_discussion"
    if v in {"AIrisk_Irrelevant", "AIGCrisk_Irrelevant"}:
        return "AIrisk_Irrelevant"

    v_low = v.lower()
    if v_low in {"aigcrisk_relevant_event", "event"}:
        return "AIrisk_relevant_event"
    if v_low in {"aigcrisk_relevant_discussion", "discussion"}:
        return "AIrisk_relevant_discussion"
    if v_low in {"aigcrisk_relevant", "ai_risk_relevant", "relevant", "true", "1", "yes", "y"}:
        return "AIrisk_relevant_event"
    if v_low in {"aigcrisk_irrelevant", "ai_risk_irrelevant", "irrelevant", "false", "0", "no", "n"}:
        return "AIrisk_Irrelevant"
    if "event" in v_low and "relevant" in v_low:
        return "AIrisk_relevant_event"
    if "discussion" in v_low and "relevant" in v_low:
        return "AIrisk_relevant_discussion"
    if "relevant" in v_low and "ir" not in v_low:
        return "AIrisk_relevant_event"
    return "AIrisk_Irrelevant"


def _extract_year_month_from_release_date(release_date: str) -> Tuple[str, str]:
    """从发布日期中提取年份和月份"""
    try:
        match = re.match(r"(\d{4})-(\d{2})", release_date)
        if match:
            year_str = match.group(1)
            month_str = match.group(2)
            year_int = int(year_str)
            month_int = int(month_str)
            if 1900 <= year_int <= 2100 and 1 <= month_int <= 12:
                return year_str, month_str
    except Exception:
        pass
    
    now = datetime.now()
    return now.strftime("%Y"), now.strftime("%m")


def _load_article_content_from_jsonl(jsonl_path: Path, line_number: int) -> Optional[str]:
    """
    从 JSONL 文件中读取指定行的文章原文内容
    
    注意：使用 UTF-8 编码读取 JSONL 文件
    """
    try:
        with jsonl_path.open("r", encoding="utf-8", errors="ignore") as fh:
            for idx, line in enumerate(fh, start=1):
                if idx == line_number:
                    article_data = json.loads(line.strip())
                    if isinstance(article_data, dict):
                        content = article_data.get("content", "").strip()
                        if not content:
                            content = article_data.get("abstract", "").strip()
                        return content
                    break
        return None
    except Exception as exc:
        logger.debug(f"[save] 读取原文内容失败 {jsonl_path}:{line_number}: {exc}")
        return None


async def _save_structured_results(
    metadata_list: List[Dict[str, Any]],
    structured_results: List[Dict[str, Any]],
    task_id: int,
    worker_label: str,
) -> None:
    """
    保存完整的结构化结果到 JSON 文件，按照
    classification_result / 年份 / 月份 组织目录。
    """
    if not structured_results or not metadata_list:
        logger.warning(
            f"[Worker {worker_label}] task {task_id} 无法保存："
            f"metadata={len(metadata_list) if metadata_list else 0}, "
            f"structured_results={len(structured_results) if structured_results else 0}"
        )
        return

    # 验证长度匹配
    if len(metadata_list) != len(structured_results):
        logger.warning(
            f"[Worker {worker_label}] task {task_id} 元数据和结果数量不匹配: "
            f"metadata={len(metadata_list)}, structured_results={len(structured_results)}"
        )
        # 取较小的长度，避免索引越界
        min_len = min(len(metadata_list), len(structured_results))
        metadata_list = metadata_list[:min_len]
        structured_results = structured_results[:min_len]

    saved_count = 0
    failed_count = 0
    skipped_count = 0

    try:
        for metadata, result in zip(metadata_list, structured_results):
            if not result:
                skipped_count += 1
                logger.debug(f"[Worker {worker_label}] task {task_id} 跳过空结果: {metadata}")
                continue

            # ---- 统一归一化分类结果 ----
            raw_cls = result.get("classification_result", "AIrisk_Irrelevant")
            classification = _normalize_label(raw_cls)
            
            # 防御性检查：验证归一化后的分类结果是否有效
            valid_classifications = {
                "AIrisk_relevant_event",
                "AIrisk_relevant_discussion",
                "AIrisk_Irrelevant",
            }
            if classification not in valid_classifications:
                logger.warning(
                    f"[Worker {worker_label}] task {task_id} 归一化后的分类结果仍无效: {classification}, "
                    f"使用默认值 AIrisk_Irrelevant (原始值: {raw_cls})"
                )
                classification = "AIrisk_Irrelevant"
            
            result["classification_result"] = classification

            # 从结果中提取发布日期（如果存在）
            release_date = result.get("release_date")
            if not release_date:
                # 尝试从 metadata 中获取
                release_date = metadata.get("release_date")
            if not release_date:
                # 如果都没有，使用当前日期作为默认值
                release_date = datetime.now().strftime("%Y-%m-%d")
            
            year, month = _extract_year_month_from_release_date(release_date)

            # 目标目录：OUTPUT_ROOT / classification / year / month
            results_dir = OUTPUT_ROOT / classification / year / month
            results_dir.mkdir(parents=True, exist_ok=True)

            # 生成结果文件名：使用文件名的hash + 行号
            jsonl_path = Path(metadata["jsonl_path"])
            file_stem = jsonl_path.stem
            line_number = metadata["line_number"]
            result_file = results_dir / f"{file_stem}_line{line_number:07d}_result.json"

            # 如果文件已存在，跳过保存（避免覆盖已处理的结果）
            if result_file.exists():
                skipped_count += 1
                logger.debug(
                    f"[Worker {worker_label}] task {task_id} 文件已存在，跳过保存: {result_file}"
                )
                continue

            # 构建最终保存的结果
            # 包含：1) LLM的结构化结果 2) 元数据（包含从 publish_time 提取的 release_date） 3) 原始文章的 title 和 content
            result_with_metadata = result.copy()
            result_with_metadata["_source_metadata"] = {
                "jsonl_path": metadata["jsonl_path"],
                "line_number": metadata["line_number"],
                "file_name": metadata["file_name"],
                "release_date": release_date,  # 从输入的 publish_time 字段提取
            }

            # ✅ 优化：优先使用 metadata 中的 title 和 content，避免重复读文件
            title = metadata.get("title", "").strip()
            content = metadata.get("content", "").strip()
            
            # 如果 metadata 中没有，回退到读取文件（向后兼容）
            if not title or not content:
                original_article_data = None
                try:
                    with jsonl_path.open("r", encoding="utf-8", errors="ignore") as fh:
                        for idx, line in enumerate(fh, start=1):
                            if idx == line_number:
                                original_article_data = json.loads(line.strip())
                                break
                except Exception as exc:
                    logger.debug(f"[save] 读取原始文章数据失败 {jsonl_path}:{line_number}: {exc}")
                
                if original_article_data and isinstance(original_article_data, dict):
                    if not title:
                        title = original_article_data.get("title", "").strip()
                        if not title:
                            title = original_article_data.get("abstract", "").strip()
                    if not content:
                        content = original_article_data.get("content", "").strip()
                        if not content:
                            content = original_article_data.get("abstract", "").strip()
            
            # 添加原始文本内容（title 和 content）
            if title:
                result_with_metadata["title"] = title
            if content:
                result_with_metadata["content"] = content

            try:
                with result_file.open("w", encoding="utf-8") as f:
                    json.dump(result_with_metadata, f, ensure_ascii=False, indent=2)
                saved_count += 1
                logger.debug(
                    f"[Worker {worker_label}] task {task_id} 保存结构化结果到: {result_file}"
                )
            except Exception as save_exc:
                failed_count += 1
                logger.error(
                    f"[Worker {worker_label}] task {task_id} 保存文件失败 {result_file}: {save_exc}",
                    exc_info=True,
                )

        logger.info(
            f"[Worker {worker_label}] task {task_id} 保存完成: "
            f"成功={saved_count}, 失败={failed_count}, 跳过={skipped_count}, 总计={len(metadata_list)}"
        )
    except Exception as exc:
        logger.error(
            f"[Worker {worker_label}] task {task_id} 保存结构化结果失败: {exc}", exc_info=True
        )


def _extract_structured_results_from_content(content: str) -> List[Dict[str, Any]]:
    """
    从LLM返回的内容中提取完整的结构化结果（符合提示词格式：包含ai_tech, ai_risk, event, classification_result）
    
    改进点：
    1. 支持字段名的大小写变体（如 "Classification Result", "classificationResult" 等）
    2. 先归一化分类结果，再验证（更宽松的验证）
    3. 自动修复字段名不一致的问题
    4. 更详细的错误日志
    """
    if not content:
        return []
    text = content.strip()
    if not text:
        return []

    # 去除 Markdown 代码块包装
    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 3:
            text = parts[1]
            if "\n" in text:
                text = "\n".join(text.split("\n")[1:])
            text = text.strip()
        else:
            text = text.strip("`").strip()

    def _find_classification_key(item: Dict[str, Any]) -> Optional[str]:
        """查找分类结果字段的键名（支持大小写变体）"""
        keys = list(item.keys())
        # 优先查找标准字段名
        if "classification_result" in keys:
            return "classification_result"
        # 查找大小写变体
        for key in keys:
            key_lower = key.lower().replace("_", "").replace(" ", "")
            if key_lower in ["classificationresult", "classificationresult", "classification"]:
                return key
        return None

    def _normalize_and_validate_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """归一化并验证单个结果项，返回规范化后的字典"""
        if not isinstance(item, dict):
            return None
        
        # 查找分类结果字段
        cls_key = _find_classification_key(item)
        if not cls_key:
            # 如果没有找到分类字段，尝试使用默认值
            logger.debug(f"[extract] 未找到分类结果字段，可用字段: {list(item.keys())}")
            # 创建一个新的字典，添加默认分类
            normalized_item = dict(item)
            normalized_item["classification_result"] = "AIrisk_Irrelevant"
            return normalized_item
        
        # 获取原始分类值
        raw_cls = item.get(cls_key)
        if raw_cls is None:
            logger.debug(f"[extract] 分类结果字段值为None，使用默认值")
            normalized_item = dict(item)
            normalized_item["classification_result"] = "AIrisk_Irrelevant"
            return normalized_item
        
        # 归一化分类结果
        normalized_cls = _normalize_label(str(raw_cls))
        
        # 验证归一化后的分类是否有效
        valid_classifications = {
            "AIrisk_relevant_event",
            "AIrisk_relevant_discussion",
            "AIrisk_Irrelevant",
        }
        if normalized_cls not in valid_classifications:
            logger.warning(
                f"[extract] 归一化后的分类结果无效: {normalized_cls} (原始值: {repr(raw_cls)}), "
                f"使用默认值 AIrisk_Irrelevant"
            )
            normalized_cls = "AIrisk_Irrelevant"
        
        # 创建规范化后的字典
        normalized_item = dict(item)
        # 如果字段名不是标准名称，统一改为标准名称
        if cls_key != "classification_result":
            normalized_item["classification_result"] = normalized_cls
            # 保留原字段（如果不同）
            if cls_key not in normalized_item or normalized_item[cls_key] != normalized_cls:
                # 可以选择删除旧字段或保留，这里保留以便调试
                pass
        else:
            normalized_item["classification_result"] = normalized_cls
        
        return normalized_item

    # 尝试直接JSON解析
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            results = []
            for item in parsed:
                normalized_item = _normalize_and_validate_item(item)
                if normalized_item:
                    results.append(normalized_item)
            if results:
                logger.debug(f"[extract] 直接JSON解析成功，提取到 {len(results)} 个结果")
                return results
            else:
                logger.warning(f"[extract] JSON解析成功但未提取到有效结果，列表长度: {len(parsed)}")
        elif isinstance(parsed, dict):
            # 如果返回的是单个字典，包装成列表
            normalized_item = _normalize_and_validate_item(parsed)
            if normalized_item:
                logger.debug(f"[extract] JSON解析成功（单个字典），包装成列表")
                return [normalized_item]
    except json.JSONDecodeError as e:
        logger.debug(f"[extract] 直接JSON解析失败: {e}")

    # 尝试从文本中提取JSON数组片段
    si, ei = text.find("["), text.rfind("]")
    if si != -1 and ei != -1 and ei > si:
        snippet = text[si : ei + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, list):
                results = []
                for item in parsed:
                    normalized_item = _normalize_and_validate_item(item)
                    if normalized_item:
                        results.append(normalized_item)
                if results:
                    logger.debug(f"[extract] 从片段提取JSON成功，提取到 {len(results)} 个结果")
                    return results
                else:
                    logger.warning(f"[extract] 片段JSON解析成功但未提取到有效结果，列表长度: {len(parsed)}")
        except json.JSONDecodeError as e:
            logger.debug(f"[extract] 从片段提取JSON失败: {e}")

    logger.warning(f"[extract] 无法从内容中提取结构化结果，内容预览: {repr(text[:200])}")
    return []


def _extract_labels_from_content(content: str) -> List[str]:
    """从内容中提取分类标签（用于向后兼容和统计）"""
    structured_results = _extract_structured_results_from_content(content)
    if structured_results:
        labels = []
        for result in structured_results:
            if isinstance(result, dict) and "classification_result" in result:
                labels.append(str(result["classification_result"]).strip())
        if labels:
            return labels
    
    if not content:
        return []
    text = content.strip()
    if not text:
        return []

    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 3:
            text = parts[1]
            if "\n" in text:
                text = "\n".join(text.split("\n")[1:])
            text = text.strip()
        else:
            text = text.strip("`").strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            collected: List[str] = []
            for item in parsed:
                if isinstance(item, dict):
                    # 优先查找分类结果字段（支持大小写变体）
                    cls_key = None
                    for key in item.keys():
                        key_lower = key.lower().replace("_", "").replace(" ", "")
                        if key_lower in ["classificationresult", "classification"]:
                            cls_key = key
                            break
                    
                    # 如果找到分类字段，使用它
                    if cls_key:
                        value = item[cls_key]
                        if isinstance(value, list):
                            collected.extend(str(x).strip() for x in value if x is not None)
                        else:
                            collected.append(str(value).strip())
                    else:
                        # 回退到其他可能的字段名
                        for key in (
                            "Classification Result",
                            "classification_result",
                            "classificationResult",
                            "label",
                            "labels",
                            "category",
                            "categories",
                            "result",
                            "results",
                        ):
                            if key in item and item[key] is not None:
                                value = item[key]
                                if isinstance(value, list):
                                    collected.extend(str(x).strip() for x in value if x is not None)
                                else:
                                    collected.append(str(value).strip())
                                break  # 找到一个就停止
                elif item is not None:
                    collected.append(str(item).strip())
            if collected:
                return collected
            return [str(x).strip() for x in parsed]
        if isinstance(parsed, dict):
            collected: List[str] = []
            # 优先查找分类结果字段
            cls_key = None
            for key in parsed.keys():
                key_lower = key.lower().replace("_", "").replace(" ", "")
                if key_lower in ["classificationresult", "classification"]:
                    cls_key = key
                    break
            
            if cls_key:
                val = parsed[cls_key]
                if isinstance(val, list):
                    collected.extend(str(x).strip() for x in val)
                elif val is not None:
                    collected.append(str(val).strip())
            else:
                # 回退到其他字段
                for key in ("label", "labels", "category", "categories", "result", "results"):
                    if key in parsed:
                        val = parsed[key]
                        if isinstance(val, list):
                            collected.extend(str(x).strip() for x in val)
                        elif val is not None:
                            collected.append(str(val).strip())
                        break
            if collected:
                return collected
    except Exception:
        pass

    si, ei = text.find("["), text.rfind("]")
    if si != -1 and ei != -1 and ei > si:
        snippet = text[si : ei + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed]
        except Exception:
            pass

    regex_matches = re.findall(
        r'"Classification Result"\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE
    )
    if regex_matches:
        return [m.strip() for m in regex_matches if m.strip()]

    return [text]


def _split_into_chunks(items: List[Any], max_chunks: int) -> List[List[Any]]:
    if max_chunks <= 1 or len(items) <= 1:
        return [items]
    chunk_size = max(1, (len(items) + max_chunks - 1) // max_chunks)
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


async def _run_subset(
    thread_id: int,
    requests_subset: List[Dict[str, Any]],
    api_key: str,
    endpoint_id: str,
    shared_stats_queue: "queue.Queue[Tuple[int, int, int, int, float, int, int, int]]",
    shared_queue: Optional["queue.Queue"] = None,
) -> None:
    """
    运行子集任务
    
    Args:
        thread_id: 线程ID
        requests_subset: 初始任务列表
        api_key: API密钥
        endpoint_id: Endpoint ID
        shared_stats_queue: 统计信息队列（现在直接存储单个任务的统计）
        shared_queue: 共享任务队列（线程安全的queue.Queue，可选），如果提供，线程会持续从该队列获取任务
    """
    logger.info(f"[Thread {thread_id}] 初始化AsyncArk客户端（endpoint={endpoint_id}），超时={CLIENT_TIMEOUT}秒")
    client = AsyncArk(api_key=api_key, timeout=CLIENT_TIMEOUT)
    queue_async: asyncio.Queue = asyncio.Queue()

    # 将初始任务放入队列
    if requests_subset:
        logger.info(f"[Thread {thread_id}] 将 {len(requests_subset)} 个初始任务放入队列...")
        for req in requests_subset:
            await queue_async.put(req)
        logger.info(f"[Thread {thread_id}] 初始任务已全部放入队列，队列大小: {queue_async.qsize()}")

    # 如果使用共享队列，需要创建一个任务来从共享队列中获取任务并放入asyncio队列
    feed_task = None
    feed_started_event = None
    if shared_queue:
        feed_started_event = asyncio.Event()
        
        async def feed_from_shared_queue():
            """从线程安全的共享队列中获取任务并放入asyncio队列"""
            task_count = 0
            timeout_count = 0
            logger.info(f"[Thread {thread_id}] feed_from_shared_queue 已启动，等待任务...")
            feed_started_event.set()  # ✅ 标记已启动
            
            while True:
                try:
                    # 使用asyncio.to_thread或run_in_executor来从线程安全的队列中获取任务
                    def get_from_queue():
                        try:
                            item = shared_queue.get(timeout=1.0)
                            return item
                        except queue.Empty:
                            return _QUEUE_TIMEOUT_SENTINEL  # 超时返回哨兵对象
                    
                    # Python 3.9+支持asyncio.to_thread
                    try:
                        request = await asyncio.to_thread(get_from_queue)
                    except AttributeError:
                        # Python < 3.9，使用loop.run_in_executor
                        loop = asyncio.get_event_loop()
                        request = await loop.run_in_executor(None, get_from_queue)
                    
                    if request is _QUEUE_TIMEOUT_SENTINEL:  # 只是超时，队列里暂时没任务
                        timeout_count += 1
                        # 每10次超时（约10秒）输出一次日志，避免日志过多
                        if timeout_count % 10 == 0:
                            logger.debug(
                                f"[Thread {thread_id}] feed_from_shared_queue 等待任务中... "
                                f"（已等待 {timeout_count} 次，已处理 {task_count} 个任务）"
                            )
                        await asyncio.sleep(0.1)
                        continue
                    
                    # 修复 Bug 1: 这里的 None 就是你在 main 里放进去的"结束信号"
                    if request is None:
                        # 等待队列中的任务被处理完，再发送结束信号
                        # 先等待一小段时间，让正在处理的任务有机会完成
                        await asyncio.sleep(0.5)
                        
                        # 检查队列中是否还有未处理的任务
                        remaining_tasks = queue_async.qsize()
                        unfinished_tasks = queue_async._unfinished_tasks
                        
                        logger.info(
                            f"[Thread {thread_id}] feed_from_shared_queue 收到结束信号，"
                            f"队列状态: 队列大小={remaining_tasks}, 未完成任务数={unfinished_tasks}, "
                            f"已处理任务数={task_count}"
                        )
                        
                        # 为每个 worker 发送一个结束标记（使用共享队列时，worker_count = PER_THREAD_WORKERS）
                        # 注意：只有在队列中没有更多任务时才发送结束信号
                        # 如果还有任务，worker 会继续处理，直到队列为空
                        for _ in range(PER_THREAD_WORKERS):
                            await queue_async.put(None)
                        shared_queue.task_done()
                        logger.info(
                            f"[Thread {thread_id}] feed_from_shared_queue 已向 {PER_THREAD_WORKERS} 个 worker 发送退出信号"
                        )
                        break
                    
                    # 正常任务
                    task_count += 1
                    await queue_async.put(request)
                    shared_queue.task_done()
                    
                    # 每100个任务输出一次日志
                    if task_count % 100 == 0:
                        logger.debug(
                            f"[Thread {thread_id}] feed_from_shared_queue 已处理 {task_count} 个任务"
                        )
                except Exception as e:
                    logger.debug(f"[Thread {thread_id}] feed_from_shared_queue 错误: {e}")
                    await asyncio.sleep(0.1)
        
        # 启动feed任务
        feed_task = asyncio.create_task(feed_from_shared_queue())
        # ✅ 修复竞态条件：等待 feed_from_shared_queue 至少启动一次循环
        await feed_started_event.wait()
        logger.debug(f"[Thread {thread_id}] feed_from_shared_queue 已确认启动")

    # 如果使用共享队列，worker数量基于PER_THREAD_WORKERS；否则基于初始任务数
    if shared_queue:
        worker_count = PER_THREAD_WORKERS
    else:
        worker_count = min(PER_THREAD_WORKERS, max(len(requests_subset) if requests_subset else 1, 1))
    logger.info(f"[Thread {thread_id}] 创建 {worker_count} 个Worker协程...")
    workers = [
        asyncio.create_task(
            _worker(
                worker_label=f"T{thread_id}-W{i+1}",
                client=client,
                queue=queue_async,
                stats_queue=shared_stats_queue,
            )
        )
        for i in range(worker_count)
    ]

    logger.info(f"[Thread {thread_id}] 等待所有任务完成...")
    
    # ✅ 关键修复：流式模式下，先等 feed_task 把 shared_queue 吃完，再 join 队列
    if feed_task is not None:
        try:
            await feed_task  # 等 shared_queue 全部转移到 queue_async，并发完所有 None
            logger.debug(f"[Thread {thread_id}] feed_from_shared_queue 已完成")
        except asyncio.CancelledError:
            logger.warning(f"[Thread {thread_id}] feed_from_shared_queue 被取消")
        except Exception as e:
            logger.error(f"[Thread {thread_id}] feed_from_shared_queue 出错: {e}", exc_info=True)
    
    # 现在 queue_async 里已经有：
    #   - 所有请求
    #   - 每个 worker 对应的一个 None 退出标记
    logger.info(f"[Thread {thread_id}] feed_task 已完成，等待队列任务完成... (队列大小: {queue_async.qsize()}, 未完成任务数: {queue_async._unfinished_tasks})")
    await queue_async.join()
    logger.info(f"[Thread {thread_id}] 队列已清空，所有任务完成 (最终队列大小: {queue_async.qsize()})")
    
    logger.info(f"[Thread {thread_id}] 等待所有Worker退出...")
    # 不用再 cancel，worker 收到 None 自然退出
    await asyncio.gather(*workers, return_exceptions=True)

    await client.close()
    logger.info(f"[Thread {thread_id}] AsyncArk客户端已关闭")

    # 统计已经实时写入 shared_stats_queue，无需汇总


def _thread_entry(
    thread_id: int,
    requests_subset: List[Dict[str, Any]],
    api_key: str,
    endpoint_id: str,
    shared_stats_queue: "queue.Queue[Tuple[int, int, int, int, float, int, int, int]]",
    shared_queue: Optional["queue.Queue"] = None,
) -> None:
    logger.info(f"[Thread {thread_id}] 启动（endpoint={endpoint_id}），初始任务数={len(requests_subset) if requests_subset else 0}")

    try:
        asyncio.run(_run_subset(thread_id, requests_subset, api_key, endpoint_id, shared_stats_queue, shared_queue))
        logger.info(f"[Thread {thread_id}] 完成。")
    except Exception as exc:
        logger.error(f"[Thread {thread_id}] 运行失败: {exc}", exc_info=True)
        # 统计已经实时写入，无需在异常时放入空列表


async def _worker(
    worker_label: str,
    client: AsyncArk,
    queue: asyncio.Queue,
    stats_queue: "queue.Queue[Tuple[int, int, int, int, float, int, int, int]]",
) -> None:
    logger.info(f"[Worker {worker_label}] started.")
    received_shutdown = False
    
    while True:
        try:
            # 如果已经收到关闭信号，检查队列是否为空
            if received_shutdown:
                if queue.empty():
                    logger.info(f"[Worker {worker_label}] 队列已空，退出。")
                    break
                # 如果队列不为空，继续处理剩余任务（非阻塞）
                try:
                    payload = queue.get_nowait()
                except asyncio.QueueEmpty:
                    logger.info(f"[Worker {worker_label}] 队列已空，退出。")
                    break
            else:
                # 正常模式：阻塞等待任务
                payload = await queue.get()
            
            # 修复 Bug 2: 显式处理结束信号 None
            if payload is None:
                queue.task_done()
                received_shutdown = True
                logger.info(f"[Worker {worker_label}] 收到结束信号，将处理完队列中剩余任务后退出（当前队列大小: {queue.qsize()}）。")
                # 不立即退出，继续处理队列中的剩余任务
                continue
        except Exception as e:
            logger.error(f"[Worker {worker_label}] 获取任务时发生异常: {e}", exc_info=True)
            # 如果已经收到关闭信号，退出
            if received_shutdown:
                break
            # 否则继续等待
            await asyncio.sleep(0.1)
            continue
        
        task_id = payload.get('task_id', 'unknown')
        logger.info(f"[Worker {worker_label}] 获取到任务 {task_id}，开始调用批量推理API...")
        try:
            # ===== 验证payload结构 =====
            # 验证model字段
            model = payload.get("model")
            if not model or not isinstance(model, str):
                raise ValueError(f"task {task_id} model字段无效: {model}")
            
            # 验证messages字段
            messages = payload.get("messages")
            if not isinstance(messages, list):
                raise ValueError(f"task {task_id} messages必须是list，但得到{type(messages)}")
            if len(messages) == 0:
                raise ValueError(f"task {task_id} messages不能为空")
            
            # 验证和清理每个message
            cleaned_messages = []
            for i, msg in enumerate(messages):
                if not isinstance(msg, dict):
                    raise ValueError(f"task {task_id} messages[{i}]必须是dict，但得到{type(msg)}")
                
                role = msg.get("role")
                content = msg.get("content")
                
                # 验证role
                if role is None:
                    raise ValueError(f"task {task_id} messages[{i}]缺少role字段")
                role = str(role).strip()
                if role not in ["system", "user", "assistant"]:
                    logger.warning(f"[Worker {worker_label}] task {task_id} messages[{i}] role值异常: {role}")
                
                # 验证和清理content
                if content is None:
                    logger.warning(f"[Worker {worker_label}] task {task_id} messages[{i}] content为None，替换为空字符串")
                    content = ""
                else:
                    content = str(content)
                    # 移除控制字符（保留换行符、制表符、回车符）
                    content = ''.join(
                        c for c in content 
                        if ord(c) >= 32 or c in '\n\t\r'
                    )
                
                cleaned_messages.append({
                    "role": role,
                    "content": content
                })
            
            # 验证temperature
            temperature = payload.get("temperature", 0.0)
            try:
                temperature = float(temperature)
            except (TypeError, ValueError):
                logger.warning(f"[Worker {worker_label}] task {task_id} temperature无效: {temperature}，使用默认值0.0")
                temperature = 0.0
            
            # 记录请求信息以便调试
            user_content = cleaned_messages[1]["content"] if len(cleaned_messages) > 1 else ""
            payload_size = len(user_content)
            logger.debug(
                f"[Worker {worker_label}] task {task_id} 请求信息: "
                f"model={model}, "
                f"article_count={payload.get('article_count', 0)}, "
                f"payload_size={payload_size} chars, "
                f"messages_count={len(cleaned_messages)}"
            )
            
            # 最终验证：尝试序列化整个请求（模拟SDK的行为）
            try:
                test_request = {
                    "model": model,
                    "messages": cleaned_messages,
                    "temperature": temperature,
                }
                json.dumps(test_request, ensure_ascii=False)
            except (TypeError, ValueError) as e:
                logger.error(
                    f"[Worker {worker_label}] task {task_id} 请求无法序列化为JSON: {e}\n"
                    f"  model: {model}\n"
                    f"  messages_count: {len(cleaned_messages)}\n"
                    f"  user_content_preview: {user_content[:200]}..."
                )
                raise
            
            # 使用全局并发限制：在调用API前获取信号量，确保异常时也能释放
            semaphore_acquired = False
            if _global_concurrency_semaphore is not None:
                # 在协程中通过asyncio.to_thread获取threading.Semaphore
                def acquire_semaphore():
                    _global_concurrency_semaphore.acquire()
                
                def release_semaphore():
                    _global_concurrency_semaphore.release()
                
                try:
                    # 获取信号量（阻塞直到有可用槽位）
                    await asyncio.to_thread(acquire_semaphore)
                    semaphore_acquired = True
                    logger.debug(f"[Worker {worker_label}] task {task_id} 获取并发槽位")
                except AttributeError:
                    # Python < 3.9，使用run_in_executor
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, acquire_semaphore)
                    semaphore_acquired = True
                    logger.debug(f"[Worker {worker_label}] task {task_id} 获取并发槽位（via executor）")
            else:
                logger.warning(f"[Worker {worker_label}] task {task_id} 全局并发限制未初始化，跳过限制")
            
            try:
                t0 = time.perf_counter()
                logger.info(
                    f"[Worker {worker_label}] task {task_id} 开始调用API "
                    f"(model={model}, messages_count={len(cleaned_messages)}, "
                    f"timeout={CLIENT_TIMEOUT}s)"
                )
                completion = await client.batch.chat.completions.create(
                    model=model,
                    messages=cleaned_messages,  # 使用清理后的messages
                    temperature=temperature,
                )
                elapsed = time.perf_counter() - t0
                logger.info(
                    f"[Worker {worker_label}] task {task_id} API调用成功返回，耗时: {elapsed:.2f}s"
                )
            finally:
                # 释放信号量（如果已获取）
                if semaphore_acquired and _global_concurrency_semaphore is not None:
                    def release_semaphore():
                        _global_concurrency_semaphore.release()
                    
                    try:
                        await asyncio.to_thread(release_semaphore)
                    except AttributeError:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, release_semaphore)
            logger.debug(f"[Worker {worker_label}] task {payload.get('task_id', 'unknown')} API调用完成，耗时: {elapsed:.2f}s")

            choices = getattr(completion, "choices", None) or []
            usage = getattr(completion, "usage", None)
            input_tokens, output_tokens = _extract_usage_tokens(usage)
            expected_count = payload.get("article_count", BATCH_SIZE)
            metadata_list = payload.get("metadata", [])

            if len(metadata_list) != expected_count:
                logger.warning(
                    f"[Worker {worker_label}] task {payload['task_id']} 元数据数量不匹配: "
                    f"expected={expected_count}, got={len(metadata_list)}"
                )

            # ---- 解析批量结果（只用第一个 choice） ----
            structured_results: List[Dict[str, Any]] = []

            if not choices:
                logger.warning(
                    f"[Worker {worker_label}] task {payload['task_id']} API返回的choices为空"
                )
            else:
                primary_choice = choices[0]
                message = getattr(primary_choice, "message", None)
                content = getattr(message, "content", "") if message else ""

                # 先尝试提取结构化结果
                structured_results = _extract_structured_results_from_content(content)
                
                # 仅在提取结构化结果失败时，才检测是否是"无法回答"
                if not structured_results:
                    # 提取失败，检测是否是"无法回答"类型的回复
                    if _is_unable_to_answer_response(content):
                        logger.warning(
                            f"[Worker {worker_label}] task {task_id} 批量请求返回'无法回答'且提取失败，"
                            f"回退到逐个请求模式。内容预览: {content[:200]}"
                        )
                        
                        # 回退到逐个请求
                        # 从payload中提取原始batch数据
                        user_message = cleaned_messages[1] if len(cleaned_messages) > 1 else None
                        system_message = cleaned_messages[0] if len(cleaned_messages) > 0 else None
                        system_prompt = system_message.get("content", "") if system_message else ""
                        
                        if user_message:
                            try:
                                # 解析user content中的batch数据
                                user_content = user_message.get("content", "")
                                batch_data = json.loads(user_content)
                                
                                if isinstance(batch_data, list) and len(batch_data) == expected_count:
                                    # 逐个发送请求
                                    logger.info(
                                        f"[Worker {worker_label}] task {task_id} 开始逐个请求，"
                                        f"共 {len(batch_data)} 篇文章"
                                    )
                                    
                                    individual_results = []
                                    total_single_input_tokens = 0
                                    total_single_output_tokens = 0
                                    
                                    for idx, article in enumerate(batch_data):
                                        result, single_input_tokens, single_output_tokens = await _process_single_article(
                                            client=client,
                                            model=model,
                                            system_prompt=system_prompt,
                                            article=article,
                                            temperature=temperature,
                                        )
                                        
                                        total_single_input_tokens += single_input_tokens
                                        total_single_output_tokens += single_output_tokens
                                        
                                        if result:
                                            individual_results.append(result)
                                        else:
                                            # 如果单个请求也失败，使用默认值
                                            logger.warning(
                                                f"[Worker {worker_label}] task {task_id} "
                                                f"文章 {idx+1}/{len(batch_data)} 单个请求失败，使用默认值"
                                            )
                                            individual_results.append({
                                                "classification_result": "AIrisk_Irrelevant",
                                                "ai_tech": None,
                                                "ai_risk": None,
                                                "event": None,
                                            })
                                    
                                    structured_results = individual_results
                                    
                                    # 更新token统计（使用逐个请求的token总和）
                                    input_tokens = total_single_input_tokens
                                    output_tokens = total_single_output_tokens
                                    
                                    logger.info(
                                        f"[Worker {worker_label}] task {task_id} 逐个请求完成，"
                                        f"成功处理 {len([r for r in structured_results if r])} 篇文章，"
                                        f"累计tokens: input={input_tokens}, output={output_tokens}"
                                    )
                                else:
                                    logger.error(
                                        f"[Worker {worker_label}] task {task_id} 无法解析batch数据，"
                                        f"batch_data类型: {type(batch_data)}, 长度: {len(batch_data) if isinstance(batch_data, list) else 'N/A'}"
                                    )
                                    structured_results = []
                            except Exception as exc:
                                logger.error(
                                    f"[Worker {worker_label}] task {task_id} 逐个请求回退失败: {exc}",
                                    exc_info=True
                                )
                                structured_results = []
                        else:
                            logger.error(
                                f"[Worker {worker_label}] task {task_id} 无法获取user message，无法回退"
                            )
                            structured_results = []

                # 长度对齐：按 expected_count 截断或填充
                if len(structured_results) != expected_count:
                    logger.warning(
                        f"[Worker {worker_label}] task {payload['task_id']}: "
                        f"expected {expected_count} structured results, got {len(structured_results)}"
                    )
                    if len(structured_results) > expected_count:
                        structured_results = structured_results[:expected_count]
                    else:
                        # 使用列表推导式确保每个元素都是独立的副本
                        default_result = {
                            "classification_result": "AIrisk_Irrelevant",
                            "ai_tech": None,
                            "ai_risk": None,
                            "event": None,
                        }
                        structured_results.extend(
                            [default_result.copy() for _ in range(expected_count - len(structured_results))]
                        )

                if len(choices) > 1:
                    logger.warning(
                        f"[Worker {worker_label}] task {payload['task_id']} 收到 {len(choices)} 个choices，"
                        f"仅使用第一个choice的结果"
                    )

            # ---- 从 structured_results 中直接统计分类 ----
            normalized_labels: List[str] = []
            for r in structured_results:
                lbl = _normalize_label(r.get("classification_result"))
                normalized_labels.append(lbl)
                # 顺手写回规范 label，避免落盘时不一致
                r["classification_result"] = lbl

            total_labels = len(normalized_labels)
            event_count = sum(1 for x in normalized_labels if x == "AIrisk_relevant_event")
            discussion_count = sum(1 for x in normalized_labels if x == "AIrisk_relevant_discussion")
            irrelevant_count = sum(1 for x in normalized_labels if x == "AIrisk_Irrelevant")

            # 计算分类占比
            if total_labels > 0:
                event_ratio = event_count / total_labels
                discussion_ratio = discussion_count / total_labels
                irrelevant_ratio = irrelevant_count / total_labels
                ratio_str = (
                    f"event={event_count}({event_ratio:.1%}), "
                    f"discussion={discussion_count}({discussion_ratio:.1%}), "
                    f"irrelevant={irrelevant_count}({irrelevant_ratio:.1%})"
                )
            else:
                ratio_str = "event=0(0%), discussion=0(0%), irrelevant=0(0%)"

            logger.info(
                f"[Worker {worker_label}] task {payload['task_id']}: "
                f"got {len(structured_results)} structured results | labels={normalized_labels} | "
                f"占比: {ratio_str} | elapsed={elapsed:.2f}s"
            )

            # ---- 保存结果 ----
            if not metadata_list:
                logger.warning(
                    f"[Worker {worker_label}] task {payload['task_id']} 缺少元数据信息，无法保存结果"
                )
            elif not structured_results:
                logger.warning(
                    f"[Worker {worker_label}] task {payload['task_id']} 没有结构化结果，无法保存"
                )
            else:
                min_len = min(len(metadata_list), len(structured_results))
                metadata_to_save = metadata_list[:min_len]
                results_to_save = structured_results[:min_len]

                if len(metadata_to_save) != len(results_to_save):
                    logger.error(
                        f"[Worker {worker_label}] task {payload['task_id']} 保存时长度仍不匹配: "
                        f"metadata={len(metadata_to_save)}, results={len(results_to_save)}"
                    )
                else:
                    await _save_structured_results(
                        metadata_to_save, results_to_save, payload["task_id"], worker_label
                    )

            # 实时写入统计到跨线程队列
            stats_tuple = (
                payload["task_id"],
                int(input_tokens or 0),
                int(output_tokens or 0),
                total_labels,  # 这里和 normalized_labels 长度一致
                elapsed,
                event_count,
                discussion_count,
                irrelevant_count,
            )
            # 使用 asyncio.to_thread 调用阻塞的 queue.Queue.put()
            try:
                await asyncio.to_thread(stats_queue.put, stats_tuple)
            except AttributeError:
                # Python < 3.9，使用 run_in_executor
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, stats_queue.put, stats_tuple)
        except ArkBadRequestError as e:
            # 专门处理BadRequest错误，记录详细信息
            messages = payload.get("messages", [])
            user_content = messages[1]["content"] if len(messages) > 1 else ""
            
            error_info = {
                "task_id": task_id,
                "error_type": "ArkBadRequestError",
                "error_message": str(e),
                "request_id": getattr(e, "request_id", None),
                "status_code": getattr(e, "status_code", None),
                "model": payload.get("model"),
                "messages_count": len(messages),
                "user_content_size": len(user_content),
                "user_content_preview": user_content[:500] if user_content else "",
            }
            
            # 尝试获取响应体
            try:
                response = getattr(e, "response", None)
                if response:
                    error_info["response_text"] = getattr(response, "text", None)
            except Exception:
                pass
            
            logger.error(
                f"[Worker {worker_label}] task {task_id} API请求解析失败:\n"
                f"{json.dumps(error_info, ensure_ascii=False, indent=2)}"
            )
            raise
        except Exception as exc:
            task_id = payload.get('task_id', 'unknown')
            logger.error(
                f"[Worker {worker_label}] task {task_id} 发生异常: {exc}",
                exc_info=True
            )
        finally:
            queue.task_done()
            logger.debug(f"[Worker {worker_label}] task {payload.get('task_id', 'unknown')} task_done() 已调用")



def main() -> None:
    global _global_concurrency_semaphore
    
    start = datetime.now()
    logger.info("=" * 60)
    logger.info(f"开始：Chinese_Data 异步{MODE_LABEL}处理")
    logger.info("=" * 60)

    # 初始化全局并发限制
    _global_concurrency_semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)
    logger.info(f"[并发控制] 全局最大并发请求数: {MAX_CONCURRENT_REQUESTS}")

    api_key = API_CONFIG.get("ARK_API_KEY", "").strip() or API_CONFIG.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "请在 config/Identification_Method-Chinese_Data-llm_filter-config.ini 的 [Chinese_Data.API] 中设置 ARK_API_KEY 或 OPENAI_API_KEY"
        )
    
    targets = _load_targets()
    k = len(targets)
    logger.info(f"启用 {k} 个{MODE_TARGET_LABEL}并行：{targets}")
    # 只显示API key的前后部分，保护敏感信息
    api_key_display = f"{api_key[:10]}...{api_key[-4:]}" if len(api_key) > 14 else "****"
    logger.info(f"使用 API Key: {api_key_display}")

    # 扫描全量数据集的 JSONL 文件
    logger.info(f"开始扫描全量数据集目录: {INPUT_ROOT}")
    
    if not INPUT_ROOT.exists():
        raise RuntimeError(f"输入目录不存在: {INPUT_ROOT}")
    
    # 修复 Bug 1: 先构建完整的文件列表，然后轮询分配到各个 endpoint
    # 这样可以确保每个文件都被分配，不会因为共用生成器而"丢文件"
    if INPUT_ROOT.is_file():
        # 如果指定的是单个文件
        if INPUT_ROOT.suffix in (".jsonl", ".json"):
            all_jsonl_files = [INPUT_ROOT]
        else:
            all_jsonl_files = []
    else:
        # 扫描所有 JSONL 文件并排序（确保顺序一致）
        all_jsonl_files = sorted(INPUT_ROOT.glob("*.jsonl"))
    
    total_files = len(all_jsonl_files)
    logger.info(f"找到 {total_files} 个 JSONL 文件")
    
    if total_files == 0:
        raise RuntimeError(f"目录 {INPUT_ROOT} 下未找到 JSONL 文件。")
    
    system_prompt = _load_system_prompt()
    
    # 构建已处理文章索引（用于跳过已处理的文章）
    logger.info("开始构建已处理文章索引...")
    processed_index = _build_processed_index(OUTPUT_ROOT)
    
    # 流式处理：边扫描文件边构建请求边启动线程
    logger.info(f"开始流式处理文件并为每个{MODE_TARGET_LABEL}构建请求...")
    
    shared_stats_queue: "queue.Queue[Tuple[int, int, int, int, float, int, int, int]]" = queue.Queue()
    executor = None
    futures = []
    interrupted = False
    
    # 计算总线程数：每个目标使用 THREAD_COUNT 个线程
    total_threads = k * THREAD_COUNT
    logger.info(f"启动线程池，共 {total_threads} 个线程（{k} 个{MODE_TARGET_LABEL} × {THREAD_COUNT} 线程/目标）")
    logger.info(f"配置：线程数={THREAD_COUNT}/目标, 每线程工作器数={PER_THREAD_WORKERS}, 批次大小={BATCH_SIZE}, 超时={CLIENT_TIMEOUT}秒")
    logger.info(f"全局并发限制：最多同时 {MAX_CONCURRENT_REQUESTS} 个API请求（跨所有线程和{MODE_TARGET_LABEL}）")
    logger.info(f"流式处理模式：线程预先启动，任务直接放入共享队列")
    
    try:
        with ThreadPoolExecutor(max_workers=total_threads) as executor:
            thread_counter = 1
            base_task_ids = [1] * k
            total_requests = 0
            file_count = 0
            
            last_task_ids = [None] * k
            
            target_shared_queues: List[queue.Queue] = [queue.Queue() for _ in range(k)]
            target_total_requests: List[int] = [0] * k
            
            target_files: List[List[Path]] = [
                [f for i, f in enumerate(all_jsonl_files) if i % k == target_idx]
                for target_idx in range(k)
            ]
            
            for target_idx in range(k):
                logger.info(
                    f"[{MODE_TARGET_LABEL} {target_idx+1} ({targets[target_idx]})] 分配到 {len(target_files[target_idx])} 个文件"
                )
            
            # ✅ 核心改动：预先启动所有线程（初始任务列表为空）
            for target_idx, target_name in enumerate(targets):
                for t in range(THREAD_COUNT):
                    futures.append(
                        executor.submit(
                            _thread_entry,
                            thread_counter,
                            [],  # 初始任务列表为空
                            api_key,
                            target_name,
                            shared_stats_queue,
                            target_shared_queues[target_idx],
                        )
                    )
                    logger.info(
                        f"[{MODE_TARGET_LABEL} {target_idx+1} ({target_name})] 预启动线程 {thread_counter}，"
                        f"初始任务为空，后续任务从共享队列获取"
                    )
                    thread_counter += 1
            
            target_request_generators = []
            for target_idx in range(k):
                target_request_generators.append(
                    _collect_requests_streaming(
                        targets[target_idx], 
                        system_prompt, 
                        iter(target_files[target_idx]),
                        base_task_ids[target_idx], 
                        processed_index
                    )
                )
            
            # 流式处理：边扫描文件边构建请求，任务直接放入共享队列
            logger.info("[collect] 开始流式扫描文件并构建请求...")
            has_jsonl_files = total_files > 0  # 修复 Bug 4: 区分"有文件"和"有新请求"
            has_new_requests = False
            target_finished = [False] * k
            
            # 使用轮询方式从各个endpoint的生成器中获取请求
            while not all(target_finished):
                any_progress = False
                
                for target_idx in range(k):
                    if target_finished[target_idx]:
                        continue
                    
                    try:
                        request, task_id = next(target_request_generators[target_idx])
                        last_task_ids[target_idx] = task_id
                        any_progress = True
                        has_new_requests = True
                        
                        if request is None:
                            continue
                        
                        target_total_requests[target_idx] += 1
                        total_requests += 1
                        
                        # ✅ 核心改动：直接放入共享队列，不再使用 buffer
                        target_shared_queues[target_idx].put(request)
                        
                        # 定期输出进度
                        if total_requests % 100 == 0:
                            logger.info(
                                f"[collect] 进度: 总请求数={total_requests} | "
                                f"各{MODE_TARGET_LABEL}请求数={target_total_requests}"
                            )
                        
                    except StopIteration:
                        target_finished[target_idx] = True
                        logger.info(
                            f"[{MODE_TARGET_LABEL} {target_idx+1} ({targets[target_idx]})] 文件扫描完成，"
                            f"共生成 {target_total_requests[target_idx]} 个请求"
                        )
                        
                        target_shared_queues[target_idx].put(None)
                        
                        if last_task_ids[target_idx] is not None:
                            base_task_ids[target_idx] = last_task_ids[target_idx] + 1
                        else:
                            logger.info(
                                f"[{MODE_TARGET_LABEL} {target_idx+1} ({targets[target_idx]})] 未产生请求（可能全部已处理），"
                                f"跳过 base_task_ids 更新"
                            )
                
                # 如果没有任何进展，短暂休眠避免CPU空转
                if not any_progress:
                    time.sleep(0.01)
            
            # 修复 Bug 4: 区分"有文件"和"有新请求"
            if not has_jsonl_files:
                raise RuntimeError(f"目录 {INPUT_ROOT} 下未找到 JSONL 文件。")
            elif not has_new_requests:
                logger.info("所有文章都已处理完，本轮未生成新任务。")
            
            logger.info(f"[collect] 文件扫描完成，共生成 {total_requests} 个请求")
            logger.info(f"[collect] 各{MODE_TARGET_LABEL}请求数统计: {target_total_requests}")
            
            if total_requests == 0:
                if has_new_requests:
                    # 理论上不会发生，有请求就不该是 0
                    raise RuntimeError("内部状态异常：has_new_requests=True 但 total_requests=0")
                else:
                    logger.info("已无新的文章需要处理，本次运行不生成任何批次任务。")
                    logger.info("程序正常退出。")
                    # 直接 return，后面统计逻辑全是 0 也无所谓
                    return
            
            logger.info(f"共生成 {total_requests} 个批次任务（跨 {k} 个{MODE_TARGET_LABEL}）")
            logger.info(f"所有线程已启动（共 {len(futures)} 个线程），等待任务完成...")

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"[Main] 线程执行异常: {exc}", exc_info=True)
    except KeyboardInterrupt:
        interrupted = True
        logger.warning("[Main] 收到中断信号 (Ctrl+C)，正在优雅关闭...")
        if executor:
            logger.info("[Main] 正在取消未完成的任务...")
            for future in futures:
                future.cancel()
        logger.info("[Main] 程序已中断，将保存已完成的统计信息")

    duration = datetime.now() - start

    total_input_tokens = 0
    total_output_tokens = 0
    total_results = 0
    stats: List[Tuple[int, int, int, int, float, int, int, int]] = []
    # ✅ 修复：使用超时机制读取统计信息，避免多线程环境下的竞态条件
    # 现在 shared_stats_queue 里存的是单个任务的统计，不是列表
    logger.info("[Main] 开始收集统计信息...")
    timeout_count = 0
    max_timeout = 10  # 最多等待10次（每次1秒），确保所有统计信息都被读取
    while timeout_count < max_timeout:
        try:
            # 使用超时机制，避免无限等待
            stat_item = shared_stats_queue.get(timeout=1.0)
            stats.append(stat_item)
            timeout_count = 0  # 重置超时计数
        except queue.Empty:
            timeout_count += 1
            # 如果队列为空且所有线程都完成了，可以退出
            if timeout_count >= 2:  # 连续2次超时，说明可能没有更多统计信息了
                # 但为了安全，继续等待几次
                pass
    
    logger.info(f"[Main] 统计信息收集完成，共收集到 {len(stats)} 条统计记录")

    total_elapsed = 0.0
    total_event = 0
    total_discussion = 0
    total_irrelevant = 0
    for (
        _,
        input_toks,
        output_toks,
        result_count,
        elapsed,
        event_count,
        discussion_count,
        irrelevant_count,
    ) in stats:
        total_input_tokens += int(input_toks or 0)
        total_output_tokens += int(output_toks or 0)
        total_results += int(result_count or 0)
        total_elapsed += float(elapsed or 0.0)
        total_event += int(event_count or 0)
        total_discussion += int(discussion_count or 0)
        total_irrelevant += int(irrelevant_count or 0)

    avg_elapsed = (total_elapsed / total_results) if total_results else 0.0

    relevant_total = total_event + total_discussion
    event_ratio = (total_event / total_results) if total_results else 0.0
    discussion_ratio = (total_discussion / total_results) if total_results else 0.0
    irrelevant_ratio = (total_irrelevant / total_results) if total_results else 0.0
    relevant_ratio = (relevant_total / total_results) if total_results else 0.0

    log_lines = [
        f"运行时间戳: {RUN_TS}",
        f"状态: {'已中断' if interrupted else '正常完成'}",
        f"总耗时: {duration}",
        f"总任务数: {total_requests}",
        f"总结果数: {total_results}",
        f"输入 tokens 总数: {total_input_tokens}",
        f"输出 tokens 总数: {total_output_tokens}",
        f"平均返回耗时: {avg_elapsed:.4f} 秒",
        f"AIrisk_relevant_event: {total_event}",
        f"AIrisk_relevant_discussion: {total_discussion}",
        f"AIrisk_Irrelevant: {total_irrelevant}",
        f"占比 -> event: {event_ratio:.4%}, discussion: {discussion_ratio:.4%}, "
        f"irrelevant: {irrelevant_ratio:.4%}, relevant(sum): {relevant_ratio:.4%}",
        f"运行模式: {RUN_MODE}",
        f"使用的{MODE_TARGET_LABEL}: {targets}",
    ]

    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines))
        fh.write("\n\n")

    logger.info(f"Total time: {duration}, total tasks: {total_requests}")
    logger.info(f"Input tokens: {total_input_tokens}, Output tokens: {total_output_tokens}")
    logger.info(f"Average latency per task: {avg_elapsed:.4f}s over {total_results} results")
    logger.info(
        f"Category distribution -> event: {total_event}, discussion: {total_discussion}, "
        f"irrelevant: {total_irrelevant}"
    )
    logger.info(
        f"Category ratio -> event: {event_ratio:.4%}, discussion: {discussion_ratio:.4%}, "
        f"irrelevant: {irrelevant_ratio:.4%}, relevant(sum): {relevant_ratio:.4%}"
    )
    logger.info(f"运行模式: {RUN_MODE}")
    logger.info(f"使用的{MODE_TARGET_LABEL}: {targets}")
    logger.info(f"Relevant files persisted under: {OUTPUT_ROOT}")
    logger.info(f"Detail log saved to: {LOG_PATH}")
    logger.info("=" * 60)
    logger.info("完成。")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
