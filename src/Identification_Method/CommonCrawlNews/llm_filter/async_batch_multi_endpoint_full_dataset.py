# 统一的 CommonCrawlNews LLM 过滤入口。
# 通过配置文件中的 RUN_MODE=batch|online 决定调用 Ark 批量推理接口或在线推理接口。

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

from volcenginesdkarkruntime import AsyncArk

BASE_DIR = Path(__file__).resolve().parents[4]
CONFIG_PATH = os.path.join(BASE_DIR, "config/Identification_Method-CommonCrawlNews-llm_filter-config.ini")
CONFIG = configparser.ConfigParser()
if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"配置文件不存在: {CONFIG_PATH}")
CONFIG.read(CONFIG_PATH, encoding="utf-8")


def _resolve_path(path_value: str, fallback: Optional[Path]) -> Path:
    """将配置中的相对路径解析为仓库根目录下的绝对路径。"""
    value = (path_value or "").strip()
    if not value:
        if fallback is None:
            raise ValueError("配置项缺少必须的路径值")
        return fallback
    candidate = Path(os.path.expanduser(os.path.expandvars(value)))
    if not candidate.is_absolute():
        candidate = (BASE_DIR / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _parse_int_list(raw: str) -> List[int]:
    values: List[int] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(int(part))
        except ValueError:
            continue
    return values


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


API_CONFIG = CONFIG["CommonCrawlNews.API"]
BATCH_CONFIG = CONFIG["CommonCrawlNews.AsyncBatch"]
ONLINE_CONFIG = CONFIG["CommonCrawlNews.AsyncOnline"]

RUN_MODE = API_CONFIG.get("RUN_MODE", "batch").strip().lower() or "batch"
if RUN_MODE not in {"batch", "online"}:
    raise ValueError(
        f"CommonCrawlNews.API.RUN_MODE 仅支持 'batch' 或 'online'，当前值: {RUN_MODE}"
    )

MODE_CONFIG = BATCH_CONFIG if RUN_MODE == "batch" else ONLINE_CONFIG
MODE_LABEL = "批量推理" if RUN_MODE == "batch" else "在线推理"
MODE_TARGET_LABEL = "推理点" if RUN_MODE == "batch" else "模型"

DEFAULT_INPUT_ROOT = (BASE_DIR / "outputs/CommonCrawlNews/keyword_filter").resolve()
INPUT_ROOT = _resolve_path(MODE_CONFIG.get("INPUT_ROOT", str(DEFAULT_INPUT_ROOT)), DEFAULT_INPUT_ROOT)
OUTPUT_ROOT = _resolve_path(
    MODE_CONFIG.get(
        "OUTPUT_DIR",
        "outputs/CommonCrawlNews/llm_filter/batch" if RUN_MODE == "batch"
        else "outputs/CommonCrawlNews/llm_filter/online",
    ),
    (BASE_DIR / (
        "outputs/CommonCrawlNews/llm_filter/batch" if RUN_MODE == "batch"
        else "outputs/CommonCrawlNews/llm_filter/online"
    )).resolve(),
)
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

MAX_FILES = _get_int(MODE_CONFIG, "MAX_FILES", 0)
TARGET_YEARS = _parse_int_list(MODE_CONFIG.get("TARGET_YEARS", ""))
TARGET_MONTHS = _parse_int_list(MODE_CONFIG.get("TARGET_MONTHS", ""))
THREAD_COUNT = _get_int(MODE_CONFIG, "THREAD_COUNT", 1)
PER_THREAD_WORKERS = _get_int(MODE_CONFIG, "PER_THREAD_WORKERS", 100 if RUN_MODE == "batch" else 25)
BATCH_SIZE = _get_int(MODE_CONFIG, "BATCH_SIZE", 5)
CLIENT_TIMEOUT = _get_int(MODE_CONFIG, "CLIENT_TIMEOUT_SECONDS", 3500 * 24 * 10)
PROMPT_PATH = _resolve_path(
    MODE_CONFIG.get("PROMPT_PATH", "prompt/Identification_Method-CommonCrawlNews-llm_filter-prompt.md"),
    (BASE_DIR / "prompt/Identification_Method-CommonCrawlNews-llm_filter-prompt.md").resolve(),
)
LOG_DIR = _resolve_path(
    MODE_CONFIG.get(
        "LOG_DIR",
        "logs/CommonCrawlNews/llm_filter/batch" if RUN_MODE == "batch"
        else "logs/CommonCrawlNews/llm_filter/online",
    ),
    (BASE_DIR / (
        "logs/CommonCrawlNews/llm_filter/batch" if RUN_MODE == "batch"
        else "logs/CommonCrawlNews/llm_filter/online"
    )).resolve(),
)
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
LOG_PATH = LOG_DIR / f"async_{RUN_MODE}_multi_endpoint_full_{RUN_TS}.log"

logger = logging.getLogger(f"commoncrawl_async_{RUN_MODE}")
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")

file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def _load_targets() -> List[str]:
    """根据 RUN_MODE 加载批量推理点或在线模型。"""
    if RUN_MODE == "batch":
        disable_multi = not _get_bool(API_CONFIG, "ENABLE_MULTI_ENDPOINTS", True)
        if disable_multi:
            primary = API_CONFIG.get("ARK_BATCH_MODEL_ID", "").strip()
            if not primary:
                raise RuntimeError(
                    "单推理点模式需要在配置中设置 ARK_BATCH_MODEL_ID。\n"
                    "请编辑 config/Identification_Method-CommonCrawlNews-llm_filter-config.ini"
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
                "请在配置中设置批量推理点：ARK_BATCH_MODEL_IDS 或 ARK_BATCH_MODEL_ID(+_2/_3)，"
                "或禁用多 endpoint 后仅设置 ARK_BATCH_MODEL_ID。"
            )
        return out

    disable_multi = not _get_bool(API_CONFIG, "ENABLE_MULTI_MODELS", True)
    if disable_multi:
        primary = API_CONFIG.get("ARK_ONLINE_MODEL_NAME", "").strip() or API_CONFIG.get("ARK_MODEL_NAME", "").strip()
        if not primary:
            raise RuntimeError(
                "单模型模式需要在配置中设置 ARK_ONLINE_MODEL_NAME 或 ARK_MODEL_NAME。\n"
                "请编辑 config/Identification_Method-CommonCrawlNews-llm_filter-config.ini"
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
            "请在配置中设置在线推理模型：ARK_ONLINE_MODEL_NAMES 或 ARK_ONLINE_MODEL_NAME(+_2/_3)，"
            "或禁用多模型后设置 ARK_MODEL_NAME。"
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
            usage_dict = usage.dict()  # type: ignore[attr-defined]
        except AttributeError:
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
    if not PROMPT_PATH.exists():
        raise FileNotFoundError(f"system prompt 文件不存在: {PROMPT_PATH}")
    return PROMPT_PATH.read_text(encoding="utf-8")


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


def _extract_date_from_html(html_path: Path) -> Optional[str]:
    """从HTML文件中提取发布日期（优先从JSON-LD结构化数据提取，格式更统一）"""
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


def _load_article(path: Path) -> Optional[Dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()
        if not lines:
            return None
        title = lines[0].strip()
        content = "".join(lines[1:]).strip()
        if not content:
            content = title
        
        # 提取发布时间的优先级：
        # 1. 从对应的HTML文件中提取（格式最统一，ISO 8601格式）
        # 2. 从TXT文件内容中提取（前几行）
        # 3. 从文件路径中提取（年份/月份）
        # 4. 使用当前日期作为默认值
        
        release_date = None
        
        # 方法1: 从对应的HTML文件提取（优先，因为格式更统一）
        try:
            html_path = path.parent.parent / "htmls" / path.name.replace(".txt", ".html")
            if html_path.exists():
                release_date = _extract_date_from_html(html_path)
        except Exception as exc:
            logger.debug(f"[collect] 从HTML提取日期失败 {path}: {exc}")
        
        # 方法2: 从TXT文件内容提取
        if not release_date:
            release_date = _extract_date_from_text(lines)
        
        # 方法3: 从文件路径提取年份和月份
        if not release_date:
            try:
                year_month_dir = path.parent.parent
                month_str = year_month_dir.name
                year_str = year_month_dir.parent.name
                
                if year_str and month_str:
                    year = int(year_str)
                    month = int(month_str)
                    release_date = f"{year:04d}-{month:02d}-01"
            except (ValueError, AttributeError, Exception) as exc:
                logger.debug(f"[collect] 从路径提取日期失败 {path}: {exc}")
        
        # 方法4: 使用当前日期作为默认值
        if not release_date:
            release_date = datetime.now().strftime("%Y-%m-%d")
            logger.debug(f"[collect] 使用默认发布时间: {release_date} for {path}")
        
        return {
            "title": title,
            "content": content,
            "release_date": release_date
        }
    except Exception as exc:
        logger.error(f"[collect] 读取文件失败 {path}: {exc}", exc_info=True)
        return None


def _collect_requests(
    endpoint_id: str, 
    system_prompt: str, 
    files: List[Path], 
    start_task_id: int = 1,
    processed_index: Optional[set] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """
    收集请求，支持传入文件列表（用于多 endpoint 场景）
    
    Args:
        endpoint_id: 批量推理点ID
        system_prompt: 系统提示词
        files: 文件列表
        start_task_id: 起始任务ID
        processed_index: 已处理文件的索引（文件名集合），如果为None则不跳过任何文件
    
    Returns:
        (requests, next_task_id): 请求列表和下一个可用的 task_id
    """
    requests: List[Dict[str, Any]] = []

    if not files:
        return requests, start_task_id

    if processed_index is None:
        processed_index = set()

    current_batch: List[Dict[str, str]] = []
    current_paths: List[Path] = []
    task_id = start_task_id
    total_files = len(files)
    processed_files = 0
    failed_files = 0
    skipped_files = 0
    last_log_time = time.time()
    log_interval = 5.0  # 每5秒输出一次进度

    def _flush_batch(batch: List[Dict[str, str]], paths: List[Path], tid: int) -> None:
        if not batch or not paths:
            return
        if len(batch) != len(paths):
            logger.warning(
                f"[collect] 批次文件数量不一致: texts={len(batch)} paths={len(paths)} (task={tid})"
            )
            return
        try:
            payload = json.dumps(batch, ensure_ascii=False)
            requests.append(
                {
                    "task_id": tid,
                    "article_count": len(batch),
                    "model": endpoint_id,
                    "file_paths": [str(p) for p in paths],
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": payload},
                    ],
                    "temperature": 0.0,
                }
            )
        except Exception as exc:
            logger.error(f"[collect] 构建批次请求失败 (task={tid}): {exc}", exc_info=True)

    logger.info(f"[collect] 开始处理 {total_files} 个文件（endpoint={endpoint_id}）...")
    
    for idx, path in enumerate(files, start=1):
        try:
            # 检查文件是否已处理
            if _is_file_processed_fast(path, processed_index):
                skipped_files += 1
                continue
            
            article = _load_article(path)
            if not article:
                failed_files += 1
                continue
            
            current_batch.append(article)
            current_paths.append(path)
            processed_files += 1
            
            if len(current_batch) >= BATCH_SIZE:
                _flush_batch(current_batch, current_paths, task_id)
                current_batch = []
                current_paths = []
                task_id += 1
            
            # 定期输出进度日志
            current_time = time.time()
            if current_time - last_log_time >= log_interval or idx == total_files:
                progress_pct = (idx / total_files) * 100
                skip_info = f" | 跳过: {skipped_files}" if skipped_files > 0 else ""
                logger.info(
                    f"[collect] 进度: {idx}/{total_files} ({progress_pct:.1f}%) | "
                    f"已处理: {processed_files} | 失败: {failed_files}{skip_info} | 已生成批次: {len(requests)}"
                )
                last_log_time = current_time
                
        except Exception as exc:
            failed_files += 1
            logger.error(f"[collect] 处理文件失败 {path}: {exc}", exc_info=True)
            continue

    # 处理最后一个不完整的批次
    if current_batch:
        _flush_batch(current_batch, current_paths, task_id)
        task_id += 1

    skip_info = f" | 跳过: {skipped_files}" if skipped_files > 0 else ""
    logger.info(
        f"[collect] 完成处理 {total_files} 个文件 | "
        f"成功: {processed_files} | 失败: {failed_files}{skip_info} | 生成批次: {len(requests)}"
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


def _build_processed_index(output_root: Path, target_years: Optional[List[int]] = None, target_months: Optional[List[int]] = None) -> set:
    """
    构建已处理文件的索引（文件名集合）
    这样可以快速检查，避免每次都扫描文件系统
    
    Args:
        output_root: 输出根目录
        target_years: 目标年份列表，如果为None或空列表则处理所有年份
        target_months: 目标月份列表，如果为None或空列表则处理所有月份
    
    Returns:
        set: 已处理文件的文件名集合（不包含路径，仅文件名）
    """
    processed_files: set = set()
    classifications = ["AIrisk_relevant_event", "AIrisk_relevant_discussion", "AIrisk_Irrelevant"]
    
    logger.info("[collect] 开始扫描输出目录，构建已处理文件索引...")
    if target_years:
        logger.info(f"[collect] 仅扫描以下年份的输出目录: {target_years}")
    if target_months:
        logger.info(f"[collect] 仅扫描以下月份的输出目录: {target_months}")
    
    start_time = time.time()
    total_files = 0
    
    for classification in classifications:
        classification_dir = output_root / classification
        if not classification_dir.exists():
            continue
        
        # 遍历年份目录（如果指定了目标年份，则过滤）
        for year_dir in classification_dir.glob("[0-9][0-9][0-9][0-9]"):
            # 如果指定了目标年份，则过滤
            if target_years:
                try:
                    year_num = int(year_dir.name)
                    if year_num not in target_years:
                        continue
                except ValueError:
                    continue
            
            # 遍历月份目录（如果指定了目标月份，则过滤）
            for month_dir in year_dir.glob("[0-9]*"):
                # 如果指定了目标月份，则过滤
                if target_months:
                    try:
                        month_num = int(month_dir.name)
                        if month_num not in target_months:
                            continue
                    except ValueError:
                        continue
                
                # 扫描该月份目录下的所有结果文件
                for result_file in month_dir.glob("*_result.json"):
                    # 提取原始文件名（去掉 _result.json 后缀）
                    # 文件名格式：{file_stem}_result.json
                    file_stem = result_file.stem.replace("_result", "")
                    processed_files.add(file_stem)
                    total_files += 1
    
    elapsed = time.time() - start_time
    logger.info(
        f"[collect] 索引构建完成，找到 {total_files} 个已处理的结果文件，"
        f"涉及 {len(processed_files)} 个唯一文件，耗时 {elapsed:.2f} 秒"
    )
    return processed_files


def _is_file_processed_fast(file_path: Path, processed_index: set) -> bool:
    """快速检查文件是否已处理（使用索引）"""
    file_stem = file_path.stem
    return file_stem in processed_index


def _extract_year_month_from_path(file_path: Path) -> Tuple[str, str]:
    """从文件路径中提取年份和月份"""
    try:
        year_month_dir = file_path.parent.parent
        month_str = year_month_dir.name
        year_str = year_month_dir.parent.name
        
        if year_str and month_str:
            try:
                year_int = int(year_str)
                month_int = int(month_str)
                if 1900 <= year_int <= 2100 and 1 <= month_int <= 12:
                    return year_str, month_str
            except ValueError:
                pass
    except Exception:
        pass
    
    now = datetime.now()
    return now.strftime("%Y"), now.strftime("%m")


def _load_article_content(file_path: Path) -> Optional[str]:
    """从文件路径读取文章原文内容"""
    try:
        with file_path.open("r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()
        if not lines:
            return None
        content = "".join(lines[1:]).strip()
        if not content:
            content = lines[0].strip()
        return content
    except Exception as exc:
        logger.debug(f"[save] 读取原文内容失败 {file_path}: {exc}")
        return None


async def _save_structured_results(
    file_paths: List[Path],
    structured_results: List[Dict[str, Any]],
    task_id: int,
    worker_label: str,
) -> None:
    """
    保存完整的结构化结果到 JSON 文件，按照
    classification_result / 年份 / 月份 组织目录，并补上原文 content 字段。
    """
    if not structured_results or not file_paths:
        logger.warning(
            f"[Worker {worker_label}] task {task_id} 无法保存："
            f"file_paths={len(file_paths) if file_paths else 0}, "
            f"structured_results={len(structured_results) if structured_results else 0}"
        )
        return

    # 验证长度匹配
    if len(file_paths) != len(structured_results):
        logger.warning(
            f"[Worker {worker_label}] task {task_id} 文件路径和结果数量不匹配: "
            f"file_paths={len(file_paths)}, structured_results={len(structured_results)}"
        )
        # 取较小的长度，避免索引越界
        min_len = min(len(file_paths), len(structured_results))
        file_paths = file_paths[:min_len]
        structured_results = structured_results[:min_len]

    saved_count = 0
    failed_count = 0
    skipped_count = 0

    try:
        for file_path, result in zip(file_paths, structured_results):
            if not result:
                skipped_count += 1
                logger.debug(f"[Worker {worker_label}] task {task_id} 跳过空结果: {file_path}")
                continue

            # ---- 统一归一化分类结果 ----
            raw_cls = result.get("classification_result", "AIrisk_Irrelevant")
            classification = _normalize_label(raw_cls)
            
            # 防御性检查：验证归一化后的分类结果是否有效（防止大模型输出异常）
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

            # 从路径提取年份和月份（失败则回退到当前年月）
            year, month = _extract_year_month_from_path(file_path)

            # 读取原文 content
            article_content = _load_article_content(file_path)
            if article_content:
                result_with_content = result.copy()
                result_with_content["content"] = article_content
            else:
                result_with_content = result
                logger.warning(
                    f"[Worker {worker_label}] task {task_id} 无法读取原文内容: {file_path}"
                )

            # 目标目录：OUTPUT_ROOT / classification / year / month
            results_dir = OUTPUT_ROOT / classification / year / month
            results_dir.mkdir(parents=True, exist_ok=True)

            result_file = results_dir / f"{file_path.stem}_result.json"

            # 如果文件已存在，记录一下（这里选择覆盖）
            if result_file.exists():
                logger.debug(
                    f"[Worker {worker_label}] task {task_id} 文件已存在，将覆盖: {result_file}"
                )

            try:
                with result_file.open("w", encoding="utf-8") as f:
                    json.dump(result_with_content, f, ensure_ascii=False, indent=2)
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
            f"成功={saved_count}, 失败={failed_count}, 跳过={skipped_count}, 总计={len(file_paths)}"
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
    target_name: str,
    shared_stats_queue: "queue.Queue[List[Tuple[int, int, int, int, float, int, int, int]]]",
) -> None:
    logger.info(f"[Thread {thread_id}] 初始化AsyncArk客户端（endpoint={endpoint_id}），超时={CLIENT_TIMEOUT}秒")
    client = AsyncArk(api_key=api_key, timeout=CLIENT_TIMEOUT)
    queue_async: asyncio.Queue = asyncio.Queue()
    local_stats_queue: asyncio.Queue = asyncio.Queue()

    logger.info(f"[Thread {thread_id}] 将 {len(requests_subset)} 个任务放入队列...")
    for req in requests_subset:
        await queue_async.put(req)
    logger.info(f"[Thread {thread_id}] 任务已全部放入队列，队列大小: {queue_async.qsize()}")

    worker_count = min(PER_THREAD_WORKERS, max(len(requests_subset), 1))
    logger.info(f"[Thread {thread_id}] 创建 {worker_count} 个Worker协程...")
    workers = [
        asyncio.create_task(
            _worker(
                worker_label=f"T{thread_id}-W{i+1}",
                client=client,
                queue=queue_async,
                stats_queue=local_stats_queue,
            )
        )
        for i in range(worker_count)
    ]

    logger.info(f"[Thread {thread_id}] 等待所有任务完成...")
    await queue_async.join()

    logger.info(f"[Thread {thread_id}] 所有任务完成，取消Worker协程...")
    for task in workers:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    await client.close()
    logger.info(f"[Thread {thread_id}] AsyncArk客户端已关闭")

    local_stats: List[Tuple[int, int, int, int, float, int, int, int]] = []
    while not local_stats_queue.empty():
        local_stats.append(local_stats_queue.get_nowait())

    shared_stats_queue.put(local_stats)


def _thread_entry(
    thread_id: int,
    requests_subset: List[Dict[str, Any]],
    api_key: str,
    endpoint_id: str,
    shared_stats_queue: "queue.Queue[List[Tuple[int, int, int, int, float, int, int, int]]]",
) -> None:
    if not requests_subset:
        shared_stats_queue.put([])
        return

    logger.info(f"[Thread {thread_id}] 启动（{MODE_TARGET_LABEL}={target_name}），任务数={len(requests_subset)}")

    try:
        asyncio.run(_run_subset(thread_id, requests_subset, api_key, target_name, shared_stats_queue))
        logger.info(f"[Thread {thread_id}] 完成。")
    except Exception as exc:
        logger.error(f"[Thread {thread_id}] 运行失败: {exc}", exc_info=True)
        shared_stats_queue.put([])


async def _worker(
    worker_label: str,
    client: AsyncArk,
    queue: asyncio.Queue,
    stats_queue: asyncio.Queue,
) -> None:
    logger.info(f"[Worker {worker_label}] started.")
    while True:
        payload = await queue.get()
        logger.info(f"[Worker {worker_label}] 获取到任务 {payload.get('task_id', 'unknown')}，开始调用{MODE_LABEL}API...")
        try:
            t0 = time.perf_counter()
            if RUN_MODE == "batch":
                completion = await client.batch.chat.completions.create(
                    model=payload["model"],
                    messages=payload["messages"],
                    temperature=payload["temperature"],
                )
            else:
                completion = await client.chat.completions.create(
                    model=payload["model"],
                    messages=payload["messages"],
                    temperature=payload["temperature"],
                )
            elapsed = time.perf_counter() - t0
            logger.debug(f"[Worker {worker_label}] task {payload.get('task_id', 'unknown')} API调用完成，耗时: {elapsed:.2f}s")

            choices = getattr(completion, "choices", None) or []
            usage = getattr(completion, "usage", None)
            input_tokens, output_tokens = _extract_usage_tokens(usage)
            expected_count = payload.get("article_count", BATCH_SIZE)
            file_paths_raw = payload.get("file_paths") or []
            file_paths = [Path(p) for p in file_paths_raw]

            if len(file_paths) != expected_count:
                logger.warning(
                    f"[Worker {worker_label}] task {payload['task_id']} 文件路径数量不匹配: "
                    f"expected={expected_count}, got={len(file_paths)}"
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

                structured_results = _extract_structured_results_from_content(content)

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
            if not file_paths:
                logger.warning(
                    f"[Worker {worker_label}] task {payload['task_id']} 缺少文件路径信息，无法保存结果"
                )
            elif not structured_results:
                logger.warning(
                    f"[Worker {worker_label}] task {payload['task_id']} 没有结构化结果，无法保存"
                )
            else:
                min_len = min(len(file_paths), len(structured_results))
                file_paths_to_save = file_paths[:min_len]
                results_to_save = structured_results[:min_len]

                if len(file_paths_to_save) != len(results_to_save):
                    logger.error(
                        f"[Worker {worker_label}] task {payload['task_id']} 保存时长度仍不匹配: "
                        f"file_paths={len(file_paths_to_save)}, results={len(results_to_save)}"
                    )
                else:
                    await _save_structured_results(
                        file_paths_to_save, results_to_save, payload["task_id"], worker_label
                    )

            stats_queue.put_nowait(
                (
                    payload["task_id"],
                    int(input_tokens or 0),
                    int(output_tokens or 0),
                    total_labels,  # 这里和 normalized_labels 长度一致
                    elapsed,
                    event_count,
                    discussion_count,
                    irrelevant_count,
                )
            )
        except Exception as exc:
            logger.error(f"[Worker {worker_label}] error: {exc}", exc_info=True)
        finally:
            queue.task_done()



def main() -> None:
    start = datetime.now()
    logger.info("=" * 60)
    logger.info(f"开始：CommonCrawlNews 异步{MODE_LABEL}处理")
    logger.info("=" * 60)

    # 从配置文件获取 API key（必须设置）
    api_key = API_CONFIG.get("ARK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "请在 config/Identification_Method-CommonCrawlNews-llm_filter-config.ini 中设置 CommonCrawlNews.API/ARK_API_KEY"
        )
    
    targets = _load_targets()
    k = len(targets)
    logger.info(f"启用 {k} 个{MODE_TARGET_LABEL}并行：{targets}")
    # 只显示API key的前后部分，保护敏感信息
    api_key_display = f"{api_key[:10]}...{api_key[-4:]}" if len(api_key) > 14 else "****"
    logger.info(f"使用 API Key: {api_key_display}")

    # 收集全量数据集的所有文件
    logger.info(f"开始扫描全量数据集目录: {INPUT_ROOT}")
    if TARGET_YEARS:
        logger.info(f"仅处理以下年份: {TARGET_YEARS}")
    if TARGET_MONTHS:
        logger.info(f"仅处理以下月份: {TARGET_MONTHS}")
    all_files: List[Path] = []
    
    for year_dir in sorted(INPUT_ROOT.glob("[0-9][0-9][0-9][0-9]")):
        # 如果指定了目标年份，则过滤
        if TARGET_YEARS:
            try:
                year_num = int(year_dir.name)
                if year_num not in TARGET_YEARS:
                    logger.debug(f"[collect] 跳过年份 {year_dir.name}（不在目标年份列表中）")
                    continue
            except ValueError:
                logger.debug(f"[collect] 无法解析年份目录名: {year_dir.name}")
                continue
        
        for month_dir in sorted(year_dir.glob("[0-9]*")):
            # 如果指定了目标月份，则过滤
            if TARGET_MONTHS:
                try:
                    month_num = int(month_dir.name)
                    if month_num not in TARGET_MONTHS:
                        logger.debug(f"[collect] 跳过月份 {year_dir.name}/{month_dir.name}（不在目标月份列表中）")
                        continue
                except ValueError:
                    logger.debug(f"[collect] 无法解析月份目录名: {month_dir.name}")
                    continue
            
            texts_dir = month_dir / "texts"
            if texts_dir.exists() and texts_dir.is_dir():
                txt_files = sorted(texts_dir.glob("*.txt"))
                all_files.extend(txt_files)
                logger.info(f"[collect] 找到 {len(txt_files)} 个文件在 {texts_dir}")
    
    # 应用 MAX_FILES 限制（如果设置了）
    if MAX_FILES > 0:
        original_count = len(all_files)
        all_files = all_files[:MAX_FILES]
        logger.info(f"[collect] 限制处理文件数: {MAX_FILES} (原始: {original_count})")
    
    logger.info(f"[collect] 总共找到 {len(all_files)} 个 txt 文件")
    
    if not all_files:
        raise RuntimeError(f"目录 {INPUT_ROOT} 下未找到 txt 文件。")

    # 将文件分配到多个目标（轮询方式）
    file_shards = _split_files_round_robin(all_files, k)
    logger.info(f"将文件分配到 {k} 个{MODE_TARGET_LABEL}:")
    for i, (target, shard) in enumerate(zip(targets, file_shards), start=1):
        logger.info(f"  {MODE_TARGET_LABEL} {i} ({target}): {len(shard)} 个文件")

    system_prompt = _load_system_prompt()
    
    # 构建已处理文件索引（用于跳过已处理的文件）
    # 只扫描目标年份和月份的输出目录，与输入文件扫描保持一致
    logger.info("开始构建已处理文件索引...")
    processed_index = _build_processed_index(
        OUTPUT_ROOT, 
        target_years=TARGET_YEARS if TARGET_YEARS else None,
        target_months=TARGET_MONTHS if TARGET_MONTHS else None
    )
    
    logger.info(f"开始为每个{MODE_TARGET_LABEL}构建请求...")
    per_target_requests: List[Tuple[str, List[Dict[str, Any]]]] = []
    base_task_id = 1
    
    for target_idx, (target, target_files) in enumerate(zip(targets, file_shards), start=1):
        try:
            logger.info(f"[{MODE_TARGET_LABEL} {target_idx} ({target})] 开始构建请求，文件数: {len(target_files)}")
            target_reqs, next_task_id = _collect_requests(target, system_prompt, target_files, base_task_id, processed_index)
            if target_reqs:
                per_target_requests.append((target, target_reqs))
                logger.info(f"[{MODE_TARGET_LABEL} {target_idx} ({target})] 成功生成 {len(target_reqs)} 个批次任务")
            else:
                logger.warning(f"[{MODE_TARGET_LABEL} {target_idx} ({target})] 未生成任何批次任务")
            base_task_id = next_task_id
        except Exception as exc:
            logger.error(f"[{MODE_TARGET_LABEL} {target_idx} ({target})] 构建请求失败: {exc}", exc_info=True)
            continue

    total_requests = sum(len(reqs) for _, reqs in per_target_requests)
    if total_requests == 0:
        raise RuntimeError("未能生成任何批次任务，请检查文件和数据")
    logger.info(f"共生成 {total_requests} 个批次任务（跨 {k} 个{MODE_TARGET_LABEL}）")

    # 为每个目标的请求分配线程
    shared_stats_queue: "queue.Queue[List[Tuple[int, int, int, int, float, int, int, int]]]" = queue.Queue()
    executor = None
    futures = []
    interrupted = False

    try:
        total_threads = k * THREAD_COUNT
        logger.info(f"启动线程池，共 {total_threads} 个线程（{k} 个{MODE_TARGET_LABEL} × {THREAD_COUNT} 线程/目标）")
        logger.info(f"配置：线程数={THREAD_COUNT}/目标, 每线程工作器数={PER_THREAD_WORKERS}, 批次大小={BATCH_SIZE}, 超时={CLIENT_TIMEOUT}秒")
        
        with ThreadPoolExecutor(max_workers=total_threads) as executor:
            thread_counter = 1
            for target, target_reqs in per_target_requests:
                if not target_reqs:
                    continue
                target_chunks = _split_into_chunks(target_reqs, max(THREAD_COUNT, 1))
                for chunk in target_chunks:
                    futures.append(
                        executor.submit(
                            _thread_entry,
                            thread_counter,
                            chunk,
                            api_key,
                            target,
                            shared_stats_queue,
                        )
                    )
                    thread_counter += 1
            
            logger.info(f"所有线程已提交到线程池（共 {len(futures)} 个线程）")

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
    while not shared_stats_queue.empty():
        stats.extend(shared_stats_queue.get())

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
