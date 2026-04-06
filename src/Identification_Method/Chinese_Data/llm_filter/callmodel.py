# -*- coding: utf-8 -*-
"""
callmodel.py — Doubao API caller + robust batch detector with per-item logging (三级分类支持)
- Resilient to non-JSON responses
- Per-item fallback when batch parsing fails (no "whole-batch irrelevant")
- Logs "title -> label" for every item (both batch path and fallback path)
- Supports three-level classification: AIrisk_relevant_event, AIrisk_relevant_discussion, AIrisk_Irrelevant
- Optional explicit logger injection; otherwise uses 'batch_logger'
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import re
import time
import json
import asyncio
from typing import Tuple, Optional, List, Any, Protocol
import logging
import requests
import configparser
from pathlib import Path

try:  # Optional dependency – only needed when using real OpenAI provider
    from openai import OpenAI
except Exception:  # pragma: no cover - import guard
    OpenAI = None  # type: ignore[assignment]

try:  # Optional dependency – only needed when using real Doubao/Ark provider
    from volcenginesdkarkruntime import AsyncArk
except Exception:  # pragma: no cover - import guard
    AsyncArk = None  # type: ignore[assignment]


class LLMProvider(Protocol):
    def generate(self, prompt: str, **kwargs: Any) -> str:  # pragma: no cover - protocol
        ...


@dataclass
class MockProvider:
    prefix: str = "[MOCK]"

    def generate(self, prompt: str, **_: Any) -> str:
        snippet = prompt.strip().splitlines()[0][:80]
        return f"{self.prefix} {snippet}"


class EchoProvider:
    def generate(self, prompt: str, **_: Any) -> str:
        return prompt


@dataclass
class OpenAIProvider:
    api_key: str
    model: str
    base_url: Optional[str] = None

    def __post_init__(self) -> None:
        if OpenAI is None:  # type: ignore[truthy-function]
            raise RuntimeError(
                "openai package is not installed; install it or use MockProvider instead."
            )
        kwargs: dict[str, Any] = {"api_key": self.api_key}
        if self.base_url:
            kwargs["base_url"] = self.base_url
        self._client = OpenAI(**kwargs)  # type: ignore[call-arg]

    def generate(self, prompt: str, **kwargs: Any) -> str:
        params: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
        }
        params.update({k: v for k, v in kwargs.items() if v is not None})
        completion = self._client.chat.completions.create(**params)  # type: ignore[attr-defined]
        choices = getattr(completion, "choices", None) or []
        if not choices:
            return ""
        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", "") if message is not None else ""
        return content or ""


@dataclass
class DoubaoArkProvider:
    api_key: str
    model: str
    timeout: int = 60

    def _ensure_available(self) -> None:
        if AsyncArk is None:  # type: ignore[truthy-function]
            raise RuntimeError(
                "volcenginesdkarkruntime is not installed; install it or use MockProvider instead."
            )

    async def _agen(self, prompt: str, **kwargs: Any) -> str:
        self._ensure_available()
        client = AsyncArk(api_key=self.api_key, timeout=self.timeout)  # type: ignore[call-arg]
        try:
            params: dict[str, Any] = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
            }
            params.update({k: v for k, v in kwargs.items() if v is not None})
            completion = await client.chat.completions.create(**params)
            choices = getattr(completion, "choices", None) or []
            if not choices:
                return ""
            message = getattr(choices[0], "message", None)
            content = getattr(message, "content", "") if message is not None else ""
            return content or ""
        finally:
            await client.close()

    def generate(self, prompt: str, **kwargs: Any) -> str:
        return asyncio.run(self._agen(prompt, **kwargs))


def from_environment(default_to_mock: bool = True) -> LLMProvider:
    provider_name = os.environ.get("LLM_PROVIDER", "").strip().lower()

    openai_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY_V1")
    if openai_key and provider_name in {"", "openai", "gpt"}:
        model = os.environ.get("LLM_MODEL") or os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini"
        base_url = os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_BASE_URL")
        try:
            logging.getLogger(__name__).info("Using OpenAIProvider from environment configuration.")
            return OpenAIProvider(api_key=openai_key, model=model, base_url=base_url)
        except Exception as exc:  # pragma: no cover - defensive
            logging.getLogger(__name__).warning(
                "Failed to initialise OpenAIProvider, falling back to mock: %s", exc
            )

    ark_key = os.environ.get("ARK_API_KEY")
    if ark_key and provider_name in {"", "doubao", "ark", "volcengine"}:
        model = (
            os.environ.get("LLM_MODEL")
            or os.environ.get("ARK_MODEL_NAME")
            or os.environ.get("ARK_ONLINE_MODEL_NAME")
            or "doubao-1-5-pro-32k-250115"
        )
        timeout = int(os.environ.get("LLM_TIMEOUT", os.environ.get("ARK_TIMEOUT", "60")))
        try:
            logging.getLogger(__name__).info("Using DoubaoArkProvider from environment configuration.")
            return DoubaoArkProvider(api_key=ark_key, model=model, timeout=timeout)
        except Exception as exc:  # pragma: no cover - defensive
            logging.getLogger(__name__).warning(
                "Failed to initialise DoubaoArkProvider, falling back to mock: %s", exc
            )

    if default_to_mock:
        logging.getLogger(__name__).info("Using MockProvider (no real LLM credentials found).")
        return MockProvider()
    logging.getLogger(__name__).info(
        "Using EchoProvider (no real LLM credentials found, default_to_mock=False)."
    )
    return EchoProvider()


BASE_DIR = Path(__file__).resolve().parents[4]
CONFIG_PATH = os.path.join(BASE_DIR, "config/Identification_Method-Chinese_Data-llm_filter-config.ini")
CONFIG = configparser.ConfigParser()
if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"配置文件不存在: {CONFIG_PATH}")
CONFIG.read(CONFIG_PATH, encoding="utf-8")
API_SECTION = CONFIG["Chinese_Data.API"]


def _load_api_credentials() -> Tuple[str, str]:
    """
    从配置文件读取 API 基础地址和凭据。
    如果未配置，回退到默认值以保持兼容。
    """
    base_url = API_SECTION.get("OPENAI_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3").strip()
    if not base_url:
        base_url = "https://ark.cn-beijing.volces.com/api/v3"

    api_key = API_SECTION.get("OPENAI_API_KEY", "").strip() or API_SECTION.get("ARK_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "请在 config/Identification_Method-Chinese_Data-llm_filter-config.ini 的 [Chinese_Data.API] 中配置 OPENAI_API_KEY 或 ARK_API_KEY"
        )

    return base_url, api_key


# ========= Logger helpers =========
def _get_logger(name: str = "batch_logger") -> logging.Logger:
    lg = logging.getLogger(name)
    if not lg.handlers:
        # 不强加 handler，交给主程序配置；加个 NullHandler 避免 "No handler" 警告
        lg.addHandler(logging.NullHandler())
    return lg


# ========= Small utils =========
def _looks_like_refusal(s: str) -> bool:
    """Heuristically detect refusal/unsupported replies (CN/EN)."""
    if not s:
        return True
    s_low = s.strip().lower()
    patterns = [
        r"无法回答", r"不能回答", r"很遗憾不能帮助你", r"无法提供",
        r"不便.*提供", r"不支持", r"无法处理", r"不予回答",
        r"抱歉.*无法", r"抱歉.*不能", r"很抱歉.*不能",
        r"sorry", r"i\s+can.?t", r"cannot", r"not\s+able\s+to\s+answer",
    ]
    for p in patterns:
        if re.search(p, s_low, flags=re.IGNORECASE):
            return True
    # 常见礼貌拒答
    if ("你好" in s or "您好" in s) and ("无法" in s or "不能" in s or "不便" in s):
        return True
    return False


def _safe_len_from_json_array_text(s: str) -> int:
    try:
        arr = json.loads(s)
        return len(arr) if isinstance(arr, list) else 0
    except Exception:
        return 0


VALID_CLASSIFICATIONS = {
    "AIrisk_relevant_event",
    "AIrisk_relevant_discussion",
    "AIrisk_Irrelevant",
}


def _blank_incident_elements() -> dict:
    return {
        "Time": None,
        "Subject": None,
        "Location": None,
        "Cause": None,
        "Process": None,
        "Result": None,
    }


def _normalize_optional_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.lower() == "none":
            return None
        return stripped
    return str(value)


def _normalize_label(v: Any) -> str:
    """尽量把各种写法收敛到三个标准标签之一"""
    if isinstance(v, bool):
        return "AIrisk_relevant_event" if v else "AIrisk_Irrelevant"

    t = str(v or "").strip()

    # 直接匹配标准格式（不转小写，保持原格式）
    if t in {"AIrisk_relevant_event", "AIGCrisk_relevant_event"}:
        return "AIrisk_relevant_event"
    if t in {"AIrisk_relevant_discussion", "AIGCrisk_relevant_discussion"}:
        return "AIrisk_relevant_discussion"
    if t in {"AIrisk_Irrelevant", "AIGCrisk_Irrelevant"}:
        return "AIrisk_Irrelevant"

    # 兼容旧格式和常见别名
    t_low = t.lower()
    if t_low in {"aigcrisk_relevant", "ai_risk_relevant", "relevant", "true", "1", "yes", "y"}:
        return "AIrisk_relevant_event"  # 旧格式归为事件类
    if t_low in {"aigcrisk_irrelevant", "ai_risk_irrelevant", "irrelevant", "false", "0", "no", "n"}:
        return "AIrisk_Irrelevant"

    # 模糊匹配
    if "event" in t_low and "relevant" in t_low:
        return "AIrisk_relevant_event"
    if "discussion" in t_low and "relevant" in t_low:
        return "AIrisk_relevant_discussion"
    if "relevant" in t_low and "ir" not in t_low:
        return "AIrisk_relevant_event"

    return "AIrisk_Irrelevant"


def _normalize_incident_elements(src: Any) -> dict:
    base = _blank_incident_elements()
    if not isinstance(src, dict):
        return base
    for k in base:
        base[k] = _normalize_optional_string(src.get(k))
    return base


def _ensure_iterable_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        iterable = value
    elif isinstance(value, (set, tuple)):
        iterable = list(value)
    elif isinstance(value, str):
        iterable = [value]
    else:
        iterable = [value]

    normalized = []
    for item in iterable:
        norm = _normalize_optional_string(item)
        normalized.append(norm)
    return normalized


def _to_structured_entry(entry: Any) -> dict:
    if isinstance(entry, dict):
        raw = dict(entry)
        label = raw.get("Classification Result")
        if not isinstance(label, str) or label not in VALID_CLASSIFICATIONS:
            label = _normalize_label(label)
        return {
            "AI_tech_elements": _ensure_iterable_list(raw.get("AI_tech_elements")),
            "AI_risk_elements": _ensure_iterable_list(raw.get("AI_risk_elements")),
            "Incident_elements": _normalize_incident_elements(raw.get("Incident_elements")),
            "Classification Result": label,
            "raw_output": raw,
        }

    label = _normalize_label(entry)
    return {
        "AI_tech_elements": [],
        "AI_risk_elements": [],
        "Incident_elements": _blank_incident_elements(),
        "Classification Result": label,
        "raw_output": entry,
    }


def _fit_structured(entries: List[dict], target_len: int) -> List[dict]:
    if len(entries) == target_len:
        return entries
    if len(entries) > target_len:
        return entries[:target_len]
    padding = [
        _to_structured_entry("AIrisk_Irrelevant")
        for _ in range(target_len - len(entries))
    ]
    return entries + padding


def _log_title_labels(items: list, labels: list, logger: logging.Logger, prefix: str = "[judge]") -> None:
    """将每条标题与对应标签写入日志（完整显示 content）"""
    try:
        n = min(len(items), len(labels))
        for i in range(n):
            title = (items[i] or {}).get("title", "")
            content = (items[i] or {}).get("content", "")
            title = (title or "").strip().replace("\n", " ")
            content = (content or "").strip()
            label_entry = labels[i]
            if isinstance(label_entry, dict):
                label_text = label_entry.get("Classification Result", "AIrisk_Irrelevant")
            else:
                label_text = str(label_entry)
            logger.info(
                f"{prefix} {i+1:>3}/{n}: «{title}» "
                f"-> {label_text}"
            )
    except Exception as e:
        logger.warning(f"[judge] 标题-标签日志记录失败: {e}", exc_info=True)


def _zero_usage() -> dict:
    return {
        "total_tokens": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
    }


def _merge_usage(dst: dict, src: dict) -> dict:
    if not dst:
        dst = _zero_usage()
    if not src:
        src = _zero_usage()
    return {
        "total_tokens": (dst.get("total_tokens", 0) + src.get("total_tokens", 0)),
        "prompt_tokens": (dst.get("prompt_tokens", 0) + src.get("prompt_tokens", 0)),
        "completion_tokens": (dst.get("completion_tokens", 0) + src.get("completion_tokens", 0)),
    }


# ========= Low-level single call =========

def call_model(
    system_prompt: str,
    user_prompt: str,
    model_name: str = "doubao-1-5-pro-32k-250115",
    temperature: float = 0.7,
    max_retries: int = 3,
    timeout_sec: float = 30.0,
    logger: Optional[logging.Logger] = None,
    provider: Optional["LLMProvider"] = None,
) -> Tuple[Optional[str], Optional[str], dict]:
    """
    Returns:
        (thinking_part, answer_part, usage_dict)
        thinking_part: None (not used)
        answer_part: str or None
        usage_dict: {
            "total_tokens": int,
            "prompt_tokens": int,
            "completion_tokens": int,
        }
    """
    lg = logger or _get_logger()

    # 如果提供了统一的 LLM provider，则优先通过 provider 调用，
    # 这样就可以统一切换 OpenAI / 豆包 / Mock，而无需改脚本本身。
    if provider is None and from_environment is not None:
        try:
            provider = from_environment()
            lg.info("[LLMProvider] Using provider from environment in call_model().")
        except Exception as e:
            lg.warning(f"[LLMProvider] from_environment() 失败，回退到旧版 HTTP 调用: {e}")
            provider = None

    if provider is not None:
        for attempt in range(max_retries):
            try:
                t0 = time.time()
                # provider 接口是简单的 generate(prompt=...)，系统提示和用户内容拼在一起
                full_prompt = f"{system_prompt.strip()}\n\n{user_prompt}"
                content = provider.generate(
                    full_prompt,
                    temperature=temperature,
                    timeout=timeout_sec,
                )
                elapsed_ms = int((time.time() - t0) * 1000)
                content = (content or "").strip()
                usage_dict = _zero_usage()  # 当前 provider 抽象不暴露 usage，统一返回 0
                lg.info(
                    f"[LLMProvider] ok {elapsed_ms}ms (tokens usage not available in provider abstraction)"
                )
                return None, content, usage_dict
            except Exception as err:
                lg.error(
                    f"[LLMProvider] 调用异常 (尝试 {attempt+1}/{max_retries}): "
                    f"{type(err).__name__}: {err}",
                    exc_info=True,
                )
                if attempt < max_retries - 1:
                    sleep_time = (attempt + 1) * 2
                    lg.info(f"[LLMProvider] {sleep_time}s 后重试...")
                    time.sleep(sleep_time)

        return None, None, _zero_usage()

    # ===== 回退路径：保持原有豆包 HTTP 调用（兼容旧配置） =====
    OPENAI_BASE_URL, OPENAI_API_KEY = _load_api_credentials()

    url = f"{OPENAI_BASE_URL.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        "temperature": temperature,
    }

    with requests.Session() as session:
        for attempt in range(max_retries):
            try:
                t0 = time.time()
                resp = session.post(url, headers=headers, json=payload, timeout=timeout_sec)
                elapsed_ms = int((time.time() - t0) * 1000)

                if resp.status_code == 200:
                    data = resp.json()
                    content = (
                        data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                    ) or ""
                    content = content.strip()

                    usage = data.get("usage") or {}
                    total_tokens = (
                        usage.get("total_tokens")
                        or usage.get("totalToken")
                        or usage.get("total")
                        or 0
                    )
                    prompt_tokens = (
                        usage.get("prompt_tokens")
                        or usage.get("promptToken")
                        or 0
                    )
                    completion_tokens = (
                        usage.get("completion_tokens")
                        or usage.get("completionToken")
                        or 0
                    )
                    usage_dict = {
                        "total_tokens": int(total_tokens or 0),
                        "prompt_tokens": int(prompt_tokens or 0),
                        "completion_tokens": int(completion_tokens or 0),
                    }
                    lg.info(
                        f"[豆包API] ok {elapsed_ms}ms, tokens={usage_dict['total_tokens']} "
                        f"(prompt={usage_dict['prompt_tokens']}, completion={usage_dict['completion_tokens']})"
                    )
                    return None, content, usage_dict

                lg.warning(f"[豆包API] 非200响应 code={resp.status_code} body={resp.text[:200]}...")
            except Exception as err:
                lg.error(
                    f"[豆包API] 调用异常 (尝试 {attempt+1}/{max_retries}): "
                    f"{type(err).__name__}: {err}",
                    exc_info=True,
                )

            if attempt < max_retries - 1:
                sleep_time = (attempt + 1) * 2  # 2s, 4s, ...
                lg.info(f"[豆包API] {sleep_time}s 后重试...")
                time.sleep(sleep_time)

    return None, None, _zero_usage()


# ========= Per-item fallback (async) =========

async def _fallback_ask_one_item(
    item_obj: dict,
    system_prompt: str,
    model_name: str,
    temperature: float,
    max_retries: int,
    timeout_sec: float,
    logger: logging.Logger,
) -> Tuple[str, dict]:
    """
    将单条新闻包装为长度=1的 JSON 数组，再调用 call_model。拒答/解析失败 -> 不相关
    返回: (label, usage_dict)
    """
    one_texts = json.dumps(
        [{"title": item_obj.get("title", ""), "content": item_obj.get("content", "")}],
        ensure_ascii=False
    )

    loop = asyncio.get_event_loop()

    def _invoke() -> Tuple[Optional[str], dict]:
        ans = call_model(
            system_prompt=system_prompt,
            user_prompt=one_texts,
            model_name=model_name,
            temperature=temperature,
            max_retries=max_retries,
            timeout_sec=timeout_sec,
            logger=logger,
        )
        # ans = (thinking, response_str, usage_dict)
        return ans[1], (ans[2] or _zero_usage())

    response_str, usage = await loop.run_in_executor(None, _invoke)

    if not response_str or _looks_like_refusal(response_str):
        return "AIrisk_Irrelevant", usage

    # 解析期望的单元素 JSON 数组
    try:
        si, ei = response_str.find('['), response_str.rfind(']')
        if si != -1 and ei != -1 and ei > si:
            arr = json.loads(response_str[si:ei + 1])
            if isinstance(arr, list) and arr:
                return _normalize_label(arr[0]), usage
    except Exception as e:
        logger.warning(f"[fallback-one] JSON解析失败: {e}", exc_info=True)

    return "AIrisk_Irrelevant", usage


# ========= Public batch API =========

async def detect_ai_risk_batches(
    text_batches: List[str],
    system_prompt: str,
    model_name: str = "doubao-1-5-pro-32k-250115",
    temperature: float = 0.7,
    max_retries: int = 3,
    timeout_sec: float = 30.0,
    logger: Optional[logging.Logger] = None,
) -> List[Tuple[List[dict], dict]]:
    """
    Args:
        text_batches: List[str] — 每个元素是一批次的 JSON 数组字符串（你的主程序已构造）
    Returns:
        List[(results: List[dict], usage: dict)]
          - results 等长于该批输入；元素为包含分类结果与提取要素的结构化字典
          - usage: {"total_tokens", "prompt_tokens", "completion_tokens"}
    """
    lg = logger or _get_logger()

    def _fetch_once(texts: str) -> Tuple[Optional[str], dict]:
        try:
            thinking, response_str, usage_dict = call_model(
                system_prompt=system_prompt,
                user_prompt=texts,
                model_name=model_name,
                temperature=temperature,
                max_retries=max_retries,
                timeout_sec=timeout_sec,
                logger=lg,
            )
            return response_str, (usage_dict or _zero_usage())
        except Exception as e:
            lg.error(f"[detect] 模型请求失败: {e}", exc_info=True)
            return None, _zero_usage()

    async def _handle_one_batch(texts: str) -> Tuple[List[dict], dict]:
        # 解析输入条数 & items
        try:
            items = json.loads(texts)
            n_items = len(items) if isinstance(items, list) else _safe_len_from_json_array_text(texts)
        except Exception:
            n_items = _safe_len_from_json_array_text(texts)
            items = None

        if n_items <= 0:
            lg.warning("[detect] 输入不是有效的 JSON 数组，返回空列表。")
            return [], _zero_usage()

        usage_total = _zero_usage()
        # 批量路径（最多 3 次外层循环；内部 call_model 也会重试）
        for attempt in range(3):
            try:
                t0 = time.time()
                response_str, usage = _fetch_once(texts)
                usage_total = _merge_usage(_zero_usage(), usage)
                lg.info(
                    f"[detect] 批次推理完成: {time.time() - t0:.2f}s, tokens={usage_total['total_tokens']} "
                    f"(prompt={usage_total['prompt_tokens']}, completion={usage_total['completion_tokens']})"
                )

                if not response_str:
                    lg.warning("[detect] 响应为空，将逐条兜底。")
                    break  # 进入兜底

                # —— 首选：解析为 JSON 数组 —— #
                si, ei = response_str.find('['), response_str.rfind(']')
                if si != -1 and ei != -1 and ei > si:
                    try:
                        arr = json.loads(response_str[si:ei + 1])
                        if isinstance(arr, list):
                            structured = [_to_structured_entry(x) for x in arr]
                            structured = _fit_structured(structured, n_items)
                            # 记录“标题 -> 标签”
                            if items is not None:
                                _log_title_labels(items, structured, lg)
                            return structured, usage_total
                        lg.warning(f"[detect] JSON 顶层非 list，进入逐条兜底。type={type(arr)}")
                        break
                    except Exception as pe:
                        lg.warning(f"[detect] 批量JSON解析失败: {pe}; 片段: {response_str[:120]}... 将逐条兜底。")
                        break

                # 非数组响应，不整批置为不相关，进入逐条兜底
                lg.warning(f"[detect] 非JSON响应，进入逐条兜底。片段: {response_str[:120]}...")
                break

            except Exception as e:
                lg.error(f"[detect] 批次处理异常（第 {attempt+1} 次）: {e}", exc_info=True)

            await asyncio.sleep(1)

        # —— 逐条兜底（不会“一刀切整批不相关”）—— #
        if items is None:
            try:
                items = json.loads(texts)
            except Exception:
                lg.warning("[detect] 无法还原 items；按长度返回不相关。")
                results = [_to_structured_entry("AIrisk_Irrelevant") for _ in range(n_items)]
                return results, usage_total

        fallback_results = await asyncio.gather(*[
            _fallback_ask_one_item(
                item_obj=it,
                system_prompt=system_prompt,
                model_name=model_name,
                temperature=temperature,
                max_retries=max_retries,
                timeout_sec=timeout_sec,
                logger=lg,
            )
            for it in items
        ])
        structured = []
        for label, usage in fallback_results:
            structured.append(_to_structured_entry(label))
            usage_total = _merge_usage(usage_total, usage)
        structured = _fit_structured(structured, n_items)

        # 记录“标题 -> 标签”
        _log_title_labels(items, structured, lg)

        return structured, usage_total

    tasks = [_handle_one_batch(batch) for batch in text_batches]
    return await asyncio.gather(*tasks)
