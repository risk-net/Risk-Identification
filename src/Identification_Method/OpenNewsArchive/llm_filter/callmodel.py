"""
统一的 LLM 调用封装：
- 优先使用当前渠道内置的 provider
- 若 provider 不可用，回退到配置化 HTTP 调用（Ark/OpenAI 兼容）
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import time
from typing import Optional, Tuple, Any, Protocol
from pathlib import Path

import configparser
import requests

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
        import asyncio

        return asyncio.run(self._agen(prompt, **kwargs))


def from_environment(default_to_mock: bool = True) -> LLMProvider:
    provider_name = os.environ.get("LLM_PROVIDER", "").strip().lower()

    openai_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY_V1")
    if openai_key and provider_name in {"", "openai", "gpt"}:
        model = os.environ.get("LLM_MODEL") or os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini"
        base_url = os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_BASE_URL")
        try:
            logger.info("Using OpenAIProvider from environment configuration.")
            return OpenAIProvider(api_key=openai_key, model=model, base_url=base_url)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to initialise OpenAIProvider, falling back to mock: %s", exc)

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
            logger.info("Using DoubaoArkProvider from environment configuration.")
            return DoubaoArkProvider(api_key=ark_key, model=model, timeout=timeout)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to initialise DoubaoArkProvider, falling back to mock: %s", exc)

    if default_to_mock:
        logger.info("Using MockProvider (no real LLM credentials found).")
        return MockProvider()
    logger.info("Using EchoProvider (no real LLM credentials found, default_to_mock=False).")
    return EchoProvider()

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[4]
CONFIG_PATH = BASE_DIR / "config/Identification_Method-OpenNewsArchive-llm_filter-config.ini"
config = configparser.ConfigParser()
if not config.read(CONFIG_PATH, encoding="utf-8"):
    raise FileNotFoundError(f"无法读取配置文件: {CONFIG_PATH}")

API_CONFIG = config["OpenNewsArchive.API"]


def _load_api_credentials() -> Tuple[str, str, str]:
    base_url = API_CONFIG.get("OPENAI_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3").strip() or "https://ark.cn-beijing.volces.com/api/v3"
    api_key = API_CONFIG.get("OPENAI_API_KEY", "").strip() or API_CONFIG.get("ARK_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "请在 config/Identification_Method-OpenNewsArchive-llm_filter-config.ini 的 [OpenNewsArchive.API] 中配置 OPENAI_API_KEY 或 ARK_API_KEY"
        )
    model_name = (
        API_CONFIG.get("ARK_MODEL_NAME", "").strip()
        or API_CONFIG.get("ARK_ONLINE_MODEL_NAME", "").strip()
        or "doubao-1-5-pro-32k"
    )
    return base_url, api_key, model_name


def _call_via_http(system_prompt: str, user_prompt: str, temperature: float = 0.3) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    base_url, api_key, model_name = _load_api_credentials()
    url = f"{base_url.rstrip('/')}/chat/completions"
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=payload, headers=headers, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP 调用失败 {response.status_code}: {response.text}")

    data = response.json()
    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    ) or ""

    usage = data.get("usage") or {}
    total_tokens = usage.get("total_tokens") or usage.get("totalTokens") or 0
    total_tokens = int(total_tokens) if total_tokens else 0
    return None, content.strip(), total_tokens


def call_model(system_prompt: str, user_prompt: str, provider: Optional["LLMProvider"] = None) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    if provider is None and from_environment is not None:
        try:
            provider = from_environment()
            logger.info("[LLMProvider] Using provider from environment in OpenNewsArchive.call_model().")
        except Exception as exc:
            logger.warning("[LLMProvider] 初始化失败，将使用 HTTP 回退: %s", exc)
            provider = None

    if provider is not None:
        try:
            start = time.time()
            content = provider.generate(f"{system_prompt.strip()}\n\n{user_prompt}", temperature=0.3)
            logger.info("Provider 返回耗时 %.3fs", time.time() - start)
            content = content or ""

            start_idx = content.find("<think>")
            end_idx = content.find("</think>")
            tokens: Optional[int] = None
            if 0 <= start_idx < end_idx:
                thinking = content[start_idx + len("<think>") : end_idx]
                answer = content[end_idx + len("</think>") :].strip()
                return thinking, answer, tokens
            return None, content.strip(), tokens
        except Exception as exc:
            logger.error("Provider 调用失败，将尝试 HTTP 回退: %s", exc, exc_info=True)

    try:
        return _call_via_http(system_prompt, user_prompt)
    except Exception as exc:
        logger.error("HTTP 模型调用失败: %s", exc, exc_info=True)
        return None, None, None
