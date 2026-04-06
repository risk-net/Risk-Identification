"""
使用大模型对热榜标题进行二次筛选：
- 批量读取关键词过滤后的 CSV
- 调用统一 call_model 接口（可对接 Ark/OpenAI）
- 根据模型返回的标签筛出 AI 风险相关的标题
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Iterable, List, Any, Dict

import configparser
import pandas as pd

from callmodel import call_model

BASE_DIR = Path(__file__).resolve().parents[4]
CONFIG_PATH = os.path.join(BASE_DIR, "config/Identification_Method-Hot_list_word_Dataset-llm_filter-config.ini")
config = configparser.ConfigParser()
if not config.read(CONFIG_PATH, encoding="utf-8"):
    raise FileNotFoundError(f"无法读取配置文件: {CONFIG_PATH}")

LLM_CONFIG = config["HotList.LLMFilter"]


def _resolve_path(value: str) -> Path:
    path = Path(value.strip())
    if not path.is_absolute():
        path = BASE_DIR / path
    return path.resolve()


INPUT_CSV = _resolve_path(
    LLM_CONFIG.get(
        "INPUT_CSV",
        "outputs/Hot_list_word_Dataset/keyword_filter/filtered_hotlist.csv",
    )
)
OUTPUT_CSV = _resolve_path(
    LLM_CONFIG.get(
        "OUTPUT_CSV",
        "outputs/Hot_list_word_Dataset/llm_filter/filtered_hotlist_llm.csv",
    )
)
PROMPT_PATH = _resolve_path(LLM_CONFIG.get("PROMPT_PATH", "prompt/Identification_Method-Hot_list_word_Datasets-llm_filter-prompt.md"))
BATCH_SIZE = max(1, LLM_CONFIG.getint("BATCH_SIZE", fallback=5))
LOG_FILE = _resolve_path(LLM_CONFIG.get("LOG_FILE", "logs/Hot_list_word_Dataset/llm_filter.log"))

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("hotlist_llm_filter")
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def _batch_dataframe(df: pd.DataFrame, batch_size: int) -> Iterable[pd.DataFrame]:
    for start in range(0, len(df), batch_size):
        yield df.iloc[start : start + batch_size]


def _format_payload(batch_df: pd.DataFrame) -> str:
    payload: List[Dict[str, Any]] = []
    for _, row in batch_df.iterrows():
        payload.append(
            {
                "id": row.get("id"),
                "title": row.get("title", ""),
                "url": row.get("url", ""),
                "date": row.get("日期", ""),
            }
        )
    return json.dumps(payload, ensure_ascii=False)


def _parse_label_list(answer: str) -> List[Any]:
    text = (answer or "").strip()
    if not text:
        return []
    candidates = [text]
    start, end = text.find("["), text.rfind("]")
    if 0 <= start < end:
        candidates.insert(0, text[start : end + 1])

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            continue
    logger.warning("无法解析模型输出，内容预览: %s", text[:200])
    return []


def _is_relevant_label(label: Any) -> bool:
    if isinstance(label, bool):
        return label
    if isinstance(label, (int, float)):
        return int(label) == 1
    value = str(label or "").strip().lower()
    return value in {
        "1",
        "true",
        "yes",
        "y",
        "relevant",
        "ai_risk_relevant",
        "airisk_relevant_event",
        "airisk_relevant_discussion",
        "aigcrisk_relevant_event",
        "aigcrisk_relevant_discussion",
    }


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"找不到输入文件: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    if df.empty:
        logger.warning("输入 CSV 为空，无需筛选。")
        return

    if not PROMPT_PATH.exists():
        raise FileNotFoundError(f"提示词文件不存在: {PROMPT_PATH}")
    system_prompt = PROMPT_PATH.read_text(encoding="utf-8")

    filtered_rows: List[Dict[str, Any]] = []

    for batch_idx, batch_df in enumerate(_batch_dataframe(df, BATCH_SIZE), start=1):
        user_prompt = _format_payload(batch_df)
        _, answer, _ = call_model(system_prompt, user_prompt)
        labels = _parse_label_list(answer or "")

        if not labels:
            logger.warning("批次 %d 未解析到任何标签，跳过。", batch_idx)
            continue

        if len(labels) != len(batch_df):
            logger.warning("批次 %d 标签数量与样本不一致：labels=%d, samples=%d", batch_idx, len(labels), len(batch_df))

        kept = 0
        for idx, label in enumerate(labels[: len(batch_df)]):
            if _is_relevant_label(label):
                filtered_rows.append(batch_df.iloc[idx].to_dict())
                kept += 1

        logger.info("批次 %d 完成：保留 %d / %d 条", batch_idx, kept, len(batch_df))

    if not filtered_rows:
        logger.warning("模型未筛出任何标题，未写出 CSV。")
        return

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(filtered_rows).to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    logger.info("筛选完成，最终保留 %d 条，结果写入 %s", len(filtered_rows), OUTPUT_CSV)


if __name__ == "__main__":
    main()
