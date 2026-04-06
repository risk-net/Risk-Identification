"""Keyword-based filtering for hot-list titles."""

from __future__ import annotations

import configparser
import csv
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from fuzzywuzzy import fuzz

BASE_DIR = Path(__file__).resolve().parents[4]
CONFIG_PATH = BASE_DIR / "config/Identification_Method-Hot_list_word_Dataset-llm_filter-config.ini"

config = configparser.ConfigParser()
if not config.read(CONFIG_PATH, encoding="utf-8"):
    raise FileNotFoundError(f"无法读取配置文件: {CONFIG_PATH}")

KEYWORD_CONFIG = config["HotList.KeywordFilter"]


def _resolve_path(value: str) -> Path:
    path = Path(value.strip())
    if not path.is_absolute():
        path = BASE_DIR / path
    return path.resolve()


KEYWORDS_FILE = _resolve_path(
    KEYWORD_CONFIG.get(
        "KEYWORDS_FILE",
        "keywords/Identification-common-keywords.txt",
    )
)
INPUT_JSON = _resolve_path(KEYWORD_CONFIG.get("INPUT_JSON", "download_dir/douyin_hotlist.json"))
RAW_CSV = _resolve_path(
    KEYWORD_CONFIG.get("RAW_CSV", "outputs/Hot_list_word_Dataset/keyword_filter/hotlist_raw.csv")
)
FILTERED_CSV = _resolve_path(
    KEYWORD_CONFIG.get(
        "FILTERED_CSV",
        "outputs/Hot_list_word_Dataset/keyword_filter/filtered_hotlist.csv",
    )
)
FUZZ_RATIO_THRESHOLD = KEYWORD_CONFIG.getint("FUZZ_RATIO_THRESHOLD", fallback=70)
LOG_FILE = _resolve_path(KEYWORD_CONFIG.get("LOG_FILE", "logs/Hot_list_word_Dataset/keyword_filter.log"))

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("hotlist_keyword_filter")


def load_keywords(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"关键词文件不存在: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return {line.strip().lower() for line in fh if line.strip()}


def contains_keyword(title: Any, keywords: set[str]) -> bool:
    if not isinstance(title, str):
        return False
    normalized = title.strip().lower()
    if not normalized:
        return False
    for keyword in keywords:
        if keyword in normalized:
            return True
        if fuzz.partial_ratio(keyword, normalized) >= FUZZ_RATIO_THRESHOLD:
            return True
    return False


def load_hotlist_json(path: Path) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        raise FileNotFoundError(f"输入 JSON 不存在: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return {"unknown_date": data}
    raise ValueError("热榜 JSON 顶层必须是对象或列表。")


def _extract_title_and_url(item: Any) -> tuple[str, str]:
    if not isinstance(item, dict):
        return "", ""

    title_obj = item.get("title")
    if isinstance(title_obj, dict):
        title = str(title_obj.get("title", "") or "").strip()
        url = str(title_obj.get("url", "") or "").strip()
        if title or url:
            return title, url

    if isinstance(title_obj, str):
        title = title_obj.strip()
        url = str(item.get("url", "") or "").strip()
        if title or url:
            return title, url

    for title_key in ("name", "text", "keyword", "hot_title"):
        title = str(item.get(title_key, "") or "").strip()
        if title:
            for url_key in ("url", "link", "href"):
                url = str(item.get(url_key, "") or "").strip()
                return title, url

    return "", ""


def _normalize_date_key(raw_date: Any) -> str:
    text = str(raw_date or "").strip()
    return text or "unknown_date"


def flatten_hotlist(data: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    id_counter = 0

    for date, items in data.items():
        if not isinstance(items, list):
            continue
        for item in items:
            title, url = _extract_title_and_url(item)
            row_date = _normalize_date_key(item.get("日期") if isinstance(item, dict) else None) if isinstance(item, dict) else ""
            if not row_date or row_date == "unknown_date":
                row_date = _normalize_date_key(date)
            rows.append(
                {
                    "id": id_counter,
                    "日期": row_date,
                    "title": title,
                    "url": url,
                }
            )
            id_counter += 1
    return rows


def write_raw_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["id", "日期", "title", "url"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    keywords = load_keywords(KEYWORDS_FILE)
    logger.info("加载关键词 %d 个", len(keywords))

    hotlist = load_hotlist_json(INPUT_JSON)
    rows = flatten_hotlist(hotlist)
    logger.info("从 %s 展平得到 %d 条热榜记录", INPUT_JSON, len(rows))

    write_raw_csv(rows, RAW_CSV)
    logger.info("已写出原始 CSV: %s", RAW_CSV)

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("输入数据为空，未生成筛选结果。")
        return

    filtered_df = df[df["title"].apply(lambda title: contains_keyword(title, keywords))]
    FILTERED_CSV.parent.mkdir(parents=True, exist_ok=True)
    filtered_df.to_csv(FILTERED_CSV, index=False, encoding="utf-8")

    logger.info("关键词筛选完成：保留 %d / %d 条，结果写入 %s", len(filtered_df), len(df), FILTERED_CSV)


if __name__ == "__main__":
    main()
