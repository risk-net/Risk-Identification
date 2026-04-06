#!/usr/bin/env python3
"""Run a lightweight keyword-based AI-news relevance check on a JSON or JSONL dataset."""

from __future__ import annotations

import argparse
import configparser
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

MODULE_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = MODULE_DIR.parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "evaluation_dataset_2000.json"
DEFAULT_KEYWORDS = PROJECT_ROOT / "keywords" / "Identification-common-keywords.txt"
DEFAULT_PHRASES = PROJECT_ROOT / "keywords" / "Identification-common-custom_phrases.txt"
DEFAULT_STOPWORDS = PROJECT_ROOT / "keywords" / "Identification-common-stopwords.txt"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "Identification_Evaluation" / "keyword_filter"
DEFAULT_CONFIG = None
TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_-]+|[\u4e00-\u9fff]+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Optional INI config file.")
    parser.add_argument("--input", type=Path, default=None, help="Input JSON or JSONL file.")
    parser.add_argument("--keywords", type=Path, default=None, help="Keywords TXT file.")
    parser.add_argument("--phrases", type=Path, default=None, help="Phrases TXT file.")
    parser.add_argument("--stopwords", type=Path, default=None, help="Stopwords TXT file.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Directory for summary and split outputs.")
    return parser.parse_args()


def load_config(path: Path | None) -> dict[str, Path]:
    defaults = {
        "input": DEFAULT_INPUT,
        "keywords": DEFAULT_KEYWORDS,
        "phrases": DEFAULT_PHRASES,
        "stopwords": DEFAULT_STOPWORDS,
        "output_dir": DEFAULT_OUTPUT_DIR,
    }
    if path is None:
        return defaults

    parser = configparser.ConfigParser()
    with path.open("r", encoding="utf-8") as fh:
        parser.read_file(fh)
    if "keyword_filter" not in parser:
        raise KeyError(f"Missing [keyword_filter] section in {path}")
    section = parser["keyword_filter"]
    resolved = {}
    for key, default in defaults.items():
        raw = section.get(key, str(default))
        candidate = Path(raw)
        resolved[key] = candidate if candidate.is_absolute() else (path.parent / candidate).resolve()
    return resolved


def load_word_list(path: Path) -> set[str]:
    with path.open("r", encoding="utf-8") as fh:
        return {line.strip().lower() for line in fh if line.strip()}


def load_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            raise ValueError(f"Input JSON must contain a list of records: {path}")
        return [item for item in data if isinstance(item, dict)]

    records = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                item = json.loads(line)
                if isinstance(item, dict):
                    records.append(item)
    return records


def extract_text(record: dict[str, Any]) -> str:
    parts = []
    for key in ("title", "content", "text", "body"):
        value = str(record.get(key) or "").strip()
        if value:
            parts.append(value)
    return "\n".join(parts)


def classify_record(record: dict[str, Any], keywords: set[str], phrases: set[str], stopwords: set[str]) -> tuple[bool, dict[str, Any]]:
    text = extract_text(record)
    lowered = text.lower()
    matched_phrases = sorted(phrase for phrase in phrases if phrase in lowered)
    tokens = [token for token in TOKEN_PATTERN.findall(lowered) if token not in stopwords]
    matched_keywords = sorted({token for token in tokens if token in keywords})
    is_relevant = bool(matched_phrases or matched_keywords)
    return is_relevant, {
        "matched_keywords": matched_keywords,
        "matched_phrases": matched_phrases,
        "token_count": len(tokens),
    }


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False))
            fh.write("\n")


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    input_path = args.input or config["input"]
    keywords_path = args.keywords or config["keywords"]
    phrases_path = args.phrases or config["phrases"]
    stopwords_path = args.stopwords or config["stopwords"]
    output_dir = args.output_dir or config["output_dir"]

    keywords = load_word_list(keywords_path)
    phrases = load_word_list(phrases_path)
    stopwords = load_word_list(stopwords_path)
    records = load_records(input_path)

    relevant: list[dict[str, Any]] = []
    unrelated: list[dict[str, Any]] = []
    keyword_counter: Counter[str] = Counter()
    phrase_counter: Counter[str] = Counter()

    for record in records:
        is_relevant, details = classify_record(record, keywords, phrases, stopwords)
        enriched = dict(record)
        enriched.update(details)
        keyword_counter.update(details["matched_keywords"])
        phrase_counter.update(details["matched_phrases"])
        if is_relevant:
            relevant.append(enriched)
        else:
            unrelated.append(enriched)

    output_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "input_file": str(input_path),
        "total_records": len(records),
        "relevant_records": len(relevant),
        "unrelated_records": len(unrelated),
        "relevant_ratio": len(relevant) / len(records) if records else 0.0,
        "top_keywords": keyword_counter.most_common(10),
        "top_phrases": phrase_counter.most_common(10),
    }

    write_jsonl(output_dir / "relevant.jsonl", relevant)
    write_jsonl(output_dir / "unrelated.jsonl", unrelated)
    with (output_dir / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    print(f"Input records: {len(records)}")
    print(f"Relevant records: {len(relevant)}")
    print(f"Unrelated records: {len(unrelated)}")
    print(f"Saved outputs under: {output_dir}")


if __name__ == "__main__":
    main()
