"""
关键词/短语筛选 OpenNewsArchive JSONL。
读取 config/Identification_Method-OpenNewsArchive-keyword_filter-config.ini
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Set, List

import configparser
import ahocorasick
import jieba
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

BASE_DIR = Path(__file__).resolve().parents[4]
CONFIG_PATH = BASE_DIR / "config/Identification_Method-OpenNewsArchive-keyword_filter-config.ini"

config = configparser.ConfigParser()
if not config.read(CONFIG_PATH, encoding="utf-8"):
    raise FileNotFoundError(f"无法读取配置文件: {CONFIG_PATH}")

section = config["OpenNewsArchive"]


def _resolve(value: str) -> Path:
    path = Path(value.strip())
    if not path.is_absolute():
        path = BASE_DIR / path
    return path.resolve()


BASE_INPUT_DIR = _resolve(section.get("BASE_INPUT_DIR", "download_dir/OpenNewsArchive"))
BASE_OUTPUT_DIR = _resolve(section.get("BASE_OUTPUT_DIR", "outputs/OpenNewsArchive"))
JSONL_FILE = _resolve(section.get("JSONL_FILE", "outputs/OpenNewsArchive/ai_related_news.jsonl"))
KEYWORDS_FILE = _resolve(section.get("KEYWORDS_FILE", "keywords/Identification-common-keywords.txt"))
PHRASES_FILE = _resolve(section.get("PHRASES_FILE", "keywords/Identification-common-custom_phrases.txt"))
LOG_FILE = _resolve(section.get("LOG_FILE", "logs/OpenNewsArchive/keyword_filter.log"))

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("opennews_keyword_filter")


def _ensure_nltk_resource(name: str, path: str) -> None:
    try:
        nltk.data.find(path)
    except LookupError:
        nltk.download(name, quiet=True)


for pkg, loc in (("punkt", "tokenizers/punkt"), ("stopwords", "corpora/stopwords"), ("wordnet", "corpora/wordnet")):
    _ensure_nltk_resource(pkg, loc)


class PhraseMatcher:
    def __init__(self, phrase_file: Path):
        self.automaton = ahocorasick.Automaton()
        self._load_phrases(phrase_file)

    def _load_phrases(self, phrase_file: Path) -> None:
        if not phrase_file.exists():
            logger.warning("短语文件 %s 不存在，将跳过短语匹配。", phrase_file)
            return
        with phrase_file.open("r", encoding="utf-8") as fh:
            for line in fh:
                phrase = line.strip()
                if phrase:
                    self.automaton.add_word(phrase.lower(), phrase.lower())
        self.automaton.make_automaton()

    def find_matches(self, text: str) -> Set[str]:
        if not self.automaton:
            return set()
        lower = text.lower()
        return {match for _, match in self.automaton.iter(lower)}


class KeywordProcessor:
    def __init__(self, keywords_file: Path, phrases_file: Path):
        self.keywords = self._load_keywords(keywords_file)
        self.lemmatizer = WordNetLemmatizer()
        self.en_stopwords = set(stopwords.words("english"))
        self.phrase_matcher = PhraseMatcher(phrases_file)

    def _load_keywords(self, path: Path) -> Set[str]:
        if not path.exists():
            logger.warning("关键词文件 %s 不存在，筛选结果将为空。", path)
            return set()
        with path.open("r", encoding="utf-8") as fh:
            return {line.strip().lower() for line in fh if line.strip()}

    def extract_keywords(self, text: str, lang: str, top_n: int = 10) -> Set[str]:
        lower_text = (text or "").lower()
        phrase_matches = self.phrase_matcher.find_matches(lower_text)

        phrase_tokens: Set[str] = set()
        if phrase_matches:
            if lang == "en":
                for phrase in phrase_matches:
                    phrase_tokens.update(word_tokenize(phrase))
            elif lang == "zh":
                for phrase in phrase_matches:
                    phrase_tokens.update(jieba.lcut(phrase))

        residual_keywords: Set[str] = set()
        if lang == "en":
            tokens = [
                self.lemmatizer.lemmatize(tok.lower())
                for tok in word_tokenize(lower_text)
                if tok.isalpha() and tok.lower() not in self.en_stopwords and tok not in phrase_tokens
            ]
            freq_dist = nltk.FreqDist(tokens)
            residual_keywords = {word for word, _ in freq_dist.most_common(top_n)}
        elif lang == "zh":
            freq: Dict[str, int] = {}
            for token in jieba.lcut(lower_text):
                if token not in phrase_tokens and len(token) > 1:
                    freq[token] = freq.get(token, 0) + 1
            residual_keywords = set(sorted(freq, key=freq.get, reverse=True)[:top_n])

        return phrase_matches.union(residual_keywords)


def iter_jsonl_files(root: Path) -> List[Path]:
    if not root.exists():
        raise FileNotFoundError(f"输入目录不存在: {root}")
    return sorted([path for path in root.rglob("*.jsonl") if path.is_file()])


def process_jsonl_file(path: Path, processor: KeywordProcessor) -> List[Dict[str, Any]]:
    ai_related_news: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                content = data.get("content") or ""
                lang = data.get("language", "en").lower()

                extracted = processor.extract_keywords(content, lang)
                if processor.keywords.intersection(extracted):
                    ai_related_news.append(data)
    except Exception as exc:
        logger.error("处理文件 %s 失败: %s", path, exc, exc_info=True)
    return ai_related_news


def safe_filename(doc_id: str) -> str:
    doc_id = doc_id or "unknown"
    sanitized = re.sub(r"[^A-Za-z0-9._-]", "_", doc_id.strip())
    return sanitized or "news"


def _parse_release_date(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""

    iso_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if iso_match:
        return iso_match.group(0)

    datetime_match = re.match(r"(\d{4})-(\d{2})-(\d{2})T", text)
    if datetime_match:
        return "-".join(datetime_match.groups())

    compact_match = re.match(r"(\d{4})(\d{2})(\d{2})$", text)
    if compact_match:
        year, month, day = compact_match.groups()
        return f"{year}-{month}-{day}"

    return ""


def _extract_release_date(news: Dict[str, Any]) -> str:
    for key in (
        "release_date",
        "date",
        "published_date",
        "published_at",
        "publish_date",
        "datePublished",
    ):
        parsed = _parse_release_date(news.get(key))
        if parsed:
            return parsed
    return datetime.now().strftime("%Y-%m-%d")


def _resolve_output_paths(news: Dict[str, Any]) -> tuple[Path, Path]:
    release_date = _extract_release_date(news)
    year, month, _ = release_date.split("-")
    month_int = int(month)
    month_dir = f"{month_int:02d}"
    bucket_dir = BASE_OUTPUT_DIR / year / month_dir
    texts_dir = bucket_dir / "texts"
    jsons_dir = bucket_dir / "jsons"
    texts_dir.mkdir(parents=True, exist_ok=True)
    jsons_dir.mkdir(parents=True, exist_ok=True)

    doc_id = safe_filename(str(news.get("id", "")))
    return texts_dir / f"{doc_id}.txt", jsons_dir / f"{doc_id}.json"


def main() -> None:
    BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    JSONL_FILE.parent.mkdir(parents=True, exist_ok=True)

    processor = KeywordProcessor(KEYWORDS_FILE, PHRASES_FILE)
    if not processor.keywords:
        logger.error("关键词列表为空，终止执行。")
        return

    jsonl_files = iter_jsonl_files(BASE_INPUT_DIR)
    logger.info("开始处理 %d 个 JSONL 文件", len(jsonl_files))

    all_ai_news: List[Dict[str, Any]] = []
    for idx, path in enumerate(jsonl_files, 1):
        matches = process_jsonl_file(path, processor)
        all_ai_news.extend(matches)
        if idx % 10 == 0 or idx == len(jsonl_files):
            logger.info("进度 [%d/%d]，当前累计命中 %d 条", idx, len(jsonl_files), len(all_ai_news))

    logger.info("筛选完成，共命中 %d 条 AI 相关新闻", len(all_ai_news))

    with JSONL_FILE.open("w", encoding="utf-8") as out_f:
        for news in all_ai_news:
            out_f.write(json.dumps(news, ensure_ascii=False) + "\n")

    for news in all_ai_news:
        txt_path, json_path = _resolve_output_paths(news)
        release_date = _extract_release_date(news)

        text_content = (news.get("title") or "") + "\n" + (news.get("content") or "")
        txt_path.write_text(text_content, encoding="utf-8")

        article_json = {
            "id": str(news.get("id", "")).strip(),
            "title": news.get("title") or "",
            "content": news.get("content") or "",
            "release_date": release_date,
            "language": news.get("language", "en"),
        }
        json_path.write_text(
            json.dumps(article_json, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
