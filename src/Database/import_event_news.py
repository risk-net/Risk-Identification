#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Event-level news import script (auto-detect jsonb / array / text) + DEBUG TRACE

Target table: ai_risk_events_news
Source table: ai_risk_relevant_news

Features:
- Auto-detect actual column types (json/jsonb/array/scalar) from information_schema.
- Auto-detect varchar max length and (default) truncate with warning (or --strict-varchar to fail).
- Parse stringified JSON in ai_tech/ai_risk/event using json.loads + ast.literal_eval.
- Insert event_time_start/end + country/province/city from file event section (not only via supplement).
- Step-by-step debug printing: show where data becomes empty.

Workflow:
1) Copy non-English sources from DB via SQL INSERT...SELECT nextval()
2) Import English sources from JSON files
3) Supplement missing fields from ai_risk_relevant_news via SQL UPDATE...FROM
"""

import os
import sys
import json
import re
import ast
import hashlib
import logging
import argparse
import configparser
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Iterable

try:
    import psycopg2
    from psycopg2.extras import execute_values, Json
except ImportError:
    print("请安装 psycopg2: pip install psycopg2-binary")
    sys.exit(1)


# =========================
# Logging
# =========================
def setup_logger() -> logging.Logger:
    log_dir = Path(__file__).resolve().parents[3] / "outputs" / "Database" / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)
    log_filename = log_dir / f"import_event_news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[file_handler, console_handler],
    )

    logger = logging.getLogger("import_event_news")
    logger.info(f"日志文件: {log_filename}")
    logger.info("=" * 100)
    logger.info("开始事件级新闻导入任务 (auto jsonb/array + debug trace)")
    logger.info("=" * 100)
    return logger


logger = setup_logger()


# =========================
# Helpers
# =========================
_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\uD800-\uDFFF\uFFFE\uFFFF]")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "Database-config.ini"


def _resolve_path(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    p = Path(value)
    if p.is_absolute():
        return str(p)
    return str((PROJECT_ROOT / p).resolve())


def load_runtime_config(config_path: Path = DEFAULT_CONFIG_PATH):
    parser = configparser.ConfigParser()
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    parser.read(config_path, encoding="utf-8")

    db_config = {
        "host": parser.get("Database", "host", fallback="localhost").strip() or "localhost",
        "database": parser.get("Database", "database", fallback="ai_risks_db").strip() or "ai_risks_db",
        "user": parser.get("Database", "user", fallback="postgres").strip() or "postgres",
        "password": parser.get("Database", "password", fallback=""),
        "port": int(parser.get("Database", "port", fallback="5432") or "5432"),
    }

    english_sources = {
        "cc_news": {
            "path": _resolve_path(parser.get("EnglishEventNewsSources", "cc_news_event_only_dir", fallback="")),
            "pattern": "**/*.json",
        },
        "aiid": {
            "path": _resolve_path(parser.get("EnglishEventNewsSources", "aiid_event_only_dir", fallback="")),
            "pattern": "*.json",
        },
        "aiaaic": {
            "path": _resolve_path(parser.get("EnglishEventNewsSources", "aiaaic_event_only_dir", fallback="")),
            "pattern": "*.json",
        },
    }
    return db_config, english_sources


def is_empty(v: Any) -> bool:
    if v is None:
        return True
    if v == "":
        return True
    if isinstance(v, (list, tuple, dict)) and len(v) == 0:
        return True
    return False


def brief(v: Any, maxlen: int = 220) -> str:
    try:
        if v is None:
            return "NULL"
        if isinstance(v, str):
            s = v.replace("\n", "\\n")
            return s[:maxlen] + ("..." if len(s) > maxlen else "")
        if isinstance(v, (dict, list)):
            s = json.dumps(v, ensure_ascii=False)
            s = s.replace("\n", "\\n")
            return s[:maxlen] + ("..." if len(s) > maxlen else "")
        s = str(v).replace("\n", "\\n")
        return s[:maxlen] + ("..." if len(s) > maxlen else "")
    except Exception as e:
        return f"<brief_error {e}>"


def clean_text(s: Any) -> Optional[str]:
    """Clean illegal chars for TEXT fields."""
    if s is None:
        return None
    s = str(s)
    s = _ILLEGAL_CHARS_RE.sub("", s)
    s = s.strip()
    return s if s else None


def sanitize_json(obj: Any) -> Any:
    """Recursively remove illegal chars inside JSON-like objects."""
    if obj is None:
        return None
    if isinstance(obj, str):
        return _ILLEGAL_CHARS_RE.sub("", obj)
    if isinstance(obj, list):
        return [sanitize_json(x) for x in obj]
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            ck = _ILLEGAL_CHARS_RE.sub("", str(k))
            out[ck] = sanitize_json(v)
        return out
    return obj


def normalize_content(content: Optional[str]) -> str:
    if not content:
        return ""
    return re.sub(r"\s+", " ", str(content)).strip()


def parse_date(date_str: Any):
    """Return datetime.date or None."""
    if date_str is None:
        return None
    s = str(date_str).strip()
    if not s or s.lower() == "null":
        return None
    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y", "%Y%m%d"]:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def generate_hash_name_from_file(json_file: Path) -> str:
    stem = json_file.stem
    original_id = stem[:-7] if stem.endswith("_result") else stem
    return hashlib.md5(original_id.encode("utf-8")).hexdigest()[:16]


def ensure_list(val: Any) -> List[Any]:
    """
    Ensure Python list for array fields.
    If val is a JSON-string like '["a","b"]' or python-literal string "['a','b']",
    try to parse to list.
    """
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, tuple):
        return list(val)
    if isinstance(val, str):
        s = val.strip()
        if s.startswith("[") and s.endswith("]"):
            # strict JSON
            try:
                x = json.loads(s)
                if isinstance(x, list):
                    return x
            except Exception:
                pass
            # python literal
            try:
                x = ast.literal_eval(s)
                if isinstance(x, list):
                    return x
            except Exception:
                pass
        # fallback: single string => one element
        return [s]
    return [val]


def parse_json_maybe(val: Any, default: Any):
    """
    Accept dict/list directly;
    if val is a stringified JSON, try json.loads;
    if json.loads fails, try ast.literal_eval (handles single-quote python dict/list strings).
    Otherwise return default.
    """
    if val is None:
        return default
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                pass
            try:
                x = ast.literal_eval(s)
                if isinstance(x, (dict, list)):
                    return x
            except Exception:
                return default
    return default


def iter_json_files(base: Path, pattern: str) -> List[Path]:
    """
    Return file list (keeps behavior similar to your previous code).
    If you need streaming later,可以再改成 generator。
    """
    return list(base.glob(pattern))


# =========================
# Importer
# =========================
class EventNewsImporter:
    def __init__(
        self,
        db_config: Dict[str, Any],
        english_sources: Dict[str, Dict[str, str]],
        strict_varchar: bool = False,
    ):
        self.db_config = db_config
        self.english_sources = english_sources
        self.strict_varchar = strict_varchar

        self.conn = None
        self.cursor = None

        self.sequence_name = "ai_risk_events_news_event_level_news_id_seq"
        self.col_types: Dict[str, str] = {}     # column_name -> {"json","array","scalar"}
        self.col_maxlen: Dict[str, int] = {}    # varchar max length

        # context for better logging
        self._ctx_news_id: Optional[int] = None
        self._ctx_file: Optional[str] = None

        # debug controls
        self.debug_import = False
        self.debug_limit = 0
        self.debug_used = 0
        self.debug_news_ids = set()
        self.debug_file_contains: Optional[str] = None
        self.debug_dump_cols = False

    # -------- Debug config --------
    def configure_debug(
        self,
        enabled: bool,
        limit: int,
        news_ids: List[int],
        file_contains: Optional[str],
        dump_cols: bool,
    ):
        self.debug_import = bool(enabled)
        self.debug_limit = max(0, int(limit or 0))
        self.debug_used = 0
        self.debug_news_ids = set(news_ids or [])
        self.debug_file_contains = file_contains
        self.debug_dump_cols = bool(dump_cols)

    def _should_debug_file(self, json_file: Path, data: Any) -> bool:
        if not self.debug_import:
            return False
        if self.debug_used >= self.debug_limit:
            return False

        path_s = str(json_file)
        if self.debug_file_contains and (self.debug_file_contains not in path_s):
            return False

        if self.debug_news_ids:
            nid = None
            try:
                if isinstance(data, dict) and data.get("news_id") is not None:
                    nid = int(data.get("news_id"))
            except Exception:
                nid = None
            if nid not in self.debug_news_ids:
                return False

        self.debug_used += 1
        return True

    def _dbg(self, tag: str, v: Any):
        t = type(v).__name__
        empty = is_empty(v)

        extra = ""
        if isinstance(v, str):
            extra = f"len={len(v)}"
        elif isinstance(v, (list, tuple)):
            extra = f"n={len(v)} head={brief(list(v)[:5])}"
        elif isinstance(v, dict):
            keys = list(v.keys())
            extra = f"keys_n={len(keys)} keys_head={keys[:15]}"

        logger.info(f"[DBG] {tag}: type={t} empty={empty} {extra} preview={brief(v)}")

    # -------- DB lifecycle --------
    def connect(self) -> bool:
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()

            self.cursor.execute(f"CREATE SEQUENCE IF NOT EXISTS {self.sequence_name}")

            # set sequence to max+1 to avoid collision
            self.cursor.execute(
                f"""
                SELECT setval(
                    '{self.sequence_name}',
                    COALESCE((SELECT MAX(event_level_news_id) FROM ai_risk_events_news), 0) + 1,
                    false
                )
                """
            )

            self.col_types, self.col_maxlen = self._load_target_schema()
            self.conn.commit()

            logger.info("数据库连接成功；序列已校准(setval=max+1)；列类型/长度已加载")
            for c in ["ai_tech", "ai_risk", "event", "ai_system_list", "event_actor_list", "event_actor_main",
                      "event_time_start", "event_country", "event_province", "event_city"]:
                if c in self.col_types:
                    logger.info(f"  column[{c}] type={self.col_types[c]} maxlen={self.col_maxlen.get(c)}")
            return True

        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")

    def truncate_target_table(self):
        """
        清空表：
        - TRUNCATE 速度快
        - 清空后把 sequence 重置到 1（看你是否需要）
        """
        logger.warning("TRUNCATE ai_risk_events_news ...")
        self.cursor.execute("TRUNCATE TABLE ai_risk_events_news")
        # reset sequence to 1
        self.cursor.execute(f"SELECT setval('{self.sequence_name}', 1, false)")
        self.conn.commit()
        logger.warning("TRUNCATE 完成，sequence 已重置为 1")

    def next_event_level_ids(self, n: int) -> List[int]:
        self.cursor.execute(
            f"SELECT nextval('{self.sequence_name}') FROM generate_series(1, %s)",
            (n,),
        )
        return [r[0] for r in self.cursor.fetchall()]

    def _load_target_schema(self) -> Tuple[Dict[str, str], Dict[str, int]]:
        """
        Load ai_risk_events_news column types:
        - json: json/jsonb
        - array: ANYARRAY
        - scalar: other
        Also load varchar max length.
        """
        sql = """
        SELECT column_name, data_type, udt_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'ai_risk_events_news'
        """
        self.cursor.execute(sql)
        types: Dict[str, str] = {}
        maxlens: Dict[str, int] = {}

        for col, data_type, udt_name, char_max in self.cursor.fetchall():
            if data_type in ("jsonb", "json"):
                types[col] = "json"
            elif data_type == "ARRAY" or (isinstance(udt_name, str) and udt_name.startswith("_")):
                types[col] = "array"
            else:
                types[col] = "scalar"

            if char_max is not None:
                try:
                    maxlens[col] = int(char_max)
                except Exception:
                    pass

        return types, maxlens

    def _varchar_guard(self, col: str, s: Optional[str]) -> Optional[str]:
        """
        Enforce varchar(maxlen). Default: truncate with warning.
        If --strict-varchar: raise ValueError.
        """
        if s is None:
            return None
        maxlen = self.col_maxlen.get(col)
        if not maxlen:
            return s
        if len(s) <= maxlen:
            return s

        # too long
        ctx = f"(news_id={self._ctx_news_id} file={self._ctx_file})"
        msg = f"varchar overflow col={col} len={len(s)} max={maxlen} {ctx} value_preview={brief(s)}"

        if self.strict_varchar:
            raise ValueError(msg)

        logger.warning("[TRUNCATE] " + msg)
        return s[:maxlen]

    def adapt_value(self, col: str, val: Any) -> Any:
        """
        Adapt Python value to match target column type.
        Also enforce varchar maxlen if applicable.
        """
        t = self.col_types.get(col, "scalar")

        # json/jsonb
        if t == "json":
            if val is None:
                return Json({})
            return Json(sanitize_json(val))

        # arrays
        if t == "array":
            arr = ensure_list(val)
            cleaned = []
            for x in arr:
                if x is None:
                    cleaned.append(None)
                elif isinstance(x, (dict, list)):
                    cleaned.append(json.dumps(sanitize_json(x), ensure_ascii=False))
                else:
                    cleaned.append(clean_text(x))
            return cleaned

        # scalar
        if isinstance(val, (dict, list)):
            s = json.dumps(sanitize_json(val), ensure_ascii=False)
            s = clean_text(s)
            return self._varchar_guard(col, s)

        if isinstance(val, str):
            s = clean_text(val)
            return self._varchar_guard(col, s)

        # date/int/bool etc.
        return val

    # -------- Step 1: Copy Chinese sources (SQL) --------
    def copy_non_english_sources_sql(self) -> int:
        logger.info("\n" + "=" * 60)
        logger.info("步骤1: SQL 批量复制非英文数据源（排除英文源 cc_news/aiid/aiaaic）")
        logger.info("=" * 60)

        english_source_names = list(self.english_sources.keys())
        placeholders = ",".join(["%s"] * len(english_source_names))

        self.cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM ai_risk_relevant_news
            WHERE classification_result = 'AIrisk_relevant_event'
              AND data_source NOT IN ({placeholders})
            """,
            english_source_names,
        )
        total_to_copy = self.cursor.fetchone()[0]
        logger.info(f"待复制记录数（非英文源）: {total_to_copy:,}")

        if total_to_copy == 0:
            return 0

        cols = [
            "event_level_news_id",
            "news_id", "data_source", "file_path",
            "archive_year", "archive_month", "archive_ym",
            "hash_name", "content_hash", "content_hash_version", "normalize_rule",
            "classification_result", "classification_std_result",
            "title", "content", "release_date",
            "ai_tech", "ai_risk", "event",
            "ai_system_list", "ai_system_type_list", "ai_system_domain_list",
            "ai_risk_description", "ai_risk_type", "ai_risk_subtype",
            "harm_type", "harm_severity",
            "affected_actor_type", "affected_actor_subtype",
            "realized_or_potential", "risk_stage",
            "event_actor_main", "event_actor_main_type", "event_actor_list",
            "event_ai_system", "event_domain", "event_type",
            "event_cause", "event_process", "event_result",
            "event_time_start_desc", "event_time_end_desc",
            "event_time_start", "event_time_end",
            "event_country", "event_province", "event_city",
            "is_duplicate", "duplicate_group_id",
        ]
        col_str = ", ".join(cols)

        insert_sql = f"""
            INSERT INTO ai_risk_events_news ({col_str})
            SELECT
                nextval('{self.sequence_name}') AS event_level_news_id,
                news_id, data_source, file_path,
                archive_year, archive_month, archive_ym,
                hash_name, content_hash, content_hash_version, normalize_rule,
                classification_result, classification_std_result,
                title, content, release_date,
                ai_tech, ai_risk, event,
                ai_system_list, ai_system_type_list, ai_system_domain_list,
                ai_risk_description, ai_risk_type, ai_risk_subtype,
                harm_type, harm_severity,
                affected_actor_type, affected_actor_subtype,
                realized_or_potential, risk_stage,
                event_actor_main, event_actor_main_type, event_actor_list,
                event_ai_system, event_domain, event_type,
                event_cause, event_process, event_result,
                event_time_start_desc, event_time_end_desc,
                event_time_start, event_time_end,
                event_country, event_province, event_city,
                is_duplicate, duplicate_group_id
            FROM ai_risk_relevant_news
            WHERE classification_result = 'AIrisk_relevant_event'
              AND data_source NOT IN ({placeholders})
        """

        self.cursor.execute(insert_sql, english_source_names)
        copied = self.cursor.rowcount
        self.conn.commit()
        logger.info(f"成功复制 {copied:,} 条记录")
        return copied

    # -------- Step 2: Import English sources --------
    def import_english_sources(self, batch_size: int = 200, max_files_per_source: int = 0) -> Tuple[int, int]:
        logger.info("\n" + "=" * 60)
        logger.info("步骤2: 批量导入英文数据源文件（含逐步调试）")
        logger.info("=" * 60)

        total_imported = 0
        total_failed = 0

        for source_name, cfg in self.english_sources.items():
            source_path = Path(cfg["path"])
            pattern = cfg["pattern"]

            if not source_path.exists():
                logger.warning(f"数据源路径不存在: {source_path}")
                continue

            files = iter_json_files(source_path, pattern)
            if max_files_per_source and max_files_per_source > 0:
                files = files[:max_files_per_source]

            logger.info(f"\n导入数据源: {source_name}")
            logger.info(f"路径: {source_path}")
            logger.info(f"找到 {len(files):,} 个文件")

            if not files:
                continue

            imported, failed = self._batch_import_files(source_name, files, batch_size=batch_size)
            total_imported += imported
            total_failed += failed

            logger.info(f"  导入完成: 成功={imported:,}, 失败={failed:,}")

        return total_imported, total_failed

    def _english_insert_columns(self) -> List[str]:
        # 包含 event_time_start/end + country/province/city
        return [
            "event_level_news_id",
            "news_id", "data_source", "file_path",
            "hash_name", "content_hash", "content_hash_version", "normalize_rule",
            "classification_result",
            "title", "content", "release_date",
            "ai_tech", "ai_risk", "event",
            "ai_system_list", "ai_system_type_list", "ai_system_domain_list",
            "ai_risk_description", "ai_risk_type", "ai_risk_subtype",
            "harm_type", "harm_severity",
            "affected_actor_type", "affected_actor_subtype",
            "realized_or_potential", "risk_stage",
            "event_actor_main", "event_actor_main_type", "event_actor_list",
            "event_ai_system", "event_domain", "event_type",
            "event_cause", "event_process", "event_result",
            "event_time_start", "event_time_end",
            "event_country", "event_province", "event_city",
        ]

    def _batch_import_files(self, source_name: str, json_files: List[Path], batch_size: int) -> Tuple[int, int]:
        imported_count = 0
        failed_count = 0
        # 记录失败文件详情
        failed_details: List[Dict[str, Any]] = []

        cols = self._english_insert_columns()
        batch_rows: List[List[Any]] = []
        id_buffer: List[int] = []

        for idx, json_file in enumerate(json_files, 1):
            try:
                with open(json_file, "r", encoding="utf-8", errors="replace") as f:
                    data = json.load(f)

                debug_this = self._should_debug_file(json_file, data)

                vals = self._build_english_values(source_name, json_file, data, cols, debug=debug_this)
                if vals is None:
                    failed_count += 1
                    # 获取详细的失败原因
                    fail_reason = getattr(self, '_last_build_failure_reason', '未知原因')
                    failed_details.append({
                        'file': str(json_file),
                        'reason': fail_reason,
                        'news_id': getattr(self, '_last_build_news_id', None),
                    })
                    continue

                if not id_buffer:
                    id_buffer = self.next_event_level_ids(max(batch_size, 200))

                event_level_id = id_buffer.pop(0)
                row = [event_level_id] + vals
                batch_rows.append(row)

                if len(batch_rows) >= batch_size:
                    self._batch_insert(cols, batch_rows)
                    imported_count += len(batch_rows)
                    batch_rows.clear()

                if idx % 2000 == 0:
                    logger.info(f"  进度: {idx:,}/{len(json_files):,} files | imported={imported_count:,} failed={failed_count:,}")

            except json.JSONDecodeError as e:
                failed_count += 1
                failed_details.append({
                    'file': str(json_file),
                    'reason': f"JSON解析错误: {e}",
                    'news_id': None,
                })
                logger.warning(f"[FAIL] JSONDecodeError file={json_file}: {e}")
            except Exception as e:
                failed_count += 1
                failed_details.append({
                    'file': str(json_file),
                    'reason': f"处理异常: {type(e).__name__}: {e}",
                    'news_id': None,
                })
                logger.warning(f"[FAIL] 处理文件失败 file={json_file}: {e}")

        if batch_rows:
            self._batch_insert(cols, batch_rows)
            imported_count += len(batch_rows)

        # 输出失败详情汇总
        if failed_details:
            logger.info("\n" + "=" * 80)
            logger.info(f"导入失败详情 (共 {len(failed_details)} 条):")
            logger.info("=" * 80)
            # 按原因分组统计
            reason_counts: Dict[str, int] = {}
            for detail in failed_details:
                reason = detail['reason'][:80] if len(detail['reason']) > 80 else detail['reason']
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

            # 显示前10类失败原因
            sorted_reasons = sorted(reason_counts.items(), key=lambda x: -x[1])[:10]
            logger.info("失败原因统计 (Top 10):")
            for reason, count in sorted_reasons:
                logger.info(f"  [{count:>4}条] {reason}")

            # 显示前5个失败文件示例
            logger.info("\n失败文件示例 (前5个):")
            for i, detail in enumerate(failed_details[:5]):
                logger.info(f"  {i+1}. {detail['file'].split('/')[-1]}")
                logger.info(f"     news_id={detail['news_id']} | 原因={detail['reason'][:100]}")

            # 保存失败详情到日志文件
            failed_log_file = Path(__file__).resolve().parents[1] / "archive" / "generated" / "logs" / f"import_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            with open(failed_log_file, 'w', encoding='utf-8') as f:
                f.write("导入失败详情\n")
                f.write("=" * 100 + "\n")
                for i, detail in enumerate(failed_details, 1):
                    f.write(f"{i}. 文件: {detail['file']}\n")
                    f.write(f"   news_id: {detail['news_id']}\n")
                    f.write(f"   原因: {detail['reason']}\n")
                    f.write("-" * 100 + "\n")
            logger.info(f"\n详细失败日志已保存到: {failed_log_file}")
            logger.info("=" * 80)

        return imported_count, failed_count

    def extract_flattened_fields(self, ai_tech: Dict[str, Any], ai_risk: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        res: Dict[str, Any] = {}

        # ai_tech
        res["ai_system_list"] = ai_tech.get("ai_system_list", []) or []
        res["ai_system_type_list"] = ai_tech.get("ai_system_type_list", []) or []
        res["ai_system_domain_list"] = ai_tech.get("ai_system_domain_list", []) or []

        # ai_risk
        res["ai_risk_description"] = ai_risk.get("ai_risk_description", "") or ""
        res["ai_risk_type"] = ai_risk.get("ai_risk_type", "") or ""
        res["ai_risk_subtype"] = ai_risk.get("ai_risk_subtype", "") or ""
        res["harm_type"] = ai_risk.get("harm_type", "") or ""
        res["harm_severity"] = ai_risk.get("harm_severity", "") or ""
        res["affected_actor_type"] = ai_risk.get("affected_actor_type", "") or ""
        res["affected_actor_subtype"] = ai_risk.get("affected_actor_subtype", "") or ""
        res["realized_or_potential"] = ai_risk.get("realized_or_potential", "") or ""
        res["risk_stage"] = ai_risk.get("risk_stage", "") or ""

        # event: 你的样例里 actor_main 是字符串；actor_list 是 list
        res["event_actor_main"] = event.get("actor_main", None)
        res["event_actor_main_type"] = event.get("actor_main_type", "") or ""
        res["event_actor_list"] = event.get("actor_list", []) or []
        res["event_ai_system"] = event.get("ai_system", "") or ""
        res["event_domain"] = event.get("domain", "") or ""
        res["event_type"] = event.get("event_type", "") or ""
        res["event_cause"] = event.get("event_cause", "") or ""
        res["event_process"] = event.get("event_process", "") or ""
        res["event_result"] = event.get("event_result", "") or ""
        return res

    def _build_english_values(
        self,
        data_source: str,
        json_file: Path,
        data: Dict[str, Any],
        cols: List[str],
        debug: bool = False,
    ) -> Optional[List[Any]]:

        # 初始化失败原因记录
        self._last_build_failure_reason = None
        self._last_build_news_id = None

        if not isinstance(data, dict):
            self._last_build_failure_reason = f"data 不是 dict 类型，实际是 {type(data).__name__}"
            return None

        file_id = data.get("news_id")
        if not file_id:
            self._last_build_failure_reason = "文件中缺少 'news_id' 字段"
            return None
        try:
            news_id = int(file_id)
            self._last_build_news_id = news_id
        except Exception as e:
            self._last_build_failure_reason = f"id 字段无法转换为 int: '{file_id}' ({e})"
            self._last_build_news_id = str(file_id)
            return None

        classification_value = "AIrisk_relevant_event"
        file_path = str(json_file)
        hash_name = generate_hash_name_from_file(json_file)

        # store ctx for logging (varchar truncate etc.)
        self._ctx_news_id = news_id
        self._ctx_file = file_path

        # ---------- DEBUG: START ----------
        if debug:
            logger.info("\n" + "=" * 120)
            logger.info(f"[DBG] START file={json_file} source_arg={data_source}")
            self._dbg("top.keys", list(data.keys()))
            self._dbg("top.news_id", data.get("news_id"))
            self._dbg("top.data_source", data.get("data_source"))
            self._dbg("top.title(raw)", data.get("title"))
            self._dbg("top.content(raw)", data.get("content"))
            self._dbg("top.release_date(raw)", data.get("release_date"))
        # ---------- DEBUG: END ----------

        # 1) title/content
        title = clean_text(data.get("title"))
        content = clean_text(data.get("content")) or ""
        norm_content = normalize_content(content)

        if debug:
            self._dbg("step1.title(clean_text)", title)
            self._dbg("step1.content(clean_text)", content)
            self._dbg("step1.norm_content(normalize)", norm_content)

        # 2) release_date + hash
        release_date = parse_date(data.get("release_date"))
        content_hash = hashlib.sha256(norm_content.encode("utf-8")).hexdigest() if norm_content else None

        if debug:
            self._dbg("step1.release_date(parsed)", release_date)
            self._dbg("step1.content_hash", content_hash)

        # 3) parse ai_tech/ai_risk/event (maybe stringified)
        raw_ai_tech = data.get("ai_tech")
        raw_ai_risk = data.get("ai_risk")
        raw_event = data.get("event")

        if debug:
            self._dbg("step2.raw.ai_tech", raw_ai_tech)
            self._dbg("step2.raw.ai_risk", raw_ai_risk)
            self._dbg("step2.raw.event", raw_event)

        ai_tech_raw = parse_json_maybe(raw_ai_tech, {})
        ai_risk_raw = parse_json_maybe(raw_ai_risk, {})
        event_raw = parse_json_maybe(raw_event, {})

        if debug:
            self._dbg("step3.parsed.ai_tech_raw", ai_tech_raw)
            self._dbg("step3.parsed.ai_risk_raw", ai_risk_raw)
            self._dbg("step3.parsed.event_raw", event_raw)

        if not isinstance(ai_tech_raw, dict):
            ai_tech_raw = {}
        if not isinstance(ai_risk_raw, dict):
            ai_risk_raw = {}
        if not isinstance(event_raw, dict):
            event_raw = {}

        ai_tech = sanitize_json(ai_tech_raw) or {}
        ai_risk = sanitize_json(ai_risk_raw) or {}
        event = sanitize_json(event_raw) or {}

        if debug:
            self._dbg("step4.sanitize.ai_tech", ai_tech)
            self._dbg("step4.sanitize.ai_risk", ai_risk)
            self._dbg("step4.sanitize.event", event)

        # 4) flatten
        flat = self.extract_flattened_fields(ai_tech=ai_tech, ai_risk=ai_risk, event=event)

        if debug:
            self._dbg("step5.flat", flat)
            self._dbg("step5.flat.ai_system_list", flat.get("ai_system_list"))
            self._dbg("step5.flat.ai_risk_type", flat.get("ai_risk_type"))
            self._dbg("step5.flat.harm_type", flat.get("harm_type"))
            self._dbg("step5.flat.event_actor_main", flat.get("event_actor_main"))
            self._dbg("step5.flat.event_actor_list", flat.get("event_actor_list"))

        # 5) event_time & location from event section
        event_time_start_raw = event.get("event_time_start")
        event_time_end_raw = event.get("event_time_end")

        event_time_start = parse_date(event_time_start_raw)
        event_time_end = parse_date(event_time_end_raw)

        # location can be None; keep None (clean_text returns None)
        event_country_raw = event.get("event_country")
        event_province_raw = event.get("event_province")
        event_city_raw = event.get("event_city")

        # IMPORTANT: avoid list/dict being dumped into varchar
        def _safe_loc(v: Any) -> Optional[str]:
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                # 极端情况下出现这种，先转短字符串并让 varchar_guard 处理（会截断+警告）
                return clean_text(json.dumps(sanitize_json(v), ensure_ascii=False))
            return clean_text(v)

        event_country = _safe_loc(event_country_raw)
        event_province = _safe_loc(event_province_raw)
        event_city = _safe_loc(event_city_raw)

        if debug:
            self._dbg("step6.event_time_start_raw", event_time_start_raw)
            self._dbg("step6.event_time_start_parsed", event_time_start)
            self._dbg("step6.event_time_end_raw", event_time_end_raw)
            self._dbg("step6.event_time_end_parsed", event_time_end)
            self._dbg("step6.event_country_raw", event_country_raw)
            self._dbg("step6.event_country_clean", event_country)
            self._dbg("step6.event_province_raw", event_province_raw)
            self._dbg("step6.event_province_clean", event_province)
            self._dbg("step6.event_city_raw", event_city_raw)
            self._dbg("step6.event_city_clean", event_city)

        values_by_col = {
            "news_id": news_id,
            "data_source": data_source,  # use folder source (authoritative)
            "file_path": file_path,

            "hash_name": hash_name,
            "content_hash": content_hash,
            "content_hash_version": 2,
            "normalize_rule": "collapse_whitespace_strip",
            "classification_result": classification_value,

            "title": title,
            "content": norm_content,
            "release_date": release_date,

            "ai_tech": ai_tech,
            "ai_risk": ai_risk,
            "event": event,

            "ai_system_list": flat.get("ai_system_list", []),
            "ai_system_type_list": flat.get("ai_system_type_list", []),
            "ai_system_domain_list": flat.get("ai_system_domain_list", []),

            "ai_risk_description": flat.get("ai_risk_description", ""),
            "ai_risk_type": flat.get("ai_risk_type", ""),
            "ai_risk_subtype": flat.get("ai_risk_subtype", ""),
            "harm_type": flat.get("harm_type", ""),
            "harm_severity": flat.get("harm_severity", ""),
            "affected_actor_type": flat.get("affected_actor_type", ""),
            "affected_actor_subtype": flat.get("affected_actor_subtype", ""),
            "realized_or_potential": flat.get("realized_or_potential", ""),
            "risk_stage": flat.get("risk_stage", ""),

            "event_actor_main": flat.get("event_actor_main", None),
            "event_actor_main_type": flat.get("event_actor_main_type", ""),
            "event_actor_list": flat.get("event_actor_list", []),
            "event_ai_system": flat.get("event_ai_system", ""),
            "event_domain": flat.get("event_domain", ""),
            "event_type": flat.get("event_type", ""),
            "event_cause": flat.get("event_cause", ""),
            "event_process": flat.get("event_process", ""),
            "event_result": flat.get("event_result", ""),

            "event_time_start": event_time_start,
            "event_time_end": event_time_end,
            "event_country": event_country,
            "event_province": event_province,
            "event_city": event_city,
        }

        out_vals: List[Any] = []

        if debug:
            non_empty_cols = []
            logger.info("[DBG] step7.values_by_col summary (insert payload):")
            for col in cols[1:]:  # skip event_level_news_id
                raw_v = values_by_col.get(col)
                if not is_empty(raw_v):
                    non_empty_cols.append(col)

                if self.debug_dump_cols:
                    logger.info(
                        f"[DBG]   col={col} target_type={self.col_types.get(col,'?')} "
                        f"raw_empty={is_empty(raw_v)} raw_preview={brief(raw_v)}"
                    )

                out_vals.append(self.adapt_value(col, raw_v))

            logger.info(f"[DBG] non_empty_cols({len(non_empty_cols)}/{len(cols)-1}) = {non_empty_cols}")
            logger.info(f"[DBG] END file={json_file}")
            logger.info("=" * 120 + "\n")
        else:
            for col in cols[1:]:
                out_vals.append(self.adapt_value(col, values_by_col.get(col)))

        # clear ctx
        self._ctx_news_id = None
        self._ctx_file = None
        return out_vals

    def _log_varchar_lengths(self, cols: List[str], row: List[Any], level: str = "ERROR"):
        """
        When insert fails due to varchar overflow, print lengths of varchar columns.
        """
        for i, col in enumerate(cols):
            maxlen = self.col_maxlen.get(col)
            if not maxlen:
                continue
            v = row[i]
            if isinstance(v, str):
                if len(v) > maxlen:
                    msg = f"[{level}] varchar overflow detail col={col} len={len(v)} max={maxlen} preview={brief(v)}"
                    logger.error(msg)

    def _batch_insert(self, cols: List[str], rows: List[List[Any]]):
        if not rows:
            return
        col_str = ", ".join(cols)
        sql = f"INSERT INTO ai_risk_events_news ({col_str}) VALUES %s"

        try:
            execute_values(self.cursor, sql, rows, page_size=len(rows))
            self.conn.commit()
        except Exception as e:
            logger.error(f"批量插入失败: {e}")
            self.conn.rollback()

            placeholders = ",".join(["%s"] * len(cols))
            single_sql = f"INSERT INTO ai_risk_events_news ({col_str}) VALUES ({placeholders})"

            # 记录批量插入失败的行
            failed_rows = []
            for row in rows:
                try:
                    self.cursor.execute(single_sql, row)
                    self.conn.commit()
                except Exception as e2:
                    self.conn.rollback()
                    failed_rows.append({
                        'news_id': row[cols.index("news_id")],
                        'file': row[cols.index("file_path")],
                        'error': str(e2),
                    })
                    # diagnostics
                    try:
                        news_id_idx = cols.index("news_id")
                        file_path_idx = cols.index("file_path")
                        logger.error(f"[FAIL INSERT] news_id={row[news_id_idx]} file={row[file_path_idx]}: {e2}")
                    except Exception:
                        logger.error(f"[FAIL INSERT] 插入失败: {e2}")

                    # if varchar overflow, print detail
                    s2 = str(e2).lower()
                    if "value too long" in s2 and "character varying" in s2:
                        try:
                            self._log_varchar_lengths(cols, row)
                        except Exception:
                            pass

            # 如果有失败行，记录到日志
            if failed_rows:
                failed_log_file = Path(__file__).resolve().parents[1] / "archive" / "generated" / "logs" / f"batch_insert_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                with open(failed_log_file, 'w', encoding='utf-8') as f:
                    f.write(f"批量插入失败详情 (共 {len(failed_rows)} 条)\n")
                    f.write("=" * 100 + "\n")
                    for i, item in enumerate(failed_rows, 1):
                        f.write(f"{i}. news_id={item['news_id']}\n")
                        f.write(f"   file: {item['file']}\n")
                        f.write(f"   error: {item['error']}\n")
                        f.write("-" * 100 + "\n")
                logger.warning(f"[FAIL INSERT] 批量插入中 {len(failed_rows)} 条记录失败，详细日志: {failed_log_file}")

    # -------- Step 3: Supplement missing fields --------
    def supplement_missing_fields_sql(self) -> int:
        logger.info("\n" + "=" * 60)
        logger.info("步骤3: SQL 批量补充缺失字段 (按 news_id + data_source 联结)")
        logger.info("=" * 60)

        english_sources = list(self.english_sources.keys())
        placeholders = ",".join(["%s"] * len(english_sources))

        update_sql = f"""
            UPDATE ai_risk_events_news t
            SET
                archive_year = COALESCE(t.archive_year, s.archive_year),
                archive_month = COALESCE(t.archive_month, s.archive_month),
                archive_ym = COALESCE(t.archive_ym, s.archive_ym),
                classification_std_result = COALESCE(t.classification_std_result, s.classification_std_result),
                event_time_start_desc = COALESCE(t.event_time_start_desc, s.event_time_start_desc),
                event_time_end_desc = COALESCE(t.event_time_end_desc, s.event_time_end_desc),
                event_time_start = COALESCE(t.event_time_start, s.event_time_start),
                event_time_end = COALESCE(t.event_time_end, s.event_time_end),
                event_country = COALESCE(t.event_country, s.event_country),
                event_province = COALESCE(t.event_province, s.event_province),
                event_city = COALESCE(t.event_city, s.event_city),
                is_duplicate = COALESCE(t.is_duplicate, s.is_duplicate),
                duplicate_group_id = COALESCE(t.duplicate_group_id, s.duplicate_group_id)
            FROM ai_risk_relevant_news s
            WHERE t.data_source IN ({placeholders})
              AND t.news_id = s.news_id
              AND t.data_source = s.data_source
        """
        try:
            self.cursor.execute(update_sql, english_sources)
            updated = self.cursor.rowcount
            self.conn.commit()
            logger.info(f"补充完成: {updated:,} 条记录")
            return updated
        except Exception as e:
            logger.error(f"补充字段失败: {e}")
            self.conn.rollback()
            return 0

    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {}

        self.cursor.execute("SELECT COUNT(*) FROM ai_risk_events_news")
        stats["total"] = self.cursor.fetchone()[0]

        self.cursor.execute(
            """
            SELECT data_source, COUNT(*)
            FROM ai_risk_events_news
            GROUP BY data_source
            ORDER BY data_source
            """
        )
        stats["by_source"] = dict(self.cursor.fetchall())

        self.cursor.execute(
            """
            SELECT classification_result, COUNT(*)
            FROM ai_risk_events_news
            GROUP BY classification_result
            """
        )
        stats["by_classification"] = dict(self.cursor.fetchall())

        return stats


# =========================
# CLI
# =========================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Import event-level news into ai_risk_events_news (auto jsonb/array) + debug trace.")

    p.add_argument("--truncate", action="store_true", help="TRUNCATE ai_risk_events_news before import.")
    p.add_argument("--skip-copy-non-english", action="store_true", help="Skip step1 SQL copy from ai_risk_relevant_news (non-english sources).")
    p.add_argument("--skip-import-english", action="store_true", help="Skip step2 import English JSON files.")
    p.add_argument("--skip-supplement", action="store_true", help="Skip step3 supplement missing fields.")
    p.add_argument("--batch-size", type=int, default=200, help="Batch size for execute_values.")
    p.add_argument("--max-files-per-source", type=int, default=0, help="Limit files per source (0 = no limit).")

    p.add_argument("--cc-news-path", type=str, default=None)
    p.add_argument("--aiid-path", type=str, default=None)
    p.add_argument("--aiaaic-path", type=str, default=None)

    # debug flags
    p.add_argument("--debug-import", action="store_true", help="对少量文件打印逐步调试信息（定位字段何时变空）")
    p.add_argument("--debug-limit", type=int, default=3, help="最多调试多少个文件（避免刷屏）")
    p.add_argument("--debug-news-ids", type=str, default="", help="只调试指定 news_id（逗号分隔，如 24274,3721）")
    p.add_argument("--debug-file-contains", type=str, default=None, help="只调试路径包含该子串的文件")
    p.add_argument("--debug-dump-cols", action="store_true", help="调试时把每个 insert 列的 raw 值也打印出来（更详细）")

    p.add_argument("--strict-varchar", action="store_true", help="varchar 超长时不截断，直接报错（便于追根因）")
    return p


def main():
    args = build_arg_parser().parse_args()

    db_config, english_sources = load_runtime_config()

    logger.info("导入配置:")
    logger.info(f"  数据库: {db_config['database']}")
    logger.info(f"  主机: {db_config['host']}:{db_config['port']}")
    logger.info(f"  batch_size: {args.batch_size}")
    if args.max_files_per_source:
        logger.info(f"  max_files_per_source: {args.max_files_per_source}")
    if args.strict_varchar:
        logger.info("  strict_varchar: True (varchar 超长将直接报错)")

    english_sources = dict(english_sources)
    if args.cc_news_path:
        english_sources["cc_news"]["path"] = args.cc_news_path
    if args.aiid_path:
        english_sources["aiid"]["path"] = args.aiid_path
    if args.aiaaic_path:
        english_sources["aiaaic"]["path"] = args.aiaaic_path

    importer = EventNewsImporter(db_config=db_config, english_sources=english_sources, strict_varchar=args.strict_varchar)

    # parse debug news ids
    dbg_news_ids: List[int] = []
    if args.debug_news_ids:
        for x in args.debug_news_ids.split(","):
            x = x.strip()
            if not x:
                continue
            try:
                dbg_news_ids.append(int(x))
            except Exception:
                pass

    importer.configure_debug(
        enabled=args.debug_import,
        limit=args.debug_limit,
        news_ids=dbg_news_ids,
        file_contains=args.debug_file_contains,
        dump_cols=args.debug_dump_cols,
    )

    try:
        if not importer.connect():
            sys.exit(1)

        if args.truncate:
            importer.truncate_target_table()

        copied = 0
        imported = 0
        failed = 0
        supplemented = 0

        if not args.skip_copy_non_english:
            copied = importer.copy_non_english_sources_sql()

        if not args.skip_import_english:
            imported, failed = importer.import_english_sources(
                batch_size=args.batch_size,
                max_files_per_source=args.max_files_per_source,
            )

        if not args.skip_supplement:
            supplemented = importer.supplement_missing_fields_sql()

        stats = importer.get_statistics()

        logger.info("\n" + "=" * 100)
        logger.info("导入完成！统计信息:")
        logger.info("=" * 100)
        logger.info(f"总记录数: {stats['total']:,}")
        logger.info(f"非英文源 SQL复制: {copied:,}")
        logger.info(f"英文源 文件导入: {imported:,}")
        logger.info(f"补充缺失字段: {supplemented:,}")
        logger.info(f"导入失败: {failed:,}")

        logger.info("\n按数据源统计:")
        for source, count in stats.get("by_source", {}).items():
            logger.info(f"  {source}: {count:,}")

        logger.info("\n按分类统计:")
        for c, count in stats.get("by_classification", {}).items():
            logger.info(f"  {c}: {count:,}")

        logger.info("=" * 100)

    except KeyboardInterrupt:
        logger.warning("收到中断信号，准备退出...")
        if importer.conn:
            importer.conn.rollback()
    except Exception as e:
        logger.error(f"导入过程出错: {e}")
        if importer.conn:
            importer.conn.rollback()
        sys.exit(1)
    finally:
        importer.close()


if __name__ == "__main__":
    main()
