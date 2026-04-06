#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI风险新闻数据导入脚本
支持从多个数据源导入JSON文件到PostgreSQL数据库
"""

import json
import os
import sys
import re
import hashlib
import argparse
import configparser
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from tqdm import tqdm
except ImportError:
    # 如果没有tqdm，使用简单的进度显示
    class DummyTqdm:
        def __init__(self, iterable=None, total=None, desc=""):
            self.iterable = iterable
            self.total = total
            self.desc = desc
            self.n = 0
            if desc:
                print(f"{desc}: 开始处理...")

        def update(self, k=1):
            self.n += k

        def set_postfix(self, *args, **kwargs):
            # no-op
            return

        def close(self):
            if self.desc:
                print(f"{self.desc}: 完成（processed={self.n}）")

        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter([])

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()
            return False

    def tqdm(iterable=None, total=None, desc=""):
        return DummyTqdm(iterable, total, desc)

try:
    import psycopg2
    from psycopg2.extras import execute_values
    from psycopg2 import sql
except ImportError:
    print("请安装 psycopg2: pip install psycopg2-binary")
    sys.exit(1)

# 配置日志 - 使用带时间戳的日志文件名
log_dir = Path(__file__).resolve().parents[3] / 'outputs' / 'Database' / 'logs'
log_dir.mkdir(exist_ok=True, parents=True)
log_filename = log_dir / f'import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

# 配置日志格式
log_format = '%(asctime)s - %(levelname)s - %(message)s'
date_format = '%Y-%m-%d %H:%M:%S'

# 创建日志处理器
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(log_format, date_format))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(log_format, date_format))

# 配置根日志记录器
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt=date_format,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)
logger.info(f"日志文件: {log_filename}")
logger.info("=" * 80)
logger.info("开始数据导入任务")
logger.info("=" * 80)

# Content hash 计算相关常量
HASH_VERSION = 2  # normalize逻辑变了就+1
_whitespace_re = re.compile(r"\s+")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "Database-config.ini"


# ==========================
# 工具函数：解析列表参数
# ==========================
def _parse_str_list(s: str) -> List[str]:
    if not s:
        return []
    out = []
    for x in s.split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


def _parse_int_list(s: str) -> List[int]:
    if not s:
        return []
    out = []
    for x in s.split(","):
        x = x.strip()
        if not x:
            continue
        out.append(int(x))
    return out


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

    db_cfg = {
        "host": parser.get("Database", "host", fallback="localhost").strip() or "localhost",
        "database": parser.get("Database", "database", fallback="ai_risks_db").strip() or "ai_risks_db",
        "user": parser.get("Database", "user", fallback="postgres").strip() or "postgres",
        "password": parser.get("Database", "password", fallback=""),
        "port": int(parser.get("Database", "port", fallback="5432") or "5432"),
    }

    data_sources = [
        {"name": "cc_news", "path": _resolve_path(parser.get("Sources", "cc_news_dir", fallback="")), "description": "CommonCrawl News"},
        {"name": "opennews", "path": _resolve_path(parser.get("Sources", "opennews_dir", fallback="")), "description": "OpenNewsArchive"},
        {"name": "aiaaic", "path": _resolve_path(parser.get("Sources", "aiaaic_dir", fallback="")), "description": "AIAAIC"},
        {"name": "aiid", "path": _resolve_path(parser.get("Sources", "aiid_dir", fallback="")), "description": "AI Incident Database"},
        {"name": "wenge", "path": _resolve_path(parser.get("Sources", "wenge_dir", fallback="")), "description": "China Wenge News"},
    ]

    classification_dirs = _parse_str_list(
        parser.get(
            "Import",
            "classification_dirs",
            fallback="AIrisk_relevant_event,AIrisk_relevant_discussion",
        )
    )
    if not classification_dirs:
        classification_dirs = ["AIrisk_relevant_event", "AIrisk_relevant_discussion"]

    return db_cfg, data_sources, classification_dirs


class DatabaseImporter:
    """数据库导入器"""
    
    def __init__(self, db_config: Dict):
        """
        初始化导入器
        
        Args:
            db_config: 数据库配置字典，包含 host, database, user, password, port
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def extract_data_source_from_path(self, file_path: str) -> str:
        """
        从文件路径中提取数据源名称
        用于确定hash_name生成策略
        """
        path_lower = file_path.lower()
        if 'cc-news' in path_lower or 'cc_news' in path_lower:
            return 'cc_news'
        elif 'opennews' in path_lower:
            return 'opennews'
        elif 'wenge' in path_lower:
            return 'wenge'
        elif 'xiaohongshu' in path_lower:
            return 'xiaohongshu'
        elif 'weibo' in path_lower:
            return 'weibo'
        elif 'toutiao' in path_lower:
            return 'toutiao'
        elif 'aiid' in path_lower:
            return 'aiid'
        elif 'aiaaic' in path_lower:
            return 'aiaaic'
        else:
            return 'unknown'

    def parse_archive_year_month_from_path(self, file_path: str, data_source: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        从文件路径解析归档年月信息

        Args:
            file_path: 文件完整路径
            data_source: 数据源名称

        Returns:
            (archive_year, archive_month, archive_ym) 元组，None表示未解析到
        """
        # 只有cc_news和wenge数据源才解析归档年月
        if data_source not in ['cc_news', 'wenge']:
            return None, None, None

        try:
            path_parts = Path(file_path).parts

            # 查找年份目录（4位数字）
            year_part = None
            month_part = None

            for part in path_parts:
                # 查找年份（4位数字，合理范围）
                if not year_part and re.fullmatch(r'20\d{2}|19\d{2}', part):
                    year = int(part)
                    if 1990 <= year <= 2100:
                        year_part = year

                # 查找月份（在找到年份后）
                elif year_part and not month_part:
                    # 尝试匹配月份：1, 01, 2, 02, ..., 12
                    if re.fullmatch(r'(0?[1-9]|1[0-2])', part):
                        month_part = int(part.lstrip('0'))  # 移除前导0
                        break  # 找到月份后停止

            # 计算archive_ym
            archive_ym = None
            if year_part and month_part:
                archive_ym = year_part * 100 + month_part

            return year_part, month_part, archive_ym

        except Exception:
            # 解析失败时返回None
            return None, None, None

    def connect(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_config.get('host', 'localhost'),
                database=self.db_config.get('database', 'ai_risks_db'),
                user=self.db_config.get('user', 'postgres'),
                password=self.db_config.get('password', ''),
                port=self.db_config.get('port', 5432)
            )
            self.cursor = self.conn.cursor()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("数据库连接已关闭")
    
    def _clean_null_chars(self, value):
        """清理字符串中的NULL字符（\x00 和 \u0000），PostgreSQL不允许字符串包含NULL字符"""
        if value is None:
            return None
        if isinstance(value, str):
            # 清理 \x00 和 \u0000
            return value.replace('\x00', '').replace('\u0000', '')
        if isinstance(value, dict):
            return {k: self._clean_null_chars(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._clean_null_chars(item) for item in value]
        return value

    def _to_text(self, v) -> str:
        """统一转换为字符串，处理各种数据类型"""
        if v is None:
            return ""
        if isinstance(v, str):
            return v
        try:
            # 结构化内容用 JSON 串更可读
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)

    def _ensure_str_list(self, v) -> list:
        """确保返回字符串列表，处理各种输入类型"""
        if v is None:
            return []
        if isinstance(v, list):
            out = []
            for x in v:
                if x is None:
                    continue
                if isinstance(x, str):
                    s = x.strip()
                    if s:
                        out.append(s)
                else:
                    # 非字符串元素也转成字符串
                    s = self._to_text(x).strip()
                    if s:
                        out.append(s)
            return out
        # 单值：包一层
        s = self._to_text(v).strip()
        return [s] if s else []

    def normalize_content(self, content: str) -> str:
        """更稳的内容规范化：去首尾空白 + 压缩所有空白为单空格"""
        return _whitespace_re.sub(" ", content).strip()

    def compute_content_hash(self, content: str) -> str:
        """计算content的SHA256哈希值（基于规范化后的文本）"""
        norm = self.normalize_content(content)
        return hashlib.sha256(norm.encode("utf-8")).hexdigest()

    def extract_flattened_fields(self, data: Dict) -> Dict:
        """从JSON数据中提取扁平化字段"""
        flattened = {}

        # 从标准化后的数据中提取
        ai_tech = data.get('ai_tech', {})
        ai_risk = data.get('ai_risk', {})
        event = data.get('event', {})

        # ai_tech 扁平化字段（确保数组字段是正确的字符串列表）
        flattened['ai_system_list'] = self._ensure_str_list(ai_tech.get('ai_system_list'))
        flattened['ai_system_type_list'] = self._ensure_str_list(ai_tech.get('ai_system_type_list'))
        flattened['ai_system_domain_list'] = self._ensure_str_list(ai_tech.get('ai_system_domain_list'))

        # ai_risk 扁平化字段（单值TEXT字段，确保类型安全）
        flattened['ai_risk_description'] = self._to_text(self._extract_single_value(ai_risk.get('ai_risk_description'))) or None
        flattened['ai_risk_type'] = self._to_text(self._extract_single_value(ai_risk.get('ai_risk_type'))) or None
        flattened['ai_risk_subtype'] = self._to_text(self._extract_single_value(ai_risk.get('ai_risk_subtype'))) or None
        flattened['harm_type'] = self._to_text(self._extract_single_value(ai_risk.get('harm_type'))) or None
        flattened['harm_severity'] = self._to_text(self._extract_single_value(ai_risk.get('harm_severity'))) or None
        flattened['affected_actor_type'] = self._to_text(self._extract_single_value(ai_risk.get('affected_actor_type'))) or None
        flattened['affected_actor_subtype'] = self._to_text(self._extract_single_value(ai_risk.get('affected_actor_subtype'))) or None
        flattened['realized_or_potential'] = self._to_text(self._extract_single_value(ai_risk.get('realized_or_potential'))) or None
        flattened['risk_stage'] = self._to_text(self._extract_single_value(ai_risk.get('risk_stage'))) or None

        # event 扁平化字段（单值TEXT字段，确保类型安全）
        flattened['event_actor_main'] = self._to_text(self._extract_single_value(event.get('actor_main'))) or None
        flattened['event_actor_main_type'] = self._to_text(self._extract_single_value(event.get('actor_main_type'))) or None
        flattened['event_actor_list'] = self._ensure_str_list(event.get('actor_list'))
        flattened['event_ai_system'] = self._to_text(self._extract_single_value(event.get('ai_system'))) or None
        flattened['event_domain'] = self._to_text(self._extract_single_value(event.get('domain'))) or None
        flattened['event_type'] = self._to_text(self._extract_single_value(event.get('event_type'))) or None
        flattened['event_cause'] = self._to_text(self._extract_single_value(event.get('event_cause'))) or None
        flattened['event_process'] = self._to_text(self._extract_single_value(event.get('event_process'))) or None
        flattened['event_result'] = self._to_text(self._extract_single_value(event.get('event_result'))) or None

        # 清理所有字段中的NULL字符
        for key, value in flattened.items():
            flattened[key] = self._clean_null_chars(value)

        return flattened
    
    def _extract_single_value(self, value):
        """从字段中提取单个值（处理列表、null、空字符串等情况）"""
        if value is None:
            return None
        if isinstance(value, list):
            # 取第一个非null、非空字符串的元素
            for item in value:
                if item is not None and item != "":
                    return item
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return value
    
    def extract_event_fields(self, event_data) -> Dict:
        """从event JSON中提取关键字段（处理列表、null、空字符串等情况）"""
        if not event_data:
            return {}
        
        # 获取原始日期描述（处理列表类型）
        start_desc = self._extract_single_value(event_data.get('event_time_start'))
        end_desc = self._extract_single_value(event_data.get('event_time_end'))
        
        # 保存原始描述（如果是列表，转换为JSON字符串）
        start_desc_original = event_data.get('event_time_start')
        end_desc_original = event_data.get('event_time_end')
        
        # 将原始描述转换为字符串（如果是列表，转为JSON）
        if isinstance(start_desc_original, list):
            start_desc_str = json.dumps(start_desc_original, ensure_ascii=False)
        elif start_desc_original is not None:
            start_desc_str = str(start_desc_original)
        else:
            start_desc_str = None
            
        if isinstance(end_desc_original, list):
            end_desc_str = json.dumps(end_desc_original, ensure_ascii=False)
        elif end_desc_original is not None:
            end_desc_str = str(end_desc_original)
        else:
            end_desc_str = None
        
        return {
            # 原始日期描述（保留原始文本，包括列表格式）
            'event_time_start_desc': start_desc_str,
            'event_time_end_desc': end_desc_str,
            # 标准化的日期（解析后的标准格式）
            'event_time_start': self._parse_date(start_desc),
            'event_time_end': self._parse_date(end_desc),
            'event_country': self._extract_single_value(event_data.get('event_country')),
            'event_province': self._extract_single_value(event_data.get('event_province')),
            'event_city': self._extract_single_value(event_data.get('event_city')),
        }
    
    def _parse_date(self, date_value) -> Optional[str]:
        """解析日期值，返回YYYY-MM-DD格式
        
        支持多种日期格式：
        - 标准格式: "2022-01-01", "2022-01", "2022"
        - 带空格: "2022 - 01 - 01" -> "2022-01-01"
        - 相对时间: "2022-01-01之前一周" -> "2022-01-01"
        - 中文描述: "2020年末" -> "2020-12-31"
        - 列表类型: ["2021-02", null] -> "2021-02" (取第一个非null元素)
        """
        if not date_value:
            return None
        
        # 处理列表类型（取第一个非null元素）
        if isinstance(date_value, list):
            for item in date_value:
                if item is not None and item != "":
                    date_value = item
                    break
            else:
                return None  # 列表中没有有效值
        
        # 确保是字符串类型
        if not isinstance(date_value, str):
            if date_value is None:
                return None
            date_value = str(date_value)
        
        # 清理日期字符串
        original_str = date_value.strip()
        if not original_str:
            return None
        date_str = original_str
        
        # 【关键修复】先从原始字符串里直接抓 YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD（含时间也不怕）
        # 必须在删除空格之前处理，避免 "2022-01-01 12:00:00" 变成 "2022-01-0112:00:00"
        m = re.search(r'(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})', original_str)
        if m:
            y, mo, d = m.group(1), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= d <= 31:
                try:
                    dt = datetime(int(y), mo, d)
                    return dt.strftime('%Y-%m-%d')
                except Exception:
                    pass
        
        # 处理相对时间描述，提取其中的日期部分
        # 如 "2022-01-01之前一周" -> "2022-01-01"
        # 匹配日期模式（YYYY-MM-DD 或 YYYY-MM 或 YYYY）
        date_pattern = r'(\d{4}(?:-\d{1,2}(?:-\d{1,2})?)?)'
        match = re.search(date_pattern, date_str)
        if match:
            date_str = match.group(1)
        
        # 处理中文描述的时间
        # "2020年末" -> "2020-12-31"
        if '年末' in original_str or '年底' in original_str:
            year_match = re.search(r'(\d{4})', original_str)
            if year_match:
                year = year_match.group(1)
                if 1900 <= int(year) <= 2100:
                    return f"{year}-12-31"
        elif '年初' in original_str:
            year_match = re.search(r'(\d{4})', original_str)
            if year_match:
                year = year_match.group(1)
                if 1900 <= int(year) <= 2100:
                    return f"{year}-01-01"
        
        # 处理带空格的日期格式，如 "2022 - 02" -> "2022-02"
        date_str = date_str.replace(' - ', '-').replace(' -', '-').replace('- ', '-')
        # 移除所有空格
        date_str = date_str.replace(' ', '')
        
        try:
            # 尝试解析各种日期格式
            if len(date_str) == 10 and date_str.count('-') == 2:
                # YYYY-MM-DD格式
                datetime.strptime(date_str, '%Y-%m-%d')
                return date_str
            elif len(date_str) == 7 and date_str.count('-') == 1:
                # YYYY-MM格式，返回该月第一天
                datetime.strptime(date_str, '%Y-%m')
                return f"{date_str}-01"
            elif len(date_str) == 4 and date_str.isdigit():
                # 只有年份，返回该年第一天
                year = int(date_str)
                if 1900 <= year <= 2100:  # 合理的年份范围
                    return f"{date_str}-01-01"
            elif 'T' in date_str:
                # ISO格式，如 "2022-01-01T12:00:00"
                date_part = date_str.split('T')[0]
                if len(date_part) == 10:
                    datetime.strptime(date_part, '%Y-%m-%d')
                    return date_part
            elif ' ' in date_str or ':' in date_str:
                # 包含时间的日期，只取日期部分
                date_part = date_str.split()[0].split(':')[0]
                if len(date_part) == 10 and date_part.count('-') == 2:
                    datetime.strptime(date_part, '%Y-%m-%d')
                    return date_part
            else:
                # 尝试直接解析其他格式
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        return dt.strftime('%Y-%m-%d')
                    except:
                        continue
        except Exception:
            # 静默处理解析失败，不输出警告
            pass
        
        # 如果所有解析都失败，尝试从原始字符串中提取年份
        year_match = re.search(r'(\d{4})', original_str)
        if year_match:
            year = int(year_match.group(1))
            if 1900 <= year <= 2100:
                # 至少返回年份的第一天
                return f"{year}-01-01"
        
        return None
    
    def _normalize_section(self, section_data):
        """标准化section数据：处理null、空字符串、列表等情况"""
        if section_data is None:
            return {}
        if isinstance(section_data, str):
            if section_data.strip() == "":
                return {}
            # 尝试解析为JSON
            try:
                parsed = json.loads(section_data)
                if isinstance(parsed, dict):
                    return parsed
                elif isinstance(parsed, list) and len(parsed) > 0:
                    # 如果是列表，取第一个元素
                    if isinstance(parsed[0], dict):
                        return parsed[0]
            except:
                pass
            return {}
        if isinstance(section_data, list):
            if len(section_data) > 0:
                # 取第一个元素
                first_item = section_data[0]
                if isinstance(first_item, dict):
                    return first_item
            return {}
        if isinstance(section_data, dict):
            return section_data
        return {}
    
    def _normalize_event_data(self, event_data):
        """标准化event数据：处理多事件情况，只取第一个值，但保留列表字段
        
        对于多事件字段（如event_time_start、event_location等），如果是列表，只取第一个值
        对于列表字段（如actor_list、object_list等），保持列表形式
        """
        if not event_data or not isinstance(event_data, dict):
            return event_data if isinstance(event_data, dict) else {}
        
        # 定义多事件字段（这些字段在单事件时应该是单值，多事件时才是列表）
        # 对于这些字段，如果是列表，只取第一个值
        MULTI_EVENT_FIELDS = [
            'event_time_start',
            'event_time_end',
            'event_location',
            'event_location_country',
            'event_location_region',
            'event_location_city',
            'event_country',
            'event_province',
            'event_city',
            # 可以根据实际情况添加其他字段
        ]
        
        normalized = {}
        for key, value in event_data.items():
            # 如果字段名以_list结尾，说明本身就是列表类型，保持列表形式
            if key.endswith('_list'):
                normalized[key] = value
            # 如果是多事件字段且是列表，只取第一个非空值
            elif key in MULTI_EVENT_FIELDS and isinstance(value, list):
                # 取第一个非null、非空字符串的元素
                first_value = None
                for item in value:
                    if item is not None and item != "":
                        first_value = item
                        break
                normalized[key] = first_value
            # 其他情况保持原样
            else:
                normalized[key] = value
        
        return normalized
    
    def validate_data(self, data: Dict, file_path: str) -> Tuple[bool, List[str]]:
        """验证数据格式（允许null、空字符串、列表等格式，允许缺少某些字段）"""
        issues = []
        
        # 检查必需字段 classification_result（必须非空，因为数据库有 NOT NULL 约束）
        cr = data.get('classification_result')
        if cr is None:
            issues.append("缺少 classification_result 字段")
        else:
            # 提取并验证值非空
            cr_value = self._to_text(self._extract_single_value(cr)).strip()
            if not cr_value:
                issues.append("classification_result 为空或无效")

        # 检查content是否有效（排除结构化空内容）
        content = data.get('content', '')
        if not content or str(content).strip() in ('', '[]', '{}', 'null'):
            issues.append("缺少或空的 content 字段")
        
        # 检查JSON格式（允许null、空字符串、列表、字典，也允许缺少字段）
        # 如果缺少字段，会在标准化时设为空字典
        required_sections = ['ai_tech', 'ai_risk', 'event']
        for section in required_sections:
            if section not in data:
                # 允许缺少字段，会在标准化时处理
                continue
            else:
                section_data = data[section]
                # 允许null、空字符串、列表、字典
                if section_data is not None and section_data != "":
                    if not isinstance(section_data, (dict, list, str)):
                        issues.append(f"{section} 字段格式错误，应为字典、列表或字符串类型")
        
        return len(issues) == 0, issues
    
    def build_row_vals(self, data_source: str, json_file: Path, data: Dict):
        """
        构建单条记录的vals，用于批量插入
        返回: (cols, vals, mapping) 元组，其中mapping是文件路径映射信息
        如果数据无效则返回None
        """
        # ==========================================
        # 1. 清理基础数据（顺序很重要：先清理，再计算hash，再提取字段）
        # ==========================================

        # 清理基础字符串字段（先转文本，再清理）
        cleaned_case_id = self._clean_null_chars(self._to_text(data.get('case_id'))) or None
        raw_news_id = data.get('news_id', data.get('case_id'))
        try:
            cleaned_news_id = int(raw_news_id) if raw_news_id not in (None, "") else None
        except Exception:
            cleaned_news_id = None
        cleaned_title = self._clean_null_chars(self._to_text(data.get('title'))) or None
        cleaned_content = self._clean_null_chars(self._to_text(data.get('content')))

        # 计算content_hash（基于normalize后的内容，只有非空内容才计算hash）
        norm_content = self.normalize_content(cleaned_content)
        content_hash = hashlib.sha256(norm_content.encode("utf-8")).hexdigest() if norm_content else None

        # ==========================================
        # 2. 准备normalized_data（优化：避免完整copy，只读访问）
        # ==========================================

        # 优化：不完整copy，直接使用原data，只在需要修改时创建新dict
        normalized_data = {
            **data,  # 浅拷贝，避免深拷贝开销
            'case_id': cleaned_case_id,
            'news_id': cleaned_news_id,
            'title': cleaned_title,
            'content': norm_content  # 使用normalize后的内容
        }

        # ==========================================
        # 3. 标准化JSON数据格式
        # ==========================================

        # 处理 ai_system 字段（某些数据源使用 ai_system 而不是 ai_tech）
        if 'ai_system' in normalized_data and 'ai_tech' not in normalized_data:
            normalized_data['ai_tech'] = normalized_data.pop('ai_system')

        # 标准化ai_tech和ai_risk（如果缺少，设为空字典）
        for section in ['ai_tech', 'ai_risk']:
            if section in normalized_data:
                normalized_data[section] = self._normalize_section(normalized_data[section])
            else:
                normalized_data[section] = {}

        # 对于event字段，使用特殊的标准化方法（处理多事件，但保留列表字段）
        if 'event' in normalized_data:
            event_section = self._normalize_section(normalized_data['event'])
            normalized_data['event'] = self._normalize_event_data(event_section)
        else:
            normalized_data['event'] = {}

        # ==========================================
        # 4. 清理JSON字段（现在normalized_data已准备好）
        # ==========================================

        cleaned_ai_tech = self._clean_null_chars(normalized_data.get('ai_tech', {}))
        cleaned_ai_risk = self._clean_null_chars(normalized_data.get('ai_risk', {}))
        cleaned_event = self._clean_null_chars(normalized_data.get('event', {}))

        # ==========================================
        # 5. 提取扁平化字段（现在cleaned_*变量已定义）
        # ==========================================

        flattened_fields = self.extract_flattened_fields({
            'ai_tech': cleaned_ai_tech,
            'ai_risk': cleaned_ai_risk,
            'event': cleaned_event
        })

        # ==========================================
        # 6. 提取hash_name和时间字段
        # ==========================================

        # 拆分变量：source_name用于写入数据库，detected_source仅用于hash_name策略选择
        source_name = data_source  # 传入的配置名
        file_path_str = str(json_file)
        detected_source = self.extract_data_source_from_path(file_path_str)

        # 生成hash_name（数据源特定的策略）
        hash_name = None

        if data_source == 'cc_news':
            # 策略1: CC-NEWS - 从文件路径中提取现有的hash_name
            if file_path_str.endswith('_result.json'):
                stem = json_file.stem  # abc_result
                hash_name = stem[:-7] if stem.endswith("_result") else stem  # abc

        elif data_source == 'opennews':
            # 策略2: OpenNewsArchive - 从路径提取id，然后进行hash
            if file_path_str.endswith('_result.json'):
                # 先提取id (如: 123_result.json -> 123)
                stem = json_file.stem
                original_id = stem[:-7] if stem.endswith("_result") else stem
                # 然后对id进行hash，保持与其他数据源的一致性
                if original_id and original_id.isdigit():
                    hash_name = hashlib.md5(original_id.encode()).hexdigest()[:16]
                else:
                    hash_name = hashlib.md5(original_id.encode()).hexdigest()[:16]

        else:
            # 策略3: 其他数据源 - 使用文件路径的hash值
            hash_name = hashlib.md5(file_path_str.encode()).hexdigest()[:16]

        # 兜底方案：如果hash_name为空，使用路径hash
        if not hash_name:
            hash_name = hashlib.md5(file_path_str.encode()).hexdigest()[:16]

        # ==========================================
        # 7. 解析归档年月信息
        # ==========================================

        archive_year, archive_month, archive_ym = self.parse_archive_year_month_from_path(file_path_str, data_source)

        # ==========================================
        # 8. 提取事件字段和时间字段
        # ==========================================

        # 提取事件字段（extract_event_fields已经处理了原始时间描述）
        event_data = normalized_data.get('event', {})
        event_fields = self.extract_event_fields(event_data)

        # ==========================================
        # 7. 数据验证
        # ==========================================

        is_valid, issues = self.validate_data(normalized_data, str(json_file))
        if not is_valid:
            # 不记录日志，避免频繁I/O，在批量插入时统一处理
            return None

        # ==========================================
        # 7.5. 准备文件路径映射信息（返回给主线程批量处理）
        # ==========================================

        # 获取文件大小
        try:
            file_size = json_file.stat().st_size
        except:
            file_size = None

        # 准备mapping信息，返回给主线程处理
        mapping = (file_path_str, hash_name, source_name, file_size)

        # ==========================================
        # 8. 清理剩余字段
        # ==========================================

        release_date = self._parse_date(data.get('release_date'))

        cleaned_start_desc = self._clean_null_chars(event_fields.get('event_time_start_desc'))
        cleaned_end_desc = self._clean_null_chars(event_fields.get('event_time_end_desc'))
        cleaned_country = self._clean_null_chars(event_fields.get('event_country'))
        cleaned_province = self._clean_null_chars(event_fields.get('event_province'))
        cleaned_city = self._clean_null_chars(event_fields.get('event_city'))

        # ==========================================
        # 9. 构建vals（用于批量插入）
        # ==========================================

        # 使用Json()处理JSONB字段，避免类型转换问题
        from psycopg2.extras import Json

        # 处理 classification_result（必须非空，因为数据库有 NOT NULL 约束）
        classification_value = self._to_text(self._extract_single_value(data.get('classification_result'))).strip()
        if not classification_value:
            # 不记录日志，避免频繁I/O，在批量插入时统一处理
            return None

        # 列定义（固定不变）
        cols = [
            "news_id", "data_source", "file_path",
            "archive_year", "archive_month", "archive_ym",  # 新增：归档年月信息
            "hash_name", "content_hash", "content_hash_version", "normalize_rule",
            "classification_result", "classification_std_result",
            "title", "content", "release_date",
            "ai_tech", "ai_risk", "event",
            "ai_system_list", "ai_system_type_list", "ai_system_domain_list",
            "ai_risk_description", "ai_risk_type", "ai_risk_subtype", "harm_type", "harm_severity",
            "affected_actor_type", "affected_actor_subtype", "realized_or_potential", "risk_stage",
            "event_actor_main", "event_actor_main_type", "event_actor_list",
            "event_ai_system", "event_domain", "event_type", "event_cause", "event_process", "event_result",
            "event_time_start_desc", "event_time_end_desc",
            "event_time_start", "event_time_end",
            "event_country", "event_province", "event_city",
        ]

        vals = [
            cleaned_news_id,
            data_source,
            str(json_file),
            archive_year, archive_month, archive_ym,  # 新增：归档年月信息
            hash_name,
            content_hash,
            HASH_VERSION,
            'collapse_whitespace_strip',
            classification_value,
            None,  # classification_std_result
            cleaned_title,
            norm_content,
            release_date,
            Json(cleaned_ai_tech),
            Json(cleaned_ai_risk),
            Json(cleaned_event),
            flattened_fields['ai_system_list'],
            flattened_fields['ai_system_type_list'],
            flattened_fields['ai_system_domain_list'],
            flattened_fields['ai_risk_description'],
            flattened_fields['ai_risk_type'],
            flattened_fields['ai_risk_subtype'],
            flattened_fields['harm_type'],
            flattened_fields['harm_severity'],
            flattened_fields['affected_actor_type'],
            flattened_fields['affected_actor_subtype'],
            flattened_fields['realized_or_potential'],
            flattened_fields['risk_stage'],
            flattened_fields['event_actor_main'],
            flattened_fields['event_actor_main_type'],
            flattened_fields['event_actor_list'],
            flattened_fields['event_ai_system'],
            flattened_fields['event_domain'],
            flattened_fields['event_type'],
            flattened_fields['event_cause'],
            flattened_fields['event_process'],
            flattened_fields['event_result'],
            cleaned_start_desc,
            cleaned_end_desc,
            event_fields.get('event_time_start'),
            event_fields.get('event_time_end'),
            cleaned_country,
            cleaned_province,
            cleaned_city
        ]

        # 验证列数和值数量匹配
        if len(cols) != len(vals):
            logger.error(f"列数和值数量不匹配: cols={len(cols)}, vals={len(vals)}, file={json_file}")
            return None

        return (cols, vals, mapping)

    def save_file_path_mapping(self, file_path: str, hash_name: str, data_source: str, file_size: Optional[int]):
        """
        保存文件路径到hash_name的映射关系
        使用SAVEPOINT保护主事务，避免单条SQL失败导致整个事务aborted
        """
        if not hash_name:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute("SAVEPOINT sp_fpm")
                cur.execute("""
                    INSERT INTO file_path_mappings (file_path, hash_name, data_source, file_size)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (file_path) DO UPDATE SET
                        hash_name = EXCLUDED.hash_name,
                        data_source = EXCLUDED.data_source,
                        file_size = EXCLUDED.file_size,
                        updated_at = CURRENT_TIMESTAMP
                """, (file_path, hash_name, data_source, file_size))
                cur.execute("RELEASE SAVEPOINT sp_fpm")
        except Exception as e:
            # 关键：回滚到savepoint，避免整个事务abort
            try:
                with self.conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_fpm")
                    cur.execute("RELEASE SAVEPOINT sp_fpm")
            except Exception:
                pass
            logger.warning(f"保存文件路径映射失败 {file_path}: {e}")

    def bulk_upsert_file_path_mappings(self, mappings: List[tuple]):
        """
        批量UPSERT文件路径映射（只在主线程调用）
        mappings格式: [(file_path, hash_name, data_source, file_size), ...]
        """
        if not mappings:
            return

        from psycopg2.extras import execute_values

        sp = "sp_fpm_bulk"
        self.cursor.execute(f"SAVEPOINT {sp}")
        try:
            sql = """
                INSERT INTO file_path_mappings (file_path, hash_name, data_source, file_size)
                VALUES %s
                ON CONFLICT (file_path) DO UPDATE SET
                    hash_name = EXCLUDED.hash_name,
                    data_source = EXCLUDED.data_source,
                    file_size = EXCLUDED.file_size,
                    updated_at = CURRENT_TIMESTAMP
            """
            execute_values(self.cursor, sql, mappings, page_size=1000)
            self.cursor.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            self.cursor.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            self.cursor.execute(f"RELEASE SAVEPOINT {sp}")
            raise

    def _bulk_insert_once(self, cols: List[str], rows: List[tuple]) -> int:
        """
        真正执行一次批量写入；返回 inserted 行数（准确）
        使用 RETURNING 1 来准确统计插入数量
        """
        from psycopg2.extras import execute_values
        
        insert_sql = f"""
            INSERT INTO ai_risk_relevant_news ({",".join(cols)})
            VALUES %s
            ON CONFLICT (data_source, file_path) WHERE file_path IS NOT NULL DO NOTHING
            RETURNING 1
        """
        
        execute_values(self.cursor, insert_sql, rows, page_size=1000)
        # RETURNING 1 的返回行数 = 实际插入行数（准确）
        return len(self.cursor.fetchall())
    
    def _bulk_insert_safe(self, cols: List[str], rows: List[tuple], files: List[Path], data_source_name: str) -> Tuple[int, int]:
        """
        批量失败时二分定位坏数据。
        返回 (inserted_count, failed_count)
        files 与 rows 对齐，用于记录坏文件。
        使用 SAVEPOINT 避免递归时互相污染。
        """
        if not rows:
            return 0, 0
        
        # 使用 SAVEPOINT 保护当前批次，避免递归时互相污染
        sp_name = "sp_" + hashlib.md5(("|".join([str(files[0]), str(len(rows))])).encode("utf-8")).hexdigest()[:10]
        self.cursor.execute(f"SAVEPOINT {sp_name}")
        
        try:
            inserted = self._bulk_insert_once(cols, rows)
            self.cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
            return inserted, 0
        except Exception as e:
            # 回滚到该 savepoint，不影响同事务里之前成功插入的其他部分
            self.cursor.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
            self.cursor.execute(f"RELEASE SAVEPOINT {sp_name}")
            
            # 批量失败，拆分
            if len(rows) == 1:
                bad_file = files[0]
                logger.error(f"单条仍失败，跳过: {bad_file} err={e}")
                # 此时事务已恢复正常，可以写日志表
                self._log_import_error(data_source_name, bad_file, str(e))
                return 0, 1
            
            # 二分递归
            mid = len(rows) // 2
            ins1, fail1 = self._bulk_insert_safe(cols, rows[:mid], files[:mid], data_source_name)
            ins2, fail2 = self._bulk_insert_safe(cols, rows[mid:], files[mid:], data_source_name)
            return ins1 + ins2, fail1 + fail2
    
    def bulk_insert(self, cols: List[str], rows: List[tuple], files: List[Path], data_source_name: str) -> Tuple[int, int]:
        """
        对外：批量插入（带失败定位）
        返回 (inserted_count, failed_count)
        """
        return self._bulk_insert_safe(cols, rows, files, data_source_name)

    # ==========================
    # 年月目录结构处理方法
    # ==========================
    def _looks_like_year_dir(self, p: Path) -> bool:
        return p.is_dir() and re.fullmatch(r"\d{4}", p.name) is not None

    def _has_year_month_structure(self, classification_path: Path) -> bool:
        """粗略判断是否存在 classification/YYYY/... 结构"""
        try:
            for child in classification_path.iterdir():
                if self._looks_like_year_dir(child):
                    return True
        except Exception:
            return False
        return False

    def iter_result_files_by_year_month(
        self,
        classification_path: Path,
        target_years: Optional[List[int]],
        target_months: Optional[List[int]],
    ):
        """
        优先：classification/YYYY/MM 目录定位（最快最准）
        如果未指定 years/months：扫全目录
        如果指定了 years/months 但目录结构不存在：回退扫全（后续可用 release_date 二次过滤）
        """
        if not target_years and not target_months:
            yield from classification_path.rglob("*_result.json")
            return

        if not self._has_year_month_structure(classification_path):
            # 不具备结构，回退全扫（必要时在读JSON后做 release_date 过滤）
            yield from classification_path.rglob("*_result.json")
            return

        years = target_years or []
        months = target_months or []

        if years and not months:
            for y in years:
                ydir = classification_path / f"{y:04d}"
                if not ydir.exists():
                    logger.warning(f"目录不存在（跳过）: {ydir}")
                    continue
                yield from ydir.rglob("*_result.json")
            return

        if years and months:
            for y in years:
                ydir = classification_path / f"{y:04d}"
                if not ydir.exists():
                    logger.warning(f"目录不存在（跳过）: {ydir}")
                    continue
                for m in months:
                    m_int = int(m)
                    candidates = [ydir / str(m_int), ydir / f"{m_int:02d}"]
                    found = False
                    for mdir in candidates:
                        if mdir.exists():
                            found = True
                            yield from mdir.rglob("*_result.json")
                    if not found:
                        logger.warning(f"月份目录不存在（跳过）: {ydir}/({m_int} or {m_int:02d})")
            return

        # 只给了 months 没给 years：不建议，但给个可预期行为（扫全再二次过滤）
        yield from classification_path.rglob("*_result.json")
    
    def insert_record(self, data_source: str, json_file: Path, data: Dict):
        """
        插入单条记录（保留用于向后兼容，但推荐使用批量插入）
        返回状态: True=inserted, None=duplicate, False=error
        """
        result = self.build_row_vals(data_source, json_file, data)
        if result is None:
            return False

        cols, vals, mapping = result
        # 对于单条插入，我们仍然需要保存mapping
        self.bulk_upsert_file_path_mappings([mapping])
        
        try:
            # 动态生成占位符
            placeholders = ",".join(["%s"] * len(cols))
            insert_query = f"""
                INSERT INTO ai_risk_relevant_news ({",".join(cols)})
                VALUES ({placeholders})
                ON CONFLICT (data_source, file_path) WHERE file_path IS NOT NULL DO NOTHING
                RETURNING 1
            """
            
            self.cursor.execute(insert_query, vals)
            result = self.cursor.fetchone()
            if result is not None:
                return True  # inserted
            else:
                return None  # duplicate
        except Exception as e:
            logger.error(f"插入记录失败 {json_file}: {e}")
            self._log_import_error(data_source, json_file, str(e))
            return False  # error
    
    def _log_quality_issue(self, file_path: Path, issues: List[str]):
        """记录数据质量问题"""
        # 这里可以记录到数据库的 data_quality_issues 表
        for issue in issues:
            logger.warning(f"数据质量问题 [{file_path}]: {issue}")
    
    def _log_import_error(self, data_source: str, file_path: Path, error_msg: str):
        """记录导入错误"""
        try:
            self.cursor.execute("""
                INSERT INTO import_logs (data_source, file_path, status, error_message)
                VALUES (%s, %s, %s, %s)
            """, (data_source, str(file_path), 'failed', error_msg))
        except:
            pass  # 如果记录日志失败，不影响主流程
    
    def _log_import_success(self, data_source: str, file_path: Path, records_count: int):
        """记录导入成功"""
        try:
            self.cursor.execute("""
                INSERT INTO import_logs (data_source, file_path, status, records_imported)
                VALUES (%s, %s, %s, %s)
            """, (data_source, str(file_path), 'success', records_count))
        except:
            pass
    
    def import_data_source(self, data_source_name: str, base_path: str,
                          classification_dirs: List[str], skip_existing: bool = True,
                          target_years: Optional[List[int]] = None,
                          target_months: Optional[List[int]] = None):
        """
        导入单个数据源

        Args:
            data_source_name: 数据源名称
            base_path: 数据源基础路径
            classification_dirs: 分类目录列表（只导入这些分类）
            skip_existing: 是否跳过已存在的文件（通过数据库约束处理）
            target_years: 目标年份列表，如果为None或空列表则处理所有年份
            target_months: 目标月份列表，如果为None或空列表则处理所有月份
        """
        base_path_obj = Path(base_path)
        if not base_path_obj.exists():
            logger.error(f"路径不存在: {base_path}")
            return

        total_files = 0
        imported_count = 0
        failed_count = 0
        skipped_count = 0
        skipped_dirs = []

        # 年月过滤配置
        if target_years:
            logger.info(f"[数据源 {data_source_name}] 仅处理以下年份: {target_years}")
        if target_months:
            logger.info(f"[数据源 {data_source_name}] 仅处理以下月份: {target_months}")

        # 检查所有可能的分类目录
        all_possible_dirs = ['AIrisk_relevant_event', 'AIrisk_relevant_discussion', 'AIrisk_Irrelevant']
        for possible_dir in all_possible_dirs:
            if possible_dir not in classification_dirs:
                skipped_dirs.append(possible_dir)

        for classification in classification_dirs:
            classification_path = base_path_obj / classification

            if not classification_path.exists():
                logger.warning(f"分类目录不存在: {classification_path}")
                continue

            logger.info(
                f"数据源: {data_source_name}, 分类: {classification}, 开始导入... "
                f"(years={target_years or 'ALL'}, months={target_months or 'ALL'}, skip_existing={skip_existing})"
            )

            # 批量插入配置（增大批量大小提升性能）
            batch_size = 2000
            batch_rows = []
            batch_files = []
            batch_mappings = []
            cols = None  # 第一次构建时设置

            # 失败计数器（减少日志I/O）
            error_count_in_batch = 0
            last_error_file = None

            # 判断是否具备 year/month 目录结构
            has_struct = self._has_year_month_structure(classification_path)
            # 若不具备结构且指定了年月，则需要在读JSON后根据 release_date 过滤
            need_release_date_filter = (not has_struct) and (bool(target_years) or bool(target_months))

            def _in_target_year_month(release_date_str: Optional[str]) -> bool:
                """release_date_str: YYYY-MM-DD"""
                if not (target_years or target_months):
                    return True
                if not release_date_str:
                    return False
                try:
                    y = int(release_date_str[0:4])
                    m = int(release_date_str[5:7])
                except Exception:
                    return False
                if target_years and y not in target_years:
                    return False
                if target_months and m not in target_months:
                    return False
                return True

            def process_file(json_file: Path):
                """线程内：读JSON +（必要时按release_date过滤）+ 构建vals"""
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    if need_release_date_filter:
                        rd = self._parse_date(data.get("release_date"))
                        if not _in_target_year_month(rd):
                            # 标记为"过滤跳过"（不计入失败）
                            return (json_file, None, ("filtered", "release_date not in target"))

                    result = self.build_row_vals(data_source_name, json_file, data)
                    return (json_file, result, None)
                except json.JSONDecodeError as e:
                    return (json_file, None, ("json_error", str(e)))
                except Exception as e:
                    return (json_file, None, ("other_error", str(e)))

            # 流式处理：边遍历边提交任务，不预先收集所有文件
            # 增加线程数以提升IO性能（IO密集型任务，可以设置更多线程）
            max_workers = min(16, (os.cpu_count() or 4) * 2)
            pbar = tqdm(desc=f"导入 {data_source_name}/{classification}")

            # 初始化处理计数器
            processed = 0

            # 获取文件迭代器（优先按目录定位，若有年月结构）
            file_iter = self.iter_result_files_by_year_month(classification_path, target_years, target_months)
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                
                # 先提交一批任务填满线程池（预热）
                for _ in range(max_workers * 2):
                    try:
                        json_file = next(file_iter)
                        total_files += 1
                        future = executor.submit(process_file, json_file)
                        futures[future] = json_file
                    except StopIteration:
                        break
                
                # 流式处理：处理所有已完成的任务，提高线程池利用率
                while futures:
                    # 处理所有已完成的任务（移除break，提高性能）
                    completed_futures = []
                    for future in as_completed(futures):
                        json_file, result, error = future.result()
                        processed += 1
                        completed_futures.append(future)
                        
                        try:
                            if error:
                                # 处理错误
                                err_type, err_msg = error
                                if err_type == "filtered":
                                    # 仅当目录不含year/month结构且指定年月时，才会走到这里
                                    # 过滤不计为失败，只计为跳过（因为它确实不属于目标年月）
                                    skipped_count += 1
                                else:
                                    failed_count += 1
                                    error_count_in_batch += 1
                                    last_error_file = json_file

                                    # 减少日志I/O：每100个错误才记录一次
                                    if error_count_in_batch % 100 == 1:
                                        if err_type == 'json_error':
                                            logger.error(f"JSON解析错误 {json_file}: {err_msg}")
                                        else:
                                            logger.error(f"处理文件失败 {json_file}: {err_msg}")

                                    if err_type == 'json_error':
                                        self._log_import_error(data_source_name, json_file, f"JSON解析错误: {err_msg}")
                                    else:
                                        self._log_import_error(data_source_name, json_file, err_msg)
                            elif result is None:
                                # 数据无效
                                failed_count += 1
                                error_count_in_batch += 1
                                last_error_file = json_file
                            else:
                                # 数据有效，加入批量
                                batch_cols, vals, mapping = result

                                # 第一次设置cols
                                if cols is None:
                                    cols = batch_cols

                                batch_rows.append(vals)
                                batch_files.append(json_file)
                                batch_mappings.append(mapping)
                                
                                # 批量插入
                                if len(batch_rows) >= batch_size:
                                    inserted, failed_in_batch = self.bulk_insert(cols, batch_rows, batch_files, data_source_name)
                                    imported_count += inserted
                                    failed_count += failed_in_batch
                                    skipped_count += len(batch_rows) - inserted - failed_in_batch

                                    # 批量UPSERT文件路径映射
                                    self.bulk_upsert_file_path_mappings(batch_mappings)

                                    self.conn.commit()
                                    batch_rows.clear()
                                    batch_files.clear()
                                    batch_mappings.clear()
                                    error_count_in_batch = 0
                            
                            # 更新进度条（减少更新频率：只在批量插入后或每500条更新）
                            if processed % 500 == 0 or (len(batch_rows) == 0 and processed > 0):
                                pbar.set_postfix({
                                    '已处理': processed,
                                    '成功': imported_count,
                                    '跳过': skipped_count,
                                    '失败': failed_count
                                })
                            pbar.update(1)
                            
                        except Exception as e:
                            logger.error(f"处理结果时出错 {json_file}: {e}")
                            failed_count += 1
                        
                        # 提交新任务（如果还有文件）
                        try:
                            json_file = next(file_iter)
                            total_files += 1
                            new_future = executor.submit(process_file, json_file)
                            futures[new_future] = json_file
                        except StopIteration:
                            # 没有更多文件了，继续处理剩余任务
                            pass
                    
                    # 批量移除已完成的任务（提高性能）
                    for future in completed_futures:
                        if future in futures:
                            del futures[future]

            # 处理剩余的批次
            if batch_rows:
                inserted, failed_in_batch = self.bulk_insert(cols, batch_rows, batch_files, data_source_name)
                imported_count += inserted
                failed_count += failed_in_batch
                skipped_count += len(batch_rows) - inserted - failed_in_batch

                # 批量UPSERT剩余的文件路径映射
                self.bulk_upsert_file_path_mappings(batch_mappings)

                self.conn.commit()
                # 更新进度条显示
                pbar.set_postfix({
                    '已处理': processed,
                    '成功': imported_count,
                    '跳过': skipped_count,
                    '失败': failed_count
                })
            
            # 批量错误汇总
            if error_count_in_batch > 0:
                logger.warning(f"本批次共有 {error_count_in_batch} 个错误（最后错误文件: {last_error_file}）")
            
            # 关闭进度条
            pbar.close()

        # 最终提交（批量插入模式下，每个分类结束时已提交）

        logger.info(f"\n数据源 {data_source_name} 导入完成:")
        logger.info(f"  总文件数: {total_files:,}")
        logger.info(f"  成功导入: {imported_count:,}")
        logger.info(f"  跳过: {skipped_count:,}")
        logger.info(f"  失败: {failed_count:,}")
        if total_files > 0:
            success_rate = (imported_count / total_files * 100)
            logger.info(f"  成功率: {success_rate:.2f}%")
        if skipped_dirs:
            logger.info(f"  已跳过分类: {', '.join(skipped_dirs)}")


def main():
    """主函数"""
    db_config, data_sources, classification_dirs = load_runtime_config()
    
    # 创建导入器
    importer = DatabaseImporter(db_config)
    
    try:
        # 连接数据库
        importer.connect()
        
        # 显示导入配置
        logger.info(f"\n导入配置:")
        logger.info(f"  数据库: {db_config.get('database', 'ai_risks_db')}")
        logger.info(f"  主机: {db_config.get('host', 'localhost')}")
        logger.info(f"  端口: {db_config.get('port', 5432)}")
        logger.info(f"  用户: {db_config.get('user', 'postgres')}")
        logger.info(f"  将导入以下分类: {', '.join(classification_dirs)}")
        logger.info(f"  跳过分类: AIrisk_Irrelevant (不相关的新闻)")
        logger.info(f"  数据源数量: {len(data_sources)}")
        logger.info(f"  日志文件: {log_filename}")
        logger.info("")
        
        # 导入所有数据源
        for source in data_sources:
            logger.info(f"\n{'='*60}")
            logger.info(f"开始导入数据源: {source['name']}")
            logger.info(f"路径: {source['path']}")
            logger.info(f"{'='*60}\n")
            
            importer.import_data_source(
                source['name'],
                source['path'],
                classification_dirs,
                skip_existing=True
            )
        
        # 生成导入汇总
        logger.info("\n" + "=" * 80)
        logger.info("所有数据源导入完成！")
        logger.info("=" * 80)
        logger.info("注意: 已跳过 AIrisk_Irrelevant 分类（不相关的新闻）")
        
        # 统计总记录数
        try:
            importer.cursor.execute("SELECT COUNT(*) FROM ai_risk_relevant_news")
            total_records = importer.cursor.fetchone()[0]
            logger.info(f"数据库中共有 {total_records:,} 条记录")
            
            # 按数据源统计
            importer.cursor.execute("""
                SELECT data_source, classification_result, COUNT(*) 
                FROM ai_risk_relevant_news 
                GROUP BY data_source, classification_result 
                ORDER BY data_source, classification_result
            """)
            logger.info("\n各数据源统计:")
            for row in importer.cursor.fetchall():
                logger.info(f"  {row[0]} - {row[1]}: {row[2]:,} 条")
        except Exception as e:
            logger.warning(f"无法统计记录数: {e}")
        
        logger.info("=" * 80)
        logger.info(f"导入日志已保存到: {log_filename}")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.warning("\n收到中断信号，准备优雅退出...")
        if importer.conn:
            logger.info("提交已成功处理的记录...")
            importer.conn.commit()
        logger.info("程序已安全退出")
    except Exception as e:
        logger.error(f"导入过程出错: {e}")
        if importer.conn:
            importer.conn.rollback()
        raise
    finally:
        importer.close()


if __name__ == '__main__':
    main()
