# 处理 WARC 文件的核心逻辑
# 1. 从 WARC 文件中提取 HTML 内容
# 2. 使用 boilerpy3 提取文本内容
# 3. 使用 PhraseMatcher 和 KeywordProcessor 进行关键词提取和短语匹配
# 4. 将结果保存到指定的输出目录，按照年份和月份进行组织
# 该模块还包含日志记录和结果统计的功能，便于后续分析和调试

import os
import gzip
from pathlib import Path
import time
import logging
import hashlib
import multiprocessing
from functools import partial
from multiprocessing import Pool, Manager
from warcio.archiveiterator import ArchiveIterator
from boilerpy3 import extractors
import jieba.analyse
from bs4 import BeautifulSoup
from langdetect import detect
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import configparser
import ahocorasick

BASE_DIR = Path(__file__).resolve().parents[4]


def resolve_path(path_value: str) -> str:
    """将配置中的路径统一解析到仓库根目录"""
    if not path_value:
        return path_value
    if os.path.isabs(path_value):
        return os.path.normpath(path_value)
    return os.path.normpath(os.path.join(BASE_DIR, path_value))


def parse_year_list(raw_years: str) -> list:
    """解析 year 字段，允许逗号分隔的多个年份"""
    if not raw_years:
        raw_years = "2022"
    return [y.strip() for y in str(raw_years).split(",") if y.strip()]


# 构建目标文件路径
config_path = os.path.join(BASE_DIR, "config/Identification_Method-CommonCrawlNews-keyword_filter-config.ini")
if not os.path.exists(config_path):
    raise FileNotFoundError(f"配置文件不存在: {config_path}")
# 初始化配置
config = configparser.ConfigParser()
config.read(config_path, encoding="utf-8")
CCN_config = config["CommonCrawlNews"]
PHRASE_FILE = resolve_path(CCN_config.get("PHRASES_FILE"))
years = parse_year_list(CCN_config.get("year", "2022"))
months_str = CCN_config.get("months", "1,2,3,4,5,6,7,8,9,10,11,12")
months = [int(m.strip()) for m in months_str.split(",")]
star_month = min(months)
end_month = max(months)
NLTK_DATA = CCN_config.get("nltk_data", "punkt,stopwords,wordnet")
NLTK_DATA = [data.strip() for data in NLTK_DATA.split(",")]
WARC_FOLDER = resolve_path(CCN_config.get("WARC_FOLDER"))
OUTPUT_DIR = resolve_path(CCN_config.get("OUTPUT_DIR"))
LOG_FILE_TEMPLATE = CCN_config.get(
    "LOG_FILE_TEMPLATE",
    "../../../../logs/keyword_filter/CommonCrawlNews_{year}_{month}.log",
)
KEYWORDS_FILE = resolve_path(CCN_config.get("KEYWORDS_FILE"))
MAX_WORKERS = CCN_config.getint("max_workers", 32)
PHRASE_FREQ_BONUS = CCN_config.getint("phrase_freq_bonus", 0)

# 初始化基础日志（仅控制台输出，文件日志在main函数中按月份动态创建）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(process)d - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_logger_for_month(year, month):
    """为特定年份和月份设置日志文件处理器"""
    # 生成日志文件路径
    month_str = str(month).zfill(2)
    log_file = LOG_FILE_TEMPLATE.format(year=year, month=month_str)
    # 如果路径是相对路径，则基于仓库根目录构建绝对路径
    log_file = resolve_path(log_file)
    
    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)
    
    # 移除现有的文件处理器（如果存在）
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            handler.close()
    
    # 添加新的文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(process)d - %(message)s'))
    logger.addHandler(file_handler)
    
    logger.info(f"日志文件已设置为: {log_file}")
    return log_file

def scan_result_statistics(year, months_list):
    """
    扫描结果文件夹，生成统计信息
    返回：字典，包含各月份的统计信息
    """
    statistics = {}
    result_dirs = ['texts', 'htmls', 'keywords', 'phrase_matches', 'residual_keywords']
    
    for month in months_list:
        month_str = str(month)
        month_stats = {
            'texts': 0,
            'htmls': 0,
            'keywords': 0,
            'phrase_matches': 0,
            'residual_keywords': 0,
            'total': 0
        }
        
        month_dir = os.path.join(OUTPUT_DIR, year, month_str)
        if not os.path.exists(month_dir):
            statistics[month_str] = month_stats
            continue
        
        # 统计各个子文件夹的文件数量
        for subdir in result_dirs:
            subdir_path = os.path.join(month_dir, subdir)
            if os.path.exists(subdir_path) and os.path.isdir(subdir_path):
                try:
                    file_count = len([f for f in os.listdir(subdir_path) 
                                     if os.path.isfile(os.path.join(subdir_path, f))])
                    month_stats[subdir] = file_count
                    month_stats['total'] += file_count
                except Exception as e:
                    logger.warning(f"统计 {subdir_path} 时出错: {e}")
        
        statistics[month_str] = month_stats
    
    return statistics

def generate_statistics_report(year, months_list, log_file=None):
    """
    生成统计报告并写入日志
    """
    stats = scan_result_statistics(year, months_list)
    
    report_lines = [
        f"\n{'='*60}",
        f"结果文件夹统计报告 - {year}年",
        f"{'='*60}"
    ]
    
    total_texts = 0
    total_htmls = 0
    total_keywords = 0
    total_phrase_matches = 0
    total_residual_keywords = 0
    
    for month in months_list:
        month_str = str(month)
        month_stats = stats.get(month_str, {})
        texts_count = month_stats.get('texts', 0)
        htmls_count = month_stats.get('htmls', 0)
        keywords_count = month_stats.get('keywords', 0)
        phrase_matches_count = month_stats.get('phrase_matches', 0)
        residual_keywords_count = month_stats.get('residual_keywords', 0)
        
        total_texts += texts_count
        total_htmls += htmls_count
        total_keywords += keywords_count
        total_phrase_matches += phrase_matches_count
        total_residual_keywords += residual_keywords_count
        
        report_lines.append(f"\n{year}年{month_str}月:")
        report_lines.append(f"  - 文本文件 (texts): {texts_count:,} 个")
        report_lines.append(f"  - HTML文件 (htmls): {htmls_count:,} 个")
        report_lines.append(f"  - 关键词文件 (keywords): {keywords_count:,} 个")
        report_lines.append(f"  - 短语匹配文件 (phrase_matches): {phrase_matches_count:,} 个")
        report_lines.append(f"  - 非短语关键词文件 (residual_keywords): {residual_keywords_count:,} 个")
    
    report_lines.append(f"\n{'-'*60}")
    report_lines.append(f"总计 ({year}年):")
    report_lines.append(f"  - 文本文件 (texts): {total_texts:,} 个")
    report_lines.append(f"  - HTML文件 (htmls): {total_htmls:,} 个")
    report_lines.append(f"  - 关键词文件 (keywords): {total_keywords:,} 个")
    report_lines.append(f"  - 短语匹配文件 (phrase_matches): {total_phrase_matches:,} 个")
    report_lines.append(f"  - 非短语关键词文件 (residual_keywords): {total_residual_keywords:,} 个")
    report_lines.append(f"{'='*60}\n")
    
    report = "\n".join(report_lines)
    
    # 写入日志
    logger.info(report)
    
    # 如果指定了日志文件，也写入文件
    if log_file and os.path.exists(log_file):
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(report)
        except Exception as e:
            logger.warning(f"写入统计报告到日志文件失败: {e}")
    
    return stats

# 初始化NLTK
def init_nltk():
    for data in NLTK_DATA:
        try:
            if data in ['punkt','punkt_tab']:
                nltk.data.find(f'tokenizers/{data}')
            else:
                nltk.data.find(f'corpora/{data}')
        except LookupError:
            nltk.download(data)
            print(f"Downloaded NLTK data: {data}")
init_nltk()
class PhraseMatcher:
    def __init__(self, phrases):
        # 统一小写 & 去重
        norm_phrases = {p.lower() for p in phrases if p}
        self.automaton = ahocorasick.Automaton()
        for phrase in norm_phrases:
            self.automaton.add_word(phrase, phrase)  # value 就存短语本身
        self.automaton.make_automaton()

    @staticmethod
    def _both_sides_space(text, start_idx, end_idx):
        """要求匹配到的短语前后字符均为单个空格' '"""
        # 左侧必须存在且为' '
        if start_idx - 1 < 0 or text[start_idx - 1] != ' ':
            return False
        # 右侧必须存在且为' '
        if end_idx + 1 >= len(text) or text[end_idx + 1] != ' ':
            return False
        return True

    def find_matches(self, text):
        text_lc = text.lower()
        matches = set()
        for end_idx, phrase in self.automaton.iter(text_lc):
            start_idx = end_idx - len(phrase) + 1
            if self._both_sides_space(text_lc, start_idx, end_idx):
                matches.add(phrase)
        return matches

    def find_matches_info(self, text):
        """
        返回：
            phrases: set[str] 去重后的短语集合（小写）
            counts:  dict[str,int] 各短语出现次数（仅统计前后均为空格的匹配）
        """
        text_lc = text.lower()
        counts = {}
        for end_idx, phrase in self.automaton.iter(text_lc):
            start_idx = end_idx - len(phrase) + 1
            if self._both_sides_space(text_lc, start_idx, end_idx):
                counts[phrase] = counts.get(phrase, 0) + 1
        phrases = set(counts.keys())
        return phrases, counts
# 关键词处理
class KeywordProcessor:
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()
        self.en_stopwords = set(stopwords.words('english'))
        self.phrase_matcher = PhraseMatcher(PHRASE_FILE)
        self.phrase_freq_bonus = PHRASE_FREQ_BONUS  # 新增：短语计数加成
        try:
            with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                self.keywords = set(line.strip().lower() for line in f if line.strip())
        except Exception as e:
            logger.error(f"Failed to load keywords: {e}")
            self.keywords = set()

    def extract_keywords(self, text, lang, top_n=15, phrase_boost=None):
        """
        返回：
          phrase_matches:       set[str]     命中的短语（去重，小写）
          residual_keywords:    set[str]     按频次降序取 top_n 的“非短语”关键词
          keywords:             set[str]     按频次降序取 top_n 的“综合（短语+非短语）关键词”
        说明：
          - 短语的计数 = 实际出现次数 + phrase_boost（默认读配置项 PHRASE_FREQ_BONUS）
          - 中文会把命中短语加入 jieba 字典，尽量按短语切分
          - 排序规则与原来一致：按频次降序取 top_n
        """
        lower_text = text.lower()
        phrase_matches, phrase_counts = self.phrase_matcher.find_matches_info(lower_text)
        # 分词时，短语按整体处理（用于“非短语”词频时避免重复计数）
        phrase_tokens = set()
        if lang == 'en':
            for phrase in phrase_matches:
                phrase_tokens.update(word_tokenize(phrase))
        elif lang == 'zh':
            # 让 jieba 倾向将短语整体切分
            for phrase in phrase_matches:
                try:
                    jieba.add_word(phrase, freq=2000000)
                except Exception:
                    pass
                phrase_tokens.update(jieba.lcut(phrase))

        # 计算短语计数（带加成）
        if phrase_boost is None:
            phrase_boost = self.phrase_freq_bonus
        boosted_phrase_counts = {p: c + phrase_boost for p, c in phrase_counts.items()}

        # 非短语的词频统计
        residual_counts = {}
        if lang == 'en':
            tokens = [
                self.lemmatizer.lemmatize(t.lower())
                for t in word_tokenize(lower_text)
                if t.isalpha() and
                   t.lower() not in self.en_stopwords and
                   t.lower() not in phrase_tokens
            ]
            for t in tokens:
                residual_counts[t] = residual_counts.get(t, 0) + 1

        elif lang == 'zh':
            tokens = [t for t in jieba.lcut(text)]
            for t in tokens:
                # 排除短语自身（短语已经通过 boosted_phrase_counts 统计）
                if t in phrase_matches:
                    continue
                if t in phrase_tokens:
                    continue
                if len(t) > 1:
                    residual_counts[t] = residual_counts.get(t, 0) + 1

        # 生成 top_n 列表
        def topn_keys_by_count(d, n):
            if not d:
                return set()
            # 频次降序；频次相同时按 key 稳定排序，避免不同平台/进程不一致
            return set([k for k, _ in sorted(d.items(), key=lambda x: (-x[1], x[0]))[:n]])

        residual_keywords = topn_keys_by_count(residual_counts, top_n)

        # 综合 = 短语（带加成的频次） + 非短语频次
        combined = dict(residual_counts)
        for p, c in boosted_phrase_counts.items():
            combined[p] = combined.get(p, 0) + c

        keywords = topn_keys_by_count(combined, top_n)

        return phrase_matches, residual_keywords, keywords, boosted_phrase_counts, residual_counts, combined



keyword_processor = KeywordProcessor()

# 内容处理工具
class ContentProcessor:
    @staticmethod
    def clean_html(html):
        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['script', 'style', 'a']):
            tag.decompose()
        return str(soup)
    
    @staticmethod
    def extract_content(html):
        try:
            if not html:
                return "empty content"
            content = extractors.ArticleExtractor().get_content(html)
            return content if content else "empty content"
        except Exception as e:
            logger.warning(f"Content extraction failed: {e}")
            return "empty content"
    
    @staticmethod
    def detect_language(text):
        try:
            code = detect(text[:1000]).lower()
            if code.startswith('zh'):
                return 'zh'
            if code.startswith('en'):
                return 'en'
            return code
        except:
            return 'unknown'


# WARC处理核心
class WARCAnalyzer:
    def __init__(self, output_dir, stats):
        self.output_dir = output_dir
        self.stats = stats
    
    def process_record(self, record):
        try:
            # 基础数据提取
            payload = record.content_stream().read().decode(errors='ignore')
            clean_html = ContentProcessor.clean_html(payload)
            main_content = ContentProcessor.extract_content(clean_html)

            # 空内容检查
            if not main_content.strip() or main_content == "empty content":
                return False

            # 语言检测和关键词提取
            lang = ContentProcessor.detect_language(main_content)
            if lang not in ("en", "zh"):
                return False

            phrase_matches, residual_keywords, keywords, phrase_counts, residual_counts, combined_counts = keyword_processor.extract_keywords(main_content, lang)
            if keyword_processor.keywords.intersection(keywords):
                    # 保存有效内容
                url = record.rec_headers.get("WARC-Target-URI", "")
                self._save_results(url, payload, main_content, phrase_matches, residual_keywords, keywords, phrase_counts, residual_counts, combined_counts)

                return True
            else:
                return False

        except Exception as e:
            logger.error(f"Record processing error: {e}")
            return False
    
    def _save_results(self, url, html, text, phrase_matches, residual_keywords, keywords, phrase_counts, residual_counts, combined_counts):
        """原子化文件保存操作"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        html_path = os.path.join(self.output_dir, "htmls")
        text_path = os.path.join(self.output_dir, "texts")
        keywords_path = os.path.join(self.output_dir, "keywords")
        residual_keywords_path = os.path.join(self.output_dir, "residual_keywords")
        phrase_matches_path = os.path.join(self.output_dir, "phrase_matches")
        os.makedirs(html_path, exist_ok=True)
        os.makedirs(text_path, exist_ok=True)
        os.makedirs(keywords_path, exist_ok=True)  # 创建关键词目录
        os.makedirs(residual_keywords_path, exist_ok=True)
        os.makedirs(phrase_matches_path, exist_ok=True)

        # 保存HTML
        temp_html = os.path.join(html_path, f"temp_{url_hash}.html")
        final_html = os.path.join(html_path, f"{url_hash}.html")
        with open(temp_html, 'w', encoding='utf-8') as f:
            f.write(f"<!-- URL: {url} -->\n{html}")
        os.rename(temp_html, final_html)

        # 保存文本内容
        temp_text = os.path.join(text_path, f"temp_{url_hash}.txt")
        final_text = os.path.join(text_path, f"{url_hash}.txt")
        with open(temp_text, 'w', encoding='utf-8') as f:
            f.write(text)
        os.rename(temp_text, final_text)

        # 保存关键词和词频
        temp_keywords = os.path.join(keywords_path, f"temp_{url_hash}.txt")
        final_keywords = os.path.join(keywords_path, f"{url_hash}.txt")
        with open(temp_keywords, 'w', encoding='utf-8') as f:
            f.write("\n".join([f"{k}:{v}" for k, v in combined_counts.items()]))  # 关键词及其频次
        os.rename(temp_keywords, final_keywords)

        # 保存短语匹配和词频
        temp_phrase_matches = os.path.join(phrase_matches_path, f"temp_{url_hash}.txt")
        final_phrase_matches = os.path.join(phrase_matches_path, f"{url_hash}.txt")
        with open(temp_phrase_matches, 'w', encoding='utf-8') as f:
            f.write("\n".join([f"{k}:{phrase_counts[k]}" for k in phrase_matches]))  # 短语及其频次
        os.rename(temp_phrase_matches, final_phrase_matches)

        # 保存非短语词及其频次
        temp_residual_keywords = os.path.join(residual_keywords_path, f"temp_{url_hash}.txt")
        final_residual_keywords = os.path.join(residual_keywords_path, f"{url_hash}.txt")
        with open(temp_residual_keywords, 'w', encoding='utf-8') as f:
            f.write("\n".join([f"{k}:{residual_counts[k]}" for k in residual_keywords]))  # 非短语词及其频次
        os.rename(temp_residual_keywords, final_residual_keywords)

        logger.info(f"Saved results for {url} with keywords and frequencies")

   
# 主处理函数
def process_warc_file(warc_path, year, month, stats,max_records=2000):
    analyzer = WARCAnalyzer(
        output_dir=os.path.join(OUTPUT_DIR, year, month),
        stats=stats
    )
    processed_count = 0
    local_stats = {'total': 0, 'ai': 0, 'failed': 0}
    try:
        with gzip.open(warc_path, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == "response":
                    local_stats['total'] += 1
                    if analyzer.process_record(record):
                        local_stats['ai'] += 1
                processed_count += 1
                # if processed_count >= max_records:
                #     break  # 只处理前100条记录，达到后停止
    except Exception as e:
        local_stats['failed'] = local_stats['total']
        logger.error(f"Failed to process {warc_path}: {e}")
    
    # 更新共享统计
    # 不需要锁（每个赋值操作自身是原子的）
    for k, v in local_stats.items():
        stats[k] += v  # 每个键的更新是独立的原子操作
    stats['files_processed'] += 1
    
    return warc_path, local_stats

def main():
    start_time = time.time()
    # 读取配置文件
    
    # 在开始处理前，先扫描并记录现有结果的统计信息
    logger.info("正在扫描结果文件夹，生成统计信息...")
    generate_statistics_report(year, months)
    
    # 初始化共享统计
    manager = Manager()
    stats = manager.dict({
        'total': 0,
        'ai': 0,
        'failed': 0,
        'files_processed': 0,
        'files_failed': 0
    })
    for year in years:
        for month in months:
            month_str = str(month)
            # 为当前月份设置日志文件
            current_log_file = setup_logger_for_month(year, month_str)
            
            warc_files = [
                os.path.join(root, f) 
                for root, _, files in os.walk(os.path.join(WARC_FOLDER, year, month_str))
                for f in files if f.endswith(".warc.gz")
            ]
            warc_files=warc_files[:]  # 限制处理的文件数量，避免过多文件导致内存问题
            
            if not warc_files:
                logger.warning(f"No WARC files found for {year}-{month_str}")
                continue
                
            logger.info(f"Processing {len(warc_files)} files for {year}-{month_str}")
            
            # 准备输出目录
            os.makedirs(os.path.join(OUTPUT_DIR, year, month_str, "texts"), exist_ok=True)
            
            # 为当前月份创建独立的统计字典
            month_stats = manager.dict({
                'total': 0,
                'ai': 0,
                'failed': 0,
                'files_processed': 0
            })
            
            # 进程池处理
            num_workers = min(MAX_WORKERS, os.cpu_count(), len(warc_files))
            with Pool(num_workers) as pool:
                results = pool.imap_unordered(
                    partial(process_warc_file, year=year, month=month_str, stats=month_stats),
                    warc_files 
                )
                
                for warc_path, local_stats in results:
                    logger.info(
                        f"Completed {os.path.basename(warc_path)}: "
                        f"Total={local_stats['total']} "
                        f"AI={local_stats['ai']} "
                        f"Failed={local_stats['failed']}"
                    )
            
            # 更新总体统计
            for k in ['total', 'ai', 'failed', 'files_processed']:
                stats[k] += month_stats[k]
            
            # 当前月份的统计摘要
            month_stats_dict = dict(month_stats)
            month_summary = (
                f"\n=== Processing Summary for {year}-{month_str} ===\n"
                f"WARC files processed in this month: {month_stats_dict['files_processed']}\n"
                f"HTML pages processed: {month_stats_dict['total']}\n"
                f"AI-related pages found: {month_stats_dict['ai']}\n"
                f"Failed pages: {month_stats_dict['failed']}\n"
            )
            
            with open(current_log_file, 'a', encoding='utf-8') as f:
                f.write(month_summary)
            
            logger.info(month_summary)
                
    # 最终统计
    end_time = time.time()
    stats_dict = dict(stats)
    
    summary = (
        f"\n=== Overall Processing Summary ===\n"
        f"Time elapsed: {end_time - start_time:.2f} seconds\n"
        f"Total WARC files processed: {stats_dict['files_processed']}\n"
        f"Total HTML pages processed: {stats_dict['total']}\n"
        f"Total AI-related pages found: {stats_dict['ai']}\n"
        f"Total failed pages: {stats_dict['failed']}\n"
    )
    
    # 写入所有年份与月份的日志文件
    for current_year in years:
        for month in months:
            month_str = str(month).zfill(2)
            log_file = resolve_path(LOG_FILE_TEMPLATE.format(year=current_year, month=month_str))
            if os.path.exists(log_file):
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(summary)
    
    logger.info(summary)
    
    # 处理完成后，再次扫描结果文件夹，生成最终统计报告
    logger.info("处理完成，正在生成最终统计报告...")
    
    for current_year in years:
        generate_statistics_report(current_year, months)
        # 将最终统计报告写入所有月份的日志文件
        for month in months:
            month_str = str(month).zfill(2)
            log_file = resolve_path(LOG_FILE_TEMPLATE.format(year=current_year, month=month_str))
            if os.path.exists(log_file):
                try:
                    stats = scan_result_statistics(current_year, [month])
                    month_stats = stats.get(month_str, {})
                    month_report = (
                        f"\n{'='*60}\n"
                        f"结果文件夹统计 - {current_year}年{month_str}月\n"
                        f"{'='*60}\n"
                        f"文本文件 (texts): {month_stats.get('texts', 0):,} 个\n"
                        f"HTML文件 (htmls): {month_stats.get('htmls', 0):,} 个\n"
                        f"关键词文件 (keywords): {month_stats.get('keywords', 0):,} 个\n"
                        f"短语匹配文件 (phrase_matches): {month_stats.get('phrase_matches', 0):,} 个\n"
                        f"非短语关键词文件 (residual_keywords): {month_stats.get('residual_keywords', 0):,} 个\n"
                        f"{'='*60}\n"
                    )
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write(month_report)
                except Exception as e:
                    logger.warning(f"写入月份统计到 {log_file} 失败: {e}")

def generate_statistics_only(year=None, months_list=None, output_log_file=None):
    """
    仅生成统计报告，不进行数据处理
    用于扫描已有结果文件夹并生成统计信息
    """
    target_years = parse_year_list(year) if year else years
    if months_list is None:
        months_str = CCN_config.get("months", "1,2,3,4,5,6,7,8,9,10,11,12")
        months_list = [int(m.strip()) for m in months_str.split(",")]
    
    last_stats = None
    for current_year in target_years:
        # 设置日志
        if output_log_file:
            log_file = resolve_path(output_log_file)
        else:
            month_str = str(months_list[0]).zfill(2) if months_list else "01"
            log_file = resolve_path(
                LOG_FILE_TEMPLATE.format(year=current_year, month=month_str)
            )
        
        setup_logger_for_month(current_year, months_list[0] if months_list else 1)
        
        logger.info(f"开始扫描结果文件夹: {OUTPUT_DIR}")
        logger.info(f"统计报告将写入: {log_file}")
        
        # 生成统计报告
        last_stats = generate_statistics_report(current_year, months_list, log_file)
    
    logger.info("统计报告生成完成！")
    return last_stats

if __name__ == "__main__":
    import sys
    multiprocessing.freeze_support()  # 对于Windows打包支持
    
    # 如果命令行参数包含 --stats-only，则仅生成统计报告
    if len(sys.argv) > 1 and sys.argv[1] == "--stats-only":
        year_arg = sys.argv[2] if len(sys.argv) > 2 else None
        months_arg = [int(m) for m in sys.argv[3].split(",")] if len(sys.argv) > 3 else None
        generate_statistics_only(year=year_arg, months_list=months_arg)
    else:
        main()
