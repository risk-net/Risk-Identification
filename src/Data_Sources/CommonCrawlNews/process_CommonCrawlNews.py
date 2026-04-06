#该文件是为了获取CommonCrawl的新闻数据 文件为wrac格式，下载指定年份，下载至指定文件夹
#注意根据实际情况 修改并添加数据保存地址和新闻获取年份 这一版已将其注释

import requests
import os
import time
import gzip
from io import BytesIO
import random
import configparser
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple
import threading

# 全局速率限制器（可选）
class RateLimiter:
    """简单的速率限制器，用于控制总体下载速度"""
    def __init__(self, max_bytes_per_second: float = None):
        self.max_bytes_per_second = max_bytes_per_second
        self.lock = threading.Lock()
        self.last_update = time.time()
        self.bytes_sent = 0
    
    def wait_if_needed(self, bytes_to_send: int):
        """如果需要，等待以确保不超过速率限制"""
        if self.max_bytes_per_second is None:
            return
        
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_update
            
            # 重置计数器（每秒重置一次）
            if elapsed >= 1.0:
                self.bytes_sent = 0
                self.last_update = current_time
                elapsed = 0
            
            # 检查是否需要等待
            if self.bytes_sent + bytes_to_send > self.max_bytes_per_second:
                wait_time = 1.0 - elapsed
                if wait_time > 0:
                    time.sleep(wait_time)
                    self.bytes_sent = 0
                    self.last_update = time.time()
            
            self.bytes_sent += bytes_to_send

# 全局速率限制器实例
_rate_limiter = None

def download_file(url: str, local_path: str, retry_interval: int, logger: logging.Logger, max_retries: int = 10, rate_limiter: RateLimiter = None) -> Tuple[bool, str]:
    """
    下载单个文件，支持断点续传
    
    Returns:
        (success, message): 下载是否成功和相关信息
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            local_file_size = 0
            headers = {}
            
            # 检查本地文件是否存在
            if os.path.exists(local_path):
                local_file_size = os.path.getsize(local_path)
                # 发送 HEAD 请求获取远程文件大小
                try:
                    head_response = requests.head(url, timeout=30)
                    head_response.raise_for_status()
                    remote_file_size = int(head_response.headers.get('Content-Length', 0))
                    
                    if remote_file_size == 0:
                        logger.warning(f"无法获取远程文件大小: {url}")
                    elif local_file_size == remote_file_size:
                        logger.info(f"文件已完整下载，跳过: {os.path.basename(local_path)} ({local_file_size / 1024 / 1024:.2f} MB)")
                        return True, "文件已存在且完整"
                    else:
                        logger.info(f"文件未完整下载，继续下载: {os.path.basename(local_path)} (本地: {local_file_size / 1024 / 1024:.2f} MB, 远程: {remote_file_size / 1024 / 1024:.2f} MB)")
                        headers = {'Range': f'bytes={local_file_size}-'}
                except Exception as e:
                    logger.warning(f"检查远程文件大小失败: {e}，将尝试重新下载")

            # 确保目录存在
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            logger.info(f"开始下载: {os.path.basename(local_path)}")
            start_time = time.time()
            response = requests.get(url, stream=True, headers=headers, timeout=300)
            response.raise_for_status()
            
            # 处理响应状态码
            if response.status_code == 206:
                logger.debug(f"从字节 {local_file_size} 处继续下载 (206 Partial Content)")
            elif response.status_code == 200:
                if local_file_size > 0:
                    logger.warning(f"收到200状态码但本地文件已存在，可能覆盖: {local_path}")
                else:
                    logger.debug("从头开始下载 (200 OK)")
            else:
                error_msg = f"不支持的响应状态码: {response.status_code}"
                logger.error(error_msg)
                return False, error_msg

            # 流式下载文件
            downloaded_size = local_file_size
            chunk_size = 8192 * 4  # 32KB chunks for better performance
            last_log_time = start_time
            
            with open(local_path, 'ab' if headers else 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        # 应用速率限制（如果启用）
                        if rate_limiter:
                            rate_limiter.wait_if_needed(len(chunk))
                        
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # 每5秒输出一次进度
                        current_time = time.time()
                        if current_time - last_log_time >= 5.0:
                            elapsed = current_time - start_time
                            speed = downloaded_size / elapsed / 1024 / 1024 if elapsed > 0 else 0
                            logger.info(f"下载进度: {os.path.basename(local_path)} - {downloaded_size / 1024 / 1024:.2f} MB ({speed:.2f} MB/s)")
                            last_log_time = current_time
            
            # 下载完成
            elapsed = time.time() - start_time
            file_size_mb = downloaded_size / 1024 / 1024
            avg_speed = file_size_mb / elapsed if elapsed > 0 else 0
            logger.info(f"下载完成: {os.path.basename(local_path)} - {file_size_mb:.2f} MB, 耗时: {elapsed:.2f}s, 平均速度: {avg_speed:.2f} MB/s")
            return True, f"下载成功 ({file_size_mb:.2f} MB)"
            
        except requests.exceptions.HTTPError as http_err:
            retry_count += 1
            error_msg = f"HTTP 错误 (状态码: {http_err.response.status_code if hasattr(http_err, 'response') else 'unknown'}): {http_err}"
            logger.error(f"{error_msg} (重试 {retry_count}/{max_retries})")
            if retry_count >= max_retries:
                return False, error_msg
        except requests.exceptions.Timeout as timeout_err:
            retry_count += 1
            error_msg = f"请求超时: {timeout_err}"
            logger.error(f"{error_msg} (重试 {retry_count}/{max_retries})")
            if retry_count >= max_retries:
                return False, error_msg
        except requests.exceptions.RequestException as req_err:
            retry_count += 1
            error_msg = f"请求错误: {req_err}"
            logger.error(f"{error_msg} (重试 {retry_count}/{max_retries})")
            if retry_count >= max_retries:
                return False, error_msg
        except Exception as e:
            retry_count += 1
            error_msg = f"未知错误: {e}"
            logger.error(f"{error_msg} (重试 {retry_count}/{max_retries})", exc_info=True)
            if retry_count >= max_retries:
                return False, error_msg

        if retry_count < max_retries:
            logger.info(f"将在 {retry_interval} 秒后重试下载 {os.path.basename(local_path)} ...")
            time.sleep(retry_interval)
    
    return False, f"达到最大重试次数 ({max_retries})"

def get_warc_file_paths(index_url: str, logger: logging.Logger, max_retries: int = 5) -> List[str]:
    """获取所有 WARC 文件路径"""
    retry_count = 0
    while retry_count < max_retries:
        try:
            logger.info(f"正在下载索引文件: {index_url}")
            response = requests.get(index_url, timeout=60)
            response.raise_for_status()
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
                file_paths = [line.decode('utf-8').strip() for line in gz if line.strip()]
            logger.info(f"索引文件下载完成，共 {len(file_paths)} 个文件路径")
            return file_paths
        except requests.exceptions.RequestException as e:
            retry_count += 1
            logger.error(f"获取 WARC 文件路径失败: {e} (重试 {retry_count}/{max_retries})")
            if retry_count < max_retries:
                retry_interval = random.randint(10, 30)
                logger.info(f"将在 {retry_interval} 秒后重试下载索引文件...")
                time.sleep(retry_interval)
        except Exception as e:
            retry_count += 1
            logger.error(f"处理索引文件时发生错误: {e} (重试 {retry_count}/{max_retries})", exc_info=True)
            if retry_count < max_retries:
                retry_interval = random.randint(10, 30)
                time.sleep(retry_interval)
    
    raise RuntimeError(f"无法获取 WARC 文件路径，已达到最大重试次数 ({max_retries})")

def setup_logger(log_file: str = None) -> logging.Logger:
    """设置日志记录器"""
    logger = logging.getLogger("CommonCrawlDownloader")
    logger.setLevel(logging.INFO)
    
    # 清除已有的处理器
    if logger.handlers:
        logger.handlers.clear()
    
    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器（如果指定了日志文件）
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def download_files_parallel(file_tasks: List[Tuple[str, str]], max_workers: int, logger: logging.Logger, rate_limiter: RateLimiter = None) -> Tuple[int, int]:
    """
    并发下载文件
    
    Args:
        file_tasks: 下载任务列表 (url, local_path)
        max_workers: 最大并发线程数
        logger: 日志记录器
        rate_limiter: 速率限制器（可选）
    
    Returns:
        (success_count, failed_count): 成功和失败的文件数量
    """
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有下载任务
        future_to_task = {
            executor.submit(
                download_file,
                url,
                local_path,
                random.randint(10, 30),
                logger,
                10,  # max_retries
                rate_limiter  # 传递速率限制器
            ): (url, local_path)
            for url, local_path in file_tasks
        }
        
        # 处理完成的任务
        for future in as_completed(future_to_task):
            url, local_path = future_to_task[future]
            try:
                success, message = future.result()
                if success:
                    success_count += 1
                else:
                    failed_count += 1
                    logger.error(f"下载失败: {os.path.basename(local_path)} - {message}")
            except Exception as e:
                failed_count += 1
                logger.error(f"下载任务异常: {os.path.basename(local_path)} - {e}", exc_info=True)
    
    return success_count, failed_count


if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parents[3]
    # 构建目标文件路径
    config_path = os.path.join(BASE_DIR, "config/Data_Sources-CommonCrawlNews-config.ini")
    
    # 读取配置文件
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    
    if "CommonCrawl" not in config:
        raise ValueError("配置文件中缺少 [CommonCrawl] 节")
    
    cc_config = config["CommonCrawl"]
    year_str = cc_config.get("year", "2023")
    years= [y.strip() for y in year_str.split(",") if y.strip()]
    months_str = cc_config.get("months", "1,2,3,4,5,6,7,8,9,10,11,12")
    months = [int(m.strip()) for m in months_str.split(",") if m.strip()]
    download_dir = os.path.join(BASE_DIR, cc_config.get("download_dir"))
    base_url = cc_config.get("base_url", "https://data.commoncrawl.org/").rstrip('/')
    max_workers = cc_config.getint("max_workers", 8)  # 默认8个并发线程
    
    # 验证配置
    if not download_dir:
        raise ValueError("配置文件中 download_dir 不能为空")
    
    # 设置日志（保存在logs/data_sources/目录下）
    log_dir = os.path.join(BASE_DIR, "logs/data_sources")
    os.makedirs(log_dir, exist_ok=True)
  
    # 速率限制配置（可选，单位：MB/s，None表示不限制）
    # 例如：如果总带宽是100Mbps (约12.5MB/s)，可以设置为10MB/s留出余量
    max_speed_mbps = cc_config.get("max_speed_mbps", None)
    rate_limiter = None
    
    
    total_files = 0
    total_success = 0
    total_failed = 0
    program_start_time = time.time()
    
    for year in years:
    # 处理每个月份
        for month in months:
            # 预先创建所有需要的目录
            month_dir = os.path.join(download_dir, year, f"{month:02d}")
            os.makedirs(month_dir, exist_ok=True)
            
            log_file = os.path.join(log_dir, f"CommonCrawl_download_{year}_{time.strftime('%Y%m%d_%H%M%S')}.log")
            logger = setup_logger(log_file)
    
            logger.info(f"\n开始处理 {year}年{month:02d}月")
            month_start_time = time.time()
            if max_speed_mbps:
                try:
                    max_speed_mbps = float(max_speed_mbps)
                    max_bytes_per_second = max_speed_mbps * 1024 * 1024  # 转换为字节/秒
                    rate_limiter = RateLimiter(max_bytes_per_second)
                    logger.info(f"启用速率限制: {max_speed_mbps} MB/s")
                except ValueError:
                    logger.warning(f"无效的速率限制值: {max_speed_mbps}，将不启用速率限制")
            else:
                logger.info("未启用速率限制（建议根据实际带宽设置 max_speed_mbps）")
            
            logger.info("=" * 60)
            logger.info("Common Crawl 新闻数据下载程序启动")
            logger.info(f"配置: 年份={year}, 月份={months}, 下载目录={download_dir}")
            logger.info(f"并发线程数: {max_workers}")
            logger.info(f"日志文件: {log_file}")
            logger.info("=" * 60)
            try:
                # 获取索引文件
                index_url = f"{base_url}/crawl-data/CC-NEWS/{year}/{month:02d}/warc.paths.gz"
                file_paths = get_warc_file_paths(index_url, logger)
                total_files += len(file_paths)
                
                logger.info(f"{year}年{month:02d}月: 找到 {len(file_paths)} 个文件需要下载")
                
                # 准备下载任务列表
                file_tasks = []
                for file_path in file_paths:
                    file_url = base_url + '/' + file_path.lstrip('/')
                    local_path = os.path.join(download_dir, year, f"{month:02d}", os.path.basename(file_path))
                    file_tasks.append((file_url, local_path))
                
                # 并发下载（传递速率限制器）
                success_count, failed_count = download_files_parallel(file_tasks, max_workers, logger, rate_limiter)
                total_success += success_count
                total_failed += failed_count
                
                month_elapsed = time.time() - month_start_time
                logger.info(f"{year}年{month:02d}月处理完成: 成功={success_count}, 失败={failed_count}, 耗时={month_elapsed:.2f}秒")
                
            except Exception as e:
                logger.error(f"处理 {year}年{month:02d}月时发生错误: {e}", exc_info=True)
                total_failed += len(file_paths) if 'file_paths' in locals() else 0
    
    # 最终统计
    total_elapsed = time.time() - program_start_time
    logger.info("=" * 60)
    logger.info("下载任务完成")
    logger.info(f"总计: 文件数={total_files}, 成功={total_success}, 失败={total_failed}")
    if total_files > 0:
        success_rate = (total_success / total_files) * 100
        logger.info(f"成功率: {success_rate:.2f}%")
    logger.info(f"总耗时: {total_elapsed:.2f}秒 ({total_elapsed/60:.2f}分钟)")
    logger.info("=" * 60)