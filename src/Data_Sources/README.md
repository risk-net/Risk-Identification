# Data Sources

`src/Data_Sources` 是项目的原始数据获取层。这里保存的是各类数据源采集、下载、格式整理和热点榜单抓取脚本，为后续的识别、筛选与对齐流程提供输入数据。

这个目录的公开定位是：

- 提供原始数据获取脚本和格式整理脚本
- 公开数据来源与获取方式
- 为后续 `Identification_Method` 和 `Database` 提供输入


## Directory Overview

| 子目录 | 主要脚本 | 功能简介 | 对应配置文件 |
| --- | --- | --- | --- |
| `CommonCrawlNews/` | `process_CommonCrawlNews.py` | 批量下载 Common Crawl News 的 WRAC/WARC 文件，支持断点续传与速率限制。 | `config/Data_Sources-CommonCrawlNews-config.ini` |
| `OpenNewsArchive/` | `data_collecting_OpenNewsArchive.py` | 通过 OpenXLab 下载 OpenNewsArchive 数据集。 | `config/Data_Sources-OpenNewsArchive-config.ini` |
| `Crawled_Dataset/` | `news_web_crawler.py`、`auto_crawler.py` | 针对自定义新闻站点的网页爬虫。 | `config/Data_Sources-Crawled_Dataset-config.ini` |
| `Hot_list_word_Dataset/` | `crawlweibo_hotlist.py` 等 | 抓取微博、抖音、知乎、V2EX、今日头条等热点榜单。 | `config/Data_Sources-Hot_list_word_Dataset-config.ini` |
| `AIAAIC/` | `process_AIAAIC.py`、`llm_crawl_news.py` | 处理 AIAAIC 数据并可选用 LLM 补充新闻抽取。 | `config/Data_Sources-AIAAIC-config.ini` |
| `AIID/` | `process_AIID.py` | 处理 AI Incident Database 原始 CSV 并转换为 JSONL。 | `config/Data_Sources-AIID-config.ini` |

## Public Boundary

建议把本目录中的脚本分成两类理解：

- 正式公开主线
  - `CommonCrawlNews/`
  - `OpenNewsArchive/`
  - `AIAAIC/process_AIAAIC.py`
  - `AIID/process_AIID.py`
- 可选或易失效抓取器
  - `Crawled_Dataset/`
  - `Hot_list_word_Dataset/`
  - `AIAAIC/llm_crawl_news.py`

后一类脚本更容易受网页结构变化、认证方式或外部服务配置影响。公开仓库保留它们，是为了说明数据获取思路和曾经的实现方式，而不是承诺长期无需修改即可运行。
但公开仓库保证后一种方式在全体数据中占比相对较少，对针对本数据集的研究影响相对较小。


## Quick Start

1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 检查并修改对应配置文件

```text
config/Data_Sources-CommonCrawlNews-config.ini
config/Data_Sources-OpenNewsArchive-config.ini
config/Data_Sources-Crawled_Dataset-config.ini
config/Data_Sources-Hot_list_word_Dataset-config.ini
config/Data_Sources-AIAAIC-config.ini
config/Data_Sources-AIID-config.ini
```

3. 从仓库根目录运行脚本

```bash
python src/Data_Sources/CommonCrawlNews/process_CommonCrawlNews.py
python src/Data_Sources/AIID/process_AIID.py
python src/Data_Sources/AIAAIC/process_AIAAIC.py
python src/Data_Sources/OpenNewsArchive/data_collecting_OpenNewsArchive.py
```

## Config Notes

- `CommonCrawlNews`
  - 需要配置年份、月份、下载目录、并发数
- `OpenNewsArchive`
  - 需要配置 OpenXLab 的 `access_key`、`secret_key` 和本地保存目录
- `Crawled_Dataset`
  - 需要配置新闻站点 URL、关键词、爬虫函数映射、最大页数和测试模式
- `Hot_list_word_Dataset`
  - 主要配置时间范围和输出文件路径
- `AIAAIC`
  - 需要配置原始 Excel 路径、处理后输出路径
- `AIID`
  - 需要配置原始 CSV 路径与输出 JSONL 路径

- 注意，`AIAAIC`和`AIID`需要下载的数据文件，本仓库已经放到了data文件夹中，数据源文件夹下的脚本是处理脚本

## Config Field Meanings

- `config/Data_Sources-CommonCrawlNews-config.ini`
  - `year`: 要下载的年份列表，逗号分隔
  - `months`: 要下载的月份列表，逗号分隔
  - `download_dir`: WRAC/WARC 文件保存目录
  - `base_url`: Common Crawl 数据根地址
  - `max_workers`: 下载并发数
  - `max_speed_mbps`: 可选下载限速
- `config/Data_Sources-OpenNewsArchive-config.ini`
  - `access_key` / `secret_key`: OpenXLab 认证信息
  - `target_path`: 数据集下载目录
  - `source_path`: 可选单文件下载路径
  - `dataset_repo`: OpenXLab 数据集标识
- `config/Data_Sources-Crawled_Dataset-config.ini`
  - `NewsSites`: 各站点基础 URL
  - `Keywords`: 各站点搜索关键词
  - `CrawlerMapping`: 站点名到爬虫函数的导入路径
  - `MaxPages`: 翻页或滚动次数上限
  - `Manual.auto_upload`: 手动运行后是否调用上传接口
  - `TestMode.enabled`: 是否默认进入测试模式
  - `TestMode.test_site`: 测试模式运行的站点名
  - `AutoCrawler.enabled`: 是否启用定时爬取
  - `AutoCrawler.schedule_time` / `schedule_day`: 定时任务计划
  - `API.base_url`: 可选上传 API 根地址
- `config/Data_Sources-Hot_list_word_Dataset-config.ini`
  - `Weibo.start_date` / `end_date`: 微博热榜抓取时间范围
  - `Weibo.output_file`: 微博输出 JSON 路径
  - `Douyin.output_file`: 抖音输出 JSON 路径
  - `Zhihu.output_file`: 知乎输出 JSON 路径
  - `V2EX.output_file`: V2EX 输出 JSON 路径
  - `Toutiao.output_file`: 今日头条输出 JSON 路径
- `config/Data_Sources-AIAAIC-config.ini`
  - `repository_xlsx`: 原始 AIAAIC Excel 路径
  - `processed_jsonl`: 结构化后的 incident/link 输出
  - `llm_output_jsonl`: LLM 补充抓取结果输出
  - `llm_error_jsonl`: LLM 抓取失败记录
  - `provider`: 模型服务提供方标识
  - `base_url`: 模型服务地址
  - `api_token`: 模型服务认证令牌
  - `chunk_token_threshold`: 文本切块阈值
  - `overlap_rate`: 相邻切块重叠比例
  - `apply_chunking`: 是否开启切块
  - `input_format`: 传给提取器的输入格式
- `config/Data_Sources-AIID-config.ini`
  - `reports_csv`: AIID reports 原始 CSV
  - `incidents_csv`: AIID incidents 原始 CSV
  - `incidents_jsonl`: incidents 导出 JSONL
  - `cases_jsonl`: cases 导出 JSONL

## Output Expectations

- 下载类脚本通常写入 `download_dir/` 或配置文件中指定的目标目录
- 数据整理脚本通常写入 `data/` 下的 JSONL 文件
- 网页爬虫通常写入 `download_dir/Crawled_Dataset/` 或热点榜单目录

这些输出一般会作为后续：

- `src/Identification_Method`
- `src/Database`

的输入。

## Notes for Public Release

- 若脚本依赖网页结构，后续页面变化可能导致失效
- 若脚本依赖第三方认证信息，公开版只保留占位配置，不包含真实密钥
- 若你只是希望复现论文主方法，通常不需要逐个重新抓取所有数据源
- 若你希望完整重建数据处理链路，本目录才是起点
