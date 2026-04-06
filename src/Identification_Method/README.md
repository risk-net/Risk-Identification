# Identification_Method

`src/Identification_Method/` 存放 AI 风险事件识别相关的数据处理脚本。

当前已经整理并保持可运行的子模块包括：

- `CommonCrawlNews`
- `OpenNewsArchive`
- `Hot_list_word_Dataset`
- `Chinese_Data`
- `AIID_AIAAIC`

整体目标是统一成：

1. 配置集中在 `config/Identification_Method-*.ini`
2. 输出统一写到 `outputs/`
3. 每个数据源只保留最小可运行主链路
4. 每个数据源目录下都有局部 `README.md`

## 快速上手

1. 安装依赖

```bash
cd /home/nlper/zlh/ai-risk-event-identification-and-alignment
uv sync --group dev
python -m nltk.downloader punkt stopwords wordnet
```

常用依赖包括：
- `boilerpy3`
- `warcio`
- `ahocorasick`
- `jieba`
- `volcenginesdkarkruntime`
- `fuzzywuzzy`
- `langdetect`
- `pandas`

2. 配置参数

- 编辑 `config/Identification_Method-*-*.ini`
- 所有相对路径都以仓库根目录为基准
- API 参数统一写在 `[*.API]` 段

3. 运行脚本

- 大部分脚本直接 `python path/to/script.py`
- 日志输出到 `logs/`
- 结果输出到 `outputs/`

## 目录速览

| 子目录 | 当前作用 | 主入口 | 配置文件 |
| --- | --- | --- | --- |
| `CommonCrawlNews` | WARC 提取、关键词初筛、LLM 识别 | `keyword_filter/process_wrac.py` `llm_filter/async_batch_multi_endpoint_full_dataset.py` | `config/Identification_Method-CommonCrawlNews-keyword_filter-config.ini` `config/Identification_Method-CommonCrawlNews-llm_filter-config.ini` |
| `OpenNewsArchive` | JSONL 关键词初筛、LLM 识别 | `keyword_filter/process_OpenNews.py` `llm_filter/async_batch_multi_endpoint_full_dataset.py` | `config/Identification_Method-OpenNewsArchive-keyword_filter-config.ini` `config/Identification_Method-OpenNewsArchive-llm_filter-config.ini` |
| `Hot_list_word_Dataset` | 热榜标题关键词初筛、LLM 二次筛选 | `keyword_filter/filter_title.py` `llm_filter/filter_title_llm.py` | `config/Identification_Method-Hot_list_word_Dataset-llm_filter-config.ini` |
| `Chinese_Data` | 中文新闻 JSONL 直接做 LLM 识别 | `llm_filter/async_batch_multi_endpoint_full_dataset.py` | `config/Identification_Method-Chinese_Data-llm_filter-config.ini` |
| `AIID_AIAAIC` | AIID/AIAAIC 标准案例在线推理识别 | `llm_filter/async_online_multi_endpoint_full_dataset.py` | `config/Identification_Method-AIID_AIAAIC-llm_filter-config.ini` |

## 各数据源主链路

### CommonCrawlNews

1. 关键词阶段  
   运行：
   `python src/Identification_Method/CommonCrawlNews/keyword_filter/process_wrac.py`

   输出：
   `outputs/CommonCrawlNews/keyword_filter/<year>/<month>/{texts,htmls,keywords,phrase_matches,residual_keywords}/`

2. LLM 阶段  
   运行：
   `python src/Identification_Method/CommonCrawlNews/llm_filter/async_batch_multi_endpoint_full_dataset.py`

   配置：
   `config/Identification_Method-CommonCrawlNews-llm_filter-config.ini`

   说明：
   通过 `RUN_MODE=batch|online` 切换批量推理或在线推理。

### OpenNewsArchive

1. 关键词阶段  
   运行：
   `python src/Identification_Method/OpenNewsArchive/keyword_filter/process_OpenNews.py`

   输出：
   - `outputs/OpenNewsArchive/ai_related_news.jsonl`
   - `outputs/OpenNewsArchive/<year>/<month>/texts/*.txt`
   - `outputs/OpenNewsArchive/<year>/<month>/jsons/*.json`

2. LLM 阶段  
   运行：
   `python src/Identification_Method/OpenNewsArchive/llm_filter/async_batch_multi_endpoint_full_dataset.py`

   配置：
   `config/Identification_Method-OpenNewsArchive-llm_filter-config.ini`

   说明：
   通过 `RUN_MODE=batch|online` 切换批量推理或在线推理。

### Hot_list_word_Dataset

1. 关键词阶段  
   运行：
   `python src/Identification_Method/Hot_list_word_Dataset/keyword_filter/filter_title.py`

   输出：
   - `outputs/Hot_list_word_Dataset/keyword_filter/hotlist_raw.csv`
   - `outputs/Hot_list_word_Dataset/keyword_filter/filtered_hotlist.csv`

2. LLM 阶段  
   运行：
   `python src/Identification_Method/Hot_list_word_Dataset/llm_filter/filter_title_llm.py`

   输出：
   - `outputs/Hot_list_word_Dataset/llm_filter/filtered_hotlist_llm.csv`

### Chinese_Data

1. 输入准备  
   `INPUT_ROOT` 支持：
   - 单个 `*.jsonl`
   - 一个包含多个 `*.jsonl` 的目录

2. LLM 阶段  
   运行：
   `python src/Identification_Method/Chinese_Data/llm_filter/async_batch_multi_endpoint_full_dataset.py`

   输出：
   `outputs/Identification_Method-Chinese_Data-llm_filter/<mode>_(full|test)/<classification>/<year>/<month>/`

   说明：
   通过 `RUN_MODE=batch|online` 切换批量推理或在线推理。

### AIID_AIAAIC

1. 输入准备  
   配置：
   `config/Identification_Method-AIID_AIAAIC-llm_filter-config.ini`

   输入：
   `INPUT_JSONL`

2. LLM 阶段  
   运行：
   `python src/Identification_Method/AIID_AIAAIC/llm_filter/async_online_multi_endpoint_full_dataset.py`

   输出：
   `outputs/AIID_AIAAIC/llm_filter/<from_database>/<classification>/`

## 配置约定

- `[*.API]`
  模型调用参数，如 `OPENAI_BASE_URL`、`OPENAI_API_KEY`、`ARK_API_KEY`

- `[*.AsyncBatch]`
  Ark 批量推理相关参数

- `[*.AsyncOnline]`
  Ark 在线推理相关参数

- `RUN_MODE`
  对已统一入口的模块，使用 `batch` 或 `online` 控制运行方式

## 输出约定

- Keyword 阶段输出：
  `outputs/<dataset>/keyword_filter/...`

- LLM 阶段输出：
  `outputs/<dataset>/llm_filter/...`
  或该数据源约定的结构化结果目录

- 日志输出：
  `logs/<dataset>/...`

## 建议阅读顺序

如果只想理解某个数据源，建议按下面顺序查看：

1. 该目录下的局部 `README.md`
2. 对应 `config/Identification_Method-*.ini`
3. 主入口脚本

当前各子目录 README：

- `src/Identification_Method/CommonCrawlNews/README.md`
- `src/Identification_Method/OpenNewsArchive/README.md`
- `src/Identification_Method/Hot_list_word_Dataset/README.md`
- `src/Identification_Method/Chinese_Data/README.md`
- `src/Identification_Method/AIID_AIAAIC/README.md`
