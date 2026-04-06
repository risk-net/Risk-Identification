# AI Risk Event Identification

AI 风险事件识别项目，覆盖从多源数据获取、关键词筛选、LLM 三分类识别到评测与数据库导入的完整链路。

## 当前项目范围

- `src/Data_Sources`: 数据采集与原始数据整理
- `src/Identification_Method`: 各数据源的关键词筛选与 LLM 识别
- `src/Identification_Evaluation`: 关键词筛选与三分类评测
- `src/Database`: PostgreSQL 导入与事件级数据构建

## 目录结构

```text
ai-risk-event-identification/
├── config/
├── data/
├── keywords/
├── download_dir/
├── prompt/
├── outputs/
├── src/
│   ├── Data_Sources/
│   ├── Identification_Method/
│   ├── Identification_Evaluation/
│   └── Database/
├── pyproject.toml
└── README.md
```

## 环境要求

- Python `>=3.11`
- 建议在仓库根目录安装依赖：

```bash
pip install -e .
python -m nltk.downloader punkt stopwords wordnet
```

说明：部分关键词筛选脚本依赖 NLTK 资源；LLM 脚本还需要在对应 `config/Identification_Method-*-llm_filter-config.ini` 中配置 API 参数。

## 快速开始（最小可运行链路）

### 1) 评测模块（无需外部数据采集）

```bash
python src/Identification_Evaluation/evaluate_keyword_filter.py
python src/Identification_Evaluation/evaluate_llm_multiclass.py
```

默认输入：`data/evaluation_dataset_2000.json`  
默认输出：`outputs/Identification_Evaluation/`

### 2) 数据获取与预处理（按需）

```bash
python src/Data_Sources/AIID/process_AIID.py
python src/Data_Sources/AIAAIC/process_AIAAIC.py
python src/Data_Sources/CommonCrawlNews/process_CommonCrawlNews.py
python src/Data_Sources/OpenNewsArchive/data_collecting_OpenNewsArchive.py
```

对应配置：
- `config/Data_Sources-AIID-config.ini`
- `config/Data_Sources-AIAAIC-config.ini`
- `config/Data_Sources-CommonCrawlNews-config.ini`
- `config/Data_Sources-OpenNewsArchive-config.ini`

### 3) 识别模块（关键词 + LLM）

示例：

```bash
python src/Identification_Method/OpenNewsArchive/keyword_filter/process_OpenNews.py
python src/Identification_Method/OpenNewsArchive/llm_filter/async_batch_multi_endpoint_full_dataset.py
```

其他主入口：
- `src/Identification_Method/CommonCrawlNews/keyword_filter/process_wrac.py`
- `src/Identification_Method/CommonCrawlNews/llm_filter/async_batch_multi_endpoint_full_dataset.py`
- `src/Identification_Method/Hot_list_word_Dataset/keyword_filter/filter_title.py`
- `src/Identification_Method/Hot_list_word_Dataset/llm_filter/filter_title_llm.py`
- `src/Identification_Method/Chinese_Data/llm_filter/async_batch_multi_endpoint_full_dataset.py`
- `src/Identification_Method/AIID_AIAAIC/llm_filter/async_online_multi_endpoint_full_dataset.py`

### 4) 数据库导入（可选）

1. 初始化表结构：

```bash
psql -h <host> -U <user> -d <database> -f src/Database/schema.sql
```

2. 执行导入：

```bash
python src/Database/import_data.py
python src/Database/import_event_news.py
```

配置文件：`config/Database-config.ini`

## 配置文件约定

- 数据源配置：`config/Data_Sources-*.ini`
- 识别方法配置：`config/Identification_Method-*-config.ini`
- 评测配置：`config/Identification_Evaluation-config.ini`
- 数据库配置：`config/Database-config.ini`

路径若为相对路径，默认相对于仓库根目录解析（以各脚本实现为准）。

## 模块文档

- `src/Data_Sources/README.md`
- `src/Identification_Method/README.md`
- `src/Identification_Evaluation/README.md`
- `src/Database/README.md`

如果只想跑通一条链路，建议优先从 `Identification_Evaluation` 开始。
