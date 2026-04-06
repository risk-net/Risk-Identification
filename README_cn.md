# Risk-Identification
[English](./README.md)

本项目用于识别 AI 风险事件，覆盖从多源数据获取、关键词筛选、LLM 三分类识别，到评测与数据库导入的完整流程。

## 项目范围

- `src/Data_Sources`：数据采集与预处理
- `src/Identification_Method`：关键词筛选与 LLM 识别
- `src/Identification_Evaluation`：关键词与三分类评测
- `src/Database`：PostgreSQL 表结构与导入脚本

## 目录结构

```text
Risk-Identification/
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

```bash
pip install -e .
python -m nltk.downloader punkt stopwords wordnet
```

说明：部分关键词脚本依赖 NLTK 资源；LLM 脚本需在
`config/Identification_Method-*-llm_filter-config.ini` 中配置 API 参数。

## 快速开始

1. 仅运行评测（无需外部采集）：

```bash
python src/Identification_Evaluation/evaluate_keyword_filter.py
python src/Identification_Evaluation/evaluate_llm_multiclass.py
```

默认输入：`data/evaluation_dataset_2000.json`  
默认输出：`outputs/Identification_Evaluation/`

2. 按需进行数据采集：

```bash
python src/Data_Sources/AIID/process_AIID.py
python src/Data_Sources/AIAAIC/process_AIAAIC.py
python src/Data_Sources/CommonCrawlNews/process_CommonCrawlNews.py
python src/Data_Sources/OpenNewsArchive/data_collecting_OpenNewsArchive.py
```

3. 识别流程（关键词 + LLM），示例：

```bash
python src/Identification_Method/OpenNewsArchive/keyword_filter/process_OpenNews.py
python src/Identification_Method/OpenNewsArchive/llm_filter/async_batch_multi_endpoint_full_dataset.py
```

4. 可选：导入数据库：

```bash
psql -h <host> -U <user> -d <database> -f src/Database/schema.sql
python src/Database/import_data.py
python src/Database/import_event_news.py
```

数据库配置：`config/Database-config.ini`

## 配置文件约定

- 数据源配置：`config/Data_Sources-*.ini`
- 识别配置：`config/Identification_Method-*-config.ini`
- 评测配置：`config/Identification_Evaluation-config.ini`
- 数据库配置：`config/Database-config.ini`

相对路径默认按仓库根目录解析（以脚本实现为准）。

## 模块文档

- `src/Data_Sources/README.md`
- `src/Identification_Method/README.md`
- `src/Identification_Evaluation/README.md`
- `src/Database/README.md`
