# Risk-Identification
[中文文档](./README_cn.md)

This project is designed to identify AI risk incidents, covering the complete workflow from multi-source data collection, keyword filtering, and LLM three-category classification to evaluation and database import.

## Project Scope
- `src/Data_Sources`: Data collection and preprocessing
- `src/Identification_Method`: Keyword filtering and LLM identification
- `src/Identification_Evaluation`: Evaluation of keywords and three-category classification
- `src/Database`: PostgreSQL table schema and import scripts

## Directory Structure
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

## Environment Requirements
- Python `>=3.11`

```bash
pip install -e .
python -m nltk.downloader punkt stopwords wordnet
```

Note: Some keyword scripts depend on NLTK resources; LLM scripts require API parameter configuration in
`config/Identification_Method-*-llm_filter-config.ini`.

## Quick Start
1. Run evaluation only (no external collection required):

```bash
python src/Identification_Evaluation/evaluate_keyword_filter.py
python src/Identification_Evaluation/evaluate_llm_multiclass.py
```

Default input: `data/evaluation_dataset_2000.json`
Default output: `outputs/Identification_Evaluation/`

2. Perform data collection as needed:

```bash
python src/Data_Sources/AIID/process_AIID.py
python src/Data_Sources/AIAAIC/process_AIAAIC.py
python src/Data_Sources/CommonCrawlNews/process_CommonCrawlNews.py
python src/Data_Sources/OpenNewsArchive/data_collecting_OpenNewsArchive.py
```

3. Identification pipeline (keywords + LLM), example:

```bash
python src/Identification_Method/OpenNewsArchive/keyword_filter/process_OpenNews.py
python src/Identification_Method/OpenNewsArchive/llm_filter/async_batch_multi_endpoint_full_dataset.py
```

4. Optional: Import to database:

```bash
psql -h <host> -U <user> -d <database> -f src/Database/schema.sql
python src/Database/import_data.py
python src/Database/import_event_news.py
```

Database configuration: `config/Database-config.ini`

## Configuration File Conventions
- Data source configurations: `config/Data_Sources-*.ini`
- Identification configurations: `config/Identification_Method-*-config.ini`
- Evaluation configurations: `config/Identification_Evaluation-config.ini`
- Database configuration: `config/Database-config.ini`

Relative paths are resolved relative to the repository root by default (subject to script implementation).

## Module Documentation
- `src/Data_Sources/README.md`
- `src/Identification_Method/README.md`
- `src/Identification_Evaluation/README.md`
- `src/Database/README.md`