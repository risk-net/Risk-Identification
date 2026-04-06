# Identification_Evaluation

该模块已收敛为最小评测包，仅保留两条能力链路：

1. 关键词筛选评测
2. 大模型三分类评测

## 目录结构

```text
src/Identification_Evaluation/
├── README.md
└── scripts/
    ├── evaluate_keyword_filter.py
    └── evaluate_llm_multiclass.py

data/Identification_Evaluation/datasets/
├── README.md
├── evaluation_dataset_2000.json
├── risk_event_annotations.json
├── risk_event_annotations_append.json
├── article_results_2000/
├── risk_related_samples_618/
└── model_false_positive_129/
```

## 快速开始

在仓库根目录执行：

### 1) 关键词筛选评测

```bash
python src/Identification_Evaluation/evaluate_keyword_filter.py
```

输出：
- `outputs/Identification_Evaluation/keyword_filter/summary.json`
- `outputs/Identification_Evaluation/keyword_filter/relevant.jsonl`
- `outputs/Identification_Evaluation/keyword_filter/unrelated.jsonl`

### 2) 大模型三分类评测

```bash
python src/Identification_Evaluation/evaluate_llm_multiclass.py
```

输出：
- `outputs/Identification_Evaluation/llm_multiclass/metrics_summary.json`

## 默认输入

- `evaluate_keyword_filter.py` 默认输入：`data/Identification_Evaluation/datasets/evaluation_dataset_2000.json`
- `evaluate_keyword_filter.py` 默认关键词文件：`keywords/Identification-common-keywords.txt`
- `evaluate_keyword_filter.py` 默认短语文件：`keywords/Identification-common-custom_phrases.txt`
- `evaluate_keyword_filter.py` 默认停用词文件：`keywords/Identification-common-stopwords.txt`
- `evaluate_llm_multiclass.py` 默认输入：`data/Identification_Evaluation/datasets/evaluation_dataset_2000.json`

## 数据文件说明

- `data/Identification_Evaluation/datasets/evaluation_dataset_2000.json`：2000 条三分类主评测集，是评测入口。
