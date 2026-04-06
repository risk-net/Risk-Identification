# 事件识别数据集说明

本目录用于存放 **AI 风险事件识别** 主流程使用的数据集。

## 当前数据集

### `evaluation_dataset_2000.json`
- 用途：识别评测主入口（关键词筛选评测、LLM 三分类评测）。
- 数据规模：2000 条样本。
- 典型字段：
  - `content`：新闻正文
  - `classification_result`：模型标签（`AIrisk_relevant_event` / `AIrisk_relevant_discussion` / `AIrisk_Irrelevant`）
  - `doc_level`：人工文档级标签（事件级/讨论级/无关）
  - `hash_name`：样本 ID
  - `ai_tech` / `ai_risk` / `event`：结构化补充字段（部分样本可能为空）

## 数据集的作用
- 用于评测AI风险事件识别中，关键词识别AI新闻和大模型识别AI风险相关新闻（事件级，讨论级和无关）的识别指标
## 与脚本的对应关系

- 关键词评测脚本默认读取：
  - `src/Identification_Evaluation/ss/evaluate_keyword_filter.py`
  - 默认输入：`data/evaluation_dataset_2000.json`
- LLM 三分类评测脚本默认读取：
  - `src/Identification_Evaluation/scripts/evaluate_llm_multiclass.py`
  - 默认输入：`data/evaluation_dataset_2000.json`


