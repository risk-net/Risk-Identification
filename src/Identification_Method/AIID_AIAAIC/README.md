# AIID_AIAAIC

这个目录对应 AIID 与 AIAAIC 案例数据集的识别流程。

当前目录只保留 `llm_filter/`。

## 目录作用

### `llm_filter/`
这一层直接对 AIID / AIAAIC 的标准案例 JSONL 做大模型识别。

核心脚本：
- `llm_filter/async_online_multi_endpoint_full_dataset.py`

它的作用是：
- 读取标准案例 JSONL
- 按批次调用在线推理模型
- 将结构化结果按来源数据库和分类写入输出目录

## 输入

脚本读取配置文件：
- `config/Identification_Method-AIID_AIAAIC-llm_filter-config.ini`

主要输入项：
- `INPUT_JSONL`
  AIID / AIAAIC 标准案例 JSONL 文件。
- `PROMPT_PATH`
  LLM 提示词文件。

每条输入记录最好包含：
- `id` 或 `original_id`
- `title`
- `text`
- `from_database`

其中：
- `from_database` 应为 `aiid` 或 `aiaaic`

## 输出

结果会输出到：
- `outputs/AIID_AIAAIC/llm_filter/<from_database>/<classification>/*_result.json`

分类包括：
- `AIrisk_relevant_event`
- `AIrisk_relevant_discussion`
- `AIrisk_Irrelevant`

每个结果文件会保留：
- `classification_result`
- 风险/事件抽取字段
- 原始 `title`
- 原始 `text`
- `id` / `original_id`
- `from_database`

## 当前最小保留集

如果只保留当前可运行主流程，建议保留：

- `llm_filter/async_online_multi_endpoint_full_dataset.py`
- 本 README
