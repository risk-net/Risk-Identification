# Chinese_Data

这个目录对应中文新闻数据集的识别流程。

需要说明的是，本中文新闻数据集并非来自于公开数据集，是本组织购买的私有数据集，data文件夹中并不会公开私有数据集。因此，Data_Sources文件夹中不会有获取Chinese_Data的相关脚本，如需获取经过处理后的总体数据，请联系我们的github

另外，由于本中文新闻数据集是购买的私有数据集，已经预先完成了关键词筛选这一部分，所以当前目录只保留 `llm_filter/`的相关脚本。

## 目录作用

### `llm_filter/`
这一层直接对中文新闻 JSONL 做大模型识别。

核心脚本：
- `llm_filter/async_batch_multi_endpoint_full_dataset.py`
- `llm_filter/callmodel.py`

其中：
- `async_batch_multi_endpoint_full_dataset.py` 是统一入口
- 通过配置文件中的 `RUN_MODE=batch|online` 决定走批量推理还是在线推理
- `callmodel.py` 提供当前渠道内部的大模型调用封装

## 输入

脚本读取配置文件：
- `config/Identification_Method-Chinese_Data-llm_filter-config.ini`

当前保留的输入方式是：
- 单个 `*.jsonl` 文件
- 或一个目录下的多个 `*.jsonl` 文件

主要输入项：
- `INPUT_ROOT`
  中文新闻 JSONL 根路径，可以是单个文件，也可以是目录。
- `PROMPT_PATH`
  LLM 提示词文件。

每条 JSONL 记录最好包含：
- `title`
- `content`

同时支持一部分兼容字段：
- `abstract`
- `publish_time`
- `html_info`

其中：
- `publish_time` / `html_info` 用来提取 `release_date`
- `title` 和 `content` 用来构造模型输入

## 处理逻辑

统一入口脚本会：

1. 扫描 `INPUT_ROOT` 下的 JSONL 文件
2. 逐行读取新闻 JSON
3. 抽取 `title`、`content`、`release_date`
4. 按批次组装请求
5. 调用 Ark 批量接口或在线接口
6. 将结构化结果按分类 / 年 / 月写入输出目录

## 输出

输出会写到：
- `outputs/Identification_Method-Chinese_Data-llm_filter/batch_full`
- `outputs/Identification_Method-Chinese_Data-llm_filter/batch_test`
- `outputs/Identification_Method-Chinese_Data-llm_filter/online_full`
- `outputs/Identification_Method-Chinese_Data-llm_filter/online_test`

结果目录结构为：
- `<output_root>/<classification>/<year>/<month>/*_result.json`

分类包括：
- `AIrisk_relevant_event`
- `AIrisk_relevant_discussion`
- `AIrisk_Irrelevant`

每个结果文件会保存：
- `classification_result`
- 风险/事件抽取字段
- 原始 `title`
- 原始 `content`
- 来源文件与行号元数据


