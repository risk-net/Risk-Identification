# OpenNewsArchive

这个目录对应 OpenNewsArchive 数据源的识别流程，分为两部分：

- `keyword_filter/`
- `llm_filter/`

## 目录作用

### `keyword_filter/`
这一层做关键词初筛，不调用大模型。

核心脚本：
- `keyword_filter/process_OpenNews.py`

它的作用是：
- 递归扫描输入目录下的所有 `*.jsonl`
- 逐行读取新闻 JSON
- 根据关键词表和短语表筛出 AI 相关新闻
- 直接输出可供 `llm_filter` 使用的 txt/json 候选文件

### `llm_filter/`
这一层做大模型识别。

核心脚本：
- `llm_filter/async_batch_multi_endpoint_full_dataset.py`
- `llm_filter/callmodel.py`

其中：
- `async_batch_multi_endpoint_full_dataset.py` 是统一入口
- 通过配置文件中的 `RUN_MODE=batch|online` 决定走批量推理还是在线推理
- `callmodel.py` 提供当前渠道内部的大模型调用封装

## `keyword_filter/process_OpenNews.py` 怎么处理数据

### 输入

脚本读取配置文件：
- `config/Identification_Method-OpenNewsArchive-keyword_filter-config.ini`

主要输入项：
- `BASE_INPUT_DIR`
  OpenNewsArchive 原始 JSONL 根目录。脚本会递归扫描这个目录下所有 `*.jsonl` 文件。
- `KEYWORDS_FILE`
  关键词文件。
- `PHRASES_FILE`
  短语文件。
- `JSONL_FILE`
  筛选结果 JSONL 的输出路径。
- `BASE_OUTPUT_DIR`
  候选文件输出根目录。

每条输入新闻 JSON 至少应包含：
- `content`

最好同时包含：
- `id`
- `title`
- `language`

其中：
- `language` 用来决定走英文分词还是中文分词
- `id` 用来生成输出 txt 文件名
- `title` 会写入输出 txt 的第一行

### 处理逻辑

脚本的处理流程是：

1. 扫描 `BASE_INPUT_DIR` 下所有 `*.jsonl`
2. 逐行读取每条新闻 JSON
3. 读取 `content` 和 `language`
4. 用 `PHRASES_FILE` 做短语匹配
5. 再根据语言提取关键词
6. 将“短语命中 + 提取出的高频词”合并
7. 只要这个集合与 `KEYWORDS_FILE` 中的关键词有交集，就保留这条新闻

实现细节：
- 英文文本使用 `nltk` 分词、去停用词、词形还原
- 中文文本使用 `jieba` 分词
- 短语匹配使用 `ahocorasick`

所以这个脚本本质上是规则筛选，不是大模型判断。

### 输出

脚本会输出两类结果：

1. 汇总 JSONL
- 路径由 `JSONL_FILE` 指定
- 默认是 `outputs/OpenNewsArchive/ai_related_news.jsonl`
- 内容是所有命中的新闻原始 JSON，一行一条

2. 单篇 txt
- 输出到 `BASE_OUTPUT_DIR/<year>/<month>/texts/`
- 每篇命中的新闻会写成一个 txt 文件
- 文件内容格式：
  - 第一行：`title`
  - 后续内容：`content`
- 文件名来自新闻的 `id`，会先做安全字符清洗

3. 单篇 json
- 输出到 `BASE_OUTPUT_DIR/<year>/<month>/jsons/`
- 每篇命中的新闻会写成一个 JSON 文件
- 字段包括：
  - `id`
  - `title`
  - `content`
  - `release_date`
  - `language`
- 这个输出可以直接给 `llm_filter` 的 `RUN_MODE=online` 使用

## 和后续 `llm_filter` 的关系

可以把这个目录理解成两阶段：

1. `keyword_filter/process_OpenNews.py`
- 先做规则初筛
- 目标是从大规模新闻里筛出“可能与 AI 相关”的候选集

2. `llm_filter/async_batch_multi_endpoint_full_dataset.py`
- 再对候选数据做大模型识别
- 输出结构化结果，例如：
  - `classification_result`
  - 风险相关字段
  - 事件相关字段

也就是说：
- `keyword_filter` 负责降噪和缩小候选范围
- `llm_filter` 负责更精细的识别和结构化抽取
- 两者现在已经直接衔接，不再需要额外的格式转换脚本

## 当前建议

如果你只想理解 OpenNewsArchive 这一块，建议按下面顺序看：

1. `keyword_filter/process_OpenNews.py`
2. `config/Identification_Method-OpenNewsArchive-keyword_filter-config.ini`
3. `llm_filter/async_batch_multi_endpoint_full_dataset.py`
4. `config/Identification_Method-OpenNewsArchive-llm_filter-config.ini`
