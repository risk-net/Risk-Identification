# CommonCrawlNews

这个目录对应 CommonCrawlNews 数据源的识别流程，分为两部分：

- `keyword_filter/`
- `llm_filter/`

## 目录作用

### `keyword_filter/`
这一层做 WARC 提取和关键词初筛，不调用大模型。

核心脚本：
- `keyword_filter/process_wrac.py`

它的作用是：
- 按年份和月份扫描 Common Crawl WARC 文件
- 提取 HTML 与正文文本
- 用关键词表和短语表筛出 AI 相关新闻
- 直接输出可供 `llm_filter` 使用的 `texts/` 候选文件

### `llm_filter/`
这一层做大模型识别。

核心脚本：
- `llm_filter/async_batch_multi_endpoint_full_dataset.py`
- `llm_filter/callmodel.py`

其中：
- `async_batch_multi_endpoint_full_dataset.py` 是统一入口
- 通过配置文件中的 `RUN_MODE=batch|online` 决定走批量推理还是在线推理
- `callmodel.py` 提供当前渠道内部的大模型调用封装

## `keyword_filter/process_wrac.py` 怎么处理数据

### 输入

脚本读取配置文件：
- `config/Identification_Method-CommonCrawlNews-keyword_filter-config.ini`

主要输入项：
- `WARC_FOLDER`
  Common Crawl WARC 根目录，默认按 `<year>/<month>/` 组织。
- `KEYWORDS_FILE`
  关键词文件。
- `PHRASES_FILE`
  短语文件。
- `OUTPUT_DIR`
  输出根目录，当前统一为 `outputs/CommonCrawlNews/keyword_filter`。

### 处理逻辑

脚本的处理流程是：

1. 扫描指定年份和月份下的 WARC 压缩包
2. 从 WARC 中提取 HTML
3. 用 `boilerpy3` 抽取正文文本
4. 用 `ahocorasick` 做短语匹配
5. 用 `nltk + jieba` 做关键词提取
6. 将命中的新闻写到按年月划分的输出目录

所以这一层本质上是规则筛选和原文落盘，不做事件级判断。

### 输出

脚本会输出到：
- `outputs/CommonCrawlNews/keyword_filter/<year>/<month>/texts/`
- `outputs/CommonCrawlNews/keyword_filter/<year>/<month>/htmls/`
- `outputs/CommonCrawlNews/keyword_filter/<year>/<month>/keywords/`
- `outputs/CommonCrawlNews/keyword_filter/<year>/<month>/phrase_matches/`
- `outputs/CommonCrawlNews/keyword_filter/<year>/<month>/residual_keywords/`

其中：
- `texts/` 是后续 `llm_filter` 的直接输入
- 其余目录主要用于追溯和调试

## 和后续 `llm_filter` 的关系

可以把这个目录理解成两阶段：

1. `keyword_filter/process_wrac.py`
- 先做 WARC 提取和规则初筛
- 目标是把海量网页压缩到“可能与 AI 相关”的候选新闻文本

2. `llm_filter/async_batch_multi_endpoint_full_dataset.py`
- 再对 `texts/` 里的候选新闻做大模型识别
- 输出结构化分类结果

也就是说：
- `keyword_filter` 负责抽取正文和降噪
- `llm_filter` 负责更精细的 AI 风险事件识别
- 两者现在已经直接衔接，不再需要额外转换脚本

## 当前最小保留集

如果只保留当前可运行主流程，建议保留：

- `keyword_filter/process_wrac.py`
- `llm_filter/async_batch_multi_endpoint_full_dataset.py`
- `llm_filter/callmodel.py`
- 本 README
