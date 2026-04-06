# Hot_list_word_Dataset

这个目录用于处理热榜/热搜标题数据，当前流程分为两步：

1. 关键词初筛
2. 大模型二次筛选

目录下当前保留的有效文件只有：

- `keyword_filter/filter_title.py`
- `llm_filter/filter_title_llm.py`
- `llm_filter/callmodel.py`

## 当前主链路

### 第一步：关键词初筛

脚本：
- `keyword_filter/filter_title.py`

作用：
- 读取热榜原始 JSON
- 展平成 CSV
- 对标题做关键词包含和模糊匹配
- 输出关键词筛选后的 CSV

配置来源：
- `config/Identification_Method-Hot_list_word_Dataset-llm_filter-config.ini`
- 使用段：`[HotList.KeywordFilter]`

默认路径约定：
- 原始输入：
  - `download_dir/douyin_hotlist.json`
- 中间输出：
  - `outputs/Hot_list_word_Dataset/keyword_filter/hotlist_raw.csv`
  - `outputs/Hot_list_word_Dataset/keyword_filter/filtered_hotlist.csv`

### 第二步：大模型二次筛选

脚本：
- `llm_filter/filter_title_llm.py`

作用：
- 读取关键词筛选后的 CSV
- 按批次组装标题数据
- 调用大模型
- 解析标签列表
- 保留模型判断为相关的标题

依赖：
- `llm_filter/callmodel.py`

配置来源：
- `config/Identification_Method-Hot_list_word_Dataset-llm_filter-config.ini`
- 使用段：
  - `[HotList.LLMFilter]`
  - `[HotList.API]`

默认路径约定：
- 输入：
  - `outputs/Hot_list_word_Dataset/keyword_filter/filtered_hotlist.csv`
- 输出：
  - `outputs/Hot_list_word_Dataset/llm_filter/filtered_hotlist_llm.csv`

## 各文件作用

### `keyword_filter/filter_title.py`

这是规则筛选脚本。

当前逻辑：
1. 读取热榜 JSON
2. 展平成标准列：
   - `id`
   - `日期`
   - `title`
   - `url`
3. 对 `title` 做关键词匹配
4. 满足以下任一条件则保留：
   - 标题直接包含关键词
   - `fuzzywuzzy.partial_ratio` 达到阈值
5. 写出筛选结果 CSV

当前兼容的输入形式包括：
- 顶层按日期分组的对象
- 顶层直接为列表
- 条目中 `title` 既可以是字符串，也可以是嵌套对象

### `llm_filter/filter_title_llm.py`

这是大模型筛选脚本。

当前逻辑：
1. 读取关键词筛选后的 CSV
2. 按 `BATCH_SIZE` 切批
3. 将每批标题转成 JSON payload
4. 调用 `call_model()`
5. 解析模型返回的标签数组
6. 保留相关标题并写出新的 CSV

### `llm_filter/callmodel.py`

这是模型调用封装层。

提供的能力包括：
- Provider 方式调用
- OpenAI / Ark / Mock / Echo 支持
- HTTP fallback

它不是业务入口脚本，但 `filter_title_llm.py` 依赖它。

## 当前最小保留集

如果只保留当前可运行主流程，建议保留：

- `keyword_filter/filter_title.py`
- `llm_filter/filter_title_llm.py`
- `llm_filter/callmodel.py`
- 本 README
