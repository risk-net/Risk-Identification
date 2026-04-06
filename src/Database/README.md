# Database

## 用途

`src/Database` 是数据库支撑层，负责把多源新闻导入 PostgreSQL，并构建下游全量对齐使用的事件级工作表。

该目录只保留最小主线：
- `schema.sql`：正式 schema
- `import_data.py`：基础导入
- `import_event_news.py`：事件级导入

## 目录结构

```text
src/Database/
├── README.md
├── schema.sql
├── import_data.py
└── import_event_news.py
```

## 关键表

- `ai_risk_relevant_news`：基础导入表。
- `ai_risk_events_news`：事件级工作表（全量对齐主输入表）。
- `v_alignment_input_v1`：跨仓数据契约视图（定义见仓库根目录 `CONTRACT.md`）。

## 配置

各脚本独立读取：`config/Database-config.ini`

至少需要填写：
- `[Database]`：`host/port/database/user/password`
- `[Paths]`：`artifacts_root/downloads_root/db_data_dir`
- `[Sources]`：各数据源目录

## 最小主线流程（仅三步）

从仓库根目录执行：

### 1) 初始化 schema

```bash
psql -h <host> -U <user> -d <database> -f src/Database/schema/schema.sql
```

### 2) 导入基础新闻

```bash
cd src
python -m Database.import_data
```

### 3) 构建事件级工作表

```bash
cd src
python -m Database.import_event_news
```

## 输出约定

运行日志与临时产物统一写到：
- `outputs/Database/`

## 与方法目录关系

`src/Event_Align_Method/full_application` 会直接读取 `ai_risk_events_news`。
因此本目录是全量推理链路的前置依赖。
