-- ============================================================
-- AI Risk News DB Schema (PostgreSQL)
-- Single formal schema for public use.
-- This file initializes:
-- - ai_risk_relevant_news
-- - ai_risk_events_news
-- - support tables for import / quality / evaluation
-- - embedding / alignment / clustering / global-event tables
-- ============================================================

-- ---------- Safety / defaults ----------
SET client_min_messages TO WARNING;

-- ---------- Drop (rebuild from scratch) ----------
DROP VIEW  IF EXISTS v_risk_type_stats CASCADE;
DROP VIEW  IF EXISTS v_time_distribution CASCADE;
DROP VIEW  IF EXISTS v_data_source_stats CASCADE;

DROP TABLE IF EXISTS global_event_merge_log CASCADE;
DROP TABLE IF EXISTS run_cluster_mappings CASCADE;
DROP TABLE IF EXISTS global_event_members CASCADE;
DROP TABLE IF EXISTS global_events CASCADE;
DROP TABLE IF EXISTS event_align_progress CASCADE;
DROP TABLE IF EXISTS event_cluster_profiles CASCADE;
DROP TABLE IF EXISTS event_cluster_members CASCADE;
DROP TABLE IF EXISTS event_clusters CASCADE;
DROP TABLE IF EXISTS event_align_edges CASCADE;
DROP TABLE IF EXISTS event_align_pair_preds CASCADE;
DROP TABLE IF EXISTS event_align_candidates CASCADE;
DROP TABLE IF EXISTS event_align_runs CASCADE;
DROP TABLE IF EXISTS embedding_artifacts CASCADE;
DROP TABLE IF EXISTS embedding_runs CASCADE;
DROP TABLE IF EXISTS ingest_batch_items CASCADE;
DROP TABLE IF EXISTS ingest_batches CASCADE;
DROP TABLE IF EXISTS eval_human_model_labels CASCADE;
DROP TABLE IF EXISTS import_logs CASCADE;
DROP TABLE IF EXISTS data_quality_issues CASCADE;
DROP TABLE IF EXISTS file_path_mappings CASCADE;
DROP TABLE IF EXISTS content_hash_mappings CASCADE;
DROP TABLE IF EXISTS ai_risk_events_news CASCADE;
DROP TABLE IF EXISTS ai_risk_relevant_news CASCADE;

-- ---------- Helper: updated_at trigger function ----------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- ============================================================
-- 1) ai_risk_relevant_news: AI风险相关新闻表
-- ============================================================
CREATE TABLE ai_risk_relevant_news (
    id BIGSERIAL PRIMARY KEY,
    news_id BIGINT,

    -- Source identifiers
    data_source VARCHAR(50) NOT NULL,     -- 'cc_news', 'opennews', 'aiaaic', 'aiid', 'wenge'
    file_path TEXT,

    -- ----------------------------------------------------------
    -- Archive time keys (from folder path, parsed in importer)
    -- 用于记录你按年月存储的文件夹归档时间；不等同于事件发生时间/发布时间
    -- 例：archive_year=2024, archive_month=7, archive_ym=202407
    -- ----------------------------------------------------------
    archive_year  SMALLINT,
    archive_month SMALLINT,
    archive_ym    INTEGER,                -- 建议导入时直接写入；也可由 year/month 推导

    -- Mapping helpers
    hash_name TEXT,                       -- file basename: "<hash_name>_result.json"
    content_hash TEXT,                    -- sha256 hex (normalized content)
    content_hash_version SMALLINT NOT NULL DEFAULT 2,
    normalize_rule TEXT NOT NULL DEFAULT 'collapse_whitespace_strip',

    -- Classification
    classification_result VARCHAR(50) NOT NULL,  -- 'AIrisk_relevant_event', 'AIrisk_relevant_discussion', 'AIrisk_Irrelevant'
    classification_std_result VARCHAR(50),

    -- Content
    title TEXT,
    content TEXT NOT NULL,
    release_date DATE,

    -- Raw JSONB (keep for flexibility)
    ai_tech JSONB,
    ai_risk JSONB,
    event JSONB,

    -- -----------------------------
    -- Flattened fields: ai_tech
    -- (arrays stored as TEXT[])
    -- -----------------------------
    ai_system_list TEXT[],
    ai_system_type_list TEXT[],
    ai_system_domain_list TEXT[],

    -- -----------------------------
    -- Flattened fields: ai_risk
    -- -----------------------------
    ai_risk_description TEXT,
    ai_risk_type TEXT,
    ai_risk_subtype TEXT,
    harm_type TEXT,
    harm_severity TEXT,
    affected_actor_type TEXT,
    affected_actor_subtype TEXT,
    realized_or_potential TEXT,
    risk_stage TEXT,

    -- -----------------------------
    -- Flattened fields: event
    -- Note: you already have structured time/location columns below.
    -- -----------------------------
    event_actor_main TEXT,
    event_actor_main_type TEXT,
    event_actor_list TEXT[],              -- array of actors

    -- Added based on your example:
    event_ai_system TEXT,
    event_domain TEXT,
    event_type TEXT,
    event_cause TEXT,
    event_process TEXT,
    event_result TEXT,

    -- Time / location (structured)
    event_time_start_desc TEXT,
    event_time_end_desc TEXT,
    event_time_start DATE,
    event_time_end DATE,
    event_country VARCHAR(100),
    event_province VARCHAR(100),
    event_city VARCHAR(100),

    -- Dedup flags (optional)
    is_duplicate BOOLEAN DEFAULT FALSE,
    duplicate_group_id BIGINT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_relevant_news_hash_hex
      CHECK (content_hash IS NULL OR content_hash ~ '^[0-9a-f]{64}$'),

    -- archive year/month basic sanity checks
    CONSTRAINT chk_relevant_news_archive_year
      CHECK (archive_year IS NULL OR (archive_year BETWEEN 1990 AND 2100)),
    CONSTRAINT chk_relevant_news_archive_month
      CHECK (archive_month IS NULL OR (archive_month BETWEEN 1 AND 12)),
    CONSTRAINT chk_relevant_news_archive_ym
      CHECK (
        archive_ym IS NULL
        OR (archive_ym BETWEEN 199001 AND 210012)
      )
);

-- updated_at trigger for ai_risk_relevant_news
DROP TRIGGER IF EXISTS trg_relevant_news_updated_at ON ai_risk_relevant_news;
CREATE TRIGGER trg_relevant_news_updated_at
BEFORE UPDATE ON ai_risk_relevant_news
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------- Indexes for ai_risk_relevant_news ----------
CREATE INDEX idx_relevant_news_data_source        ON ai_risk_relevant_news(data_source);
CREATE INDEX idx_relevant_news_news_id            ON ai_risk_relevant_news(news_id);
CREATE INDEX idx_relevant_news_classification     ON ai_risk_relevant_news(classification_result);
CREATE INDEX idx_relevant_news_classification_std ON ai_risk_relevant_news(classification_std_result);

CREATE INDEX idx_relevant_news_release_date       ON ai_risk_relevant_news(release_date);
CREATE INDEX idx_relevant_news_file_path          ON ai_risk_relevant_news(file_path);

CREATE INDEX idx_relevant_news_hash_name          ON ai_risk_relevant_news(hash_name);
CREATE INDEX idx_relevant_news_content_hash       ON ai_risk_relevant_news(content_hash);
CREATE INDEX idx_relevant_news_hash_version       ON ai_risk_relevant_news(content_hash_version);

CREATE INDEX idx_relevant_news_event_time_start   ON ai_risk_relevant_news(event_time_start);
CREATE INDEX idx_relevant_news_event_country      ON ai_risk_relevant_news(event_country);
CREATE INDEX idx_relevant_news_duplicate_group    ON ai_risk_relevant_news(duplicate_group_id);

-- archive time indexes
CREATE INDEX idx_relevant_news_archive_year_month ON ai_risk_relevant_news(archive_year, archive_month);
CREATE INDEX idx_relevant_news_archive_ym         ON ai_risk_relevant_news(archive_ym);

-- JSONB GIN indexes
CREATE INDEX idx_relevant_news_ai_tech_gin ON ai_risk_relevant_news USING gin(ai_tech);
CREATE INDEX idx_relevant_news_ai_risk_gin ON ai_risk_relevant_news USING gin(ai_risk);
CREATE INDEX idx_relevant_news_event_gin   ON ai_risk_relevant_news USING gin(event);

-- Prevent duplicate import for same source+path (when path exists)
CREATE UNIQUE INDEX uq_relevant_news_source_path
  ON ai_risk_relevant_news(data_source, file_path)
  WHERE file_path IS NOT NULL;

-- ============================================================
-- 2) Content hash mapping table (representative mapping)
--    Each content_hash keeps the first hash_name encountered
-- ============================================================
CREATE TABLE content_hash_mappings (
    content_hash TEXT PRIMARY KEY,              -- 64 hex sha256
    hash_name TEXT NOT NULL,
    data_source VARCHAR(50),
    file_path TEXT,
    hash_version SMALLINT NOT NULL DEFAULT 2,
    normalize_rule TEXT NOT NULL DEFAULT 'collapse_whitespace_strip',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chm_hash_hex CHECK (content_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_chm_hash_name     ON content_hash_mappings(hash_name);
CREATE INDEX idx_chm_data_source   ON content_hash_mappings(data_source);
CREATE INDEX idx_chm_hash_version  ON content_hash_mappings(hash_version);

-- updated_at trigger for content_hash_mappings
DROP TRIGGER IF EXISTS trg_chm_updated_at ON content_hash_mappings;
CREATE TRIGGER trg_chm_updated_at
BEFORE UPDATE ON content_hash_mappings
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- 3) File path to hash_name mapping table
--    记录每个文件路径对应的hash_name，便于追踪和管理
-- ============================================================
CREATE TABLE file_path_mappings (
    file_path TEXT PRIMARY KEY,                    -- 文件完整路径
    hash_name TEXT NOT NULL,                       -- 生成的hash_name
    data_source VARCHAR(50) NOT NULL,              -- 数据源
    file_size BIGINT,                              -- 文件大小（字节）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fpm_hash_name     ON file_path_mappings(hash_name);
CREATE INDEX idx_fpm_data_source   ON file_path_mappings(data_source);
CREATE INDEX idx_fpm_source_hash   ON file_path_mappings(data_source, hash_name);
CREATE INDEX idx_fpm_source_created_at ON file_path_mappings(data_source, created_at DESC);

-- updated_at trigger for file_path_mappings
DROP TRIGGER IF EXISTS trg_fpm_updated_at ON file_path_mappings;
CREATE TRIGGER trg_fpm_updated_at
BEFORE UPDATE ON file_path_mappings
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- 4) Data quality issues table
-- ============================================================
CREATE TABLE data_quality_issues (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT REFERENCES ai_risk_relevant_news(id) ON DELETE CASCADE,
    issue_type VARCHAR(50) NOT NULL,     -- 'missing_field', 'invalid_format', 'parse_error'
    issue_description TEXT,
    field_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quality_issue_type ON data_quality_issues(issue_type);
CREATE INDEX idx_quality_event_id   ON data_quality_issues(event_id);

-- ============================================================
-- 5) Import logs table
-- ============================================================
CREATE TABLE import_logs (
    id BIGSERIAL PRIMARY KEY,
    data_source VARCHAR(50) NOT NULL,
    file_path TEXT NOT NULL,
    status VARCHAR(20) NOT NULL,         -- 'success', 'failed', 'skipped'
    error_message TEXT,
    records_imported INTEGER DEFAULT 0,
    import_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_import_data_source ON import_logs(data_source);
CREATE INDEX idx_import_status      ON import_logs(status);
CREATE INDEX idx_import_time        ON import_logs(import_time);

-- ============================================================
-- 6) Evaluation table: human vs model labels
-- ============================================================
CREATE TABLE eval_human_model_labels (
    hash TEXT PRIMARY KEY,                -- sample id (usually file basename)
    name TEXT,                            -- original filename (optional)
    data_source VARCHAR(50),
    file_path TEXT,

    -- Human labels
    human_is_relevant BOOLEAN,
    human_type VARCHAR(20),               -- 'event' / 'discussion' / NULL

    -- Model labels
    model_classification_result VARCHAR(50),
    model_is_relevant BOOLEAN,
    model_type VARCHAR(20),               -- 'event' / 'discussion' / NULL

    -- Standard / truth labels
    classification_std_result VARCHAR(50),
    truth_is_relevant BOOLEAN,
    truth_type VARCHAR(20),               -- 'event' / 'discussion' / NULL

    model_version TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_eval_hm_data_source ON eval_human_model_labels(data_source);

-- updated_at trigger for eval table
DROP TRIGGER IF EXISTS trg_eval_updated_at ON eval_human_model_labels;
CREATE TRIGGER trg_eval_updated_at
BEFORE UPDATE ON eval_human_model_labels
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- 7) Event-Level Working Table
-- ============================================================
CREATE TABLE IF NOT EXISTS ai_risk_events_news (
    event_level_news_id BIGSERIAL PRIMARY KEY,
    news_id BIGINT NOT NULL,
    event_id BIGINT,

    data_source VARCHAR(50) NOT NULL,
    file_path TEXT,
    archive_year SMALLINT,
    archive_month SMALLINT,
    archive_ym INTEGER,

    hash_name TEXT,
    content_hash TEXT,
    content_hash_version SMALLINT NOT NULL DEFAULT 2,
    normalize_rule TEXT NOT NULL DEFAULT 'collapse_whitespace_strip',

    classification_result VARCHAR(50) NOT NULL,
    classification_std_result VARCHAR(50),

    title TEXT,
    content TEXT NOT NULL,
    release_date DATE,

    ai_tech JSONB,
    ai_risk JSONB,
    event JSONB,

    ai_system_list TEXT[],
    ai_system_type_list TEXT[],
    ai_system_domain_list TEXT[],

    ai_risk_description TEXT,
    ai_risk_type TEXT,
    ai_risk_subtype TEXT,
    harm_type TEXT,
    harm_severity TEXT,
    affected_actor_type TEXT,
    affected_actor_subtype TEXT,
    realized_or_potential TEXT,
    risk_stage TEXT,

    event_actor_main TEXT,
    event_actor_main_type TEXT,
    event_actor_list TEXT[],
    event_ai_system TEXT,
    event_domain TEXT,
    event_type TEXT,
    event_cause TEXT,
    event_process TEXT,
    event_result TEXT,

    event_time_start_desc TEXT,
    event_time_end_desc TEXT,
    event_time_start DATE,
    event_time_end DATE,
    event_country VARCHAR(100),
    event_province VARCHAR(100),
    event_city VARCHAR(100),

    is_duplicate BOOLEAN DEFAULT FALSE,
    duplicate_group_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ai_risk_events_news_news_id UNIQUE (news_id)
);

CREATE INDEX IF NOT EXISTS idx_events_news_news_id ON ai_risk_events_news(news_id);
CREATE INDEX IF NOT EXISTS idx_events_news_event_id ON ai_risk_events_news(event_id);
CREATE INDEX IF NOT EXISTS idx_events_news_source ON ai_risk_events_news(data_source);
CREATE INDEX IF NOT EXISTS idx_events_news_archive ON ai_risk_events_news(archive_year, archive_month);
CREATE INDEX IF NOT EXISTS idx_events_news_country ON ai_risk_events_news(event_country);
CREATE INDEX IF NOT EXISTS idx_events_news_hash_name ON ai_risk_events_news(hash_name);
CREATE INDEX IF NOT EXISTS idx_events_news_classification ON ai_risk_events_news(classification_result);

-- ============================================================
-- 8) Embedding / Alignment / Clustering / Global Event Tables
-- ============================================================
-- ============================================================
-- 1) Ingestion batches (monthly incremental)
-- ============================================================

CREATE TABLE IF NOT EXISTS ingest_batches (
    id BIGSERIAL PRIMARY KEY,
    batch_month DATE NOT NULL,                 -- e.g., 2026-01-01
    batch_scope TEXT NOT NULL DEFAULT 'all',    -- 'all' or per-source scope
    status TEXT NOT NULL DEFAULT 'created',     -- created/running/done/failed
    note TEXT,
    config JSONB,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_batch_status CHECK (status IN ('created','running','done','failed')),
    CONSTRAINT chk_batch_scope_nonempty CHECK (length(batch_scope) > 0)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_ingest_batches_month_scope') THEN
    ALTER TABLE ingest_batches
      ADD CONSTRAINT uq_ingest_batches_month_scope UNIQUE (batch_month, batch_scope);
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_ingest_batches_updated_at') THEN
    CREATE TRIGGER trg_ingest_batches_updated_at
    BEFORE UPDATE ON ingest_batches
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_ingest_batches_month  ON ingest_batches(batch_month);
CREATE INDEX IF NOT EXISTS idx_ingest_batches_status ON ingest_batches(status);

CREATE TABLE IF NOT EXISTS ingest_batch_items (
    batch_id BIGINT NOT NULL REFERENCES ingest_batches(id) ON DELETE CASCADE,
    news_id  BIGINT NOT NULL REFERENCES ai_risk_events_news(news_id) ON DELETE CASCADE,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_id, news_id)
);

CREATE INDEX IF NOT EXISTS idx_ingest_batch_items_news  ON ingest_batch_items(news_id);
CREATE INDEX IF NOT EXISTS idx_ingest_batch_items_batch ON ingest_batch_items(batch_id);

-- ============================================================
-- 2) Embedding metadata (vectors stored as files)
-- ============================================================

CREATE TABLE IF NOT EXISTS embedding_runs (
    id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,                  -- e.g., 'bge-m3'
    model_version TEXT,                        -- ckpt tag/hash
    embedding_dim INTEGER,
    dtype TEXT NOT NULL DEFAULT 'float32',
    is_normalized BOOLEAN NOT NULL DEFAULT TRUE,
    normalize_rule TEXT NOT NULL DEFAULT 'collapse_whitespace_strip',
    fields JSONB,                              -- which fields + build rules
    config JSONB,                              -- batch_size/pooling/etc
    config_hash TEXT,                          -- sha256 hex (optional)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_embedding_dtype CHECK (dtype IN ('float16','float32','float64')),
    CONSTRAINT chk_embedding_confighash_hex CHECK (config_hash IS NULL OR config_hash ~ '^[0-9a-f]{64}$')
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_embedding_runs_updated_at') THEN
    CREATE TRIGGER trg_embedding_runs_updated_at
    BEFORE UPDATE ON embedding_runs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_embed_runs_model        ON embedding_runs(model_name);
CREATE INDEX IF NOT EXISTS idx_embed_runs_created_at   ON embedding_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_embed_runs_fields_gin   ON embedding_runs USING gin(fields);
CREATE INDEX IF NOT EXISTS idx_embed_runs_config_gin   ON embedding_runs USING gin(config);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relname = 'uq_embed_runs_hash'
  ) THEN
    CREATE UNIQUE INDEX uq_embed_runs_hash
      ON embedding_runs(config_hash)
      WHERE config_hash IS NOT NULL;
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS embedding_artifacts (
    embed_run_id BIGINT NOT NULL REFERENCES embedding_runs(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    emb_file_path TEXT NOT NULL,
    id_map_path TEXT NOT NULL,
    index_type TEXT,
    index_file_path TEXT,
    n_rows BIGINT,
    dim INTEGER,
    dtype TEXT NOT NULL DEFAULT 'float32',
    is_normalized BOOLEAN NOT NULL DEFAULT TRUE,
    checksum TEXT,
    extra JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (embed_run_id, field_name),
    CONSTRAINT chk_artifact_dtype CHECK (dtype IN ('float16','float32','float64')),
    CONSTRAINT chk_artifact_checksum_hex CHECK (checksum IS NULL OR checksum ~ '^[0-9a-f]{64}$')
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_embedding_artifacts_updated_at') THEN
    CREATE TRIGGER trg_embedding_artifacts_updated_at
    BEFORE UPDATE ON embedding_artifacts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_embed_artifacts_field ON embedding_artifacts(field_name);
CREATE INDEX IF NOT EXISTS idx_embed_artifacts_run   ON embedding_artifacts(embed_run_id);

-- ============================================================
-- 3) Alignment runs + run-level artifacts (candidates/preds/edges/clusters)
-- ============================================================

CREATE TABLE IF NOT EXISTS event_align_runs (
    id BIGSERIAL PRIMARY KEY,

    embed_run_id BIGINT REFERENCES embedding_runs(id) ON DELETE SET NULL,
    batch_id BIGINT REFERENCES ingest_batches(id) ON DELETE SET NULL,

    run_name TEXT,
    run_desc TEXT,

    recall_topk_event INTEGER NOT NULL DEFAULT 150,
    recall_topk_text  INTEGER NOT NULL DEFAULT 150,
    rerank_topk       INTEGER NOT NULL DEFAULT 100,

    rerank_model TEXT NOT NULL DEFAULT 'bge-m3',
    pair_model   TEXT NOT NULL DEFAULT 'deepwide',

    edge_rule TEXT NOT NULL DEFAULT 'either',
    cluster_method TEXT NOT NULL DEFAULT 'closure',

    threshold FLOAT,
    config JSONB,
    model_versions JSONB,

    status TEXT NOT NULL DEFAULT 'created',
    error_message TEXT,

    started_at TIMESTAMP,
    finished_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_edge_rule CHECK (edge_rule IN ('either','mutual','custom')),
    CONSTRAINT chk_cluster_method CHECK (cluster_method IN ('closure','complete_link','custom')),
    CONSTRAINT chk_run_status CHECK (status IN ('created','running','done','failed')),
    CONSTRAINT chk_topk_positive CHECK (recall_topk_event > 0 AND recall_topk_text > 0 AND rerank_topk > 0)
);

-- Backward compatible: if you created an older version of event_align_runs earlier
ALTER TABLE event_align_runs ADD COLUMN IF NOT EXISTS embed_run_id BIGINT;
ALTER TABLE event_align_runs ADD COLUMN IF NOT EXISTS batch_id BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_event_align_runs_embed_run') THEN
    ALTER TABLE event_align_runs
      ADD CONSTRAINT fk_event_align_runs_embed_run
      FOREIGN KEY (embed_run_id) REFERENCES embedding_runs(id) ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_event_align_runs_batch') THEN
    ALTER TABLE event_align_runs
      ADD CONSTRAINT fk_event_align_runs_batch
      FOREIGN KEY (batch_id) REFERENCES ingest_batches(id) ON DELETE SET NULL;
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_event_align_runs_updated_at') THEN
    CREATE TRIGGER trg_event_align_runs_updated_at
    BEFORE UPDATE ON event_align_runs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_ealign_runs_status     ON event_align_runs(status);
CREATE INDEX IF NOT EXISTS idx_ealign_runs_created_at ON event_align_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ealign_runs_batch      ON event_align_runs(batch_id);
CREATE INDEX IF NOT EXISTS idx_ealign_runs_config_gin ON event_align_runs USING gin(config);

CREATE TABLE IF NOT EXISTS event_align_candidates (
    run_id BIGINT NOT NULL REFERENCES event_align_runs(id) ON DELETE CASCADE,
    query_id BIGINT NOT NULL,
    candidate_id BIGINT NOT NULL,
    recall_event_rank INTEGER,
    recall_event_score FLOAT,
    recall_text_rank INTEGER,
    recall_text_score FLOAT,
    rerank_rank INTEGER,
    rerank_score FLOAT,
    is_kept BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, query_id, candidate_id),
    CONSTRAINT chk_cand_not_self CHECK (query_id <> candidate_id)
);

CREATE INDEX IF NOT EXISTS idx_ealign_cand_query     ON event_align_candidates(run_id, query_id);
CREATE INDEX IF NOT EXISTS idx_ealign_cand_candidate ON event_align_candidates(run_id, candidate_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relname = 'idx_ealign_cand_kept'
  ) THEN
    CREATE INDEX idx_ealign_cand_kept
      ON event_align_candidates(run_id, query_id, rerank_rank)
      WHERE is_kept = TRUE;
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS event_align_pair_preds (
    run_id BIGINT NOT NULL REFERENCES event_align_runs(id) ON DELETE CASCADE,
    query_id BIGINT NOT NULL,
    candidate_id BIGINT NOT NULL,
    pred_label BOOLEAN NOT NULL,
    pred_prob FLOAT,
    model_version TEXT,
    extra JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, query_id, candidate_id),
    CONSTRAINT chk_pred_not_self CHECK (query_id <> candidate_id)
);

CREATE INDEX IF NOT EXISTS idx_ealign_pred_query ON event_align_pair_preds(run_id, query_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relname = 'idx_ealign_pred_positive'
  ) THEN
    CREATE INDEX idx_ealign_pred_positive
      ON event_align_pair_preds(run_id, query_id, candidate_id)
      WHERE pred_label = TRUE;
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS event_align_edges (
    run_id BIGINT NOT NULL REFERENCES event_align_runs(id) ON DELETE CASCADE,
    src_id BIGINT NOT NULL,
    dst_id BIGINT NOT NULL,
    edge_rule TEXT NOT NULL DEFAULT 'either',
    edge_prob FLOAT,
    support JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, src_id, dst_id),
    CONSTRAINT chk_edge_order CHECK (src_id < dst_id)
);

CREATE INDEX IF NOT EXISTS idx_ealign_edges_src ON event_align_edges(run_id, src_id);
CREATE INDEX IF NOT EXISTS idx_ealign_edges_dst ON event_align_edges(run_id, dst_id);

CREATE TABLE IF NOT EXISTS event_clusters (
    event_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES event_align_runs(id) ON DELETE CASCADE,
    representative_news_id BIGINT,
    cluster_size INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_event_clusters_run_event') THEN
    ALTER TABLE event_clusters
      ADD CONSTRAINT uq_event_clusters_run_event UNIQUE (run_id, event_id);
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_eclusters_run ON event_clusters(run_id);
CREATE INDEX IF NOT EXISTS idx_eclusters_rep ON event_clusters(run_id, representative_news_id);

CREATE TABLE IF NOT EXISTS event_cluster_members (
    run_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    news_id BIGINT NOT NULL REFERENCES ai_risk_events_news(news_id) ON DELETE CASCADE,
    role TEXT,
    member_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, event_id, news_id)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_ecm_run_event') THEN
    ALTER TABLE event_cluster_members
      ADD CONSTRAINT fk_ecm_run_event
      FOREIGN KEY (run_id, event_id) REFERENCES event_clusters(run_id, event_id) ON DELETE CASCADE;
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_ecm_news  ON event_cluster_members(run_id, news_id);
CREATE INDEX IF NOT EXISTS idx_ecm_event ON event_cluster_members(run_id, event_id);

CREATE TABLE IF NOT EXISTS event_cluster_profiles (
    run_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    canonical_title TEXT,
    time_start DATE,
    time_end DATE,
    country VARCHAR(100),
    province VARCHAR(100),
    city VARCHAR(100),
    actor_main TEXT,
    ai_system TEXT,
    domain TEXT,
    event_type TEXT,
    risk_type TEXT,
    risk_stage TEXT,
    profile JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, event_id)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_ecp_run_event') THEN
    ALTER TABLE event_cluster_profiles
      ADD CONSTRAINT fk_ecp_run_event
      FOREIGN KEY (run_id, event_id) REFERENCES event_clusters(run_id, event_id) ON DELETE CASCADE;
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_ecp_time_start ON event_cluster_profiles(run_id, time_start);
CREATE INDEX IF NOT EXISTS idx_ecp_event_type ON event_cluster_profiles(run_id, event_type);

CREATE TABLE IF NOT EXISTS event_align_progress (
    run_id BIGINT NOT NULL REFERENCES event_align_runs(id) ON DELETE CASCADE,
    shard_id INTEGER NOT NULL,
    phase TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    processed_count BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, shard_id, phase),
    CONSTRAINT chk_phase CHECK (phase IN ('embed','recall','rerank','predict','edges','cluster','profile')),
    CONSTRAINT chk_progress_status CHECK (status IN ('pending','running','done','failed'))
);

CREATE INDEX IF NOT EXISTS idx_ealign_progress_run ON event_align_progress(run_id);

-- ============================================================
-- 4) Global event store (stable across months)
-- ============================================================

CREATE TABLE IF NOT EXISTS global_events (
    global_event_id BIGSERIAL PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'open',
    canonical_title TEXT,
    representative_news_id BIGINT REFERENCES ai_risk_events_news(news_id) ON DELETE SET NULL,

    time_start DATE,
    time_end DATE,
    country VARCHAR(100),
    province VARCHAR(100),
    city VARCHAR(100),

    actor_main TEXT,
    ai_system TEXT,
    domain TEXT,
    event_type TEXT,

    risk_type TEXT,
    risk_stage TEXT,

    first_seen_at TIMESTAMP,
    last_seen_at  TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_global_event_status CHECK (status IN ('open','closed','merged'))
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_global_events_updated_at') THEN
    CREATE TRIGGER trg_global_events_updated_at
    BEFORE UPDATE ON global_events
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_global_events_status ON global_events(status);
CREATE INDEX IF NOT EXISTS idx_global_events_type   ON global_events(event_type);
CREATE INDEX IF NOT EXISTS idx_global_events_actor  ON global_events(actor_main);
CREATE INDEX IF NOT EXISTS idx_global_events_time   ON global_events(time_start);

CREATE TABLE IF NOT EXISTS global_event_members (
    global_event_id BIGINT NOT NULL REFERENCES global_events(global_event_id) ON DELETE CASCADE,
    news_id BIGINT NOT NULL REFERENCES ai_risk_events_news(news_id) ON DELETE CASCADE,

    batch_id BIGINT REFERENCES ingest_batches(id) ON DELETE SET NULL,
    run_id BIGINT REFERENCES event_align_runs(id) ON DELETE SET NULL,

    role TEXT,
    member_score FLOAT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (global_event_id, news_id)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relname = 'uq_global_event_members_news'
  ) THEN
    CREATE UNIQUE INDEX uq_global_event_members_news
      ON global_event_members(news_id);
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_gem_event ON global_event_members(global_event_id);
CREATE INDEX IF NOT EXISTS idx_gem_batch ON global_event_members(batch_id);
CREATE INDEX IF NOT EXISTS idx_gem_run   ON global_event_members(run_id);

CREATE TABLE IF NOT EXISTS run_cluster_mappings (
    run_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    global_event_id BIGINT NOT NULL REFERENCES global_events(global_event_id) ON DELETE CASCADE,
    mapping_method TEXT NOT NULL DEFAULT 'auto',
    mapping_score FLOAT,
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, event_id)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_rcm_run_event') THEN
    ALTER TABLE run_cluster_mappings
      ADD CONSTRAINT fk_rcm_run_event
      FOREIGN KEY (run_id, event_id) REFERENCES event_clusters(run_id, event_id) ON DELETE CASCADE;
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_rcm_global_event ON run_cluster_mappings(global_event_id);
CREATE INDEX IF NOT EXISTS idx_rcm_run          ON run_cluster_mappings(run_id);

CREATE TABLE IF NOT EXISTS global_event_merge_log (
    id BIGSERIAL PRIMARY KEY,
    src_global_event_id BIGINT NOT NULL REFERENCES global_events(global_event_id) ON DELETE CASCADE,
    dst_global_event_id BIGINT NOT NULL REFERENCES global_events(global_event_id) ON DELETE CASCADE,
    merge_reason TEXT,
    merge_method TEXT DEFAULT 'manual',
    merge_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_merge_not_self CHECK (src_global_event_id <> dst_global_event_id)
);

CREATE INDEX IF NOT EXISTS idx_gemlog_src ON global_event_merge_log(src_global_event_id);
CREATE INDEX IF NOT EXISTS idx_gemlog_dst ON global_event_merge_log(dst_global_event_id);

-- ============================================================
-- 9) Views
-- ============================================================
-- View: data source stats (基于 ai_risk_relevant_news)
CREATE OR REPLACE VIEW v_data_source_stats AS
SELECT
    data_source,
    classification_result,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN event_time_start IS NOT NULL THEN 1 END) AS with_date_count,
    COUNT(CASE WHEN event_country IS NOT NULL THEN 1 END) AS with_country_count,
    MIN(event_time_start) AS earliest_event,
    MAX(event_time_start) AS latest_event
FROM ai_risk_relevant_news
GROUP BY data_source, classification_result
ORDER BY data_source, classification_result;

-- View: time distribution
CREATE OR REPLACE VIEW v_time_distribution AS
SELECT
    DATE_TRUNC('year', event_time_start) AS year,
    DATE_TRUNC('month', event_time_start) AS month,
    data_source,
    COUNT(*) AS event_count
FROM ai_risk_relevant_news
WHERE event_time_start IS NOT NULL
GROUP BY year, month, data_source
ORDER BY year DESC, month DESC, data_source;

-- View: risk type stats (from flattened column first; fallback jsonb)
CREATE OR REPLACE VIEW v_risk_type_stats AS
SELECT
    data_source,
    COALESCE(ai_risk_type, ai_risk->>'ai_risk_type') AS risk_type,
    COUNT(*) AS count
FROM ai_risk_relevant_news
WHERE COALESCE(ai_risk_type, ai_risk->>'ai_risk_type') IS NOT NULL
GROUP BY data_source, COALESCE(ai_risk_type, ai_risk->>'ai_risk_type')
ORDER BY data_source, count DESC;

-- ============================================================
-- End of file
