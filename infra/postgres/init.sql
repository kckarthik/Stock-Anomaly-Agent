-- ================================================================
--  TimescaleDB Init Script
--  Creates all schemas, tables, and hypertables.
-- ================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── Silver schema ────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.ohlcv_1min (
    symbol        VARCHAR(10)   NOT NULL,
    sector        VARCHAR(50),
    bar_time      TIMESTAMPTZ   NOT NULL,
    date          DATE,
    hour          SMALLINT,
    minute        SMALLINT,
    open          NUMERIC(12,4),
    high          NUMERIC(12,4),
    low           NUMERIC(12,4),
    close         NUMERIC(12,4) NOT NULL,
    volume        BIGINT,
    tick_count    INTEGER,
    bar_start     TIMESTAMPTZ,
    bar_end       TIMESTAMPTZ,
    processed_at  TIMESTAMPTZ   DEFAULT NOW()
);

SELECT create_hypertable('silver.ohlcv_1min', 'bar_time', if_not_exists => TRUE);
-- Unique constraint prevents duplicate bars if a message is reprocessed.
-- Must include the partitioning key (bar_time) for TimescaleDB hypertables.
ALTER TABLE silver.ohlcv_1min ADD CONSTRAINT uq_silver_symbol_bar_time UNIQUE (symbol, bar_time);
CREATE INDEX IF NOT EXISTS idx_silver_symbol_time ON silver.ohlcv_1min (symbol, bar_time DESC);

-- ── Gold schema (dbt creates tables; we pre-create reports) ──────
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.investigation_reports (
    id                    SERIAL,
    report_id             VARCHAR(50)  NOT NULL,
    anomaly_id            TEXT         UNIQUE,
    symbol                VARCHAR(10)  NOT NULL,
    sector                VARCHAR(50),
    anomaly_type          VARCHAR(50),
    anomaly_score         NUMERIC(10,3),
    detected_at           TIMESTAMPTZ,
    investigated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    severity              VARCHAR(10)  CHECK (severity IN ('HIGH','MEDIUM','LOW')),
    hypothesis            TEXT,
    evidence_summary      TEXT,
    conclusion            TEXT,
    confidence            VARCHAR(10),
    recommended_action    TEXT,
    findings_json         JSONB,
    react_steps           JSONB        DEFAULT '[]'::jsonb,
    steps_taken           SMALLINT     DEFAULT 0,
    llm_time_seconds      NUMERIC(8,2),
    total_time_seconds    NUMERIC(8,2),
    PRIMARY KEY (id, investigated_at)
);

SELECT create_hypertable(
    'gold.investigation_reports', 'investigated_at',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_reports_symbol   ON gold.investigation_reports (symbol, investigated_at DESC);
CREATE INDEX IF NOT EXISTS idx_reports_severity ON gold.investigation_reports (severity, investigated_at DESC);

-- ── Observability schema — LLM traces (replaces Langfuse) ────────
CREATE SCHEMA IF NOT EXISTS obs;

CREATE TABLE IF NOT EXISTS obs.llm_traces (
    id            SERIAL,
    called_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    trace_id      VARCHAR(50),
    symbol        VARCHAR(10),
    anomaly_type  VARCHAR(50),
    model         VARCHAR(50),
    prompt        TEXT,
    response      TEXT,
    input_tokens  INTEGER      DEFAULT 0,
    output_tokens INTEGER      DEFAULT 0,
    latency_ms    INTEGER,
    success       BOOLEAN      DEFAULT TRUE,
    error_message TEXT,
    PRIMARY KEY (id, called_at)
);

SELECT create_hypertable('obs.llm_traces', 'called_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_traces_symbol ON obs.llm_traces (symbol, called_at DESC);
CREATE INDEX IF NOT EXISTS idx_traces_success ON obs.llm_traces (success, called_at DESC);

-- ── Agent activity log ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.agent_activity_log (
    id         SERIAL PRIMARY KEY,
    logged_at  TIMESTAMPTZ  DEFAULT NOW(),
    event_type VARCHAR(50),
    symbol     VARCHAR(10),
    details    JSONB
);
