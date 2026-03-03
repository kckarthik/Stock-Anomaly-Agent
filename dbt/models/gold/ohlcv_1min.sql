-- models/gold/ohlcv_1min.sql
-- ============================================================
-- Gold base table: clean 1-min OHLCV bars per symbol.
-- All other Gold models reference this.
-- TimescaleDB hypertable (set up in init.sql) for fast
-- time-range queries by Grafana and the agent.
-- ============================================================

{{ config(
    materialized = 'table',
    post_hook    = "SELECT create_hypertable('{{ this.schema }}.{{ this.identifier }}', 'bar_time', if_not_exists => TRUE, migrate_data => TRUE)"
) }}

SELECT
    symbol,
    sector,
    bar_time,
    bar_date,
    bar_hour,
    bar_minute,
    open,
    high,
    low,
    close,
    volume,
    tick_count,
    price_change_pct,
    dollar_volume,
    bar_range,
    processed_at
FROM {{ ref('stg_silver_bars') }}
