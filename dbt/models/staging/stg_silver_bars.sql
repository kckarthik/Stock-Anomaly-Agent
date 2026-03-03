-- models/staging/stg_silver_bars.sql
-- ============================================================
-- Staging view over silver.ohlcv_1min table.
-- Silver data is written by the consumer from MinIO Parquet
-- files that dbt reads after the consumer loads them into PG.
-- Adds derived columns: price_change_pct, dollar_volume,
--   bar_timestamp as proper timestamptz.
-- ============================================================

{{ config(materialized='view') }}

SELECT
    symbol,
    sector,
    CAST(bar_time AS TIMESTAMPTZ)           AS bar_time,
    date::DATE                              AS bar_date,
    hour::SMALLINT                          AS bar_hour,
    minute::SMALLINT                        AS bar_minute,
    open,
    high,
    low,
    close,
    volume,
    tick_count,

    -- derived
    ROUND(((close - open) / NULLIF(open, 0)) * 100, 4) AS price_change_pct,
    ROUND(close * volume, 2)                            AS dollar_volume,
    ROUND(high - low, 4)                                AS bar_range,
    CAST(processed_at AS TIMESTAMPTZ)                   AS processed_at

FROM silver.ohlcv_1min
WHERE close IS NOT NULL
  AND volume IS NOT NULL
  AND volume > 0
