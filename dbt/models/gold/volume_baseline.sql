-- models/gold/volume_baseline.sql
-- ============================================================
-- 20-day average volume per symbol per hour-of-day.
-- Used to compute volume z-scores: is today's volume
-- unusual FOR THIS TIME OF DAY?
-- This matters because market open (9:30am) always has higher
-- volume than mid-afternoon — normalizing by hour is critical.
-- ============================================================

{{ config(materialized='table') }}

SELECT
    symbol,
    bar_hour,
    COUNT(*)                        AS sample_bars,
    ROUND(AVG(volume)::NUMERIC, 2)  AS avg_volume_20d,
    ROUND(STDDEV(volume)::NUMERIC, 2) AS stddev_volume_20d,
    MIN(volume)                     AS min_volume_20d,
    MAX(volume)                     AS max_volume_20d,
    MAX(bar_date)                   AS baseline_through

FROM {{ ref('ohlcv_1min') }}
WHERE bar_date >= CURRENT_DATE - INTERVAL '20 days'

GROUP BY symbol, bar_hour
HAVING COUNT(*) >= 5   -- need at least 5 samples for a reliable baseline
