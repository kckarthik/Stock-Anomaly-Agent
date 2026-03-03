-- models/gold/anomaly_feed.sql
-- ============================================================
-- THE table the agent reads every 5 minutes.
-- Unified feed of all anomalies (volume + price) not yet
-- investigated by the agent.
--
-- Agent scheduler reads:
--   SELECT * FROM gold.anomaly_feed
--   WHERE investigated = FALSE
--   ORDER BY severity_score DESC
-- ============================================================

{{ config(materialized='table') }}

WITH volume_flags AS (
    SELECT
        symbol,
        sector,
        bar_time,
        'volume_spike'                          AS anomaly_type,
        volume_zscore                           AS anomaly_score,
        CASE anomaly_severity
            WHEN 'extreme' THEN 3
            WHEN 'high'    THEN 2
            ELSE 1
        END                                     AS severity_score,
        anomaly_severity                        AS severity_label,
        JSON_BUILD_OBJECT(
            'volume_zscore',   volume_zscore,
            'volume_ratio',    volume_ratio,
            'current_volume',  current_volume,
            'avg_volume_20d',  avg_volume_20d,
            'close',           close,
            'price_change_pct', price_change_pct
        )                                       AS context

    FROM {{ ref('volume_anomalies') }}
    WHERE is_anomaly = TRUE
),

price_flags AS (
    SELECT
        symbol,
        sector,
        bar_time,
        'price_move'                            AS anomaly_type,
        ABS(price_change_5min_pct)              AS anomaly_score,
        CASE
            WHEN ABS(price_change_5min_pct) > 5 THEN 3
            WHEN ABS(price_change_5min_pct) > 3 THEN 2
            ELSE 1
        END                                     AS severity_score,
        CASE
            WHEN ABS(price_change_5min_pct) > 5 THEN 'extreme'
            WHEN ABS(price_change_5min_pct) > 3 THEN 'high'
            ELSE 'medium'
        END                                     AS severity_label,
        JSON_BUILD_OBJECT(
            'price_change_5min_pct', price_change_5min_pct,
            'price_change_1min_pct', price_change_1min_pct,
            'current_price',         current_price,
            'move_direction',        move_direction,
            'volume',                volume
        )                                       AS context

    FROM {{ ref('price_anomalies') }}
    WHERE is_anomaly = TRUE
),

all_anomalies AS (
    SELECT * FROM volume_flags
    UNION ALL
    SELECT * FROM price_flags
)

SELECT
    MD5(CONCAT(a.symbol::TEXT, '|', a.bar_time::TEXT, '|', a.anomaly_type::TEXT)) AS anomaly_id,
    a.symbol,
    a.sector,
    a.bar_time,
    a.anomaly_type,
    ROUND(a.anomaly_score::NUMERIC, 3)  AS anomaly_score,
    a.severity_score,
    a.severity_label,
    a.context,
    -- Mark as investigated if a report already exists for this anomaly
    (ir.anomaly_id IS NOT NULL)         AS investigated,
    ir.investigated_at                  AS investigated_at,
    NOW()                               AS detected_at

FROM all_anomalies a
LEFT JOIN gold.investigation_reports ir
    ON ir.anomaly_id = MD5(CONCAT(a.symbol::TEXT, '|', a.bar_time::TEXT, '|', a.anomaly_type::TEXT))
ORDER BY a.severity_score DESC, a.anomaly_score DESC
