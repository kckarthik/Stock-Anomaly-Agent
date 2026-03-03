
  
    

  create  table "stockdb"."gold"."volume_anomalies__dbt_tmp"
  
  
    as
  
  (
    -- models/gold/volume_anomalies.sql
-- ============================================================
-- Flags bars where volume significantly exceeds the 20-day
-- baseline for that symbol at that hour of day.
-- Z-score = (current_volume - avg) / stddev
-- Flagged when z-score > VOLUME_ZSCORE_THRESHOLD (default 2.5)
-- ============================================================



WITH latest_bars AS (
    SELECT *
    FROM "stockdb"."gold"."ohlcv_1min"
    WHERE bar_time >= NOW() - INTERVAL '2 days'
),

joined AS (
    SELECT
        b.symbol,
        b.sector,
        b.bar_time,
        b.bar_date,
        b.bar_hour,
        b.volume           AS current_volume,
        b.close,
        b.price_change_pct,
        vb.avg_volume_20d,
        vb.stddev_volume_20d,

        -- z-score: how many std devs above baseline
        CASE
            WHEN vb.stddev_volume_20d > 0
            THEN ROUND(
                ((b.volume - vb.avg_volume_20d) / vb.stddev_volume_20d)::NUMERIC,
                2
            )
            ELSE 0
        END AS volume_zscore,

        -- simple ratio: current / 20d avg
        CASE
            WHEN vb.avg_volume_20d > 0
            THEN ROUND((b.volume / vb.avg_volume_20d)::NUMERIC, 2)
            ELSE 1
        END AS volume_ratio

    FROM latest_bars b
    LEFT JOIN "stockdb"."gold"."volume_baseline" vb
        ON b.symbol   = vb.symbol
       AND b.bar_hour = vb.bar_hour
)

SELECT
    *,
    volume_zscore > 2.5 AS is_anomaly,
    CASE
        WHEN volume_zscore > 4.0 THEN 'extreme'
        WHEN volume_zscore > 2.5 THEN 'high'
        ELSE 'normal'
    END AS anomaly_severity
FROM joined
ORDER BY volume_zscore DESC
  );
  