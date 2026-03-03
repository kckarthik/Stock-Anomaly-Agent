
  
    

  create  table "stockdb"."gold"."price_anomalies__dbt_tmp"
  
  
    as
  
  (
    -- models/gold/price_anomalies.sql
-- ============================================================
-- Flags bars where price moved more than 2% in under 5 minutes.
-- Uses LAG() to compare current close vs 5 bars ago.
-- ============================================================



WITH windowed AS (
    SELECT
        symbol,
        sector,
        bar_time,
        bar_date,
        bar_hour,
        close,
        volume,

        -- price 5 minutes ago
        LAG(close, 5) OVER (
            PARTITION BY symbol
            ORDER BY bar_time
        ) AS close_5min_ago,

        -- price 1 minute ago
        LAG(close, 1) OVER (
            PARTITION BY symbol
            ORDER BY bar_time
        ) AS close_1min_ago

    FROM "stockdb"."gold"."ohlcv_1min"
    WHERE bar_time >= NOW() - INTERVAL '2 days'
)

SELECT
    symbol,
    sector,
    bar_time,
    bar_date,
    bar_hour,
    close                                               AS current_price,
    close_5min_ago,
    volume,

    -- 5-min price change %
    CASE
        WHEN close_5min_ago > 0
        THEN ROUND(
            ((close - close_5min_ago) / close_5min_ago * 100)::NUMERIC,
            3
        )
        ELSE 0
    END AS price_change_5min_pct,

    -- 1-min price change %
    CASE
        WHEN close_1min_ago > 0
        THEN ROUND(
            ((close - close_1min_ago) / close_1min_ago * 100)::NUMERIC,
            3
        )
        ELSE 0
    END AS price_change_1min_pct,

    -- flag
    ABS(
        CASE WHEN close_5min_ago > 0
        THEN ((close - close_5min_ago) / close_5min_ago * 100)
        ELSE 0 END
    ) > 2.0 AS is_anomaly,

    -- direction
    CASE
        WHEN close > close_5min_ago THEN 'up'
        WHEN close < close_5min_ago THEN 'down'
        ELSE 'flat'
    END AS move_direction

FROM windowed
WHERE close_5min_ago IS NOT NULL
ORDER BY ABS(
    CASE WHEN close_5min_ago > 0
    THEN ((close - close_5min_ago) / close_5min_ago * 100)
    ELSE 0 END
) DESC
  );
  