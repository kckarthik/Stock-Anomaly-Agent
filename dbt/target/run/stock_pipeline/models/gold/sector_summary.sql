
  
    

  create  table "stockdb"."gold"."sector_summary__dbt_tmp"
  
  
    as
  
  (
    -- models/gold/sector_summary.sql
-- ============================================================
-- Sector-level aggregation of the latest bar per symbol.
-- Agent uses this to check: "is AAPL moving alone or
-- is the entire tech sector moving?"
-- ============================================================



WITH latest_per_symbol AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        sector,
        bar_time,
        close,
        volume,
        price_change_pct,
        dollar_volume
    FROM "stockdb"."gold"."ohlcv_1min"
    WHERE bar_time >= NOW() - INTERVAL '30 minutes'
    ORDER BY symbol, bar_time DESC
)

SELECT
    sector,
    COUNT(symbol)                                   AS symbol_count,
    ROUND(AVG(price_change_pct)::NUMERIC, 3)        AS avg_price_change_pct,
    ROUND(STDDEV(price_change_pct)::NUMERIC, 3)     AS stddev_price_change,
    SUM(dollar_volume)                              AS total_dollar_volume,
    ROUND(AVG(dollar_volume)::NUMERIC, 2)           AS avg_dollar_volume,
    COUNT(*) FILTER (WHERE price_change_pct > 0)    AS symbols_up,
    COUNT(*) FILTER (WHERE price_change_pct < 0)    AS symbols_down,
    COUNT(*) FILTER (WHERE price_change_pct = 0)    AS symbols_flat,
    MAX(bar_time)                                   AS as_of,
    NOW()                                           AS computed_at

FROM latest_per_symbol
GROUP BY sector
  );
  