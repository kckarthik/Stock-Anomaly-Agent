
  
    

  create  table "stockdb"."gold"."ohlcv_1min__dbt_tmp"
  
  
    as
  
  (
    -- models/gold/ohlcv_1min.sql
-- ============================================================
-- Gold base table: clean 1-min OHLCV bars per symbol.
-- All other Gold models reference this.
-- TimescaleDB hypertable (set up in init.sql) for fast
-- time-range queries by Grafana and the agent.
-- ============================================================



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
FROM "stockdb"."public"."stg_silver_bars"
  );
  