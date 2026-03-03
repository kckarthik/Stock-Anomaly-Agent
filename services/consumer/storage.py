"""
storage.py — MinIO Parquet writer for Bronze and Silver layers
================================================================
Bronze: raw Kafka messages, never modified, partitioned by symbol/date/hour
Silver: aggregated OHLCV 1-min bars, cleaned and typed
"""

import io
import uuid
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from minio import Minio
from config import MinioConfig, TimescaleConfig

log = logging.getLogger(__name__)


def get_client() -> Minio:
    return Minio(
        MinioConfig.ENDPOINT,
        access_key = MinioConfig.ACCESS_KEY,
        secret_key = MinioConfig.SECRET_KEY,
        secure     = MinioConfig.SECURE,
    )


def _partition_path(symbol: str, layer: str = "") -> str:
    """
    Returns: symbol=AAPL/date=2026-02-26/hour=10/abc123.parquet
    """
    now = datetime.now(timezone.utc)
    return (
        f"symbol={symbol}/"
        f"date={now.strftime('%Y-%m-%d')}/"
        f"hour={now.strftime('%H')}/"
        f"{uuid.uuid4().hex[:8]}.parquet"
    )


def write_parquet(client: Minio, bucket: str, df: pd.DataFrame, symbol: str) -> str:
    """
    Write a DataFrame as Snappy-compressed Parquet to MinIO.
    Returns the object path written.
    """
    path  = _partition_path(symbol)
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    client.put_object(
        bucket_name  = bucket,
        object_name  = path,
        data         = buf,
        length       = buf.getbuffer().nbytes,
        content_type = "application/octet-stream",
    )

    log.debug(f"📦 {bucket}/{path}  ({len(df)} rows)")
    return path


def write_silver_to_db(df: pd.DataFrame) -> None:
    """
    Write Silver OHLCV bars to silver.ohlcv_1min in TimescaleDB.
    Required so dbt staging model has data to transform into Gold.
    """
    if df.empty:
        return

    cols = [
        "symbol", "sector", "bar_time", "date", "hour", "minute",
        "open", "high", "low", "close", "volume", "tick_count",
        "bar_start", "bar_end", "processed_at",
    ]
    rows = [tuple(r) for r in df[cols].itertuples(index=False, name=None)]

    try:
        conn = psycopg2.connect(TimescaleConfig.dsn())
        cur  = conn.cursor()
        try:
            psycopg2.extras.execute_batch(
                cur,
                """INSERT INTO silver.ohlcv_1min
                       (symbol, sector, bar_time, date, hour, minute,
                        open, high, low, close, volume, tick_count,
                        bar_start, bar_end, processed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (symbol, bar_time) DO NOTHING""",
                rows,
                page_size=500,
            )
            conn.commit()
            log.debug(f"💾 TimescaleDB silver.ohlcv_1min ← {len(rows)} rows")
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        log.error(f"write_silver_to_db failed: {e}")
