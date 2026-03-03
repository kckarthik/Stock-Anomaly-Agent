"""
config.py — Central configuration for all services.
Reads from environment variables (injected via docker-compose env_file).
Import this in any service: from config import KafkaConfig, ...
"""

import os


class KafkaConfig:
    BROKER   = os.getenv("KAFKA_BROKER",    "kafka:9092")
    TOPIC    = os.getenv("KAFKA_TOPIC",     "raw_quotes")
    GROUP_ID = os.getenv("KAFKA_GROUP_ID",  "stock-consumer-group")


class MinioConfig:
    ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    SECURE     = os.getenv("MINIO_SECURE",     "false").lower() == "true"


class TimescaleConfig:
    HOST     = os.getenv("TIMESCALE_HOST",     "timescaledb")
    PORT     = int(os.getenv("TIMESCALE_PORT", "5432"))
    DB       = os.getenv("TIMESCALE_DB",       "stockdb")
    USER     = os.getenv("TIMESCALE_USER",     "stock")
    PASSWORD = os.getenv("TIMESCALE_PASSWORD", "stock123")

    @classmethod
    def dsn(cls) -> str:
        return (
            f"host={cls.HOST} port={cls.PORT} "
            f"dbname={cls.DB} user={cls.USER} password={cls.PASSWORD}"
        )

    @classmethod
    def url(cls) -> str:
        return (
            f"postgresql://{cls.USER}:{cls.PASSWORD}"
            f"@{cls.HOST}:{cls.PORT}/{cls.DB}"
        )


class RedisConfig:
    URL = os.getenv("REDIS_URL", "redis://redis:6379/0")


class OllamaConfig:
    HOST  = os.getenv("OLLAMA_HOST",  "http://ollama:11434")
    MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:0.5b")



class PipelineConfig:
    POLL_INTERVAL_SECONDS    = int(os.getenv("POLL_INTERVAL_SECONDS",   "60"))
    AGENT_SCHEDULE_MINUTES   = int(os.getenv("AGENT_SCHEDULE_MINUTES",  "5"))
    VOLUME_ZSCORE_THRESHOLD  = float(os.getenv("VOLUME_ZSCORE_THRESHOLD","2.5"))
    PRICE_CHANGE_THRESHOLD   = float(os.getenv("PRICE_CHANGE_THRESHOLD", "2.0"))
    MAX_INVESTIGATION_STEPS  = int(os.getenv("MAX_INVESTIGATION_STEPS",  "5"))
    AGENT_RETRY_DELAY        = int(os.getenv("AGENT_RETRY_DELAY_SECONDS","30"))
    AGENT_MAX_RETRIES        = int(os.getenv("AGENT_MAX_RETRIES",        "3"))
    MAX_REACT_STEPS          = int(os.getenv("MAX_REACT_STEPS",          "4"))
    MIN_REACT_STEPS          = int(os.getenv("MIN_REACT_STEPS",          "2"))


# ── Watchlist ──────────────────────────────────────────────────────
# 20 liquid symbols across 4 sectors for peer comparison
WATCHLIST = {
    "Technology": ["AAPL", "MSFT", "GOOGL", "NVDA", "META"],
    "Finance":    ["JPM",  "BAC",  "GS",    "MS",   "WFC"],
    "Healthcare": ["JNJ",  "PFE",  "UNH",   "ABBV", "MRK"],
    "Energy":     ["XOM",  "CVX",  "COP",   "SLB",  "EOG"],
}

ALL_SYMBOLS = [sym for syms in WATCHLIST.values() for sym in syms]

SYMBOL_TO_SECTOR = {
    sym: sector
    for sector, syms in WATCHLIST.items()
    for sym in syms
}
