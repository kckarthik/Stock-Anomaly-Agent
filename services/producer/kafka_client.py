"""
kafka_client.py — Kafka producer wrapper
Handles connection retries, topic creation, and JSON serialization.
"""

import json
import time
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import KafkaConfig

log = logging.getLogger(__name__)


def ensure_topic(retries: int = 20, delay: int = 5) -> None:
    """Create raw_quotes topic if it doesn't exist."""
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KafkaConfig.BROKER)
            topic = NewTopic(
                name               = KafkaConfig.TOPIC,
                num_partitions     = 4,
                replication_factor = 1,
            )
            admin.create_topics([topic])
            admin.close()
            log.info(f"✅ Topic '{KafkaConfig.TOPIC}' created")
            return
        except TopicAlreadyExistsError:
            log.info(f"✅ Topic '{KafkaConfig.TOPIC}' already exists")
            return
        except Exception as e:
            log.warning(f"Topic creation attempt {attempt}/{retries}: {e}")
            time.sleep(delay)
    log.warning(f"Could not create topic after {retries} attempts — auto-create should handle it")


def create_producer(retries: int = 20, delay: int = 5) -> KafkaProducer:
    """Create Kafka producer, retrying until broker is ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers = KafkaConfig.BROKER,
                value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
                key_serializer    = lambda k: k.encode("utf-8"),
                acks              = "all",
                retries           = 3,
                linger_ms         = 100,
                batch_size        = 16384,
            )
            log.info(f"✅ Kafka producer connected → {KafkaConfig.BROKER}")
            ensure_topic()
            return producer
        except Exception as e:
            log.warning(f"Kafka not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError(f"Cannot connect to Kafka after {retries} attempts")


def publish(producer: KafkaProducer, quote: dict) -> None:
    """Publish one quote message to Kafka, keyed by symbol."""
    producer.send(
        topic = KafkaConfig.TOPIC,
        key   = quote["symbol"],
        value = quote,
    )
