"""
Validate docker-compose.yml structure without running Docker.
"""
import os
import yaml

COMPOSE_PATH = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")


def _load():
    with open(COMPOSE_PATH) as f:
        return yaml.safe_load(f)


def test_compose_loads():
    config = _load()
    assert "services" in config


def test_required_services_present():
    services = _load()["services"]
    required = [
        "kafka", "timescaledb", "minio", "redis",
        "producer", "consumer", "grafana", "prometheus",
        "celery-worker", "agent-scheduler",
    ]
    for svc in required:
        assert svc in services, f"Service '{svc}' missing from docker-compose.yml"


def test_grafana_not_on_latest_tag():
    """Grafana must be pinned — 'latest' caused breaking changes in production."""
    image = _load()["services"]["grafana"]["image"]
    assert image != "grafana/grafana:latest", (
        "Grafana must use a pinned version (e.g. grafana/grafana:11.4.0), not 'latest'"
    )


def test_timescaledb_has_healthcheck():
    services = _load()["services"]
    assert "healthcheck" in services["timescaledb"], (
        "timescaledb must have a healthcheck so dependent services wait correctly"
    )


def test_kafka_has_healthcheck():
    services = _load()["services"]
    assert "healthcheck" in services["kafka"], (
        "kafka must have a healthcheck so dependent services wait correctly"
    )


def test_no_service_uses_host_network():
    services = _load()["services"]
    for name, cfg in services.items():
        assert cfg.get("network_mode") != "host", (
            f"Service '{name}' uses host network mode — use bridge network instead"
        )


def test_env_file_not_hardcoded_in_compose():
    """Secrets should come from .env, not be hardcoded in docker-compose.yml."""
    with open(COMPOSE_PATH) as f:
        raw = f.read()
    # Real Langfuse keys start with pk-lf- followed by a UUID
    assert "pk-lf-" not in raw or "placeholder" in raw, (
        "Real Langfuse public key found in docker-compose.yml — use .env instead"
    )
