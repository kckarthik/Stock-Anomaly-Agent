"""
Validate dbt model SQL files without a database connection.
"""
import glob
import os

DBT_MODELS = os.path.join(os.path.dirname(__file__), "..", "dbt", "models")
DBT_PROJECT = os.path.join(os.path.dirname(__file__), "..", "dbt", "dbt_project.yml")


def _all_sql_files():
    return glob.glob(os.path.join(DBT_MODELS, "**", "*.sql"), recursive=True)


def test_dbt_project_file_exists():
    assert os.path.exists(DBT_PROJECT), "dbt_project.yml not found"


def test_gold_models_all_present():
    gold_path = os.path.join(DBT_MODELS, "gold")
    expected = [
        "anomaly_feed.sql",
        "ohlcv_1min.sql",
        "price_anomalies.sql",
        "volume_anomalies.sql",
        "volume_baseline.sql",
        "sector_summary.sql",
    ]
    for model in expected:
        path = os.path.join(gold_path, model)
        assert os.path.exists(path), f"Gold model '{model}' is missing"


def test_staging_model_present():
    path = os.path.join(DBT_MODELS, "staging", "stg_silver_bars.sql")
    assert os.path.exists(path), "Staging model stg_silver_bars.sql is missing"


def test_all_models_contain_select():
    sql_files = _all_sql_files()
    assert len(sql_files) > 0, "No SQL model files found under dbt/models/"
    for path in sql_files:
        with open(path) as f:
            content = f.read().upper()
        assert "SELECT" in content, f"{os.path.basename(path)} does not contain a SELECT statement"


def test_no_dbt_utils_dependency():
    """dbt_utils is not installed — any usage will break dbt run."""
    for path in _all_sql_files():
        with open(path) as f:
            content = f.read()
        assert "dbt_utils" not in content, (
            f"{os.path.basename(path)} references dbt_utils which is not installed. "
            "Use pure SQL equivalents instead."
        )


def test_anomaly_feed_unions_both_anomaly_types():
    path = os.path.join(DBT_MODELS, "gold", "anomaly_feed.sql")
    with open(path) as f:
        content = f.read()
    assert "price_anomalies" in content, "anomaly_feed.sql must reference price_anomalies"
    assert "volume_anomalies" in content, "anomaly_feed.sql must reference volume_anomalies"


def test_no_hardcoded_schema_names():
    """Models should use ref() not hardcoded schema.table references."""
    for path in _all_sql_files():
        with open(path) as f:
            content = f.read()
        # Allow silver. references (source data) but flag public. hardcoding
        assert "public." not in content.lower(), (
            f"{os.path.basename(path)} hardcodes 'public.' schema — use ref() or source() instead"
        )
