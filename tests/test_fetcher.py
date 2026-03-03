"""
Unit tests for pure business logic — no infrastructure required.
"""
import pandas as pd


def test_last_completed_bar_skips_zero_volume():
    """
    yfinance returns the current in-progress bar with Volume=0.
    The fetcher must skip it and use the last COMPLETED bar.
    """
    data = {
        "Open":   [100.0, 101.0, 102.0],
        "High":   [101.0, 102.0, 103.0],
        "Low":    [99.0,  100.0, 101.0],
        "Close":  [100.5, 101.5, 102.0],
        "Volume": [50000, 75000, 0],       # last bar is in-progress
    }
    hist = pd.DataFrame(data)

    last_completed = hist[hist["Volume"] > 0].iloc[-1]

    assert last_completed["Volume"] == 75000
    assert last_completed["Close"] == 101.5


def test_last_completed_bar_when_all_have_volume():
    """When all bars have volume, the true last bar is returned."""
    data = {
        "Open":   [100.0, 101.0, 102.0],
        "High":   [101.0, 102.0, 103.0],
        "Low":    [99.0,  100.0, 101.0],
        "Close":  [100.5, 101.5, 102.5],
        "Volume": [50000, 75000, 60000],
    }
    hist = pd.DataFrame(data)

    last_completed = hist[hist["Volume"] > 0].iloc[-1]

    assert last_completed["Volume"] == 60000
    assert last_completed["Close"] == 102.5


def test_empty_history_after_volume_filter():
    """Edge case: all bars have zero volume — filter returns empty DataFrame."""
    data = {
        "Open":   [100.0],
        "High":   [101.0],
        "Low":    [99.0],
        "Close":  [100.5],
        "Volume": [0],
    }
    hist = pd.DataFrame(data)

    filtered = hist[hist["Volume"] > 0]

    assert len(filtered) == 0


def test_anomaly_zscore_threshold_logic():
    """
    Volume anomaly: flag if current volume exceeds threshold * std deviations
    above the rolling average. Pure arithmetic — no DB needed.
    """
    avg_volume = 1_000_000
    std_volume = 200_000
    threshold = 2.5

    normal_volume = 1_400_000   # 2.0 std above avg — should NOT flag
    anomaly_volume = 1_600_000  # 3.0 std above avg — should flag

    def is_anomaly(vol):
        return (vol - avg_volume) / std_volume > threshold

    assert not is_anomaly(normal_volume)
    assert is_anomaly(anomaly_volume)


def test_price_change_threshold_logic():
    """
    Price anomaly: flag if absolute % change exceeds threshold.
    """
    threshold_pct = 2.0

    prev_close = 100.0
    small_move = 101.5   # 1.5% — should NOT flag
    big_move = 103.0     # 3.0% — should flag

    def pct_change(current, prev):
        return abs((current - prev) / prev * 100)

    assert pct_change(small_move, prev_close) < threshold_pct
    assert pct_change(big_move, prev_close) >= threshold_pct
