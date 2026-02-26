"""Tests for the ML pipeline."""

import json
import numpy as np
import pandas as pd

from db import engine


def test_cost_tracker(reset_db):
    """Test cost tracking and budget checking."""
    from services.cost_tracker import log_cost, check_budget, estimate_cost, estimate_tokens

    # Test token estimation
    tokens = estimate_tokens("Hello world, this is a test message.")
    assert tokens > 0

    # Test cost estimation
    cost = estimate_cost("claude-sonnet", 1000, 500)
    assert cost > 0

    # Log a cost
    log_cost("claude-sonnet", tokens_in=1000, tokens_out=500, agent_id="test_agent")

    costs = engine.query("SELECT * FROM api_costs")
    assert len(costs) == 1
    assert costs[0]["provider"] == "claude-sonnet"
    assert costs[0]["agent_id"] == "test_agent"

    # Budget should still be allowed
    budget = check_budget()
    assert budget["allowed"] is True


def test_budget_enforcement(reset_db):
    """Test that budget limits are enforced."""
    from services.cost_tracker import log_cost, check_budget

    # Blow the daily budget with a large cost
    log_cost("claude-sonnet", cost_usd=100.0, agent_id="big_spender")

    budget = check_budget()
    assert budget["allowed"] is False
    assert "erschöpft" in budget["reason"].lower() or "exceeded" in budget["reason"].lower()


def test_feature_engineering(sample_trades, sample_markets):
    """Test feature matrix building with sample data."""
    # We need enough trades for the feature engineering to work
    # Add more trades
    for i in range(20):
        engine.execute(
            """INSERT INTO trades (market_id, market_question, side, amount_usd, price, status, result, pnl, executed_at)
               VALUES (?, ?, ?, ?, ?, 'executed', ?, ?, ?)""",
            (f"m{(i%3)+1}", f"Market {i}", "YES" if i % 2 == 0 else "NO",
             float(10 + i), 0.5 + (i % 5) * 0.08,
             "win" if i % 3 != 0 else "loss",
             float(5 if i % 3 != 0 else -10),
             f"2026-02-{(i%28)+1:02d}T12:00:00"),
        )

    from ml.feature_engineering import build_feature_matrix
    X, y = build_feature_matrix()

    if not X.empty:
        assert len(X) == len(y)
        assert "entry_price" in X.columns or "bet_size" in X.columns
        assert set(y.unique()).issubset({0, 1})


class _DummyModel:
    """Module-level dummy model so it can be pickled."""
    def predict(self, X):
        return np.zeros(len(X))


def test_model_store(reset_db, tmp_path):
    """Test model save and load."""
    import config
    original = config.DATA_DIR
    config.DATA_DIR = tmp_path

    from ml.model_store import save_model, load_model, get_next_version, get_model_history

    model = _DummyModel()
    version = get_next_version("test_model")
    assert version == 1

    path = save_model(model, "test_model", version, {"accuracy": 0.85}, ["f1", "f2"], 100)
    assert path is not None

    # Load it back
    loaded = load_model("test_model")
    assert loaded is not None

    # Check history
    history = get_model_history("test_model")
    assert len(history) == 1
    assert history[0]["is_active"] == 1

    # Next version should be 2
    assert get_next_version("test_model") == 2

    config.DATA_DIR = original


def test_evaluation_no_model(reset_db):
    """Test evaluation when no model exists."""
    from ml.evaluation import evaluate_active_model
    result = evaluate_active_model("xgboost")
    assert result["ok"] is False


def test_performance_timeline(reset_db):
    """Test performance timeline with no data."""
    from ml.evaluation import get_performance_timeline
    timeline = get_performance_timeline()
    assert isinstance(timeline, list)
