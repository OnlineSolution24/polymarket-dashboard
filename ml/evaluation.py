"""
Model evaluation and comparison utilities.
"""

import json
import numpy as np
import pandas as pd

from db import engine
from ml.model_store import load_model, get_model_history


def evaluate_active_model(model_type: str = "xgboost") -> dict:
    """
    Evaluate the currently active model against recent trades.
    Returns evaluation metrics.
    """
    model = load_model(model_type)
    if model is None:
        return {"ok": False, "error": f"Kein aktives {model_type} Modell gefunden"}

    # Get model metadata
    meta = engine.query_one(
        "SELECT * FROM ml_models WHERE model_type = ? AND is_active = 1",
        (model_type,),
    )
    if not meta:
        return {"ok": False, "error": "Modell-Metadaten nicht gefunden"}

    feature_cols = json.loads(meta["feature_cols"])

    # Build features for recent trades (not used in training)
    from ml.feature_engineering import build_feature_matrix
    X, y = build_feature_matrix()

    if X.empty:
        return {"ok": False, "error": "Keine Daten für Evaluation"}

    # Use only features the model knows
    available = [c for c in feature_cols if c in X.columns]
    missing = [c for c in feature_cols if c not in X.columns]

    if missing:
        # Add missing columns as zeros
        for col in missing:
            X[col] = 0

    X_eval = X[feature_cols]

    try:
        predictions = model.predict(X_eval)
        probabilities = model.predict_proba(X_eval)[:, 1] if hasattr(model, "predict_proba") else predictions.astype(float)

        from sklearn.metrics import accuracy_score, f1_score
        accuracy = accuracy_score(y, predictions)
        f1 = f1_score(y, predictions, zero_division=0)

        return {
            "ok": True,
            "model_type": model_type,
            "version": meta["version"],
            "samples": len(X_eval),
            "accuracy": float(accuracy),
            "f1": float(f1),
            "predictions": predictions.tolist(),
            "probabilities": probabilities.tolist(),
            "actual": y.tolist(),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_performance_timeline() -> list[dict]:
    """Get model performance over time for charting."""
    history = get_model_history()
    timeline = []

    for model in history:
        metrics = json.loads(model["metrics"])
        timeline.append({
            "model_type": model["model_type"],
            "version": model["version"],
            "trained_at": model["trained_at"],
            "accuracy": metrics.get("accuracy", 0),
            "f1": metrics.get("f1", 0),
            "auc_roc": metrics.get("auc_roc", 0),
            "training_rows": model["training_rows"],
            "is_active": bool(model["is_active"]),
        })

    return timeline


def get_feature_importance(model_type: str = "xgboost") -> dict:
    """Get feature importance from active model."""
    meta = engine.query_one(
        "SELECT metrics FROM ml_models WHERE model_type = ? AND is_active = 1",
        (model_type,),
    )
    if not meta:
        return {}

    metrics = json.loads(meta["metrics"])
    return metrics.get("feature_importance", {})
