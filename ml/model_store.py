"""
Model storage and versioning.
Saves/loads ML models with metadata tracking in DB.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import joblib

from config import DATA_DIR
from db import engine

logger = logging.getLogger(__name__)

MODELS_DIR = DATA_DIR / "models"


def save_model(
    model,
    model_type: str,
    version: int,
    metrics: dict,
    feature_cols: list[str],
    training_rows: int,
) -> str:
    """Save a trained model and record it in DB."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"{model_type}_v{version}.joblib"
    model_path = str(MODELS_DIR / filename)

    joblib.dump(model, model_path)

    # Deactivate previous models of same type
    engine.execute(
        "UPDATE ml_models SET is_active = 0 WHERE model_type = ?",
        (model_type,),
    )

    # Store metadata
    engine.execute(
        """INSERT INTO ml_models (model_type, version, metrics, feature_cols, model_path, is_active, trained_at, training_rows)
           VALUES (?, ?, ?, ?, ?, 1, ?, ?)""",
        (model_type, version, json.dumps(metrics), json.dumps(feature_cols),
         model_path, datetime.utcnow().isoformat(), training_rows),
    )

    logger.info(f"Model saved: {model_type} v{version} -> {model_path}")
    return model_path


def load_model(model_type: str, version: int | None = None):
    """Load a model. If version is None, load the active one."""
    if version:
        row = engine.query_one(
            "SELECT model_path FROM ml_models WHERE model_type = ? AND version = ?",
            (model_type, version),
        )
    else:
        row = engine.query_one(
            "SELECT model_path FROM ml_models WHERE model_type = ? AND is_active = 1",
            (model_type,),
        )

    if not row:
        return None

    model_path = row["model_path"]
    if not Path(model_path).exists():
        logger.error(f"Model file not found: {model_path}")
        return None

    return joblib.load(model_path)


def get_next_version(model_type: str) -> int:
    """Get the next version number for a model type."""
    row = engine.query_one(
        "SELECT MAX(version) as max_v FROM ml_models WHERE model_type = ?",
        (model_type,),
    )
    return (row["max_v"] or 0) + 1 if row else 1


def get_model_history(model_type: str | None = None) -> list[dict]:
    """Get model training history."""
    if model_type:
        return engine.query(
            "SELECT * FROM ml_models WHERE model_type = ? ORDER BY trained_at DESC",
            (model_type,),
        )
    return engine.query("SELECT * FROM ml_models ORDER BY trained_at DESC")


def compare_models(model_type: str) -> dict | None:
    """Compare current active model with previous version."""
    models = engine.query(
        "SELECT * FROM ml_models WHERE model_type = ? ORDER BY version DESC LIMIT 2",
        (model_type,),
    )
    if len(models) < 2:
        return None

    current = models[0]
    previous = models[1]

    current_metrics = json.loads(current["metrics"])
    previous_metrics = json.loads(previous["metrics"])

    comparison = {
        "current_version": current["version"],
        "previous_version": previous["version"],
        "improvements": {},
        "degradations": {},
    }

    for key in ["accuracy", "f1", "auc_roc"]:
        if key in current_metrics and key in previous_metrics:
            diff = current_metrics[key] - previous_metrics[key]
            entry = {
                "current": current_metrics[key],
                "previous": previous_metrics[key],
                "diff": diff,
            }
            if diff > 0.001:
                comparison["improvements"][key] = entry
            elif diff < -0.001:
                comparison["degradations"][key] = entry

    return comparison
