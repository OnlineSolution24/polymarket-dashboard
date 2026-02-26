"""
ML Training Pipeline.
Trains XGBoost and LightGBM models on trade history.
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, classification_report

from ml.feature_engineering import build_feature_matrix
from ml.model_store import save_model, get_next_version

logger = logging.getLogger(__name__)


def train_models(test_size: float = 0.2) -> dict:
    """
    Train both XGBoost and LightGBM models.
    Returns training results with metrics for both.
    """
    X, y = build_feature_matrix()

    if X.empty or len(X) < 20:
        return {"ok": False, "error": f"Zu wenig Daten: {len(X)} Samples (min. 20)"}

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42, stratify=y if len(y.unique()) > 1 else None,
    )

    results = {}

    # --- XGBoost ---
    try:
        import xgboost as xgb

        xgb_model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
        )
        xgb_model.fit(X_train, y_train)

        y_pred = xgb_model.predict(X_test)
        y_proba = xgb_model.predict_proba(X_test)[:, 1] if len(y.unique()) > 1 else y_pred.astype(float)

        xgb_metrics = _compute_metrics(y_test, y_pred, y_proba)

        # Cross-validation
        cv_scores = cross_val_score(xgb_model, X, y, cv=min(5, len(X) // 5), scoring="accuracy")
        xgb_metrics["cv_accuracy_mean"] = float(cv_scores.mean())
        xgb_metrics["cv_accuracy_std"] = float(cv_scores.std())

        # Feature importance
        importance = dict(zip(X.columns, xgb_model.feature_importances_))
        xgb_metrics["feature_importance"] = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True)[:15])

        # Save model
        version = get_next_version("xgboost")
        model_path = save_model(xgb_model, "xgboost", version, xgb_metrics, list(X.columns), len(X_train))

        results["xgboost"] = {
            "ok": True,
            "metrics": xgb_metrics,
            "version": version,
            "model_path": model_path,
        }
        logger.info(f"XGBoost v{version}: accuracy={xgb_metrics['accuracy']:.3f}, f1={xgb_metrics['f1']:.3f}")

    except ImportError:
        results["xgboost"] = {"ok": False, "error": "xgboost not installed"}
    except Exception as e:
        results["xgboost"] = {"ok": False, "error": str(e)}
        logger.error(f"XGBoost training failed: {e}")

    # --- LightGBM ---
    try:
        import lightgbm as lgb

        lgb_model = lgb.LGBMClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            verbose=-1,
        )
        lgb_model.fit(X_train, y_train)

        y_pred = lgb_model.predict(X_test)
        y_proba = lgb_model.predict_proba(X_test)[:, 1] if len(y.unique()) > 1 else y_pred.astype(float)

        lgb_metrics = _compute_metrics(y_test, y_pred, y_proba)

        cv_scores = cross_val_score(lgb_model, X, y, cv=min(5, len(X) // 5), scoring="accuracy")
        lgb_metrics["cv_accuracy_mean"] = float(cv_scores.mean())
        lgb_metrics["cv_accuracy_std"] = float(cv_scores.std())

        importance = dict(zip(X.columns, lgb_model.feature_importances_))
        lgb_metrics["feature_importance"] = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True)[:15])

        version = get_next_version("lightgbm")
        model_path = save_model(lgb_model, "lightgbm", version, lgb_metrics, list(X.columns), len(X_train))

        results["lightgbm"] = {
            "ok": True,
            "metrics": lgb_metrics,
            "version": version,
            "model_path": model_path,
        }
        logger.info(f"LightGBM v{version}: accuracy={lgb_metrics['accuracy']:.3f}, f1={lgb_metrics['f1']:.3f}")

    except ImportError:
        results["lightgbm"] = {"ok": False, "error": "lightgbm not installed"}
    except Exception as e:
        results["lightgbm"] = {"ok": False, "error": str(e)}
        logger.error(f"LightGBM training failed: {e}")

    results["ok"] = any(r.get("ok") for r in results.values() if isinstance(r, dict))
    results["training_samples"] = len(X_train)
    results["test_samples"] = len(X_test)
    results["features"] = list(X.columns)

    return results


def _compute_metrics(y_true, y_pred, y_proba) -> dict:
    """Compute classification metrics."""
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
    }

    try:
        metrics["auc_roc"] = float(roc_auc_score(y_true, y_proba))
    except Exception:
        metrics["auc_roc"] = 0.0

    # Confusion matrix values
    from sklearn.metrics import confusion_matrix
    cm = confusion_matrix(y_true, y_pred)
    if cm.shape == (2, 2):
        metrics["true_negatives"] = int(cm[0, 0])
        metrics["false_positives"] = int(cm[0, 1])
        metrics["false_negatives"] = int(cm[1, 0])
        metrics["true_positives"] = int(cm[1, 1])

    return metrics
