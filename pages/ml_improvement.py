"""
Tab 5: ML Self-Improvement
Train XGBoost/LightGBM models, compare performance, view feature importance.
Full training pipeline with versioning and comparison.
"""

import json
import streamlit as st
import plotly.graph_objects as go

from db import engine
from ml.feature_engineering import build_feature_matrix
from ml.model_store import get_model_history, compare_models
from ml.evaluation import get_performance_timeline, get_feature_importance

CHART_LAYOUT = dict(template="plotly_dark", margin=dict(l=40, r=20, t=40, b=40), font=dict(size=12))


def render():
    st.header("ML Self-Improvement")

    # --- Data Status ---
    X, y = build_feature_matrix()
    n_samples = len(X)
    n_wins = int(y.sum()) if not y.empty else 0
    n_losses = n_samples - n_wins

    sc = st.columns(4)
    with sc[0]:
        st.metric("Datenpunkte", n_samples)
    with sc[1]:
        st.metric("Wins / Losses", f"{n_wins} / {n_losses}")
    with sc[2]:
        st.metric("Features", len(X.columns) if not X.empty else 0)
    with sc[3]:
        ready = n_samples >= 20
        st.metric("Training bereit", "Ja" if ready else f"Nein ({n_samples}/20)")

    st.progress(min(n_samples / 100, 1.0), text=f"{n_samples}/100 Trades (ideal)")

    st.divider()

    # --- Training ---
    st.subheader("Modell Training")

    col_train, col_info = st.columns([1, 2])
    with col_train:
        if st.button("Jetzt trainieren", type="primary", disabled=n_samples < 20):
            _run_training()
    with col_info:
        if n_samples < 20:
            st.warning("Mindestens 20 abgeschlossene Trades nötig für Training.")
        else:
            st.caption("Trainiert XGBoost und LightGBM auf allen verfügbaren Daten.")

    st.divider()

    # --- Model History ---
    st.subheader("Trainierte Modelle")
    history = get_model_history()

    if history:
        for model in history:
            metrics = json.loads(model["metrics"])
            active_badge = " **[AKTIV]**" if model["is_active"] else ""
            with st.expander(
                f"{model['model_type']} v{model['version']}{active_badge} — "
                f"Acc: {metrics.get('accuracy', 0):.1%} F1: {metrics.get('f1', 0):.1%}",
                expanded=bool(model["is_active"]),
            ):
                mc = st.columns(4)
                with mc[0]:
                    st.metric("Accuracy", f"{metrics.get('accuracy', 0):.3f}")
                with mc[1]:
                    st.metric("F1 Score", f"{metrics.get('f1', 0):.3f}")
                with mc[2]:
                    st.metric("AUC-ROC", f"{metrics.get('auc_roc', 0):.3f}")
                with mc[3]:
                    st.metric("CV Accuracy", f"{metrics.get('cv_accuracy_mean', 0):.3f} +/- {metrics.get('cv_accuracy_std', 0):.3f}")

                st.caption(f"Trainiert: {model['trained_at'][:16]} | Samples: {model['training_rows']}")

                # Feature importance
                fi = metrics.get("feature_importance", {})
                if fi:
                    _render_feature_importance(fi, model["model_type"], model["version"])
    else:
        st.info("Noch keine Modelle trainiert.")

    st.divider()

    # --- Model Comparison ---
    st.subheader("Modell-Vergleich")
    for model_type in ["xgboost", "lightgbm"]:
        comparison = compare_models(model_type)
        if comparison:
            st.markdown(f"**{model_type}** v{comparison['current_version']} vs v{comparison['previous_version']}")

            if comparison["improvements"]:
                for key, data in comparison["improvements"].items():
                    st.success(f"  {key}: {data['previous']:.3f} -> {data['current']:.3f} (+{data['diff']:.3f})")

            if comparison["degradations"]:
                for key, data in comparison["degradations"].items():
                    st.error(f"  {key}: {data['previous']:.3f} -> {data['current']:.3f} ({data['diff']:.3f})")

            if not comparison["improvements"] and not comparison["degradations"]:
                st.caption(f"  Keine signifikanten Änderungen.")

    # --- Performance Timeline ---
    timeline = get_performance_timeline()
    if len(timeline) > 1:
        st.subheader("Performance über Zeit")
        _render_performance_timeline(timeline)

    st.divider()

    # --- Features Info ---
    if not X.empty:
        with st.expander("Feature-Details"):
            st.markdown(f"**{len(X.columns)} Features:**")
            for col in X.columns:
                st.caption(f"• {col}")


def _run_training():
    """Execute ML training pipeline."""
    with st.spinner("Training läuft... (XGBoost + LightGBM)"):
        from ml.trainer import train_models
        results = train_models()

    if not results.get("ok"):
        st.error(f"Training fehlgeschlagen: {results.get('error', 'Unbekannter Fehler')}")
        return

    st.success(f"Training abgeschlossen! ({results['training_samples']} Train / {results['test_samples']} Test)")

    for model_type in ["xgboost", "lightgbm"]:
        r = results.get(model_type, {})
        if r.get("ok"):
            m = r["metrics"]
            st.markdown(
                f"**{model_type} v{r['version']}**: "
                f"Accuracy={m['accuracy']:.3f}, F1={m['f1']:.3f}, AUC={m.get('auc_roc', 0):.3f}"
            )
        else:
            st.warning(f"{model_type}: {r.get('error', 'Fehler')}")


def _render_feature_importance(fi: dict, model_type: str, version: int):
    """Render feature importance chart."""
    sorted_fi = sorted(fi.items(), key=lambda x: x[1], reverse=True)
    names = [f[0] for f in sorted_fi[:12]]
    values = [f[1] for f in sorted_fi[:12]]

    fig = go.Figure(go.Bar(
        y=names, x=values, orientation="h",
        marker_color="#ff7f0e",
    ))
    fig.update_layout(**CHART_LAYOUT, height=300,
                      title=f"Feature Importance ({model_type} v{version})",
                      yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)


def _render_performance_timeline(timeline: list[dict]):
    """Render model performance over versions."""
    import pandas as pd
    df = pd.DataFrame(timeline)

    fig = go.Figure()
    for mt in df["model_type"].unique():
        mt_df = df[df["model_type"] == mt].sort_values("version")
        fig.add_trace(go.Scatter(
            x=mt_df["version"], y=mt_df["accuracy"],
            mode="lines+markers", name=f"{mt} Accuracy",
        ))
        fig.add_trace(go.Scatter(
            x=mt_df["version"], y=mt_df["f1"],
            mode="lines+markers", name=f"{mt} F1",
            line=dict(dash="dash"),
        ))

    fig.update_layout(**CHART_LAYOUT, height=350, title="Performance über Modell-Versionen",
                      xaxis_title="Version", yaxis_title="Score")
    st.plotly_chart(fig, use_container_width=True)
