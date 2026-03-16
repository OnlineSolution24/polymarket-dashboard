"""
Alpha Scanner — Find under-the-radar wallets with strong trading performance.
Scans Polymarket leaderboard, enriches wallet data, filters and scores.
"""

import streamlit as st
import pandas as pd

from services.alpha_scanner_service import (
    AlphaScannerService,
    FilterConfig,
    FILTER_PRESETS,
)
from components.status_cards import kpi_row


SCAN_DEPTH_MAP = {
    "Schnell (100)": 100,
    "Normal (300)": 300,
    "Tief (500)": 500,
}

CATEGORIES = [
    "OVERALL", "POLITICS", "SPORTS", "CRYPTO",
    "CULTURE", "WEATHER", "ECONOMICS", "TECH", "FINANCE",
]


def render():
    st.header("Alpha Scanner")

    # --- Session state init ---
    if "alpha_scan_result" not in st.session_state:
        st.session_state.alpha_scan_result = None
    if "alpha_scanning" not in st.session_state:
        st.session_state.alpha_scanning = False

    # --- Header row: info + scan controls ---
    hc1, hc2, hc3 = st.columns([2, 1, 1])
    with hc1:
        result = st.session_state.alpha_scan_result
        if result:
            st.caption(
                f"Letzter Scan: {result.timestamp.strftime('%d.%m.%Y %H:%M')} "
                f"({result.scan_duration_seconds:.0f}s)"
            )
        else:
            st.caption("Noch kein Scan durchgeführt.")
    with hc2:
        depth = st.selectbox(
            "Scan-Tiefe",
            list(SCAN_DEPTH_MAP.keys()),
            index=1,
            key="alpha_depth",
            label_visibility="collapsed",
        )
    with hc3:
        scan_clicked = st.button(
            "SCAN STARTEN" if not result else "RESCAN",
            type="primary",
            use_container_width=True,
        )

    # --- Run scan ---
    if scan_clicked and not st.session_state.alpha_scanning:
        st.session_state.alpha_scanning = True
        max_w = SCAN_DEPTH_MAP[depth]

        progress_bar = st.progress(0)
        status_text = st.empty()

        def on_progress(current: int, total: int, message: str):
            progress_bar.progress(min(current / max(total, 1), 1.0))
            status_text.text(message)

        service = AlphaScannerService()
        try:
            scan_result = service.scan_wallets(
                max_wallets=max_w, progress_callback=on_progress,
            )
            st.session_state.alpha_scan_result = scan_result
        except Exception as e:
            st.error(f"Scan fehlgeschlagen: {e}")
        finally:
            service.client.close()
            st.session_state.alpha_scanning = False
            progress_bar.empty()
            status_text.empty()
        st.rerun()

    # --- No results yet ---
    result = st.session_state.alpha_scan_result
    if not result or not result.wallets:
        st.info(
            "Starte einen Scan, um Wallets zu analysieren. "
            "Der Scanner durchsucht das Polymarket-Leaderboard nach profitablen Tradern."
        )
        return

    # --- Reset handling (must happen before widgets) ---
    if st.session_state.get("_alpha_do_reset"):
        st.session_state["_alpha_do_reset"] = False
        st.session_state["alpha_preset"] = "Standard"

    # --- Presets ---
    preset_name = st.radio(
        "Filter-Preset",
        list(FILTER_PRESETS.keys()),
        horizontal=True,
        key="alpha_preset",
    )
    preset = FILTER_PRESETS[preset_name]

    # --- Quick filters ---
    qc1, qc2, qc3 = st.columns([2, 2, 1])
    with qc1:
        min_pnl = st.slider(
            "Min 7D PnL ($)", 0, 50000, int(preset.min_pnl_7d),
            step=500,
        )
    with qc2:
        max_views = st.slider(
            "Max Views", 0, 100000, preset.max_views,
            step=100,
        )
    with qc3:
        if st.button("RESET"):
            st.session_state["_alpha_do_reset"] = True
            st.rerun()

    # --- Advanced filters ---
    with st.expander("Erweiterte Filter"):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            min_trades = st.slider(
                "Min Trades/Tag", 0.0, 50.0, float(preset.min_trades_day),
                step=0.5,
            )
            max_trades = st.slider(
                "Max Trades/Tag", 1.0, 200.0, float(preset.max_trades_day),
                step=1.0,
            )
            max_pos = st.slider(
                "Max Aktive Positionen", 1, 500, int(preset.max_active_pos),
            )
        with fc2:
            min_roi = st.slider(
                "Min 7D ROI (%)", 0.0, 500.0, float(preset.min_roi_7d),
                step=5.0,
            )
            min_pnl_30d = st.slider(
                "Min 30D PnL ($)", 0, 100000, int(preset.min_pnl_30d),
                step=500,
            )
            min_volume = st.slider(
                "Min Volume ($)", 0, 500000, int(preset.min_volume),
                step=1000,
            )
        with fc3:
            min_wallet_age = st.slider(
                "Min Wallet-Alter (Tage)", 0, 365, int(preset.min_wallet_age),
            )
            min_consistency = st.slider(
                "Min Konsistenz (Tage/7)", 1, 7, int(preset.min_consistency),
            )
            min_win_rate = st.slider(
                "Min Win Rate (%)", 0, 100, int(preset.min_win_rate),
                step=5,
            )

        vc1, vc2 = st.columns(2)
        with vc1:
            verified_opt = st.radio(
                "Verified",
                ["Egal", "Nur Verified", "Nur Unverified"],
                horizontal=True,
                key="alpha_verified",
            )
        with vc2:
            cats = st.multiselect(
                "Kategorien",
                CATEGORIES,
                default=preset.categories,
                key="alpha_cats",
            )

    # Build active filter config
    verified_map = {"Egal": "any", "Nur Verified": "verified", "Nur Unverified": "unverified"}
    active_filter = FilterConfig(
        min_pnl_7d=min_pnl,
        max_views=max_views,
        min_trades_day=min_trades,
        max_trades_day=max_trades,
        max_active_pos=max_pos,
        min_roi_7d=min_roi,
        min_pnl_30d=min_pnl_30d,
        min_volume=min_volume,
        min_wallet_age=min_wallet_age,
        min_consistency=min_consistency,
        min_win_rate=min_win_rate,
        verified=verified_map.get(verified_opt, "any"),
        categories=cats,
    )

    # Apply filters
    filtered = AlphaScannerService.filter_wallets(result.wallets, active_filter)

    # --- KPI cards ---
    avg_roi = (
        sum(w.roi_7d for w in filtered) / len(filtered)
        if filtered else 0
    )
    kpi_row([
        {"label": "Wallets Scanned", "value": result.total_scanned},
        {"label": "Passed Filters", "value": len(filtered)},
        {"label": "Matches", "value": len(filtered)},
        {"label": "Avg 7D ROI", "value": f"{avg_roi:.1f}%"},
    ])

    st.divider()

    # --- Sort ---
    sc1, sc2 = st.columns([3, 1])
    with sc1:
        sort_col = st.selectbox(
            "Sortieren nach",
            ["Alpha Score", "Radar Score", "7D PnL", "7D ROI %",
             "30D PnL", "Win Rate", "Trades/Day", "Active Pos"],
            key="alpha_sort",
        )
    with sc2:
        sort_asc = st.toggle("Aufsteigend", False, key="alpha_sort_dir")

    # --- Build dataframe ---
    if not filtered:
        st.warning("Keine Wallets entsprechen den Filtern. Versuche die Filter zu lockern.")
        return

    sort_key_map = {
        "Alpha Score": "Alpha Score",
        "Radar Score": "Radar Score",
        "7D PnL": "7D PnL",
        "7D ROI %": "7D ROI %",
        "30D PnL": "30D PnL",
        "Win Rate": "Win Rate",
        "Trades/Day": "Trades/Day",
        "Active Pos": "Active Pos",
    }

    rows = []
    for w in filtered:
        name = w.username or w.pseudonym or w.address[:12]
        if w.verified:
            name += " \u2713"
        rows.append({
            "Wallet": name,
            "Profil": f"https://polymarket.com/profile/{w.address}",
            "Alpha Score": w.alpha_score,
            "Radar Score": w.radar_score,
            "7D PnL": round(w.pnl_7d, 2),
            "7D ROI %": round(w.roi_7d, 2),
            "30D PnL": round(w.pnl_30d, 2),
            "Active Pos": w.active_positions,
            "Trades/Day": round(w.trades_per_day, 2),
            "Win Rate": round(w.win_rate, 1),
            "Views": w.views,
            "Alter (Tage)": w.wallet_age_days,
            "Konsistenz": w.consistency_days,
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(sort_key_map[sort_col], ascending=sort_asc).reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = "Rank"

    st.dataframe(
        df,
        use_container_width=True,
        column_config={
            "Profil": st.column_config.LinkColumn("Profil", display_text="Öffnen"),
            "Alpha Score": st.column_config.ProgressColumn(
                "Alpha Score", min_value=0, max_value=1, format="%.2f",
            ),
            "Radar Score": st.column_config.ProgressColumn(
                "Radar Score", min_value=0, max_value=1, format="%.2f",
            ),
            "7D PnL": st.column_config.NumberColumn("7D PnL", format="$%.0f"),
            "7D ROI %": st.column_config.NumberColumn("7D ROI", format="%.1f%%"),
            "30D PnL": st.column_config.NumberColumn("30D PnL", format="$%.0f"),
            "Win Rate": st.column_config.NumberColumn("Win Rate", format="%.0f%%"),
            "Trades/Day": st.column_config.NumberColumn("Trades/D", format="%.2f"),
        },
    )

    # --- Footer ---
    st.caption(
        f"{len(filtered)} Wallets | "
        f"Klicke 'Öffnen' in der Profil-Spalte um das Polymarket-Profil zu sehen"
    )
