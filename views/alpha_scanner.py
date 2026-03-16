"""
Alpha Scanner — Find under-the-radar wallets with strong trading performance.
Scans Polymarket leaderboard, enriches wallet data, filters and scores.
"""

import streamlit as st
import pandas as pd

from services.alpha_scanner_service import (
    AlphaScannerService,
    FilterConfig,
    BUILTIN_PRESETS,
    load_all_presets,
    save_custom_preset,
    delete_custom_preset,
    load_watchlist,
    add_to_watchlist,
    remove_from_watchlist,
    load_copy_trades,
    add_copy_trade,
    remove_copy_trade,
    remove_all_copy_trades_for_wallet,
)
from services.data_api_client import DataAPIClient
from components.status_cards import kpi_row


SCAN_DEPTH_MAP = {
    "Schnell (100)": 100,
    "Normal (300)": 300,
    "Tief (500)": 500,
}


def render():
    st.header("Alpha Scanner")

    # --- Session state init ---
    if "alpha_scan_result" not in st.session_state:
        st.session_state.alpha_scan_result = None
    if "alpha_scanning" not in st.session_state:
        st.session_state.alpha_scanning = False

    # --- Tabs: Scanner + Watchlist + Copy Trades ---
    tab_scan, tab_watchlist, tab_copytrades = st.tabs(
        ["Scanner", "Watchlist", "Copy Trades"]
    )

    with tab_scan:
        _render_scanner()

    with tab_watchlist:
        _render_watchlist()

    with tab_copytrades:
        _render_copy_trades()


def _render_scanner():
    """Main scanner tab."""

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
            "SCAN STARTEN" if not st.session_state.alpha_scan_result else "RESCAN",
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

    # --- Presets (built-in + custom) ---
    all_presets = load_all_presets()

    pc1, pc2 = st.columns([4, 1])
    with pc1:
        preset_name = st.selectbox(
            "Filter-Preset",
            list(all_presets.keys()),
            key="alpha_preset",
        )
    with pc2:
        if preset_name not in BUILTIN_PRESETS:
            if st.button("Preset löschen", key="alpha_del_preset"):
                delete_custom_preset(preset_name)
                st.session_state["alpha_preset"] = "Standard"
                st.rerun()

    preset = all_presets.get(preset_name, FilterConfig())

    # --- Quick filters ---
    qc1, qc2, qc3 = st.columns([2, 2, 1])
    with qc1:
        min_pnl = st.slider(
            "Min 7D PnL ($)", 0, 50000, int(preset.min_pnl_7d),
            step=500,
        )
    with qc2:
        max_volume = st.slider(
            "Max Volume ($)", 0, 10000000, int(preset.max_volume),
            step=10000,
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

        # --- Save current filter as custom preset ---
        st.divider()
        sc1, sc2 = st.columns([3, 1])
        with sc1:
            new_preset_name = st.text_input(
                "Preset-Name", placeholder="Mein Filter...",
                key="alpha_new_preset_name",
            )
        with sc2:
            save_preset_clicked = st.button("Preset speichern", key="alpha_save_preset")

    # Build active filter config
    verified_map = {"Egal": "any", "Nur Verified": "verified", "Nur Unverified": "unverified"}
    active_filter = FilterConfig(
        min_pnl_7d=min_pnl,
        max_volume=max_volume,
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
    )

    # Save preset if requested (after active_filter is built)
    if save_preset_clicked:
        pname = st.session_state.get("alpha_new_preset_name", "").strip()
        if pname:
            save_custom_preset(pname, active_filter)
            st.success(f"Preset '{pname}' gespeichert!")
            st.rerun()
        else:
            st.warning("Bitte einen Namen eingeben.")

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
    so1, so2 = st.columns([3, 1])
    with so1:
        sort_col = st.selectbox(
            "Sortieren nach",
            ["Alpha Score", "Radar Score", "7D PnL", "7D ROI %",
             "30D PnL", "Win Rate", "Trades/Day", "Active Pos", "Volume"],
            key="alpha_sort",
        )
    with so2:
        sort_asc = st.toggle("Aufsteigend", False, key="alpha_sort_dir")

    # --- Build dataframe ---
    if not filtered:
        st.warning("Keine Wallets entsprechen den Filtern. Versuche die Filter zu lockern.")
        return

    # Load watchlist for favorite markers
    watchlist = load_watchlist()
    wl_addresses = {w["address"] for w in watchlist}

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
            "Volume": round(w.volume, 0),
            "Alter (Tage)": w.wallet_age_days,
            "Konsistenz": w.consistency_days,
            "Favorit": "\u2b50" if w.address in wl_addresses else "",
            "_address": w.address,
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = "Rank"

    # Display table
    display_cols = [c for c in df.columns if not c.startswith("_")]
    st.dataframe(
        df[display_cols],
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
            "Volume": st.column_config.NumberColumn("Volume", format="$%.0f"),
        },
    )

    # --- Quick Favorite: one-click add/remove per wallet ---
    st.divider()
    st.subheader("Favoriten verwalten")

    # Build wallet list for display
    wallet_list = []
    for _, r in df.iterrows():
        addr = r["_address"]
        is_fav = addr in wl_addresses
        wallet_list.append({
            "name": r["Wallet"],
            "address": addr,
            "is_fav": is_fav,
        })

    # Show in a compact grid: 4 columns per row
    cols_per_row = 4
    for row_start in range(0, min(len(wallet_list), 20), cols_per_row):
        cols = st.columns(cols_per_row)
        for col_idx, winfo in enumerate(wallet_list[row_start:row_start + cols_per_row]):
            with cols[col_idx]:
                if winfo["is_fav"]:
                    if st.button(
                        f"\u2b50 {winfo['name'][:18]}",
                        key=f"unfav_{winfo['address'][:10]}",
                        use_container_width=True,
                    ):
                        remove_from_watchlist(winfo["address"])
                        st.rerun()
                else:
                    if st.button(
                        f"\u2606 {winfo['name'][:18]}",
                        key=f"fav_{winfo['address'][:10]}",
                        use_container_width=True,
                    ):
                        add_to_watchlist(winfo["address"], winfo["name"])
                        st.rerun()

    if len(wallet_list) > 20:
        st.caption(f"... und {len(wallet_list) - 20} weitere Wallets")

    # --- Footer ---
    st.caption(
        f"{len(filtered)} Wallets | "
        f"Klicke \u2606 um ein Wallet als Favorit zu speichern"
    )


def _render_watchlist():
    """Watchlist tab with per-wallet market selection for copy trading."""
    watchlist = load_watchlist()

    if not watchlist:
        st.info(
            "Deine Watchlist ist leer. Scanne Wallets und markiere "
            "interessante Trader als Favorit (\u2606)."
        )
        return

    st.caption(f"{len(watchlist)} Wallets auf der Watchlist")

    copy_trades = load_copy_trades()

    for i, entry in enumerate(watchlist):
        addr = entry["address"]
        name = entry.get("username", addr[:12])

        with st.container():
            # Header row
            wc1, wc2, wc3 = st.columns([4, 2, 1])
            with wc1:
                st.markdown(f"**{name}**")
                note = entry.get("note", "")
                if note:
                    st.caption(f"Notiz: {note}")
            with wc2:
                st.link_button(
                    "Profil öffnen",
                    f"https://polymarket.com/profile/{addr}",
                )
            with wc3:
                if st.button("Entfernen", key=f"wl_rm_{i}"):
                    remove_from_watchlist(addr)
                    remove_all_copy_trades_for_wallet(addr)
                    st.rerun()

            # Expandable: show positions and select markets to copy
            with st.expander(f"Positionen von {name} laden & copy traden"):
                _render_wallet_positions(addr, name, copy_trades)

            st.divider()


def _render_wallet_positions(
    wallet_address: str, wallet_name: str, copy_trades: list[dict]
):
    """Show a wallet's active positions and allow selecting individual markets."""

    # Cache positions in session state to avoid re-fetching on every rerun
    cache_key = f"_wl_positions_{wallet_address[:10]}"

    if st.button("Positionen laden", key=f"load_pos_{wallet_address[:10]}"):
        client = DataAPIClient(timeout=20)
        try:
            positions = client.get_user_positions(wallet_address, limit=50)
            # Keep only active positions (size > 0)
            active = [
                p for p in positions
                if float(p.get("size", 0) or 0) > 0
            ]
            st.session_state[cache_key] = active
        except Exception as e:
            st.error(f"Fehler: {e}")
            return
        finally:
            client.close()

    positions = st.session_state.get(cache_key)
    if not positions:
        st.caption("Klicke 'Positionen laden' um die aktiven Märkte zu sehen.")
        return

    # Get currently tracked condition_ids for this wallet
    tracked_ids = {
        t["condition_id"]
        for t in copy_trades
        if t["wallet_address"] == wallet_address
    }

    st.caption(f"{len(positions)} aktive Positionen")

    for j, pos in enumerate(positions):
        title = pos.get("title", pos.get("slug", "Unbekannter Markt"))
        outcome = pos.get("outcome", "?")
        size = float(pos.get("size", 0) or 0)
        avg_price = float(pos.get("avgPrice", 0) or 0)
        cur_price = float(pos.get("curPrice", 0) or 0)
        cash_pnl = float(pos.get("cashPnl", 0) or 0)
        condition_id = pos.get("conditionId", pos.get("condition_id", ""))

        is_tracked = condition_id in tracked_ids

        pc1, pc2, pc3, pc4 = st.columns([4, 1, 1, 1])
        with pc1:
            pnl_color = "green" if cash_pnl >= 0 else "red"
            st.markdown(
                f"**{title[:60]}** — {outcome}  \n"
                f"Size: {size:.1f} | Avg: {avg_price:.2f} → {cur_price:.2f} | "
                f"PnL: :{pnl_color}[${cash_pnl:.2f}]"
            )
        with pc2:
            st.caption(f"${size * cur_price:.0f}")
        with pc3:
            pct = ((cur_price - avg_price) / avg_price * 100) if avg_price > 0 else 0
            st.caption(f"{pct:+.1f}%")
        with pc4:
            if is_tracked:
                if st.button(
                    "Entfernen",
                    key=f"ct_rm_{wallet_address[:8]}_{j}",
                ):
                    remove_copy_trade(wallet_address, condition_id)
                    st.rerun()
            else:
                if st.button(
                    "Copy Trade",
                    key=f"ct_add_{wallet_address[:8]}_{j}",
                    type="primary",
                ):
                    add_copy_trade(
                        wallet_address=wallet_address,
                        wallet_name=wallet_name,
                        market_title=title,
                        condition_id=condition_id,
                        outcome=outcome,
                        size=size,
                    )
                    st.rerun()


def _render_copy_trades():
    """Copy Trades tab — overview of all selected market positions."""
    trades = load_copy_trades()

    if not trades:
        st.info(
            "Noch keine Copy Trades ausgewählt. Gehe zur Watchlist, "
            "lade die Positionen eines Wallets und wähle einzelne Märkte aus."
        )
        return

    st.caption(f"{len(trades)} Märkte zum Copy Traden ausgewählt")

    # Group by wallet
    by_wallet: dict[str, list[dict]] = {}
    for t in trades:
        key = t.get("wallet_name", t["wallet_address"][:12])
        by_wallet.setdefault(key, []).append(t)

    for wallet_name, wallet_trades in by_wallet.items():
        st.subheader(wallet_name)

        for k, t in enumerate(wallet_trades):
            tc1, tc2, tc3, tc4 = st.columns([5, 1, 1, 1])
            with tc1:
                st.markdown(f"**{t['market_title'][:70]}**")
                st.caption(f"Outcome: {t['outcome']} | Size: {t.get('size', 0):.1f}")
            with tc2:
                added = t.get("added_at", "")[:10]
                st.caption(f"Seit: {added}")
            with tc3:
                st.link_button(
                    "Profil",
                    f"https://polymarket.com/profile/{t['wallet_address']}",
                )
            with tc4:
                if st.button(
                    "Entfernen",
                    key=f"ctrm_{t['wallet_address'][:8]}_{k}",
                ):
                    remove_copy_trade(t["wallet_address"], t["condition_id"])
                    st.rerun()

        st.divider()
