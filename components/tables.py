"""
Reusable table display components.
"""

import streamlit as st
import pandas as pd


def market_table(markets: list[dict]) -> None:
    """Display markets in a formatted dataframe."""
    if not markets:
        st.info("Keine Märkte geladen.")
        return

    df = pd.DataFrame(markets)
    display_cols = {
        "question": "Markt",
        "yes_price": "YES",
        "no_price": "NO",
        "volume": "Volumen",
        "liquidity": "Liquidität",
        "sentiment_score": "Sentiment",
        "calculated_edge": "Edge",
    }
    available_cols = [c for c in display_cols if c in df.columns]
    df_display = df[available_cols].rename(columns=display_cols)

    # Format prices as percentages
    for col in ["YES", "NO"]:
        if col in df_display.columns:
            df_display[col] = df_display[col].apply(lambda x: f"{x:.1%}" if x else "-")

    # Format volume/liquidity as USD
    for col in ["Volumen", "Liquidität"]:
        if col in df_display.columns:
            df_display[col] = df_display[col].apply(lambda x: f"${x:,.0f}" if x else "-")

    st.dataframe(df_display, use_container_width=True, hide_index=True)


def trades_table(trades: list[dict]) -> None:
    """Display trades history."""
    if not trades:
        st.info("Keine Trades vorhanden.")
        return

    df = pd.DataFrame(trades)
    st.dataframe(df, use_container_width=True, hide_index=True)


def agent_table(agents: list[dict]) -> None:
    """Display agents overview."""
    if not agents:
        st.info("Keine Agents konfiguriert.")
        return

    df = pd.DataFrame(agents)
    st.dataframe(df, use_container_width=True, hide_index=True)
