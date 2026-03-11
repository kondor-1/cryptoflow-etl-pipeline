"""
app.py - CryptoFlow Dashboard
==============================
Streamlit dashboard connected to the cryptoflow PostgreSQL warehouse.
Reads from warehouse.fact_market_snapshot and warehouse.dim_coin.

Pages:
    1. Top 10 by Market Cap
    2. Gainers & Losers (24h)
    3. Historical Price (BTC & ETH)
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from datetime import date

# ─── CONFIG ───────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="CryptoFlow Dashboard",
    page_icon="📊",
    layout="wide",
)

# ─── DATABASE CONNECTION ──────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    """
    Create a cached SQLAlchemy engine.

    📚 @st.cache_resource caches the engine across all user sessions.
    Without this, Streamlit would create a new DB connection on every
    page interaction — expensive and unnecessary.
    """
    db_user     = os.getenv("DB_USER", "cryptoflow")
    db_password = os.getenv("DB_PASSWORD", "cryptoflow")
    db_host     = os.getenv("DB_HOST", "cryptoflow-db")
    db_port     = os.getenv("DB_PORT", "5432")
    db_name     = os.getenv("DB_NAME", "cryptoflow")
    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


@st.cache_data(ttl=300)
def load_latest_snapshot():
    """
    Load the most recent day's fact data joined with dim_coin.
    Cached for 5 minutes (ttl=300) — no need to re-query on every click.
    """
    engine = get_engine()
    query = """
        SELECT
            c.name,
            c.symbol,
            f.price_usd,
            f.market_cap_usd,
            f.volume_24h_usd,
            f.pct_change_24h,
            f.pct_change_7d,
            f.market_cap_rank,
            f.date_id
        FROM warehouse.fact_market_snapshot f
        JOIN warehouse.dim_coin c ON f.coin_id = c.coin_id
        WHERE f.date_id = (
            SELECT MAX(date_id) FROM warehouse.fact_market_snapshot
        )
        ORDER BY f.market_cap_rank ASC
    """
    return pd.read_sql(query, con=engine)


@st.cache_data(ttl=300)
def load_historical_prices():
    """
    Load daily price history for Bitcoin and Ethereum.
    """
    engine = get_engine()
    query = """
        SELECT
            c.name,
            c.symbol,
            f.price_usd,
            f.date_id,
            CAST(CAST(f.date_id AS TEXT) AS DATE) AS date
        FROM warehouse.fact_market_snapshot f
        JOIN warehouse.dim_coin c ON f.coin_id = c.coin_id
        WHERE c.coingecko_id IN ('bitcoin', 'ethereum')
        ORDER BY f.date_id ASC
    """
    return pd.read_sql(query, con=engine)


# ─── HEADER ───────────────────────────────────────────────────────────────────

st.title("📊 CryptoFlow Dashboard")
st.caption("Daily crypto market data — powered by CoinGecko API + PostgreSQL")

# Load data
try:
    df = load_latest_snapshot()
    hist_df = load_historical_prices()
except Exception as e:
    st.error(f"Could not connect to the database: {e}")
    st.info("Make sure the pipeline has run at least once and the database has data.")
    st.stop()

if df.empty:
    st.warning("No data found. Run the Airflow pipeline first.")
    st.stop()

# Date badge
latest_date = str(df["date_id"].iloc[0])
formatted_date = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"
st.markdown(f"**Latest data:** {formatted_date}  |  **Coins tracked:** {len(df)}")

st.divider()

# ─── CHART 1: TOP 10 BY MARKET CAP ───────────────────────────────────────────

st.subheader("🏆 Top 10 by Market Cap")

top10 = df.head(10).copy()
top10["market_cap_b"] = top10["market_cap_usd"] / 1e9  # convert to billions
top10["label"] = top10["name"] + " (" + top10["symbol"].str.upper() + ")"

fig1 = px.bar(
    top10.sort_values("market_cap_b"),
    x="market_cap_b",
    y="label",
    orientation="h",
    text=top10.sort_values("market_cap_b")["price_usd"].apply(
        lambda x: f"${x:,.0f}"
    ),
    color="market_cap_b",
    color_continuous_scale="teal",
    labels={"market_cap_b": "Market Cap (USD Billions)", "label": ""},
)
fig1.update_traces(textposition="outside")
fig1.update_layout(
    coloraxis_showscale=False,
    height=420,
    margin=dict(l=10, r=80, t=20, b=20),
    yaxis=dict(tickfont=dict(size=13)),
)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ─── CHART 2: GAINERS & LOSERS ────────────────────────────────────────────────

st.subheader("📈 Top 5 Gainers & Losers (24h)")

col1, col2 = st.columns(2)

gainers = df.nlargest(5, "pct_change_24h")[["name", "symbol", "pct_change_24h", "price_usd"]].copy()
losers  = df.nsmallest(5, "pct_change_24h")[["name", "symbol", "pct_change_24h", "price_usd"]].copy()

gainers["label"] = gainers["name"] + " (" + gainers["symbol"].str.upper() + ")"
losers["label"]  = losers["name"]  + " (" + losers["symbol"].str.upper() + ")"

with col1:
    st.markdown("**🟢 Gainers**")
    fig_gain = px.bar(
        gainers.sort_values("pct_change_24h"),
        x="pct_change_24h",
        y="label",
        orientation="h",
        text=gainers.sort_values("pct_change_24h")["pct_change_24h"].apply(
            lambda x: f"+{x:.2f}%"
        ),
        color="pct_change_24h",
        color_continuous_scale="Greens",
        labels={"pct_change_24h": "24h Change (%)", "label": ""},
    )
    fig_gain.update_traces(textposition="outside")
    fig_gain.update_layout(
        coloraxis_showscale=False,
        height=320,
        margin=dict(l=10, r=60, t=10, b=20),
    )
    st.plotly_chart(fig_gain, use_container_width=True)

with col2:
    st.markdown("**🔴 Losers**")
    fig_loss = px.bar(
        losers.sort_values("pct_change_24h", ascending=False),
        x="pct_change_24h",
        y="label",
        orientation="h",
        text=losers.sort_values("pct_change_24h", ascending=False)["pct_change_24h"].apply(
            lambda x: f"{x:.2f}%"
        ),
        color="pct_change_24h",
        color_continuous_scale="Reds_r",
        labels={"pct_change_24h": "24h Change (%)", "label": ""},
    )
    fig_loss.update_traces(textposition="outside")
    fig_loss.update_layout(
        coloraxis_showscale=False,
        height=320,
        margin=dict(l=10, r=60, t=10, b=20),
    )
    st.plotly_chart(fig_loss, use_container_width=True)

st.divider()

# ─── CHART 3: HISTORICAL PRICE ────────────────────────────────────────────────

st.subheader("📉 Bitcoin & Ethereum — Historical Price (USD)")

if hist_df.empty or len(hist_df["date_id"].unique()) < 2:
    st.info("Not enough historical data yet. The chart will populate after a few days of pipeline runs.")
else:
    fig3 = px.line(
        hist_df,
        x="date",
        y="price_usd",
        color="name",
        color_discrete_map={"Bitcoin": "#F7931A", "Ethereum": "#627EEA"},
        labels={"price_usd": "Price (USD)", "date": "Date", "name": ""},
        markers=True,
    )
    fig3.update_layout(
        height=400,
        margin=dict(l=10, r=10, t=20, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(tickprefix="$", tickformat=",.0f"),
    )
    st.plotly_chart(fig3, use_container_width=True)

# ─── FOOTER ───────────────────────────────────────────────────────────────────

st.divider()
st.caption("CryptoFlow ETL Pipeline · github.com/kondor-1/cryptoflow-etl-pipeline · Data source: CoinGecko API")