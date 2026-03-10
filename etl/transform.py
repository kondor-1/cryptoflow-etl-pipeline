"""
transform.py - Data Transformation Module
==========================================
Single responsibility: take raw data from the bronze layer and produce
clean, typed DataFrames ready to load into the silver and gold layers.

No API calls, no database writes here.
Those concerns belong to extract.py and load.py.

Output DataFrames (in load order):
    1. clean_df      -> staging.market_data_clean  (Silver)
    2. dim_date_df   -> warehouse.dim_date          (Gold)
    3. dim_coin_df   -> warehouse.dim_coin          (Gold)
    4. fact_df       -> warehouse.fact_market_snapshot (Gold)
"""

import logging
import pandas as pd
import numpy as np
from datetime import date, timezone

logger = logging.getLogger(__name__)

# ─── BRONZE → SILVER ──────────────────────────────────────────────────────────

def clean_market_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw bronze data into a clean, typed silver DataFrame.

    Takes the TEXT-only bronze DataFrame and applies:
    - Null handling (string "None" -> actual NaN -> filled or dropped)
    - Type casting (TEXT -> NUMERIC, TIMESTAMPTZ, INTEGER)
    - Deduplication on (coingecko_id, last_updated)

    Args:
        raw_df: DataFrame from raw.api_response — all TEXT columns.

    Returns:
        Clean DataFrame matching the staging.market_data_clean schema.
    """
    # 📚 CONCEPT: Always work on a copy
    #   Never mutate the input DataFrame. If something goes wrong downstream,
    #   the caller still has the original data intact.
    df = raw_df.copy()

    # ── Step 1: Replace string "None" and "nan" with actual NaN ───────────────
    # Bronze stores everything as TEXT including Python's str(None) = "None".
    # We need real NaN values so pandas handles nulls correctly.
    df = df.replace({"None": np.nan, "nan": np.nan, "": np.nan})

    # ── Step 2: Drop rows with no coingecko_id or price ───────────────────────
    # A row without an ID or price is useless — we cannot identify the coin
    # or build any meaningful metric from it.
    before = len(df)
    df = df.dropna(subset=["coingecko_id", "current_price"])
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with null coingecko_id or current_price.")

    # ── Step 3: Cast numeric columns ──────────────────────────────────────────
    # 📚 CONCEPT: Why errors="coerce"?
    #   If a value cannot be converted to a number (e.g. "N/A"),
    #   errors="coerce" turns it into NaN instead of raising an exception.
    #   This keeps the pipeline running — we log the issue and move on.
    #   A pipeline that crashes on one bad value is worse than one that
    #   records NaN and keeps processing.

    NUMERIC_COLS = {
        "current_price":        "float64",
        "market_cap":           "float64",
        "total_volume":         "float64",
        "high_24h":             "float64",
        "low_24h":              "float64",
        "price_change_pct_24h": "float64",
        "price_change_pct_7d":  "float64",
        "circulating_supply":   "float64",
        "market_cap_rank":      "Int64",   # Int64 (capital I) supports NaN; int64 does not
    }

    for col, dtype in NUMERIC_COLS.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    # ── Step 4: Parse last_updated timestamp ──────────────────────────────────
    # CoinGecko returns ISO 8601 strings like "2024-01-15T12:34:56.789Z"
    # utc=True ensures the result is timezone-aware (TIMESTAMPTZ in PostgreSQL)
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True, errors="coerce")

    # ── Step 5: Deduplicate ───────────────────────────────────────────────────
    # The UNIQUE constraint on staging is (coingecko_id, last_updated).
    # If we somehow fetched the same snapshot twice, keep only the first.
    before = len(df)
    df = df.drop_duplicates(subset=["coingecko_id", "last_updated"], keep="first")
    dupes = before - len(df)
    if dupes > 0:
        logger.warning(f"Removed {dupes} duplicate (coingecko_id, last_updated) rows.")

    # ── Step 6: Select and order final columns ────────────────────────────────
    SILVER_COLS = [
        "coingecko_id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_pct_24h",
        "price_change_pct_7d",
        "circulating_supply",
        "market_cap_rank",
        "last_updated",
    ]

    df = df[SILVER_COLS].reset_index(drop=True)

    logger.info(f"Silver layer: {len(df)} clean rows ready for staging.")
    return df


# ─── SILVER → GOLD (DIMENSIONS) ───────────────────────────────────────────────

def build_dim_date(clean_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the dim_date DataFrame from the last_updated timestamps in clean_df.

    📚 CONCEPT: The date dimension
        Rather than storing raw dates in the fact table and writing
        complex date functions in every query, we pre-compute all date
        attributes once here. Analysts can then filter by is_weekend,
        month_name, or quarter without any date arithmetic.

    Args:
        clean_df: Silver DataFrame from clean_market_data().

    Returns:
        DataFrame matching warehouse.dim_date schema.
        date_id uses YYYYMMDD integer format (e.g. 20240115).
    """
    # Extract unique dates from the last_updated timestamps
    unique_dates = clean_df["last_updated"].dt.date.unique()

    rows = []
    for d in unique_dates:
        pd_date = pd.Timestamp(d)
        rows.append({
            "date_id":     int(pd_date.strftime("%Y%m%d")),  # e.g. 20240115
            "date":        d,
            "day_of_week": pd_date.dayofweek,                # 0=Monday, 6=Sunday
            "day_name":    pd_date.strftime("%A"),            # "Monday", "Tuesday"...
            "month":       pd_date.month,
            "month_name":  pd_date.strftime("%B"),            # "January", "February"...
            "quarter":     pd_date.quarter,
            "year":        pd_date.year,
            "is_weekend":  pd_date.dayofweek >= 5,            # True for Sat/Sun
        })

    dim_date_df = pd.DataFrame(rows)
    logger.info(f"dim_date: {len(dim_date_df)} date rows built.")
    return dim_date_df


def build_dim_coin(clean_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the dim_coin DataFrame from the clean silver data.

    📚 CONCEPT: SCD Type 1
        If a coin's name or symbol changes, we overwrite the old value.
        We don't track history of what the coin used to be called.
        The upsert in load.py handles this via ON CONFLICT DO UPDATE.

    Args:
        clean_df: Silver DataFrame from clean_market_data().

    Returns:
        DataFrame matching warehouse.dim_coin schema (no surrogate key —
        that is generated by PostgreSQL's BIGSERIAL on insert).
    """
    dim_coin_df = (
        clean_df[["coingecko_id", "symbol", "name"]]
        .drop_duplicates(subset=["coingecko_id"])
        .reset_index(drop=True)
    )

    logger.info(f"dim_coin: {len(dim_coin_df)} coin rows built.")
    return dim_coin_df


# ─── SILVER → GOLD (FACT TABLE) ───────────────────────────────────────────────

def _get_tier_id(rank) -> int:
    """
    Map a market cap rank integer to a tier_id (1-4).

    Matches the seed data in sql/seed_dimensions.sql:
        1 = Mega Cap   (rank 1-10)
        2 = Large Cap  (rank 11-50)
        3 = Mid Cap    (rank 51-150)
        4 = Small Cap  (rank 151-250)

    Args:
        rank: market_cap_rank integer. May be NaN.

    Returns:
        tier_id integer (1-4). Returns 4 if rank is null or out of range.
    """
    try:
        rank = int(rank)
        if rank <= 10:
            return 1
        elif rank <= 50:
            return 2
        elif rank <= 150:
            return 3
        else:
            return 4
    except (ValueError, TypeError):
        return 4  # default to Small Cap if rank is null


def build_fact_market_snapshot(
    clean_df: pd.DataFrame,
    engine,
) -> pd.DataFrame:
    """
    Build the fact_market_snapshot DataFrame.

    📚 CONCEPT: The fact table only stores FKs and measures.
        - date_id: looked up from dim_date (YYYYMMDD integer)
        - coin_id: looked up from dim_coin (surrogate key from DB)
        - tier_id: derived from market_cap_rank via _get_tier_id()
        - All numeric measures come directly from clean_df

    Args:
        clean_df: Silver DataFrame from clean_market_data().
        engine:   SQLAlchemy engine — needed to look up coin_id surrogate keys.

    Returns:
        DataFrame matching warehouse.fact_market_snapshot schema.
    """
    # ── Step 1: Look up coin_id surrogate keys from dim_coin ──────────────────
    # 📚 CONCEPT: Why look up from the DB instead of computing locally?
    #   The surrogate key (coin_id) is generated by PostgreSQL's BIGSERIAL.
    #   We don't control what number it assigns — we have to ask the DB
    #   "what coin_id did you give to 'bitcoin'?" after loading dim_coin.

    coin_lookup = pd.read_sql(
        "SELECT coin_id, coingecko_id FROM warehouse.dim_coin",
        con=engine,
    )

    df = clean_df.merge(coin_lookup, on="coingecko_id", how="left")

    missing_coins = df["coin_id"].isna().sum()
    if missing_coins > 0:
        logger.warning(f"{missing_coins} coins not found in dim_coin — they will be skipped.")
        df = df.dropna(subset=["coin_id"])

    # ── Step 2: Build date_id from last_updated ────────────────────────────────
    df["date_id"] = df["last_updated"].dt.strftime("%Y%m%d").astype(int)

    # ── Step 3: Derive tier_id from market_cap_rank ────────────────────────────
    df["tier_id"] = df["market_cap_rank"].apply(_get_tier_id)

    # ── Step 4: Select and rename columns to match fact table schema ───────────
    fact_df = df[[
        "date_id",
        "coin_id",
        "tier_id",
        "current_price",
        "market_cap",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_pct_24h",
        "price_change_pct_7d",
        "circulating_supply",
        "market_cap_rank",
    ]].rename(columns={
        "current_price":        "price_usd",
        "market_cap":           "market_cap_usd",
        "total_volume":         "volume_24h_usd",
        "price_change_pct_24h": "pct_change_24h",
        "price_change_pct_7d":  "pct_change_7d",
    })

    fact_df["coin_id"] = fact_df["coin_id"].astype(int)

    logger.info(f"fact_market_snapshot: {len(fact_df)} rows built.")
    return fact_df