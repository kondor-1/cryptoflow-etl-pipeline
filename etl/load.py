"""
load.py - PostgreSQL Load Module
==================================
Single responsibility: write clean DataFrames into the correct
PostgreSQL tables using upsert logic.

No API calls, no transformations here.
Those concerns belong to extract.py and transform.py.

Load order (respects FK dependencies):
    1. staging.market_data_clean
    2. warehouse.dim_date
    3. warehouse.dim_coin
    4. warehouse.fact_market_snapshot
"""

import logging
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _upsert(
    df: pd.DataFrame,
    schema: str,
    table: str,
    conflict_cols: list[str],
    update_cols: list[str],
    engine,
) -> None:
    """
    Generic upsert: INSERT ... ON CONFLICT (conflict_cols) DO UPDATE.

    📚 CONCEPT: Why upsert instead of plain INSERT?

        If the pipeline runs twice on the same day (restart, retry, bug fix),
        a plain INSERT would create duplicate rows. The UNIQUE constraints
        we defined in create_tables.sql prevent that — but they raise an
        exception on conflict instead of handling it gracefully.

        INSERT ... ON CONFLICT DO UPDATE handles this cleanly:
        - First run of the day: inserts new rows
        - Second run of the day: updates existing rows with fresh values
        - Result is always identical regardless of how many times you run it

        This property is called IDEMPOTENCY — a pipeline is idempotent
        if running it N times produces the same result as running it once.
        Idempotency is non-negotiable in production pipelines.

    Args:
        df:             DataFrame to upsert.
        schema:         Target PostgreSQL schema.
        table:          Target PostgreSQL table name.
        conflict_cols:  Columns that define uniqueness (match UNIQUE constraint).
        update_cols:    Columns to overwrite on conflict.
        engine:         SQLAlchemy engine.
    """
    if df.empty:
        logger.warning(f"Empty DataFrame — skipping upsert into {schema}.{table}.")
        return

    # Build the SQL dynamically from the DataFrame columns
    cols        = list(df.columns)
    col_names   = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])
    conflict     = ", ".join(conflict_cols)
    updates      = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = text(f"""
        INSERT INTO {schema}.{table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict})
        DO UPDATE SET {updates}
    """)

    # 📚 CONCEPT: Why chunk the inserts?
    #   Sending 250 rows in one giant SQL statement is fine for this project.
    #   But at scale (millions of rows), one huge statement can exhaust memory
    #   or hit DB statement size limits. Chunking is the safe default habit.

    CHUNK_SIZE = 500
    total = 0

    with engine.begin() as conn:
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE]
            conn.execute(sql, chunk.to_dict(orient="records"))
            total += len(chunk)

    logger.info(f"Upserted {total} rows into {schema}.{table}.")


# ─── PUBLIC LOAD FUNCTIONS ────────────────────────────────────────────────────

def load_silver(clean_df: pd.DataFrame, engine) -> None:
    """
    Load the clean silver DataFrame into staging.market_data_clean.

    Conflict key: (coingecko_id, last_updated) — matches the UNIQUE
    constraint uq_staging_coin_snapshot defined in create_tables.sql.

    Args:
        clean_df: Output of transform.clean_market_data().
        engine:   SQLAlchemy engine.
    """
    _upsert(
        df=clean_df,
        schema="staging",
        table="market_data_clean",
        conflict_cols=["coingecko_id", "last_updated"],
        update_cols=[
            "symbol", "name", "current_price", "market_cap",
            "total_volume", "high_24h", "low_24h",
            "price_change_pct_24h", "price_change_pct_7d",
            "circulating_supply", "market_cap_rank",
        ],
        engine=engine,
    )


def load_dim_date(dim_date_df: pd.DataFrame, engine) -> None:
    """
    Load the date dimension into warehouse.dim_date.

    Conflict key: date_id (YYYYMMDD integer PK).
    On conflict we update all attributes — safe because date attributes
    never actually change for a given date.

    Args:
        dim_date_df: Output of transform.build_dim_date().
        engine:      SQLAlchemy engine.
    """
    _upsert(
        df=dim_date_df,
        schema="warehouse",
        table="dim_date",
        conflict_cols=["date_id"],
        update_cols=["date", "day_of_week", "day_name", "month",
                     "month_name", "quarter", "year", "is_weekend"],
        engine=engine,
    )


def load_dim_coin(dim_coin_df: pd.DataFrame, engine) -> None:
    """
    Load the coin dimension into warehouse.dim_coin.

    Conflict key: coingecko_id (natural key, UNIQUE constraint).
    On conflict we update symbol and name — SCD Type 1, overwrite old values.

    Note: coin_id (surrogate key) is NOT in the DataFrame.
    PostgreSQL generates it via BIGSERIAL on first insert and keeps it
    stable on subsequent upserts (DO UPDATE does not touch the PK).

    Args:
        dim_coin_df: Output of transform.build_dim_coin().
        engine:      SQLAlchemy engine.
    """
    _upsert(
        df=dim_coin_df,
        schema="warehouse",
        table="dim_coin",
        conflict_cols=["coingecko_id"],
        update_cols=["symbol", "name"],
        engine=engine,
    )


def load_fact(fact_df: pd.DataFrame, engine) -> None:
    """
    Load the fact table into warehouse.fact_market_snapshot.

    Conflict key: (date_id, coin_id) — one row per coin per day.
    On conflict we update all measures with the latest values.

    📚 CONCEPT: Why update measures on conflict?
        CoinGecko updates prices continuously. If the pipeline runs
        twice in one day (e.g. after a bug fix), the second run should
        overwrite the first with fresher data — not be silently ignored.
        DO UPDATE SET ensures the most recent run always wins.

    Args:
        fact_df: Output of transform.build_fact_market_snapshot().
        engine:  SQLAlchemy engine.
    """
    _upsert(
        df=fact_df,
        schema="warehouse",
        table="fact_market_snapshot",
        conflict_cols=["date_id", "coin_id"],
        update_cols=[
            "tier_id", "price_usd", "market_cap_usd", "volume_24h_usd",
            "high_24h", "low_24h", "pct_change_24h", "pct_change_7d",
            "circulating_supply", "market_cap_rank",
        ],
        engine=engine,
    )


def load_all(
    clean_df: pd.DataFrame,
    dim_date_df: pd.DataFrame,
    dim_coin_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    engine,
) -> None:
    """
    Run all four load functions in the correct FK dependency order.

    Order matters:
        1. Silver first — fact table references staging indirectly via dates
        2. dim_date — fact table references date_id
        3. dim_coin — fact table references coin_id
        4. fact last — all FKs must exist before inserting fact rows

    Args:
        clean_df:    Silver DataFrame.
        dim_date_df: Date dimension DataFrame.
        dim_coin_df: Coin dimension DataFrame.
        fact_df:     Fact table DataFrame.
        engine:      SQLAlchemy engine.
    """
    logger.info("Starting load sequence...")
    load_silver(clean_df, engine)
    load_dim_date(dim_date_df, engine)
    load_dim_coin(dim_coin_df, engine)
    load_fact(fact_df, engine)
    logger.info("Load sequence complete.")