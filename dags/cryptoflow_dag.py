"""
cryptoflow_dag.py - Airflow DAG
================================
Orchestrates the full CryptoFlow ELT pipeline on a daily schedule.

Task dependency chain:
    extract_to_bronze
        >> transform_to_silver
        >> load_silver
        >> load_dimensions
        >> build_and_load_fact
        >> validate_pipeline

📚 CONCEPT: Why Airflow?
    A plain Python script runs once and exits. Airflow adds:
    - Scheduling     — run automatically every day at a set time
    - Retries        — if a task fails, retry N times before alerting
    - Visibility     — UI shows every run, every task, every log
    - Dependencies   — tasks only run when upstream tasks succeed
    - History        — full audit trail of every pipeline execution
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.extract import fetch_all_coins, save_to_bronze
from etl.transform import (
    clean_market_data,
    build_dim_date,
    build_dim_coin,
    build_fact_market_snapshot,
)
from etl.load import load_silver, load_dim_date, load_dim_coin, load_fact

logger = logging.getLogger(__name__)

# ─── DATABASE CONNECTION ───────────────────────────────────────────────────────
# 📚 CONCEPT: Why define the engine inside each task function?
#   Airflow workers can run tasks in separate processes or even separate
#   machines. A SQLAlchemy engine created at module level would not be
#   safely shared across processes. Creating it inside each task function
#   guarantees a fresh, clean connection every time.

import os
from dotenv import load_dotenv
load_dotenv()

def _get_engine():
    """Create and return a SQLAlchemy engine from environment variables."""
    db_user     = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    db_host     = os.getenv("DB_HOST", "localhost")
    db_port     = os.getenv("DB_PORT", "5432")
    db_name     = os.getenv("DB_NAME", "cryptoflow")
    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


# ─── DAG DEFAULT ARGUMENTS ────────────────────────────────────────────────────
# 📚 CONCEPT: default_args
#   These settings apply to every task in the DAG unless overridden.
#   retries=2 means each task gets 2 retry attempts before marking as failed.
#   retry_delay=5min means Airflow waits 5 minutes between retry attempts.
#   This gives transient issues (network blip, DB briefly unavailable) time
#   to resolve before the next attempt.

default_args = {
    "owner":           "cryptoflow",
    "depends_on_past": False,           # Each run is independent
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,          # Set to True + add email when in production
    "email_on_retry":   False,
}


# ─── TASK FUNCTIONS ───────────────────────────────────────────────────────────
# 📚 CONCEPT: PythonOperator tasks
#   Each task is a plain Python function. Airflow wraps it in a PythonOperator
#   which handles scheduling, retries, and logging automatically.
#   Tasks communicate via XCom (Airflow's key-value store) when needed,
#   but for simplicity this pipeline reads from the DB between tasks
#   instead of passing DataFrames through XCom.

def task_extract_to_bronze(**context):
    """
    Task 1: Fetch all coins from CoinGecko and save to raw.api_response.
    """
    engine = _get_engine()
    coins = fetch_all_coins()
    save_to_bronze(coins, engine)
    logger.info(f"Bronze: {len(coins)} coins saved.")


def task_transform_and_load_silver(**context):
    """
    Task 2: Read bronze, clean data, load into staging.market_data_clean.

    Reads from raw.api_response so the task is self-contained and
    can be retried independently without re-calling the API.
    """
    engine = _get_engine()

    # Read today's bronze records only
    # 📚 We filter by today to avoid reprocessing old data on a retry
    raw_df = pd.read_sql(
        """
        SELECT * FROM raw.api_response
        WHERE ingested_at >= CURRENT_DATE
        """,
        con=engine,
    )

    if raw_df.empty:
        raise ValueError("No bronze records found for today. Extract task may have failed.")

    clean_df = clean_market_data(raw_df)
    load_silver(clean_df, engine)


def task_load_dimensions(**context):
    """
    Task 3: Build and load dim_date and dim_coin from silver data.

    Must run AFTER load_silver so staging.market_data_clean has today's data.
    Must run BEFORE build_and_load_fact so coin_id lookups succeed.
    """
    engine = _get_engine()

    clean_df = pd.read_sql(
        """
        SELECT * FROM staging.market_data_clean
        WHERE ingested_at >= CURRENT_DATE
        """,
        con=engine,
    )

    dim_date_df = build_dim_date(clean_df)
    dim_coin_df = build_dim_coin(clean_df)

    load_dim_date(dim_date_df, engine)
    load_dim_coin(dim_coin_df, engine)


def task_build_and_load_fact(**context):
    """
    Task 4: Build and load fact_market_snapshot.

    Must run AFTER load_dimensions so coin_id surrogate keys exist in dim_coin.
    """
    engine = _get_engine()

    clean_df = pd.read_sql(
        """
        SELECT * FROM staging.market_data_clean
        WHERE ingested_at >= CURRENT_DATE
        """,
        con=engine,
    )

    fact_df = build_fact_market_snapshot(clean_df, engine)
    load_fact(fact_df, engine)


def task_validate_pipeline(**context):
    """
    Task 5: Verify all tables have data for today. Raises if any table is empty.

    📚 CONCEPT: Pipeline validation task
        A validation task at the end of the DAG is a professional pattern.
        It catches silent failures — cases where a task succeeded technically
        but loaded 0 rows. Without this, you would not know the pipeline
        produced no data until an analyst complains.
    """
    engine = _get_engine()

    checks = {
        "raw.api_response":                "ingested_at >= CURRENT_DATE",
        "staging.market_data_clean":       "ingested_at >= CURRENT_DATE",
        "warehouse.dim_coin":              "1=1",
        "warehouse.fact_market_snapshot":  "ingested_at >= CURRENT_DATE",
    }

    with engine.connect() as conn:
        for table, condition in checks.items():
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
            ).scalar()

            if count == 0:
                raise ValueError(f"Validation failed: {table} has 0 rows for today.")

            logger.info(f"Validation passed: {table} has {count} rows.")

    logger.info("All validation checks passed. Pipeline is healthy.")


# ─── DAG DEFINITION ───────────────────────────────────────────────────────────

with DAG(
    dag_id="cryptoflow_daily",          # Must be unique across all DAGs
    default_args=default_args,
    description="Daily ELT pipeline: CoinGecko API -> PostgreSQL star schema",
    schedule_interval="@daily",         # Runs once per day at midnight UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,                      # 📚 catchup=False: do NOT backfill all
                                        # missed runs since start_date.
                                        # Without this, Airflow would try to run
                                        # the pipeline for every day since Jan 2025
                                        # the first time you deploy it.
    tags=["cryptoflow", "crypto", "etl"],
) as dag:

    # ── Define tasks ──────────────────────────────────────────────────────────
    extract_to_bronze = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=task_extract_to_bronze,
    )

    transform_and_load_silver = PythonOperator(
        task_id="transform_and_load_silver",
        python_callable=task_transform_and_load_silver,
    )

    load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions,
    )

    build_and_load_fact = PythonOperator(
        task_id="build_and_load_fact",
        python_callable=task_build_and_load_fact,
    )

    validate_pipeline = PythonOperator(
        task_id="validate_pipeline",
        python_callable=task_validate_pipeline,
    )

    # ── Define dependencies ───────────────────────────────────────────────────
    # 📚 CONCEPT: The >> operator
    #   task_a >> task_b means "task_b only runs after task_a succeeds".
    #   This is the core of Airflow's DAG — a directed acyclic graph of
    #   task dependencies. If any task fails, all downstream tasks are skipped.

    (
        extract_to_bronze
        >> transform_and_load_silver
        >> load_dimensions
        >> build_and_load_fact
        >> validate_pipeline
    )