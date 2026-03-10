"""
extract.py - CoinGecko API Client
==================================
Single responsibility: fetch raw market data from the CoinGecko API
and persist it to the bronze layer (raw.api_response).

No cleaning, no type casting, no business logic here.
Those concerns belong to transform.py.
"""

import os
import logging
from datetime import datetime, timezone

import requests
import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from dotenv import load_dotenv

load_dotenv()

# ─── CONSTANTS ────────────────────────────────────────────────────────────────
BASE_URL        = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
COINS_PER_PAGE  = int(os.getenv("COINS_PER_PAGE", 100))
TOTAL_PAGES     = int(os.getenv("TOTAL_PAGES", 3))
REQUEST_TIMEOUT = 30  # seconds

# ─── LOGGING ──────────────────────────────────────────────────────────────────
# 📚 CONCEPT: Logging vs print()
#   print() is for scripts. logging is for pipelines.
#   Logging gives timestamps, severity levels (INFO/WARNING/ERROR),
#   and Airflow captures it automatically — print() gets swallowed.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── RETRY DECORATOR ──────────────────────────────────────────────────────────
# 📚 CONCEPT: Exponential Backoff
#   APIs rate-limit or temporarily fail. A naive retry immediately hammers
#   the server again — making things worse. Exponential backoff waits
#   progressively longer between attempts:
#       attempt 1 fails: wait 2s
#       attempt 2 fails: wait 4s
#       attempt 3 fails: wait 8s  (up to max 60s)
#
#   tenacity implements this cleanly as a decorator.

@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_market_data(page: int) -> list[dict]:
    """
    Fetch one page of market data from the CoinGecko /coins/markets endpoint.

    Args:
        page: Page number to fetch (1-indexed). Each page returns COINS_PER_PAGE
              coins ordered by market cap descending.

    Returns:
        List of raw coin dicts as returned by the CoinGecko API.

    Raises:
        requests.HTTPError: If the API returns a non-2xx status after all retries.
        ValueError: If the API returns an empty list.
    """
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency":             "usd",
        "order":                   "market_cap_desc",
        "per_page":                COINS_PER_PAGE,
        "page":                    page,
        "sparkline":               False,
        "price_change_percentage": "24h,7d",
    }

    logger.info(f"Fetching page {page} from CoinGecko...")
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

    # 📚 raise_for_status() converts HTTP error codes (4xx, 5xx) into Python
    # exceptions. Without this, a 429 or 500 would return silently and we
    # would try to parse an error response as valid data.
    response.raise_for_status()

    data = response.json()

    if not data:
        raise ValueError(f"CoinGecko returned an empty response for page {page}.")

    logger.info(f"Page {page}: received {len(data)} coins.")
    return data


def fetch_all_coins(pages: int = TOTAL_PAGES) -> list[dict]:
    """
    Fetch market data for the top N pages of coins from CoinGecko.

    Paginates through the API and concatenates all results into a single list.
    Each page is fetched with retry logic via fetch_market_data().

    Args:
        pages: Number of pages to fetch. Defaults to TOTAL_PAGES env var (3).

    Returns:
        Flat list of raw coin dicts. Length = pages * COINS_PER_PAGE.
    """
    all_coins = []

    for page in range(1, pages + 1):
        coins = fetch_market_data(page)
        all_coins.extend(coins)

    logger.info(f"Extraction complete. Total coins fetched: {len(all_coins)}")
    return all_coins


def save_to_bronze(data: list[dict], engine) -> None:
    """
    Persist raw API response data to the bronze layer (raw.api_response).

    All values are stored as strings — no type casting at this stage.
    ingested_at is set by the DB default (NOW()), not by Python.

    Args:
        data:   List of raw coin dicts from fetch_all_coins().
        engine: SQLAlchemy engine connected to the cryptoflow database.
    """
    # 📚 CONCEPT: Why flatten to a DataFrame first?
    #   CoinGecko returns ~30 fields per coin. We only defined 13 columns
    #   in raw.api_response. DataFrame column selection is the cleanest
    #   way to project exactly the columns we want before writing to the DB.

    BRONZE_COLUMNS = [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_percentage_24h",
        "price_change_percentage_7d_in_currency",
        "circulating_supply",
        "market_cap_rank",
        "last_updated",
    ]

    df = pd.DataFrame(data)[BRONZE_COLUMNS]

    df = df.rename(columns={
        "id":                                     "coingecko_id",
        "price_change_percentage_24h":            "price_change_pct_24h",
        "price_change_percentage_7d_in_currency": "price_change_pct_7d",
    })

    # Cast everything to string. Bronze stores TEXT only, no exceptions.
    # Nulls become the string "None" — transform.py handles them properly.
    df = df.astype(str)

    # 📚 if_exists="append" adds rows without touching existing ones.
    #    Never use "replace" in a pipeline — it destroys your audit trail.
    df.to_sql(
        name="api_response",
        schema="raw",
        con=engine,
        if_exists="append",
        index=False,
    )

    logger.info(f"Bronze layer: inserted {len(df)} rows into raw.api_response.")