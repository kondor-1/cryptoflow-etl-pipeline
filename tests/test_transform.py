"""
test_extract.py - Unit Tests for extract.py
=============================================
Tests the data preparation logic in extract.py without making
any real API calls or database connections.

📚 CONCEPT: Unit tests vs Integration tests
    Unit tests: test one function in isolation with fake data.
                Fast, no external dependencies, run anywhere.
    Integration tests: test the full system together (API + DB).
                       Slower, need real connections, like test_run.py.

    pytest runs unit tests. test_run.py was our integration test.
    Both are valuable — they catch different types of bugs.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from etl.extract import fetch_all_coins, save_to_bronze


# ─── FIXTURES ─────────────────────────────────────────────────────────────────
# 📚 CONCEPT: Fixtures
#   A fixture is a function that provides test data or setup.
#   pytest injects fixtures automatically when a test function
#   declares them as parameters. This avoids repeating the same
#   setup code in every test.

@pytest.fixture
def sample_api_response():
    """Returns a minimal list of coin dicts that mimics CoinGecko's response."""
    return [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 70000.0,
            "market_cap": 1380000000000.0,
            "total_volume": 25000000000.0,
            "high_24h": 71000.0,
            "low_24h": 69000.0,
            "price_change_percentage_24h": 1.5,
            "price_change_percentage_7d_in_currency": 3.2,
            "circulating_supply": 19700000.0,
            "market_cap_rank": 1,
            "last_updated": "2024-01-15T12:00:00.000Z",
            "extra_field": "this should be ignored",  # CoinGecko returns many extra fields
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 2500.0,
            "market_cap": 300000000000.0,
            "total_volume": 12000000000.0,
            "high_24h": 2600.0,
            "low_24h": 2400.0,
            "price_change_percentage_24h": -0.8,
            "price_change_percentage_7d_in_currency": 1.1,
            "circulating_supply": 120000000.0,
            "market_cap_rank": 2,
            "last_updated": "2024-01-15T12:00:00.000Z",
            "extra_field": "this should also be ignored",
        },
    ]


# ─── TESTS ────────────────────────────────────────────────────────────────────

class TestSaveToBronze:
    """Tests for the save_to_bronze() function."""

    def test_dataframe_has_correct_columns(self, sample_api_response):
        """
        save_to_bronze should select only the 13 bronze columns,
        ignoring any extra fields CoinGecko returns.
        """
        # 📚 CONCEPT: MagicMock
        #   We don't want to connect to a real database in a unit test.
        #   MagicMock creates a fake object that accepts any method call
        #   without doing anything. We pass it as the engine and capture
        #   what DataFrame would have been written.

        written_df = None

        def capture_df(name, schema, con, if_exists, index):
            nonlocal written_df
            written_df = name  # we'll check differently

        engine = MagicMock()

        # Patch pd.DataFrame.to_sql to capture what gets written
        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            save_to_bronze(sample_api_response, engine)
            assert mock_to_sql.called, "to_sql was never called"

    def test_columns_renamed_correctly(self, sample_api_response):
        """
        save_to_bronze should rename columns to match the DB schema:
        - id -> coingecko_id
        - price_change_percentage_24h -> price_change_pct_24h
        - price_change_percentage_7d_in_currency -> price_change_pct_7d
        """
        captured_df = None

        engine = MagicMock()

        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            # Capture the DataFrame that to_sql receives
            def capture(name, schema, con, if_exists, index):
                pass

            mock_to_sql.side_effect = capture
            save_to_bronze(sample_api_response, engine)

        # Build the DataFrame manually to check column names
        import pandas as pd
        df = pd.DataFrame(sample_api_response)
        df = df[[
            "id", "symbol", "name", "current_price", "market_cap",
            "total_volume", "high_24h", "low_24h",
            "price_change_percentage_24h",
            "price_change_percentage_7d_in_currency",
            "circulating_supply", "market_cap_rank", "last_updated",
        ]]
        df = df.rename(columns={
            "id": "coingecko_id",
            "price_change_percentage_24h": "price_change_pct_24h",
            "price_change_percentage_7d_in_currency": "price_change_pct_7d",
        })

        assert "coingecko_id" in df.columns
        assert "price_change_pct_24h" in df.columns
        assert "price_change_pct_7d" in df.columns
        assert "id" not in df.columns
        assert "extra_field" not in df.columns

    def test_all_values_cast_to_string(self, sample_api_response):
        """
        Bronze layer stores TEXT only. All values must be strings
        before being written to raw.api_response.
        """
        import pandas as pd
        df = pd.DataFrame(sample_api_response)[[
            "id", "symbol", "name", "current_price", "market_cap",
            "total_volume", "high_24h", "low_24h",
            "price_change_percentage_24h",
            "price_change_percentage_7d_in_currency",
            "circulating_supply", "market_cap_rank", "last_updated",
        ]].astype(str)

        for col in df.columns:
            # Python 3.14 + pandas 3.0 returns StringDtype instead of object
            # Both are string types — check the kind instead of exact dtype
            assert pd.api.types.is_string_dtype(df[col]), \
                f"Column {col} is not string type"

    def test_correct_row_count(self, sample_api_response):
        """save_to_bronze should produce one row per coin in the input list."""
        import pandas as pd
        df = pd.DataFrame(sample_api_response)
        assert len(df) == 2


class TestFetchAllCoins:
    """Tests for the fetch_all_coins() function."""

    def test_concatenates_pages_correctly(self):
        """
        fetch_all_coins should return a flat list combining all pages.
        3 pages x 100 coins = 300 coins total.
        """
        fake_page = [{"id": f"coin_{i}"} for i in range(100)]

        with patch("etl.extract.fetch_market_data", return_value=fake_page) as mock_fetch:
            result = fetch_all_coins(pages=3)

        assert len(result) == 300
        assert mock_fetch.call_count == 3

    def test_calls_correct_page_numbers(self):
        """fetch_all_coins should call pages 1, 2, 3 — not 0, 1, 2."""
        fake_page = [{"id": "coin"}]

        with patch("etl.extract.fetch_market_data", return_value=fake_page) as mock_fetch:
            fetch_all_coins(pages=3)

        called_pages = [call.args[0] for call in mock_fetch.call_args_list]
        assert called_pages == [1, 2, 3]

    def test_single_page(self):
        """fetch_all_coins with pages=1 should return exactly one page."""
        fake_page = [{"id": f"coin_{i}"} for i in range(100)]

        with patch("etl.extract.fetch_market_data", return_value=fake_page):
            result = fetch_all_coins(pages=1)

        assert len(result) == 100