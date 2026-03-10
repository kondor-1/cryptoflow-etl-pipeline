# Naming Conventions

This document defines the naming standards for every artifact in the CryptoFlow
project — SQL objects, Python code, files, Git branches, and Airflow components.

All contributors (including future you) must follow these conventions. Consistent
naming is not cosmetic: it makes code predictable, searchable, and easier to
review. Recruiters and senior engineers notice it immediately.

---

## Table of Contents

1. [SQL Conventions](#1-sql-conventions)
2. [Python Conventions](#2-python-conventions)
3. [File & Directory Conventions](#3-file--directory-conventions)
4. [Airflow Conventions](#4-airflow-conventions)
5. [Git Conventions](#5-git-conventions)
6. [Environment Variables](#6-environment-variables)
7. [Quick Reference Cheatsheet](#7-quick-reference-cheatsheet)

---

## 1. SQL Conventions

### 1.1 Keywords

All SQL reserved words are written in **UPPERCASE**. Everything you define
(table names, column names, aliases) is written in **lowercase snake_case**.

```sql
-- ✅ Correct
SELECT coin_id, price_usd FROM warehouse.fact_market_snapshot WHERE date_id = 20240115;

-- ❌ Wrong
select Coin_ID, Price_USD from warehouse.Fact_Market_Snapshot where Date_ID = 20240115;
```

### 1.2 Schemas

Schemas use lowercase single-word names that describe the medallion layer they represent.

| Schema      | Layer  | Purpose                          |
|-------------|--------|----------------------------------|
| `raw`       | Bronze | Untransformed API data           |
| `staging`   | Silver | Cleaned and validated data       |
| `warehouse` | Gold   | Dimensional model for analytics  |

### 1.3 Table Names

Tables use `snake_case`. Prefixes encode the table's role in the data model:

| Table Type      | Prefix   | Example                              |
|-----------------|----------|--------------------------------------|
| Raw/bronze      | none     | `raw.api_response`                   |
| Staging/silver  | none     | `staging.market_data_clean`          |
| Dimension       | `dim_`   | `warehouse.dim_coin`                 |
| Fact            | `fact_`  | `warehouse.fact_market_snapshot`     |

**Rules:**
- Always prefix dimension tables with `dim_`
- Always prefix fact tables with `fact_`
- Use singular nouns, not plural (`dim_coin` not `dim_coins`)
- Be descriptive — `fact_market_snapshot` beats `fact_data`

```sql
-- ✅ Correct
warehouse.dim_coin
warehouse.fact_market_snapshot

-- ❌ Wrong
warehouse.Coins
warehouse.dimCoin
warehouse.fact_data
```

### 1.4 Column Names

Columns use `snake_case`. The naming pattern depends on the column's role:

#### Primary Keys
- **Raw/staging tables:** `id` (technical surrogate, no business meaning)
- **Warehouse dimension tables:** `{table_name_singular}_id`
- **Warehouse fact tables:** `{table_name_singular}_id`
- **Exception — dim_date:** uses `date_id` in YYYYMMDD integer format

```sql
-- ✅ Correct
raw.api_response          → id
staging.market_data_clean → id
warehouse.dim_coin        → coin_id
warehouse.dim_date        → date_id   (format: YYYYMMDD)
warehouse.fact_market_snapshot → snapshot_id
```

#### Foreign Keys
Foreign keys use the **exact same name** as the primary key they reference.
This makes the relationship self-documenting.

```sql
-- ✅ Correct — FK name matches the PK it references
fact_market_snapshot.coin_id  references dim_coin.coin_id
fact_market_snapshot.date_id  references dim_date.date_id

-- ❌ Wrong — ambiguous name
fact_market_snapshot.fk_coin
fact_market_snapshot.coin_ref
```

#### Boolean Columns
Always prefix with `is_`, `has_`, or `was_`.

```sql
-- ✅ Correct
is_weekend
is_essential
has_price_data

-- ❌ Wrong
weekend
essential
price_data_exists
```

#### Timestamp Columns
- `ingested_at` — when the pipeline loaded the row (always `TIMESTAMPTZ`)
- `updated_at` — when the row was last updated
- `{event}_at` — when a specific event occurred

#### Percentage / Ratio Columns
Include the unit in the name to prevent ambiguity.

```sql
-- ✅ Correct — unit is clear
pct_change_24h      -- percent
volume_24h_usd      -- USD
market_cap_usd      -- USD
high_low_ratio      -- dimensionless ratio

-- ❌ Wrong — ambiguous
change_24h          -- change in what? percent? USD?
volume              -- volume in what currency?
```

### 1.5 Constraints

Named constraints use a type prefix followed by the table and column(s):

| Constraint Type | Prefix | Pattern                        | Example                       |
|-----------------|--------|--------------------------------|-------------------------------|
| Unique          | `uq_`  | `uq_{table}_{column(s)}`       | `uq_staging_coin_snapshot`    |
| Check           | `chk_` | `chk_{table}_{column}`         | `chk_fact_price_positive`     |
| Foreign Key     | `fk_`  | `fk_{table}_{referenced_table}`| `fk_fact_dim_coin`            |

Inline `PRIMARY KEY` and `NOT NULL` constraints do not need names.

```sql
-- ✅ Correct
CONSTRAINT uq_staging_coin_snapshot UNIQUE (coingecko_id, last_updated)
CONSTRAINT uq_fact_date_coin UNIQUE (date_id, coin_id)

-- ❌ Wrong
CONSTRAINT unique1 UNIQUE (coingecko_id, last_updated)
CONSTRAINT my_unique UNIQUE (date_id, coin_id)
```

### 1.6 Indexes

```
idx_{schema}_{table}_{column(s)}
```

```sql
-- ✅ Correct
idx_raw_api_response_ingested_at
idx_staging_market_data_coingecko_id
idx_fact_market_snapshot_date_id

-- ❌ Wrong
index1
my_index
api_response_idx
```

---

## 2. Python Conventions

This project follows [PEP 8](https://peps.python.org/pep-0008/) — the official
Python style guide. All professional Python teams follow it.

### 2.1 General Case Rules

| Thing               | Convention     | Example                          |
|---------------------|----------------|----------------------------------|
| Variables           | `snake_case`   | `raw_df`, `coin_id`, `page_num`  |
| Functions           | `snake_case`   | `fetch_market_data()`            |
| Constants           | `UPPER_SNAKE_CASE` | `COINS_PER_PAGE`, `BASE_URL` |
| Classes             | `PascalCase`   | `CoinGeckoClient`                |
| Private functions   | `_snake_case`  | `_parse_response()`              |
| Modules (files)     | `snake_case`   | `extract.py`, `transform.py`     |
| Packages (folders)  | `snake_case`   | `etl/`, `tests/`                 |

### 2.2 Function Naming

Functions are named as `verb_noun` — they describe the *action* on the *subject*:

```python
# ✅ Correct — verb + noun
def fetch_market_data(page: int) -> list[dict]: ...
def clean_market_data(df: pd.DataFrame) -> pd.DataFrame: ...
def build_dim_coin(df: pd.DataFrame) -> pd.DataFrame: ...
def load_to_postgres(df, schema, table, engine) -> None: ...

# ❌ Wrong — too vague, noun-only, or CamelCase
def getData(): ...
def process(): ...
def MarketData(): ...
```

### 2.3 DataFrame Variable Names

DataFrames are always suffixed with `_df`. This makes it immediately clear
that the variable is a DataFrame (not a list, dict, or string):

```python
# ✅ Correct
raw_df          = extract_raw_data()
clean_df        = clean_market_data(raw_df)
dim_coin_df     = build_dim_coin(clean_df)
fact_df         = build_fact_market_snapshot(clean_df, dims)

# ❌ Wrong — ambiguous type
raw             = extract_raw_data()
data            = clean_market_data(raw)
coins           = build_dim_coin(data)
```

### 2.4 Constants

Module-level constants (values that never change during a run) are
`UPPER_SNAKE_CASE` and defined at the top of the file, below imports:

```python
# ✅ Correct
COINS_PER_PAGE  = 100
TOTAL_PAGES     = 3
REQUEST_TIMEOUT = 30  # seconds
BACKOFF_WAIT    = 2   # seconds base for exponential backoff
```

### 2.5 Type Hints

All function signatures include type hints. This makes the code self-documenting
and is expected in any serious Python codebase:

```python
# ✅ Correct
def fetch_market_data(page: int) -> list[dict]:
def clean_market_data(df: pd.DataFrame) -> pd.DataFrame:
def load_to_postgres(df: pd.DataFrame, schema: str, table: str, engine) -> None:

# ❌ Wrong — no type information
def fetch_market_data(page):
def clean_market_data(df):
```

### 2.6 Docstrings

Every function has a docstring. One-liners are fine for simple functions.
Complex functions use the multi-line format:

```python
# Simple function — one-liner is fine
def get_tier_id(rank: int) -> int:
    """Map a market cap rank integer to a tier_id (1–4)."""

# Complex function — multi-line
def fetch_all_coins(pages: int = 3) -> list[dict]:
    """
    Fetch market data for the top N pages of coins from CoinGecko.

    Args:
        pages: Number of pages to fetch (100 coins per page).

    Returns:
        List of raw coin dicts as returned by the CoinGecko API.

    Raises:
        ValueError: If the API returns an empty response.
        requests.HTTPError: If a non-retryable HTTP error occurs.
    """
```

---

## 3. File & Directory Conventions

### 3.1 File Names

All file names use `snake_case`. No spaces, no hyphens, no capital letters
(except `README.md` — that's a universal standard).

```
# ✅ Correct
create_tables.sql
test_transform.py
data_dictionary.md

# ❌ Wrong
CreateTables.sql
test-transform.py
DataDictionary.md
```

### 3.2 SQL File Names

SQL files are named by their *action*:

| File                    | Purpose                                       |
|-------------------------|-----------------------------------------------|
| `create_schemas.sql`    | CREATE SCHEMA statements                      |
| `create_tables.sql`     | CREATE TABLE statements (DDL)                 |
| `seed_dimensions.sql`   | Static INSERT statements for reference data   |
| `analytics_queries.sql` | Business analysis queries                     |

### 3.3 Python Module Names

| Module          | Responsibility                                  |
|-----------------|-------------------------------------------------|
| `extract.py`    | API calls only — no transformations             |
| `transform.py`  | All data cleaning and modeling — no I/O         |
| `load.py`       | Database writes only — no business logic        |

This separation is called the **Single Responsibility Principle**. Each module
does exactly one thing. If a bug is reported in the cleaning logic, you know to
look in `transform.py` and nowhere else.

### 3.4 Test File Names

Test files mirror the module they test, prefixed with `test_`:

```
etl/extract.py    →  tests/test_extract.py
etl/transform.py  →  tests/test_transform.py
etl/load.py       →  tests/test_load.py
```

---

## 4. Airflow Conventions

### 4.1 DAG IDs

DAG IDs use `snake_case` and follow the pattern `{project}_{schedule}`:

```python
# ✅ Correct
dag_id = "cryptoflow_daily"

# ❌ Wrong
dag_id = "CryptoFlow"
dag_id = "my-dag"
dag_id = "cryptoflow"
```

### 4.2 Task IDs

Task IDs use `snake_case` and follow the verb_noun pattern — specifically
`{verb}_to_{layer}` or `{verb}_{subject}` to make the DAG graph self-explanatory:

```python
# ✅ Correct — the graph reads like a sentence
"setup_database"
"extract_to_bronze"
"transform_to_silver"
"build_gold_dimensions"
"build_gold_fact"
"validate_pipeline"

# ❌ Wrong — vague or inconsistent
"task1"
"extract"
"do_transform"
"LoadData"
```

---

## 5. Git Conventions

### 5.1 Branch Names

Branches follow the pattern `{type}/{phase-number}-{short-description}`:

```bash
# ✅ Correct
phase/0-project-setup
phase/1-database-setup
phase/2-extract
phase/3-transform
fix/null-handling-in-transform
docs/update-readme

# ❌ Wrong
my-branch
work
new
fix1
```

### 5.2 Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/).
The format is: `{type}: {short description in present tense}`

| Type       | When to use                                          |
|------------|------------------------------------------------------|
| `feat`     | Adding new functionality                             |
| `fix`      | Fixing a bug                                         |
| `docs`     | Documentation only changes                           |
| `refactor` | Code change that neither fixes a bug nor adds a feat |
| `test`     | Adding or updating tests                             |
| `chore`    | Config, dependencies, tooling                        |

```bash
# ✅ Correct
git commit -m "feat: add CoinGecko API pagination in extract.py"
git commit -m "fix: handle null pct_change_7d in transform"
git commit -m "docs: add naming conventions markdown"
git commit -m "test: add unit tests for clean_market_data"
git commit -m "chore: pin psycopg2-binary to 2.9.9"

# ❌ Wrong
git commit -m "stuff"
git commit -m "fixed it"
git commit -m "WIP"
git commit -m "changes"
```

---

## 6. Environment Variables

Environment variable names use `UPPER_SNAKE_CASE` and are prefixed by their
category to make the `.env` file easy to scan:

```bash
# ✅ Correct — grouped by category prefix
DB_HOST
DB_PORT
DB_NAME
DB_USER
DB_PASSWORD

COINGECKO_BASE_URL
COINS_PER_PAGE
TOTAL_PAGES

AIRFLOW_HOME
```

In Python, environment variables are loaded via `python-dotenv` and immediately
assigned to a named constant:

```python
# ✅ Correct — load once, use the constant everywhere
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL")
```

---

## 7. Quick Reference Cheatsheet

| Context                    | Convention         | Example                              |
|----------------------------|--------------------|--------------------------------------|
| SQL keywords               | UPPERCASE          | `SELECT`, `CREATE TABLE`, `WHERE`    |
| Schema names               | lowercase          | `raw`, `staging`, `warehouse`        |
| Table names                | `snake_case`       | `fact_market_snapshot`               |
| Dimension tables           | `dim_` prefix      | `dim_coin`, `dim_date`               |
| Fact tables                | `fact_` prefix     | `fact_market_snapshot`               |
| Column names               | `snake_case`       | `price_usd`, `market_cap_rank`       |
| PK in raw/staging          | `id`               | `id BIGSERIAL PRIMARY KEY`           |
| PK in warehouse            | `{table}_id`       | `coin_id`, `snapshot_id`             |
| FK columns                 | same name as PK    | `coin_id` references `dim_coin`      |
| Boolean columns            | `is_` / `has_`     | `is_weekend`, `has_price_data`       |
| Unique constraints         | `uq_` prefix       | `uq_fact_date_coin`                  |
| Index names                | `idx_` prefix      | `idx_fact_market_snapshot_date_id`   |
| Python variables           | `snake_case`       | `raw_df`, `coin_id`                  |
| Python constants           | `UPPER_SNAKE_CASE` | `COINS_PER_PAGE`, `BASE_URL`         |
| Python functions           | `verb_noun`        | `fetch_market_data()`, `build_dim_coin()` |
| DataFrames                 | `_df` suffix       | `raw_df`, `clean_df`, `fact_df`      |
| Python classes             | `PascalCase`       | `CoinGeckoClient`                    |
| SQL filenames              | `snake_case.sql`   | `create_tables.sql`                  |
| Python filenames           | `snake_case.py`    | `extract.py`, `test_transform.py`    |
| Airflow DAG IDs            | `snake_case`       | `cryptoflow_daily`                   |
| Airflow task IDs           | `verb_to_layer`    | `extract_to_bronze`, `build_gold_fact` |
| Git branches               | `type/description` | `phase/2-extract`                    |
| Git commits                | `type: description`| `feat: add retry logic in extract`   |
| Environment variables      | `UPPER_SNAKE_CASE` | `DB_HOST`, `COINGECKO_BASE_URL`      |
