-- =============================================================================
-- CryptoFlow — Table Definitions (DDL)
-- File: sql/create_tables.sql
--
-- Run AFTER create_schemas.sql.
-- Safe to re-run — all statements use IF NOT EXISTS.
--
-- Tables created (in dependency order):
--   1. raw.api_response          — Bronze: raw API data
--   2. staging.market_data_clean — Silver: cleaned data
--   3. warehouse.dim_date        — Gold: date dimension
--   4. warehouse.dim_coin        — Gold: coin dimension
--   5. warehouse.dim_market_tier — Gold: market tier dimension (static)
--   6. warehouse.fact_market_snapshot — Gold: central fact table
-- =============================================================================
--
-- 📚 CONCEPT: DDL vs DML
--
--   DDL = Data Definition Language: SQL that DEFINES structure (CREATE, ALTER, DROP)
--   DML = Data Manipulation Language: SQL that MOVES data (INSERT, UPDATE, DELETE)
--
--   This file is pure DDL — it creates the containers. Later, our Python ETL
--   code will use DML to fill them with data.
--
-- 📚 CONCEPT: Data Types Matter
--
--   Choosing the right data type for each column is a core DE skill. Wrong types
--   cause bugs, slow queries, and storage waste. Key types used here:
--
--   BIGSERIAL       — Auto-incrementing integer. Perfect for surrogate primary keys.
--   VARCHAR(n)      — Variable-length string with a max length. Use for known-length
--                     strings like symbols ('btc' is never > 20 chars).
--   TEXT            — Unlimited string. Use when length is unpredictable (URLs, names).
--   NUMERIC(20, 8)  — Exact decimal number. NEVER use FLOAT for financial data —
--                     floats have rounding errors. NUMERIC is exact.
--   INTEGER         — Whole number. Good for ranks, counts, IDs.
--   BOOLEAN         — True/False. Good for flags like is_weekend.
--   DATE            — Calendar date (no time). Good for the date dimension.
--   TIMESTAMPTZ     — Timestamp WITH timezone. Always use this over TIMESTAMP for
--                     any data that crosses timezones. CoinGecko returns UTC times.
--
-- =============================================================================


-- =============================================================================
-- 1. RAW LAYER (Bronze)
-- =============================================================================
--
-- This table stores the raw, flattened CoinGecko API response exactly as we
-- received it — no cleaning, no transformations. Every field is TEXT because
-- we don't want type casting to fail and lose data at the ingestion stage.
--
-- 📚 CONCEPT: Why store raw data at all?
--
--   This is the "audit trail" principle. If our transform logic has a bug and
--   corrupts the silver data, we can always re-process from this bronze table
--   without making another API call. You cannot un-delete a live API response,
--   but you can always re-read a table.
--
--   Notice: all columns are TEXT here. We deliberately accept everything as a
--   string. Type casting happens in the transform step, not here.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.api_response (
    id               BIGSERIAL    PRIMARY KEY,

    -- CoinGecko fields stored as raw TEXT — no casting at ingestion
    coingecko_id     TEXT,
    symbol           TEXT,
    name             TEXT,
    current_price    TEXT,
    market_cap       TEXT,
    total_volume     TEXT,
    high_24h         TEXT,
    low_24h          TEXT,
    price_change_pct_24h TEXT,
    price_change_pct_7d  TEXT,
    circulating_supply   TEXT,
    market_cap_rank      TEXT,
    last_updated         TEXT,

    -- Pipeline metadata: when did WE ingest this record?
    -- This is different from last_updated (which is when CoinGecko updated it).
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Index on ingested_at so we can quickly query "all records from today"
CREATE INDEX IF NOT EXISTS idx_raw_api_response_ingested_at
    ON raw.api_response (ingested_at);


-- =============================================================================
-- 2. STAGING LAYER (Silver)
-- =============================================================================
--
-- Same data as bronze, but now with proper data types applied. This is the
-- "clean" version of the raw data. Nulls have been handled, types are correct,
-- and duplicates have been removed.
--
-- 📚 CONCEPT: Why a separate staging layer?
--
--   Separating "raw" from "clean" means our cleaning logic is isolated in one
--   place (transform.py). If the cleaning rules change, we update one function,
--   not the entire pipeline. This is the Single Responsibility Principle applied
--   to data engineering.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.market_data_clean (
    -- PK follows convention: {table_singular}_id for staging/warehouse tables
    market_data_id   BIGSERIAL    PRIMARY KEY,

    coingecko_id     VARCHAR(100) NOT NULL,
    symbol           VARCHAR(20)  NOT NULL,
    name             VARCHAR(200) NOT NULL,

    -- 📚 NUMERIC(precision, scale)
    --   precision = total digits  |  scale = digits after decimal point
    --   NUMERIC(20, 8) can store: 123456789012.12345678
    --   We use this for all financial figures to avoid float rounding errors.
    current_price        NUMERIC(20, 8),
    market_cap           NUMERIC(24, 2),
    total_volume         NUMERIC(24, 2),
    high_24h             NUMERIC(20, 8),
    low_24h              NUMERIC(20, 8),
    price_change_pct_24h NUMERIC(10, 4),  -- e.g. -3.4521 (percent)
    price_change_pct_7d  NUMERIC(10, 4),
    circulating_supply   NUMERIC(24, 2),
    market_cap_rank      INTEGER,

    last_updated         TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- 📚 UNIQUE CONSTRAINT
    --   This prevents the same coin snapshot from the same update being
    --   inserted twice. The combination of (coingecko_id, last_updated)
    --   uniquely identifies one market snapshot for one coin.
    CONSTRAINT uq_staging_coin_snapshot UNIQUE (coingecko_id, last_updated)
);

CREATE INDEX IF NOT EXISTS idx_staging_market_data_coingecko_id
    ON staging.market_data_clean (coingecko_id);

CREATE INDEX IF NOT EXISTS idx_staging_market_data_last_updated
    ON staging.market_data_clean (last_updated);


-- =============================================================================
-- 3. WAREHOUSE LAYER — DIMENSIONS (Gold)
-- =============================================================================
--
-- 📚 CONCEPT: Surrogate Keys vs Natural Keys
--
--   A NATURAL KEY is an identifier that comes from the source system.
--   For coins, the natural key is coingecko_id (e.g. 'bitcoin', 'ethereum').
--
--   A SURROGATE KEY is an artificial integer we generate ourselves (BIGSERIAL).
--   e.g. coin_id = 1, 2, 3, ...
--
--   We use surrogate keys in the warehouse because:
--   1. They're integers — joins on integers are MUCH faster than joins on strings.
--   2. They're stable — if CoinGecko ever renames a coin's ID, our warehouse keys
--      don't change (we just update the natural key in the dim table).
--   3. They're standard practice — Kimball recommends surrogate keys throughout.
--
--   The fact table stores surrogate keys as foreign keys. Analysts join through
--   the dimension tables to get the human-readable natural key when needed.
-- =============================================================================

-- ─── dim_date ─────────────────────────────────────────────────────────────────
--
-- The date dimension is one of the most important dimensions in any warehouse.
-- It turns a plain date into a rich set of attributes that allow flexible
-- time-based analysis without writing complex date functions in every query.
--
-- Rather than storing date_id as an auto-increment, we use YYYYMMDD format
-- (e.g. 20240115 for Jan 15 2024). This makes the FK self-documenting —
-- you can read fact_market_snapshot.date_id = 20240115 and immediately know
-- what date it refers to, without joining to dim_date.

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id      INTEGER      PRIMARY KEY,  -- Format: YYYYMMDD e.g. 20240115

    date         DATE         NOT NULL UNIQUE,
    day_of_week  INTEGER      NOT NULL,  -- 0=Monday ... 6=Sunday (Python convention)
    day_name     VARCHAR(10)  NOT NULL,  -- 'Monday', 'Tuesday', etc.
    month        INTEGER      NOT NULL,  -- 1-12
    month_name   VARCHAR(10)  NOT NULL,  -- 'January', 'February', etc.
    quarter      INTEGER      NOT NULL,  -- 1-4
    year         INTEGER      NOT NULL,
    is_weekend   BOOLEAN      NOT NULL
);


-- ─── dim_coin ─────────────────────────────────────────────────────────────────
--
-- One row per unique cryptocurrency. This dimension is "slowly changing" —
-- coin names and symbols rarely change, but if they do, we update here and
-- everything downstream automatically reflects the change via the FK.
--
-- 📚 CONCEPT: SCD Type 1
--   When a dimension attribute changes, we simply overwrite the old value.
--   This is called a Type 1 Slowly Changing Dimension. For coin names/symbols,
--   Type 1 is fine — we don't need history of what a coin used to be called.

CREATE TABLE IF NOT EXISTS warehouse.dim_coin (
    coin_id         BIGSERIAL    PRIMARY KEY,  -- Surrogate key
    coingecko_id    VARCHAR(100) NOT NULL UNIQUE,  -- Natural key from CoinGecko
    symbol          VARCHAR(20)  NOT NULL,
    name            VARCHAR(200) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_warehouse_dim_coin_coingecko_id
    ON warehouse.dim_coin (coingecko_id);


-- ─── dim_market_tier ──────────────────────────────────────────────────────────
--
-- This is a STATIC dimension — it has exactly 4 rows and never changes.
-- It classifies coins by their market cap rank into business-meaningful tiers.
--
-- 📚 CONCEPT: Designed/Derived Dimensions
--   Not all dimensions come directly from a source system. This one is derived
--   from business logic: "what do we call a coin ranked 1-10?" This enrichment
--   is a core DE skill — adding context that makes the data more useful for
--   analysts. Seeded in sql/seed_dimensions.sql after table creation.

CREATE TABLE IF NOT EXISTS warehouse.dim_market_tier (
    tier_id      INTEGER      PRIMARY KEY,
    tier_name    VARCHAR(50)  NOT NULL UNIQUE,
    rank_min     INTEGER      NOT NULL,
    rank_max     INTEGER      NOT NULL,
    description  TEXT
);


-- =============================================================================
-- 4. WAREHOUSE LAYER — FACT TABLE (Gold)
-- =============================================================================
--
-- 📚 CONCEPT: The Fact Table
--
--   The fact table is the heart of the star schema. It stores:
--   - FOREIGN KEYS to all dimension tables (the "who, what, when, where")
--   - MEASURES — the numeric values we actually want to analyse (price, volume)
--
--   Key rules from Kimball that this table follows:
--   1. One row = one grain event (one coin on one day)
--   2. All measures are numeric — no strings in the fact table
--   3. Fact table only references dimensions — never other facts
--   4. The table is append-friendly — new days add new rows, old rows don't change
--
-- 📚 CONCEPT: Idempotency with UNIQUE CONSTRAINT
--
--   The UNIQUE constraint on (date_id, coin_id) is critical. It's what allows
--   our upsert logic (INSERT ... ON CONFLICT DO UPDATE) to work correctly.
--   If the pipeline runs twice on the same day, the second run UPDATES the
--   existing rows instead of inserting duplicates.
--   An idempotent pipeline is a reliable pipeline.
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_market_snapshot (
    snapshot_id      BIGSERIAL    PRIMARY KEY,

    -- Foreign keys to dimension tables
    -- ON DELETE RESTRICT means: don't allow deleting a dimension row that
    -- is still referenced by a fact row. This protects referential integrity.
    date_id          INTEGER      NOT NULL REFERENCES warehouse.dim_date(date_id)
                                      ON DELETE RESTRICT,
    coin_id          BIGINT       NOT NULL REFERENCES warehouse.dim_coin(coin_id)
                                      ON DELETE RESTRICT,
    tier_id          INTEGER      NOT NULL REFERENCES warehouse.dim_market_tier(tier_id)
                                      ON DELETE RESTRICT,

    -- Measures: all NUMERIC — exact decimal, no float rounding errors
    price_usd            NUMERIC(20, 8),
    market_cap_usd       NUMERIC(24, 2),
    volume_24h_usd       NUMERIC(24, 2),
    high_24h             NUMERIC(20, 8),
    low_24h              NUMERIC(20, 8),
    pct_change_24h       NUMERIC(10, 4),
    pct_change_7d        NUMERIC(10, 4),
    circulating_supply   NUMERIC(24, 2),
    market_cap_rank      INTEGER,

    -- When the pipeline loaded this row (our timestamp, not CoinGecko's)
    ingested_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- This constraint is what makes upsert / idempotency work
    CONSTRAINT uq_fact_date_coin UNIQUE (date_id, coin_id)
);

-- Indexes on FK columns speed up joins dramatically on large datasets
CREATE INDEX IF NOT EXISTS idx_warehouse_fact_market_snapshot_date_id
    ON warehouse.fact_market_snapshot (date_id);

CREATE INDEX IF NOT EXISTS idx_warehouse_fact_market_snapshot_coin_id
    ON warehouse.fact_market_snapshot (coin_id);

CREATE INDEX IF NOT EXISTS idx_warehouse_fact_market_snapshot_tier_id
    ON warehouse.fact_market_snapshot (tier_id);
