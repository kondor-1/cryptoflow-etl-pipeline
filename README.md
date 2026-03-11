# CryptoFlow ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Tests](https://img.shields.io/badge/Tests-14%20passing-brightgreen)

A production-style daily ELT pipeline that ingests live cryptocurrency market data for the top 300 coins from the CoinGecko REST API, transforms it through a Medallion architecture, and loads it into a Kimball star schema in PostgreSQL — fully orchestrated by Apache Airflow and containerized with Docker.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│                                                                     │
│              CoinGecko REST API (public, no key required)           │
│              GET /coins/markets — 300 coins, 3 pages/day            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER — raw schema                        │
│                                                                     │
│   raw.api_response                                                  │
│   All TEXT columns — full audit trail, no type casting              │
│   append-only — every run adds new rows                             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  clean + cast + deduplicate
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER — staging schema                    │
│                                                                     │
│   staging.market_data_clean                                         │
│   Typed columns — NUMERIC, TIMESTAMPTZ, INTEGER                     │
│   Deduplicated on (coingecko_id, last_updated)                      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  model into star schema
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER — warehouse schema                    │
│                                                                     │
│   warehouse.dim_date          warehouse.dim_coin                    │
│   date_id (YYYYMMDD)          coin_id (BIGSERIAL surrogate PK)      │
│   day/month/quarter attrs     coingecko_id (natural key)            │
│   is_weekend flag             symbol, name (SCD Type 1)             │
│                                                                     │
│   warehouse.dim_market_tier   warehouse.fact_market_snapshot        │
│   tier_id 1-4                 grain: one row per coin per day       │
│   Mega/Large/Mid/Small Cap    FKs to all 3 dims                     │
│   static seed data            price, volume, market cap, pct change │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION                               │
│                                                                     │
│   Apache Airflow — cryptoflow_daily DAG (@daily)                    │
│                                                                     │
│   extract_to_bronze                                                 │
│        >> transform_and_load_silver                                 │
│             >> load_dimensions                                      │
│                  >> build_and_load_fact                             │
│                       >> validate_pipeline                          │
│                                                                     │
│   retries=2, retry_delay=5min, catchup=False                        │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         INFRASTRUCTURE                              │
│                                                                     │
│   Docker Compose — 5 services                                       │
│   cryptoflow-db  (PostgreSQL 16, port 5433)                         │
│   airflow-db     (PostgreSQL 16, internal only)                     │
│   airflow-init   (one-time setup)                                   │
│   airflow-webserver  (port 8080)                                    │
│   airflow-scheduler  (runs DAGs on schedule)                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Extraction | requests, tenacity (exponential backoff) |
| Transformation | pandas |
| Loading | SQLAlchemy, psycopg2 |
| Orchestration | Apache Airflow 2.9 |
| Database | PostgreSQL 16 |
| Containerization | Docker, Docker Compose |
| Testing | pytest (14 unit tests) |
| Configuration | python-dotenv |

---

## Data Model

The Gold layer implements a **Kimball star schema** with the following grain:

> One row per coin per day in `fact_market_snapshot`

```
                    dim_date
                   (date_id PK)
                       │
                       │ FK
                       ▼
dim_coin ──FK──► fact_market_snapshot ◄──FK── dim_market_tier
(coin_id PK)    (snapshot_id PK)              (tier_id PK)
                (date_id FK)                  Mega Cap   rank 1-10
                (coin_id FK)                  Large Cap  rank 11-50
                (tier_id FK)                  Mid Cap    rank 51-150
                ── price_usd                  Small Cap  rank 151-250
                ── market_cap_usd
                ── volume_24h_usd
                ── pct_change_24h
                ── pct_change_7d
                ── circulating_supply
                ── market_cap_rank
```

**Key design decisions:**
- Surrogate keys (BIGSERIAL) in warehouse dims — decoupled from source system IDs
- Natural key (`coingecko_id`) preserved as UNIQUE constraint in `dim_coin`
- SCD Type 1 on `dim_coin` — coin name/symbol changes overwrite old values
- Idempotent upserts via `INSERT ... ON CONFLICT DO UPDATE` — safe to re-run
- `dim_market_tier` is a static seed dimension — never changes at runtime

---

## Project Structure

```
cryptoflow-etl-pipeline/
├── Dockerfile                  Custom Airflow image with project deps
├── docker-compose.yml          5-service stack definition
├── .dockerignore               Excludes .env, .venv, logs from image
├── requirements.txt            Python dependencies
├── .env.example                Environment variable template
├── .gitignore
├── README.md
│
├── etl/
│   ├── extract.py              CoinGecko API client, pagination, retry
│   ├── transform.py            Bronze→Silver→Gold transformation logic
│   └── load.py                 PostgreSQL upsert for all 4 tables
│
├── dags/
│   └── cryptoflow_dag.py       Airflow DAG — 5 tasks, @daily schedule
│
├── sql/
│   ├── 01_create_schemas.sql   raw, staging, warehouse schemas
│   ├── 02_create_tables.sql    All 6 tables with indexes and constraints
│   ├── 03_seed_dimensions.sql  dim_market_tier static data
│   └── analytics_queries.sql  5 analytical SQL queries
│
├── tests/
│   ├── test_extract.py         7 unit tests for extract module
│   └── test_transform.py       14 unit tests for transform module
│
└── docs/
    └── naming_conventions.md   SQL, Python, Git naming standards
```

---

## Quickstart

### Prerequisites
- Docker Desktop running
- Git

### 1. Clone the repo
```bash
git clone https://github.com/kondor-1/cryptoflow-etl-pipeline.git
cd cryptoflow-etl-pipeline
```

### 2. Configure environment
```bash
cp .env.example .env
# No changes needed for local Docker setup — defaults work out of the box
```

### 3. Start the stack
```bash
docker compose up --build
```

First run takes 3-5 minutes to download images and build. When you see:
```
airflow-webserver | [INFO] Listening at: http://0.0.0.0:8080
```
The stack is ready.

### 4. Open Airflow UI
Go to **http://localhost:8080**
- Username: `admin`
- Password: `admin`

### 5. Run the pipeline
1. Find `cryptoflow_daily` in the DAG list
2. Toggle it on (click the switch on the left)
3. Click the ▶ button to trigger a manual run
4. Watch all 5 tasks turn green

### 6. Connect to the database
Use any PostgreSQL client (pgAdmin, DBeaver, psql):
- Host: `localhost`
- Port: `5433`
- Database: `cryptoflow`
- Username: `cryptoflow`
- Password: `cryptoflow`

---

## Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run all unit tests
pytest tests/ -v
```

Expected output:
```
tests/test_extract.py::TestSaveToBronze::test_dataframe_has_correct_columns PASSED
tests/test_extract.py::TestSaveToBronze::test_columns_renamed_correctly PASSED
...
14 passed in 81s
```

---

## Key Engineering Decisions

**Why Medallion architecture?**
Clear separation between raw (audit trail), clean (typed), and modeled (analytics-ready) data. Each layer has a single responsibility and can be rebuilt independently if upstream logic changes.

**Why Kimball star schema in the Gold layer?**
Optimized for analytical queries — simple joins, pre-aggregated dimensions, consistent grain. The fact table answers "what happened to coin X on date Y" with a single row lookup.

**Why Python transforms instead of SQL/dbt?**
At this data volume (300 rows/day), Python/pandas is more testable — unit tests run without a database connection. In a production system at scale, Gold layer transforms would move to dbt models for better lineage and maintainability.

**Why two separate PostgreSQL instances in Docker?**
Airflow's metadata (DAG runs, task states, logs) is completely separate from the pipeline data. Mixing them would couple two independent concerns and complicate backups and scaling.

**Why `INSERT ... ON CONFLICT DO UPDATE` instead of DELETE + INSERT?**
Idempotency. The pipeline can be re-run any number of times and always produces the same result. DELETE + INSERT risks data loss if the process fails between the two operations.

**Why exponential backoff on the CoinGecko API?**
CoinGecko's public API rate-limits at ~10 requests/minute. Exponential backoff (10s, 20s, 40s...) gives the API time to recover without hammering it repeatedly, which would extend the rate limit window.

---

## Analytics Queries

Sample queries available in `sql/analytics_queries.sql`:

```sql
-- Top 10 coins by market cap today
SELECT c.name, f.price_usd, f.market_cap_usd, f.market_cap_rank
FROM warehouse.fact_market_snapshot f
JOIN warehouse.dim_coin c ON f.coin_id = c.coin_id
WHERE f.date_id = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER
ORDER BY f.market_cap_rank
LIMIT 10;

-- 7-day price change leaders
SELECT c.name, f.pct_change_7d
FROM warehouse.fact_market_snapshot f
JOIN warehouse.dim_coin c ON f.coin_id = c.coin_id
WHERE f.date_id = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER
ORDER BY f.pct_change_7d DESC
LIMIT 10;
```

## Pipeline in Action
<img width="1893" height="876" alt="image" src="https://github.com/user-attachments/assets/d4da3be6-a90f-4d9e-a1af-d2b28f91cf68" />


---

## Roadmap

- [ ] AWS migration: S3 (bronze), Glue Jobs (transform), Redshift Serverless (warehouse)
- [ ] Lambda + EventBridge to replace Airflow scheduler in cloud
- [ ] dbt models for Gold layer transformations
- [ ] Metabase dashboard connected to warehouse
- [ ] Historical backfill (365 days OHLCV data)

---

## Author

**Alessandro Xavier Kon López**
Data Engineer
[github.com/kondor-1](https://github.com/kondor-1) · alessandroxavierk@gmail.com
