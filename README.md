# CryptoFlow ETL Pipeline

> A local end-to-end ETL pipeline that ingests live cryptocurrency market data
> from the CoinGecko public API, transforms it through a three-layer Medallion
> architecture, and loads it into a PostgreSQL data warehouse — orchestrated
> daily by Apache Airflow.

---

## Architecture

```
CoinGecko API  →  Extract  →  Bronze (raw)  →  Transform  →  Silver (staging)  →  Gold (warehouse)
                                                                                         ↓
                                                                               Airflow DAG (daily)
```

**Medallion layers** (all inside a single PostgreSQL database):

| Layer  | Schema      | Description                                      |
|--------|-------------|--------------------------------------------------|
| Bronze | `raw`       | Raw API response stored as-is, TEXT columns      |
| Silver | `staging`   | Cleaned, typed, validated data                   |
| Gold   | `warehouse` | Kimball star schema — optimised for analytics    |

**Star Schema (Gold layer):**

```
             dim_date
                │
dim_coin ── fact_market_snapshot ── dim_market_tier
```

---

## Tech Stack

| Tool              | Purpose                             |
|-------------------|-------------------------------------|
| Python 3.10+      | ETL logic                           |
| Pandas            | Data transformation                 |
| requests          | CoinGecko API client                |
| tenacity          | Retry with exponential backoff      |
| PostgreSQL        | Data warehouse (local)              |
| SQLAlchemy        | Database connection layer           |
| Apache Airflow    | Pipeline orchestration & scheduling |
| pytest            | Unit testing                        |

---

## Project Structure

```
cryptoflow-etl-pipeline/
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── docs/
│   ├── project_plan.docx
│   └── data_dictionary.md
├── etl/
│   ├── extract.py       # CoinGecko API client
│   ├── transform.py     # Cleaning + dimensional modeling
│   └── load.py          # PostgreSQL upsert loader
├── sql/
│   ├── create_schemas.sql
│   ├── create_tables.sql
│   ├── seed_dimensions.sql
│   └── analytics_queries.sql
├── dags/
│   └── cryptoflow_dag.py
└── tests/
    ├── test_extract.py
    └── test_transform.py
```

---

## Setup & Run

### Prerequisites
- Python 3.10+
- PostgreSQL running locally
- Git

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/cryptoflow-etl-pipeline.git
cd cryptoflow-etl-pipeline
```

### 2. Create a virtual environment and install dependencies
```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 4. Set up the database
```bash
# Create the database first
psql -U postgres -c "CREATE DATABASE cryptoflow;"

# Run the setup SQL scripts in order
psql -U postgres -d cryptoflow -f sql/create_schemas.sql
psql -U postgres -d cryptoflow -f sql/create_tables.sql
psql -U postgres -d cryptoflow -f sql/seed_dimensions.sql
```

### 5. Run the pipeline manually (without Airflow)
```bash
python -c "
from etl.extract import fetch_all_coins
from etl.transform import run_transforms
from etl.load import load_all
data = fetch_all_coins()
transformed = run_transforms(data)
load_all(transformed)
print('Pipeline complete.')
"
```

### 6. Run with Airflow
```bash
export AIRFLOW_HOME=~/airflow
airflow standalone   # Starts web server + scheduler
# Visit http://localhost:8080 — default login: admin / (printed in terminal)
# Enable the 'cryptoflow_daily' DAG and trigger a run
```

### 7. Run tests
```bash
pytest tests/ -v
```

---

## Analytics Queries

After running the pipeline, connect to the database and run the queries in
`sql/analytics_queries.sql`. They answer:

1. Top 10 coins by average market cap (last 30 days)
2. Bitcoin & Ethereum daily price volatility (last 90 days)
3. Market tier breakdown on the most recent snapshot date
4. 7-day biggest winners and losers
5. Pipeline health check — daily ingestion counts

---

## Data Source

**CoinGecko Public API** — No API key required.

- Endpoint: `GET /coins/markets?vs_currency=usd&per_page=100`
- Covers top 250 coins by market cap
- Rate limit: ~30 requests/minute (pipeline uses 3 calls/day)
- Docs: https://docs.coingecko.com/reference/introduction

---

## Build Status

- [x] Phase 0 — Project specification & architecture
- [x] Phase 1 — Database setup (schemas, tables, seed data)
- [ ] Phase 2 — Extract (CoinGecko API client)
- [ ] Phase 3 — Transform (cleaning + star schema build)
- [ ] Phase 4 — Load (PostgreSQL upsert)
- [ ] Phase 5 — Orchestration (Airflow DAG)
- [ ] Phase 6 — Analytics queries + unit tests
