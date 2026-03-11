-- =============================================================================
-- CryptoFlow — Schema Creation
-- File: sql/create_schemas.sql
-- Run this ONCE before anything else.
-- =============================================================================
--
-- 📚 CONCEPT: What is a PostgreSQL Schema?
--
--   A schema is like a folder inside your database. It lets you group related
--   tables together and keep different layers of data separated.
--
--   Without schemas, all your tables would live in one flat list (the default
--   "public" schema). With schemas, we get:
--
--       cryptoflow (database)
--       ├── raw        (bronze layer — untouched API data)
--       ├── staging    (silver layer — cleaned and validated)
--       └── warehouse  (gold layer — dimensional model for analytics)
--
--   This mirrors the Medallion Architecture used in production data platforms.
--   Knowing this pattern and being able to explain it is a great interview point.
--
-- 💡 IF EXISTS: Makes the script safe to re-run. Without it, you'd get an
--    error if the schema already exists. With it, nothing happens if it's
--    already there. Always write idempotent SQL scripts.
-- =============================================================================

-- Bronze layer: raw API responses, stored exactly as received
CREATE SCHEMA IF NOT EXISTS raw;

-- Silver layer: cleaned, typed, validated data
CREATE SCHEMA IF NOT EXISTS staging;

-- Gold layer: dimensional model, ready for analytics
CREATE SCHEMA IF NOT EXISTS warehouse;
