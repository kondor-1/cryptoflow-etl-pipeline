-- =============================================================================
-- CryptoFlow — Seed Static Dimension Data
-- File: sql/seed_dimensions.sql
--
-- Run AFTER create_tables.sql.
-- Seeds the 4 static rows in warehouse.dim_market_tier.
--
-- 📚 CONCEPT: Static / Reference Dimensions
--
--   Some dimensions don't come from an API or a source system — they encode
--   business rules that WE define. dim_market_tier is a perfect example:
--   there's no CoinGecko field called "tier". We invented the concept based
--   on the market_cap_rank field and the business need to group coins.
--
--   These static dimensions are called "reference data" or "seed data" and
--   they are version-controlled in SQL files, not managed by the ETL pipeline.
--   They change rarely (maybe once a year if the business changes its tier
--   definitions). When they do change, you update this file and re-run it.
--
-- 📚 CONCEPT: ON CONFLICT DO NOTHING
--
--   This makes the script safe to re-run. If the 4 rows already exist
--   (because you ran it before), the INSERT is silently skipped.
--   If you run it on a fresh database, the 4 rows are inserted normally.
--   This is the seed-data equivalent of CREATE TABLE IF NOT EXISTS.
-- =============================================================================

INSERT INTO warehouse.dim_market_tier (tier_id, tier_name, rank_min, rank_max, description)
VALUES
    (1, 'Mega Cap',  1,   10,  'The 10 largest cryptocurrencies by market cap. Includes Bitcoin and Ethereum. Highest liquidity, most institutional interest.'),
    (2, 'Large Cap', 11,  50,  'Established coins with significant but not dominant market caps. Strong liquidity and relatively lower volatility than smaller caps.'),
    (3, 'Mid Cap',   51,  150, 'Growing projects with moderate market caps and liquidity. Higher reward potential but also higher risk than large cap.'),
    (4, 'Small Cap', 151, 250, 'Earlier-stage or niche projects. Lower liquidity, higher volatility, higher risk. Some may graduate to higher tiers over time.')
ON CONFLICT (tier_id) DO NOTHING;
