-- =============================================================================
-- CryptoFlow — Analytics Queries
-- File: sql/analytics_queries.sql
--
-- These 5 queries demonstrate that the pipeline produced correct, useful data.
-- Run them after at least one full pipeline execution.
-- Screenshots of results should be included in the README.
--
-- 📚 CONCEPT: Why are these queries important for the portfolio?
--
--   The pipeline's job is to produce data that answers real business questions.
--   These queries are the "proof of value" — they show that the star schema
--   design works, the data is clean and correct, and an analyst could actually
--   use this warehouse to make decisions.
--
--   Notice that every query starts from the fact table and JOINs to dimensions.
--   That's the Kimball star schema pattern in action: start at the centre,
--   join outward to get context.
-- =============================================================================


-- =============================================================================
-- Query 1: Top 10 Coins by Average Market Cap (Last 30 Days)
--
-- Business question: "Which coins have had the largest average market cap
-- over the past month? This tells us the dominant players in the market."
--
-- 📚 Note the join pattern: fact → dim_coin to get the name,
--    fact → dim_date to filter by date range.
--    This is the standard star schema query structure.
-- =============================================================================

SELECT
    dc.name                              AS coin_name,
    dc.symbol                            AS symbol,
    ROUND(AVG(f.market_cap_usd), 0)      AS avg_market_cap_usd,
    ROUND(AVG(f.price_usd), 4)           AS avg_price_usd,
    MIN(f.market_cap_rank)               AS best_rank_in_period
FROM warehouse.fact_market_snapshot f
JOIN warehouse.dim_coin dc ON f.coin_id = dc.coin_id
JOIN warehouse.dim_date dd ON f.date_id = dd.date_id
WHERE dd.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY dc.name, dc.symbol
ORDER BY avg_market_cap_usd DESC
LIMIT 10;


-- =============================================================================
-- Query 2: Bitcoin & Ethereum Price Volatility — Last 90 Days
--
-- Business question: "How volatile have the two largest coins been recently?
-- High/Low ratio per day measures intraday swing."
--
-- 📚 A ratio above 1.05 means the price swung by more than 5% intraday.
--    This is a simple but effective volatility metric derived entirely from
--    data already in the fact table — no new API call needed.
-- =============================================================================

SELECT
    dd.date,
    dc.name                                       AS coin,
    ROUND(f.price_usd, 2)                         AS closing_price_usd,
    ROUND(f.pct_change_24h, 2)                    AS pct_change_24h,
    ROUND(f.high_24h / NULLIF(f.low_24h, 0), 4)  AS high_low_ratio
FROM warehouse.fact_market_snapshot f
JOIN warehouse.dim_coin dc ON f.coin_id = dc.coin_id
JOIN warehouse.dim_date dd ON f.date_id = dd.date_id
WHERE dc.coingecko_id IN ('bitcoin', 'ethereum')
  AND dd.date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY dc.name, dd.date DESC;


-- =============================================================================
-- Query 3: Market Tier Breakdown — Most Recent Snapshot
--
-- Business question: "On the most recent day we have data, what is the total
-- trading volume and average price change across each market tier?"
--
-- 📚 This query uses a subquery to find the latest date, then filters to it.
--    dim_market_tier is joined here — this is the derived dimension we built
--    from business logic. It adds context that doesn't exist in the raw API.
-- =============================================================================

SELECT
    dmt.tier_name,
    COUNT(f.snapshot_id)                      AS coins_in_tier,
    ROUND(SUM(f.volume_24h_usd) / 1e9, 2)    AS total_volume_billions_usd,
    ROUND(AVG(f.pct_change_24h), 2)           AS avg_pct_change_24h,
    ROUND(AVG(f.market_cap_usd) / 1e9, 2)    AS avg_market_cap_billions_usd
FROM warehouse.fact_market_snapshot f
JOIN warehouse.dim_market_tier dmt ON f.tier_id = dmt.tier_id
JOIN warehouse.dim_date dd ON f.date_id = dd.date_id
WHERE dd.date = (SELECT MAX(date) FROM warehouse.dim_date)
GROUP BY dmt.tier_name, dmt.tier_id
ORDER BY dmt.tier_id;


-- =============================================================================
-- Query 4: 7-Day Winners and Losers
--
-- Business question: "Which coins gained the most and lost the most over the
-- past 7 days? Top 5 winners and top 5 losers."
--
-- 📚 UNION ALL combines two result sets vertically. Both sides must have the
--    same column names and compatible types. The 'category' column is a
--    literal string we add to label each group — useful for display.
-- =============================================================================

(
    SELECT
        'Winner'              AS category,
        dc.name               AS coin_name,
        dc.symbol,
        ROUND(f.pct_change_7d, 2) AS pct_change_7d,
        ROUND(f.price_usd, 4)    AS current_price_usd
    FROM warehouse.fact_market_snapshot f
    JOIN warehouse.dim_coin dc ON f.coin_id = dc.coin_id
    JOIN warehouse.dim_date dd ON f.date_id = dd.date_id
    WHERE dd.date = (SELECT MAX(date) FROM warehouse.dim_date)
      AND f.pct_change_7d IS NOT NULL
    ORDER BY f.pct_change_7d DESC
    LIMIT 5
)
UNION ALL
(
    SELECT
        'Loser'               AS category,
        dc.name               AS coin_name,
        dc.symbol,
        ROUND(f.pct_change_7d, 2) AS pct_change_7d,
        ROUND(f.price_usd, 4)    AS current_price_usd
    FROM warehouse.fact_market_snapshot f
    JOIN warehouse.dim_coin dc ON f.coin_id = dc.coin_id
    JOIN warehouse.dim_date dd ON f.date_id = dd.date_id
    WHERE dd.date = (SELECT MAX(date) FROM warehouse.dim_date)
      AND f.pct_change_7d IS NOT NULL
    ORDER BY f.pct_change_7d ASC
    LIMIT 5
)
ORDER BY category, pct_change_7d DESC;


-- =============================================================================
-- Query 5: Pipeline Health Check — Daily Ingestion Counts
--
-- Business question (for the DE, not the analyst): "Is the pipeline running
-- correctly? How many coins were loaded per day over the last 7 runs?"
--
-- 📚 CONCEPT: Observability
--   A production pipeline always includes queries that monitor the pipeline
--   itself — not just the business data. If you see 250 coins one day and
--   180 the next, something went wrong with the API call. This query is
--   the simplest form of pipeline monitoring. Including it shows you think
--   about operational concerns, not just the happy path.
-- =============================================================================

SELECT
    dd.date,
    COUNT(f.snapshot_id)         AS coins_loaded,
    ROUND(SUM(f.volume_24h_usd) / 1e9, 2) AS total_market_volume_billions,
    MIN(f.ingested_at)           AS pipeline_start,
    MAX(f.ingested_at)           AS pipeline_end
FROM warehouse.fact_market_snapshot f
JOIN warehouse.dim_date dd ON f.date_id = dd.date_id
WHERE dd.date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dd.date
ORDER BY dd.date DESC;
