-- ============================================================
--  PetroLens Analytics — Energy Data Warehouse
--  PostgreSQL init script
--  Runs automatically at container startup via docker-entrypoint-initdb.d
--  Safe to re-run: uses CREATE TABLE IF NOT EXISTS everywhere
-- ============================================================

-- ------------------------------------------------------------
-- 1. EIA API — energy_prices
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_prices (
    id          SERIAL PRIMARY KEY,
    period      VARCHAR(20)  NOT NULL,
    area        TEXT,
    product     TEXT,
    process     TEXT,
    series      VARCHAR(50)  NOT NULL,
    ingested_at TIMESTAMP    DEFAULT NOW(),
    CONSTRAINT uq_energy_prices UNIQUE (period, series)
);

-- ------------------------------------------------------------
-- 2. Alpha Vantage — commodity_spot_prices
--    Commodities: WTI, NATURAL_GAS, COPPER (monthly)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commodity_spot_prices (
    id          SERIAL PRIMARY KEY,
    commodity   VARCHAR(50)  NOT NULL,
    date        VARCHAR(20)  NOT NULL,
    price       NUMERIC(12, 4),
    unit        VARCHAR(100),
    ingested_at TIMESTAMP    DEFAULT NOW(),
    CONSTRAINT uq_commodity_spot UNIQUE (commodity, date)
);

-- ------------------------------------------------------------
-- 3. GlobalPetrolPrices scraper — fuel_prices
--    One row per country / fuel type / update date
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fuel_prices (
    id          SERIAL PRIMARY KEY,
    country     VARCHAR(100) NOT NULL,
    fuel_type   VARCHAR(50)  NOT NULL,
    price_usd   NUMERIC(8, 4),
    updated_at  DATE         NOT NULL,
    ingested_at TIMESTAMP    DEFAULT NOW(),
    CONSTRAINT uq_fuel_prices UNIQUE (country, fuel_type, updated_at)
);

-- ------------------------------------------------------------
-- 4. NewsAPI — energy_news
--    url is the natural dedup key (unique per article)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_news (
    id           SERIAL PRIMARY KEY,
    title        TEXT,
    source_name  VARCHAR(200),
    published_at TIMESTAMP,
    url          TEXT         UNIQUE,
    content      TEXT,
    ingested_at  TIMESTAMP    DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Indexes for common query patterns (dashboards + monitoring)
-- ------------------------------------------------------------

-- energy_prices: time-series queries
CREATE INDEX IF NOT EXISTS idx_energy_prices_period
    ON energy_prices (period);

-- commodity_spot_prices: per-commodity time series
CREATE INDEX IF NOT EXISTS idx_commodity_date
    ON commodity_spot_prices (commodity, date);

-- fuel_prices: country rankings
CREATE INDEX IF NOT EXISTS idx_fuel_country
    ON fuel_prices (country, fuel_type);

-- energy_news: latest-first feed
CREATE INDEX IF NOT EXISTS idx_news_published
    ON energy_news (published_at DESC);

-- pipeline freshness monitoring (used by Metabase dashboard 5)
CREATE INDEX IF NOT EXISTS idx_energy_prices_ingested
    ON energy_prices (ingested_at);
CREATE INDEX IF NOT EXISTS idx_commodity_ingested
    ON commodity_spot_prices (ingested_at);
CREATE INDEX IF NOT EXISTS idx_fuel_ingested
    ON fuel_prices (ingested_at);
CREATE INDEX IF NOT EXISTS idx_news_ingested
    ON energy_news (ingested_at);