CREATE TABLE IF NOT EXISTS energy_prices (
    id          SERIAL PRIMARY KEY,
    series_id   VARCHAR(100) NOT NULL,
    period      DATE         NOT NULL,
    value       NUMERIC(12, 4),
    unit        VARCHAR(50),
    ingested_at TIMESTAMP    DEFAULT NOW(),
    CONSTRAINT uq_series_period UNIQUE (series_id, period)
);

CREATE INDEX IF NOT EXISTS idx_energy_period  ON energy_prices(period);
CREATE INDEX IF NOT EXISTS idx_energy_series  ON energy_prices(series_id);