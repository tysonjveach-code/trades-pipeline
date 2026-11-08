-- Run this once to create the tables:
--   psql -d trades_db -f sql/schema.sql

-- One row per time you run the pipeline on a file.
CREATE TABLE IF NOT EXISTS load_runs (
    run_id       SERIAL PRIMARY KEY,
    file_name    TEXT NOT NULL,
    file_hash    TEXT NOT NULL UNIQUE,      -- UNIQUE = same file can't be loaded twice
    started_at   TIMESTAMP DEFAULT NOW(),
    rows_loaded  INT,
    rows_cleaned INT,
    rows_failed  INT,
    rows_skipped INT
);

-- The clean, good data. This is where your trades end up.
CREATE TABLE IF NOT EXISTS trades (
    trade_id    TEXT PRIMARY KEY,           -- PRIMARY KEY = same trade can't be stored twice
    symbol      TEXT NOT NULL,
    side        TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price       NUMERIC(12, 4) NOT NULL CHECK (price > 0),
    quantity    INT NOT NULL CHECK (quantity > 0),
    trade_time  TIMESTAMP NOT NULL,
    run_id      INT REFERENCES load_runs(run_id)
);

-- Every row that failed, the original text, and why it failed.
CREATE TABLE IF NOT EXISTS load_errors (
    error_id    SERIAL PRIMARY KEY,
    run_id      INT REFERENCES load_runs(run_id),
    row_number  INT,
    raw_row     TEXT,
    reason      TEXT,
    logged_at   TIMESTAMP DEFAULT NOW()
);
