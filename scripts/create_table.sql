CREATE TABLE IF NOT EXISTS indonesian_stocks (
    date DATE,
    ticker VARCHAR(7),
    close FLOAT,
    high FLOAT,
    low FLOAT,
    open FLOAT,
    inserted_at_timestamp_wib TIMESTAMP,
    PRIMARY KEY (date, ticker)
);
