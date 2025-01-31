CREATE DATABASE "stonks";

\connect "stonks"

CREATE TABLE IF NOT EXISTS stocks (
    adj_close DECIMAL,
    adj_high DECIMAL,
    adj_low DECIMAL,
    adj_open DECIMAL,
    adj_volume NUMERIC ,
    close DECIMAL ,
    date TIMESTAMP WITH TIME ZONE ,
    dividend DECIMAL ,
    exchange VARCHAR(10) ,
    high DECIMAL ,
    low DECIMAL ,
    open DECIMAL ,
    split_factor DECIMAL ,
    symbol VARCHAR(10),
    volume NUMERIC ,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS tickers (
    name VARCHAR(255) NOT NULL,
    symbol VARCHAR(10) NOT NULL PRIMARY KEY,
    has_intraday BOOLEAN NOT NULL,
    has_eod BOOLEAN NOT NULL,
    country VARCHAR(100),
    stock_exchange_name VARCHAR(255) NOT NULL,
    stock_exchange_acronym VARCHAR(50) NOT NULL,
    stock_exchange_mic VARCHAR(50) NOT NULL,
    stock_exchange_country VARCHAR(100),
    stock_exchange_country_code VARCHAR(10),
    stock_exchange_city VARCHAR(100),
    stock_exchange_website VARCHAR(255)
);