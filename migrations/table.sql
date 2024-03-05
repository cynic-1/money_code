CREATE USER cypher WITH PASSWORD 'qwer1234';
ALTER USER cypher WITH SUPERUSER;

CREATE TABLE IF NOT EXISTS prices_8h
    (
        symbol                TEXT                            NOT NULL,
        exchange              TEXT                            NOT NULL,
        timestamp             BIGINT                          NOT NULL, 
        open                  NUMERIC                         NOT NULL,
        high                  NUMERIC                         NOT NULL,
        low                   NUMERIC                         NOT NULL,
        close                 NUMERIC                         NOT NULL,
        vbtc                  NUMERIC                         NOT NULL,    
        usd_ema_12            NUMERIC,
        usd_ema_144           NUMERIC,
        usd_ema_169           NUMERIC,
        usd_ema_576           NUMERIC,
        usd_ema_676           NUMERIC,
        btc_ema_12            NUMERIC,
        btc_ema_144           NUMERIC,
        btc_ema_169           NUMERIC,
        btc_ema_576           NUMERIC,
        btc_ema_676           NUMERIC,
        count                 INT                            NOT NULL,
    CONSTRAINT unique_symbol_exchange_timestamp UNIQUE (exchange, symbol, timestamp)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS exchange_time_symbol ON prices_8h (exchange, timestamp, symbol);

    CREATE TABLE IF NOT EXISTS token_info
    (
        symbol                TEXT                            NOT NULL,
        exchange              TEXT                            NOT NULL,
        full_name             TEXT                            NOT NULL, 
        latest_timestamp      BIGINT                          NOT NULL,
        CONSTRAINT unique_symbol_exchange UNIQUE (symbol, exchange)
    );