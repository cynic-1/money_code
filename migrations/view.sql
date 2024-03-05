DROP schema IF EXISTS api CASCADE;
create schema api;
DROP ROLE IF EXISTS prices_api;
create role prices_api noinherit login password '1234qwer';
grant usage on schema api to prices_api;

CREATE OR REPLACE view api.mexc_top_tokens_vs_btc as 
    select token_info.symbol, token_info.exchange, token_info.full_name, prices_8h.timestamp, prices_8h.open, prices_8h.high, prices_8h.low, prices_8h.close, prices_8h.vbtc, 
    prices_8h.usd_ema_12, prices_8h.usd_ema_144, prices_8h.usd_ema_169,prices_8h.usd_ema_576, prices_8h.usd_ema_676, prices_8h.btc_ema_12, prices_8h.btc_ema_144, prices_8h.btc_ema_169, prices_8h.btc_ema_576    
    from token_info 
    INNER join prices_8h 
    on token_info.symbol = prices_8h.symbol
        AND token_info.exchange = prices_8h.exchange
        AND token_info.latest_timestamp = prices_8h.timestamp
    where token_info.exchange='mexc'
        and prices_8h.btc_ema_12 > prices_8h.btc_ema_144 and prices_8h.btc_ema_12 > prices_8h.btc_ema_169
        and prices_8h.btc_ema_144 > prices_8h.btc_ema_576 and prices_8h.btc_ema_169 > prices_8h.btc_ema_676  
    and prices_8h.count > 676;
grant SELECT on api.mexc_top_tokens_vs_btc to prices_api;

CREATE OR REPLACE view api.gate_top_tokens_vs_btc as 
    select token_info.symbol, token_info.exchange, token_info.full_name, prices_8h.timestamp, prices_8h.open, prices_8h.high, prices_8h.low, prices_8h.close, prices_8h.vbtc, 
    prices_8h.usd_ema_12, prices_8h.usd_ema_144, prices_8h.usd_ema_169,prices_8h.usd_ema_576, prices_8h.usd_ema_676, prices_8h.btc_ema_12, prices_8h.btc_ema_144, prices_8h.btc_ema_169, prices_8h.btc_ema_576    
    from token_info 
    INNER join prices_8h 
    on token_info.symbol = prices_8h.symbol
        AND token_info.exchange = prices_8h.exchange
        AND token_info.latest_timestamp = prices_8h.timestamp
    where token_info.exchange='gate'
        and prices_8h.btc_ema_12 > prices_8h.btc_ema_144 and prices_8h.btc_ema_12 > prices_8h.btc_ema_169
        and prices_8h.btc_ema_144 > prices_8h.btc_ema_576 and prices_8h.btc_ema_169 > prices_8h.btc_ema_676  
        and prices_8h.count > 676
        and prices_8h.symbol NOT LIKE '%3L'
        and prices_8h.symbol NOT LIKE '%5L';
grant SELECT on api.gate_top_tokens_vs_btc to prices_api;

CREATE OR REPLACE view api.gate_top_tokens_vs_btc_short_term as 
    select token_info.symbol, token_info.exchange, token_info.full_name, prices_8h.timestamp, prices_8h.open, prices_8h.high, prices_8h.low, prices_8h.close, prices_8h.vbtc, 
    prices_8h.usd_ema_12, prices_8h.usd_ema_144, prices_8h.usd_ema_169,prices_8h.usd_ema_576, prices_8h.usd_ema_676, prices_8h.btc_ema_12, prices_8h.btc_ema_144, prices_8h.btc_ema_169, prices_8h.btc_ema_576    
    from token_info 
    INNER join prices_8h 
    on token_info.symbol = prices_8h.symbol
        AND token_info.exchange = prices_8h.exchange
        AND token_info.latest_timestamp = prices_8h.timestamp
    where token_info.exchange='gate'
        and prices_8h.btc_ema_12 > prices_8h.btc_ema_144 and prices_8h.btc_ema_12 > prices_8h.btc_ema_169
        and prices_8h.count > 169;
grant SELECT on api.gate_top_tokens_vs_btc_short_term to prices_api;


CREATE OR REPLACE view api.top_tokens_vs_btc as 
    SELECT top_tokens.* FROM  (
    SELECT * FROM api.gate_top_tokens_vs_btc
    UNION ALL
    SELECT mexc.* FROM api.mexc_top_tokens_vs_btc as mexc
    LEFT JOIN api.gate_top_tokens_vs_btc as gate
    ON mexc.full_name = gate.full_name
    WHERE gate.full_name is NULL) as top_tokens
    ORDER BY top_tokens.btc_ema_12 - top_tokens.btc_ema_144
    ASC;
grant SELECT on api.top_tokens_vs_btc to prices_api;

CREATE OR REPLACE view api.mexc_top_tokens_from_atl as 
    WITH latest_prices as (
        select symbol, vbtc from prices_8h
        WHERE exchange = 'mexc' and timestamp > (1706659200000 - 1000::bigint * 3600 * 24 * 365)),
    latest_vbtc as (
        select symbol, vbtc from prices_8h
        WHERE exchange = 'mexc' and timestamp = 1706659200000 AND count > 676),
    min_vbtc as (SELECT symbol, min(vbtc) as min_vbtc
        FROM latest_prices
        GROUP BY symbol)
    SELECT latest_vbtc.symbol, latest_vbtc.vbtc/min_vbtc.min_vbtc as times_from_atl
    FROM latest_vbtc
    INNER JOIN min_vbtc
    ON latest_vbtc.symbol = min_vbtc.symbol
    ORDER BY latest_vbtc.vbtc/min_vbtc.min_vbtc DESC
    LIMIT 100;
grant SELECT on api.mexc_top_tokens_from_atl to prices_api;

CREATE OR REPLACE view api.mexc_top_tokens_from_ath as 
    WITH max_vbtc as (
        select symbol, max(vbtc) as max_vbtc 
        from prices_8h
        WHERE exchange = 'mexc'
        GROUP BY symbol),
    latest_vbtc as (
        select symbol, vbtc from prices_8h
        WHERE exchange = 'mexc' and timestamp = 1706659200000 AND count > 676)
    SELECT latest_vbtc.symbol, latest_vbtc.vbtc/max_vbtc.max_vbtc as times_from_ath, latest_vbtc.vbtc, max_vbtc.max_vbtc 
    FROM latest_vbtc
    INNER JOIN max_vbtc
    ON latest_vbtc.symbol = max_vbtc.symbol
    ORDER BY latest_vbtc.vbtc/max_vbtc.max_vbtc DESC
    LIMIT 100;
grant SELECT on api.mexc_top_tokens_from_ath to prices_api;
