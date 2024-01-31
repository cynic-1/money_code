DROP schema IF EXISTS api CASCADE;
create schema api;
DROP ROLE IF EXISTS prices_api;
create role prices_api noinherit login password '1234qwer';
grant usage on schema api to prices_api;

CREATE OR REPLACE view api.mexc_top_tokens_vs_btc as 
    select * from prices_8h 
    where exchange = 'mexc'
    and timestamp = (select max(timestamp) from prices_8h) 
    and btc_ema_12 > btc_ema_144 and btc_ema_12 > btc_ema_169 
    and btc_ema_144 > btc_ema_576 and btc_ema_169 > btc_ema_676  
    and count > 676
    ORDER BY btc_ema_12/btc_ema_144 DESC;
grant SELECT on api.mexc_top_tokens_vs_btc to prices_api;

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