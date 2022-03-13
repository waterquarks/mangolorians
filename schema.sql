drop table if exists order_book;
drop table if exists liquidity;
drop view if exists average_liquidity_per_minute;
drop table if exists slippages;
drop view if exists average_slippages_per_minute;
drop table if exists trades;
drop table if exists funding_rates;

create table order_book
(
    exchange        text,
    symbol          text,
    timestamp       bigint,
    local_timestamp bigint,
    is_snapshot     boolean,
    side            text,
    price           numeric,
    amount          numeric
);

create table liquidity
(
    exchange,
    symbol,
    buy,
    sell,
    timestamp
);

create view average_liquidity_per_minute as
with
    entries as (
        select
           exchange,
           symbol,
           round(buy) as buy,
           round(sell) as sell,
           cast(strftime('%s', datetime(strftime('%Y-%m-%dT%H:%M:00', datetime(timestamp / 1e6, 'unixepoch')))) as integer)  as timestamp
        from liquidity
    )
select
    exchange,
    symbol,
    round(avg(buy)) as buy,
    round(avg(sell)) as sell,
    timestamp
from entries
group by exchange, symbol, timestamp
order by "timestamp";

create table slippages
(
  exchange,
  symbol,
  "buy_50K",
  "buy_100K",
  "buy_200K",
  "buy_500K",
  "buy_1M",
  "sell_50K",
  "sell_100K",
  "sell_200K",
  "sell_500K",
  "sell_1M",
  timestamp
);

create view average_slippages_per_minute as
with
    entries as (
        select
            exchange,
            symbol,
            buy_50K,
            buy_100K,
            buy_200K,
            buy_500K,
            buy_1M,
            sell_50K,
            sell_100K,
            sell_200K,
            sell_500K,
            sell_1M,
            cast(strftime('%s', datetime(strftime('%Y-%m-%dT%H:%M:00', datetime(timestamp / 1e6, 'unixepoch')))) as integer)  as timestamp
        from slippages
    )
select
    exchange,
    symbol,
    avg(buy_50K) as buy_50K,
    avg(buy_100K) as buy_100K,
    avg(buy_200K) as buy_200K,
    avg(buy_500K) as buy_500K,
    avg(buy_1M) as buy_1M,
    avg(sell_50K) as sell_50K,
    avg(sell_100K) as sell_100K,
    avg(sell_200K) as sell_200K,
    avg(sell_500K) as sell_500K,
    avg(sell_1M) as sell_1M,
    timestamp
from entries
group by exchange, symbol, timestamp
order by "timestamp";

create table trades (
    exchange text,
    symbol text,
    timestamp numeric,
    taker text,
    taker_order text,
    taker_client_order_id text,
    maker text,
    maker_order text,
    maker_client_order_id text,
    side text,
    price numeric,
    amount numeric
);

create table funding_rates (
    exchange text,
    symbol text,
    rate text,
    open_interest text,
    "from" text,
    "to" text
)