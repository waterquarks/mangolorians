begin;
drop view average_liquidity_per_minute;
drop view average_slippages_per_minute;
drop view latest_slippages;

create table liquidity_indexed (
                                   exchange text,
                                   symbol text,
                                   buy numeric,
                                   sell numeric,
                                   timestamp text,
                                   primary key (exchange, symbol, timestamp)
) without rowid;

insert into liquidity_indexed
select distinct
    exchange,
    symbol,
    buy,
    sell,
    strftime('%Y-%m-%dT%H:%M:%fZ', "timestamp" / 1e6, 'unixepoch') as timestamp
from liquidity;

drop table liquidity;

alter table liquidity_indexed rename to liquidity;

create table slippages_indexed
(
    exchange text,
    symbol text,
    buy_50K numeric,
    buy_100K numeric,
    buy_200K numeric,
    buy_500K numeric,
    buy_1M numeric,
    sell_50K numeric,
    sell_100K numeric,
    sell_200K numeric,
    sell_500K numeric,
    sell_1M numeric,
    timestamp text,
    primary key (exchange, symbol, timestamp)
)
    without rowid;

insert into slippages_indexed
select distinct
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
    strftime('%Y-%m-%dT%H:%M:%fZ', "timestamp" / 1e6, 'unixepoch') as timestamp
from slippages;

drop table slippages;

alter table slippages_indexed rename to slippages;

create table order_book_deltas
(
    exchange        text,
    symbol          text,
    timestamp       bigint,
    local_timestamp bigint,
    is_snapshot     boolean,
    side            text,
    price           numeric,
    amount          numeric,
    primary key (exchange, symbol, local_timestamp, side, price)
) without rowid;

insert into order_book_deltas
select distinct
    exchange,
    symbol,
    strftime('%Y-%m-%dT%H:%M:%fZ', "timestamp" / 1e6, 'unixepoch') as "timestamp",
    strftime('%Y-%m-%dT%H:%M:%fZ', local_timestamp / 1e6, 'unixepoch') as local_timestamp,
    is_snapshot,
    side,
    price,
    amount
from order_book;

drop table order_book;
commit;
