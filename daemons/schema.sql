create schema if not exists native;

create table if not exists native.orderbooks (
    exchange text,
    symbol text,
    message jsonb,
    local_timestamp timestamptz
);

create index on native.orderbooks (local_timestamp);

create index on native.orderbooks (exchange, symbol);

create table if not exists native.trades (
    exchange text,
    symbol text,
    message jsonb,
    local_timestamp timestamptz
);

create index on native.trades (local_timestamp);

create index on native.trades (exchange, symbol);