#!/bin/bash

psql -d mangolorians << EOF
begin;
drop table if exists trades;
create table trades (
    id integer,
    market text,
    price numeric,
    quantity numeric,
    maker text,
    maker_order_id text,
    maker_client_order_id text,
    taker text,
    taker_order_id text,
    taker_client_order_id text,
    maker_fee numeric,
    taker_fee numeric,
    taker_side text,
    "timestamp" timestamptz,
    primary key (market, id, "timestamp")
);
create index on trades (market, "timestamp");
commit;
EOF

psql -q postgres://waterquarks:IgWTTJNAY3JEkcy9@replica-event-history-maximilian-5ee2.a.timescaledb.io:25548/event-history << EOF | psql -d mangolorians -c "copy trades from stdin csv header"
copy (
    select "seqNum" as id
         , name as market
         , price
         , quantity
         , maker
         , "makerOrderId" as maker_order_id
         , "makerClientOrderId" as maker_client_order_isd
         , taker
         , "takerOrderId" as taker_order_id
         , "takerClientOrderId" as taker_client_order_id
         , "makerFee" as maker_fee
         , "takerFee" as taker_fee
         , "takerSide" as taker_side
         , "loadTimestamp" as "timestamp"
    from perp_event
    inner join perp_market_meta using (address)
    where "loadTimestamp" < date_trunc('day', current_timestamp at time zone 'utc')
    order by market, "seqNum"
) to stdout csv header;
EOF