#!/bin/bash

psql -d mangolorians << EOF
begin;
drop table if exists perp_liquidations;
create table perp_liquidations (
    market text,
    id integer,
    price numeric,
    quantity numeric,
    liquidatee text,
    liquidator text,
    liquidation_fee numeric,
    "timestamp" timestamptz,
    primary key (market, id)
);
create index on perp_liquidations (market, "timestamp");
commit;
EOF

psql -q postgres://waterquarks:AVNS_t_f_PhdRlTj0wGz@replica-mango-stats-maximilian-5ee2.a.timescaledb.io:25548/trade-history << EOF | psql -d mangolorians -c "copy perp_liquidations from stdin csv header"
copy (
  select name as market
       , "seqNum" as id
       , price
       , quantity
       , liqee as liquidatee
       , liqor as liquidator
       , "liquidationFee" as liquidation_fee
       , "loadTimestamp" as "timestamp"
  from perp_liquidation_event
  inner join perp_market_meta using (address)
  where "loadTimestamp" < date_trunc('day', current_timestamp at time zone 'utc')
) to stdout csv header;
EOF