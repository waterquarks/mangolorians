#!/bin/bash

psql -q postgres://waterquarks:IgWTTJNAY3JEkcy9@replica-event-history-maximilian-5ee2.a.timescaledb.io:25548/event-history << EOF | psql -d mangolorians -c "copy perp_liquidations from stdin csv header"
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
  where "loadTimestamp" >= date_trunc('day', current_timestamp at time zone 'utc') - interval '1 day'
    and "loadTimestamp" < date_trunc('day', current_timestamp at time zone 'utc')
) to stdout csv header;
EOF