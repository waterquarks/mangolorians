#!/bin/bash

psql -d mangolorians << EOF
begin;
drop table if exists perp_funding_rates;
create table perp_funding_rates (
    market text,
    avg_funding_rate numeric,
    avg_oracle_price numeric,
    avg_open_interest numeric,
    "hour" timestamptz,
    primary key (market, hour)
);
commit;
EOF

psql -q postgres://waterquarks:AVNS_t_f_PhdRlTj0wGz@replica-mango-stats-maximilian-5ee2.a.timescaledb.io:25548/mango_stats_v3 << EOF | psql -d mangolorians -c "copy perp_funding_rates from stdin csv header"
copy (
  with
    stats as (
        select "name"
             , first("longFunding", "time") as start_long_funding
             , first("shortFunding", "time") as start_short_funding
             , last("longFunding", "time") as end_long_funding
             , last("shortFunding", "time") as end_short_funding
             , avg("baseOraclePrice") as avg_base_oracle_price
             , avg("openInterest") / 2 as avg_open_interest
             , date_trunc('hour', "time") as "hour"
        from perp_market_stats
        where "mangoGroup" = 'mainnet.1'
        group by "name", "hour"
        order by "name", "hour" desc
    ),
    stats_with_funding as (
        select "name"
             , (start_long_funding + start_short_funding) / 2 as start_funding
             , (end_long_funding + end_short_funding) / 2 as end_funding
             , avg_base_oracle_price
             , avg_open_interest
             , "hour"
        from stats
        order by "name", "hour" desc
    )
  select "name" as market
     , ((end_funding - start_funding) / avg_base_oracle_price)
           /
       case when name = 'ADA-PERP' then 1e4
            when name = 'AVAX-PERP' then 1e2
            when name = 'BNB-PERP' then 1e2
            when name = 'BTC-PERP' then 1
            when name = 'ETH-PERP' then 1e1
            when name = 'FTT-PERP' then 1e3
            when name = 'LUNA-PERP' then 1e2
            when name = 'MNGO-PERP' then 1e4
            when name = 'RAY-PERP' then 1e3
            when name = 'SOL-PERP' then 1e2
            when name = 'SRM-PERP' then 1e3
       end as funding_rate
     , avg_base_oracle_price
     , avg_open_interest
           /
       case
         when name = 'ADA-PERP' then 1
         when name = 'AVAX-PERP' then 1e2
         when name = 'BNB-PERP' then 1e3
         when name = 'BTC-PERP' then 1e4
         when name = 'ETH-PERP' then 1e3
         when name = 'FTT-PERP' then 1e1
         when name = 'LUNA-PERP' then 1e2
         when name = 'MNGO-PERP' then 1
         when name = 'RAY-PERP' then 1e1
         when name = 'SOL-PERP' then 1e2
         when name = 'SRM-PERP' then 1e1
       end
     , hour
  from stats_with_funding
  where "hour" <= date_trunc('day', current_timestamp at time zone 'utc')
  order by market, "hour" desc
) to stdout csv header;
EOF