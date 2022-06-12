#!/bin/bash

psql -d mangolorians << EOF
begin;
drop table if exists aprs;
create table aprs (
    symbol text,
    avg_deposit_rate_pct numeric,
    total_deposits numeric,
    avg_borrow_rate_pct numeric,
    total_borrows numeric,
    "hour" timestamptz,
    primary key (symbol, hour)
);
commit;
EOF

psql -q postgres://waterquarks:AVNS_t_f_PhdRlTj0wGz@replica-mango-stats-maximilian-5ee2.a.timescaledb.io:25548/mango_stats_v3 << EOF | psql -d mangolorians -c "copy aprs from stdin csv header"
copy (
    select name,
           avg("depositRate" * 100) as avg_deposit_rate_pct,
           avg("totalDeposits") as total_deposits,
           avg("borrowRate" * 100) as avg_borrow_rate_pct,
           avg("totalBorrows") as total_borrows,
           time_bucket('1 hour', "time") as hour
    from spot_market_stats
    where "mangoGroup" = 'mainnet.1'
    group by name, hour
    order by name, hour desc
) to stdout csv header;
EOF