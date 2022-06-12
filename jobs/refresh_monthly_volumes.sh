#!/bin/bash

psql -d mangolorians << EOF
begin;
drop table if exists monthly_volumes;
create table monthly_volumes as
with
    volumes_per_day as (
        select sum(price * quantity) as volume
             , extract(epoch from date_trunc('month', "timestamp")) * 1e3 as month
        from trades
        group by month
        order by month
    ),
    series as (
        select json_agg(json_build_array(month, volume)) as volumes
        from volumes_per_day
    )
select json_agg(json_build_object('name', 'Volumes', 'data', volumes)) from series;

drop table if exists monthly_volumes_by_instrument;
create table monthly_volumes_by_instrument as
with
    volumes_per_day as (
        select market
             , sum(price * quantity) as volume
             , extract(epoch from date_trunc('month', "timestamp")) * 1e3 as month
        from trades
        group by market, month
        order by market, month
    ),
    series as (
        select market
             , json_agg(json_build_array(month, volume)) as volumes
        from volumes_per_day
        group by market
    )
select json_agg(json_build_object('name', market, 'data', volumes)) from series;
end;
EOF