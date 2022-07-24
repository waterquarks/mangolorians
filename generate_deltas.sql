create table deltas (
    symbol text,
    is_snapshot integer,
    side text,
    order_id text,
    account text,
    price real,
    amount real,
    slot integer,
    timestamp text,
    local_timestamp text
);

insert into deltas
with entries as (
    select
        json_extract(json(content), '$.type') = 'l3snapshot' as is_snapshot,
        json_extract(json(content), '$.market') as market,
        case
            when json_extract(json(content), '$.type') = 'l3snapshot'
                then (
                    select
                        json_group_array(
                            json_array(
                                case
                                    when json_extract(json(value), '$.side') = 'buy' then 'bids'
                                    when json_extract(json(value), '$.side') = 'sell' then 'asks'
                                end,
                                json_extract(json(value), '$.orderId'),
                                json_extract(json(value), '$.account'),
                                cast(json_extract(json(value), '$.price') as real),
                                cast(json_extract(json(value), '$.size') as real)
                            )
                        )
                    from
                        (
                            select value from json_each(json_extract(json(content), '$.bids'))
                            union all
                            select value from json_each(json_extract(json(content), '$.asks'))
                        )
                )
            when json_extract(json(content), '$.type') = 'open'
                then json_array(
                        json_array(
                            case
                                when json_extract(json(content), '$.side') = 'buy' then 'bids'
                                when json_extract(json(content), '$.side') = 'sell' then 'asks'
                            end,
                            json_extract(json(content), '$.orderId'),
                            json_extract(json(content), '$.account'),
                            cast(json_extract(json(content), '$.price') as real),
                            cast(json_extract(json(content), '$.size') as real)
                        )
                    )
            when json_extract(json(content), '$.type') = 'done'
                then json_array(
                    json_array(
                        case
                            when json_extract(json(content), '$.side') = 'buy' then 'bids'
                            when json_extract(json(content), '$.side') = 'sell' then 'asks'
                        end,
                        json_extract(json(content), '$.orderId'),
                        json_extract(json(content), '$.account'),
                        0,
                        0
                    )
                )
           end as orders
         , json_extract(json(content), '$.slot') as slot
         , json_extract(json(content), '$.timestamp') as timestamp
         , local_timestamp
    from messages
    where json_extract(json(content), '$.type') in ('l3snapshot', 'open', 'done')
    order by local_timestamp
)
select
    market,
    is_snapshot,
    json_extract(json(value), '$[0]') as side,
    json_extract(json(value), '$[1]') as order_id,
    json_extract(json(value), '$[2]') as account,
    json_extract(json(value), '$[3]') as price,
    json_extract(json(value), '$[4]') as amount,
    slot,
    timestamp,
    local_timestamp
from entries, json_each(orders)
order by local_timestamp;
