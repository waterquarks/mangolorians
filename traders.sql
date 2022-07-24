copy ( 
    with
        spot_volumes_by_kind_and_instrument as (
            select "openOrders" as open_orders_account
                 , "baseCurrency" || '/' || "quoteCurrency" as instrument
                 , maker
                 , count("orderId") as trades_count
                 , round(sum(cast(case when bid then "nativeQuantityPaid" when not bid then "nativeQuantityReleased" end as bigint)) / 1e6) as volume
            from event
            left join owner using ("openOrders")
            where owner = '9BVcYqEQxyccuwznvxXqDkSJFavvTyheiTYk231T1A8S'
              and "quoteCurrency" = 'USDC'
              and fill
              and "loadTimestamp" > now() - interval '1 week'
            group by open_orders_account, instrument, maker
            order by open_orders_account desc
        ),
        spot_traders as (
            select instrument
                 , mango_account
                 , coalesce(sum(trades_count), 0) as trades_count
                 , coalesce(sum(case when not maker then volume end), 0) as taker_volume
                 , coalesce(sum(case when maker then volume end), 0) as maker_volume
                 , coalesce(sum(volume), 0) as total_volume
            from spot_volumes_by_kind_and_instrument
            inner join transactions_v3.open_orders_account using (open_orders_account)
            group by mango_account, instrument
            order by total_volume desc
        ),
        perp_takers as (
            select address
                 , taker as mango_account
                 , count(*) as taker_trades_count
                 , sum(price * quantity)::bigint as taker_volume
            from perp_event
            where "loadTimestamp" > now() - interval '1 week'
            group by address, taker
        ),
        perp_makers as (
            select address
                 , maker as mango_account
                 , count(*) as maker_trades_count
                 , sum(price * quantity)::bigint as maker_volume
            from perp_event
            where "loadTimestamp" > now() - interval '1 week'
            group by address, maker
        ),
        perp_traders as (
            select name as instrument
                 , mango_account as mango_account
                 , coalesce(taker_trades_count, 0) + coalesce(maker_trades_count, 0) as trades_count
                 , coalesce(taker_volume, 0) as taker_volume
                 , coalesce(maker_volume, 0) as maker_volume
                 , coalesce(taker_volume, 0) + coalesce(maker_volume, 0) as total_volume
            from perp_takers
                full join perp_makers using (address, mango_account)
                inner join perp_market_meta using (address)
            order by total_volume desc
        ),
        traders as (
            select * from spot_traders union all select * from perp_traders
        )
    select instrument,
           mango_account,
           owner as mango_account_owner, -- SOL Address
           delegate as mango_account_delegate, -- SOL address
           referrer as mango_account_referrer, -- Mango account
           trades_count,
           taker_volume,
           maker_volume,
           total_volume
    from traders
    left join transactions_v3.mango_account_owner_distinct using (mango_account)
    left join transactions_v3.mango_account_delegate_current using (mango_account)
    left join transactions_v3.mango_account_referrer_current on mango_account_referrer_current.referree = mango_account
    order by total_volume desc
) to stdout csv header;
