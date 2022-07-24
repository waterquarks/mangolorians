copy (
    with
        spot_trades as (
            select mango_account
                 , "baseCurrency" || '/' || "quoteCurrency" as instrument
                 , case when maker then 'maker' when not maker then 'taker' end as liquidity_type
                 , abs(cast(case when bid then "nativeQuantityPaid" when not bid then "nativeQuantityReleased" end as bigint) / 1e6) as volume
                 , "loadTimestamp" as created_at
            from event
            left join owner using ("openOrders")
            left join transactions_v3.open_orders_account on open_orders_account = "openOrders"
            where owner = '9BVcYqEQxyccuwznvxXqDkSJFavvTyheiTYk231T1A8S'
              and "quoteCurrency" = 'USDC'
              and fill
        ),
        perp_taker_trades as (
            select taker as mango_account
                 , name as instrument
                 , 'taker' as liquidity_type
                 , abs(price * quantity) as volume
                 , "loadTimestamp" as created_at
            from perp_event
            inner join perp_market_meta using (address)
        ),
        perp_maker_trades as (
            select maker as mango_account
                 , name as instrument
                 , 'maker' as liquidity_type
                 , abs(price * quantity) as volume
                 , "loadTimestamp" as created_at
            from perp_event
            inner join perp_market_meta using (address)
        ),
        perp_trades as (
            select * from perp_maker_trades
            union all
            select * from perp_taker_trades
        ),
        trades as (
            select * from spot_trades
            union all
            select * from perp_trades
        )
    select mango_account,
           owner as mango_account_owner,
           delegate as mango_account_delegate,
           referrer as mango_account_referrer,
           instrument,
           liquidity_type,
           volume,
           created_at
    from trades
    left join transactions_v3.mango_account_owner_distinct using (mango_account)
    left join transactions_v3.mango_account_delegate_current using (mango_account)
    left join transactions_v3.mango_account_referrer_current on mango_account_referrer_current.referree = mango_account
    where created_at < date_trunc('day', current_timestamp)
) to stdout csv header;