import psycopg2
import psycopg2.extras
import websockets
import json
import asyncio


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('create schema if not exists mango')

    cur.execute('drop table if exists mango.orders')

    cur.execute("""
        create table if not exists mango.orders (
            market text,
            account text,
            side text,
            id text,
            client_id text,
            price real,
            size real,
            added_at timestamptz,
            primary key (market, side, id)
        )
    """)

    conn.commit()

    async for connection in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
        try:
            message = {
                'op': 'subscribe',
                'channel': 'level3',
                'markets': [
                    'BTC-PERP',
                    'SOL-PERP',
                    'MNGO-PERP',
                    'ADA-PERP',
                    'AVAX-PERP',
                    'BNB-PERP',
                    'ETH-PERP',
                    'FTT-PERP',
                    'LUNA-PERP',
                    'MNGO-PERP',
                    'RAY-PERP',
                    'SRM-PERP'
                ]
            }

            await connection.send(json.dumps(message))

            trail = {}

            async for response in connection:
                entry = json.loads(response)

                if entry['type'] not in {'l3snapshot', 'open', 'done'}:
                    continue

                lag = trail.get(entry['market']) or None

                if (lag is not None) and (entry['slot'] > lag['slot']):
                    cur.execute("""
                        select
                            account,
                            market,
                            target_liquidity,
                            target_spread
                        from tranches
                        inner join target_spreads using (market)
                        where market = %s
                    """, [entry['market']])

                    for account, market, target_liquidity, target_spread in cur.fetchall():
                        cur.execute("""
                            insert into spreads
                            with
                                orders as (
                                    select
                                        side,
                                        price,
                                        sum(size) as size,
                                        price * sum(size) as volume,
                                        sum(price * sum(size)) over (partition by side order by case when side = 'buy' then - price when side = 'sell' then price end) as cumulative_volume
                                    from mango.orders
                                    where market = %(market)s and account = %(account)s
                                    group by side, price
                                    order by side, case when side = 'buy' then - price when side = 'sell' then price end
                                ),
                                fills as (
                                    select
                                        side, price, fill, sum(fill) over (partition by side order by case when side = 'buy' then - price when side = 'sell' then price end) as cumulative_fill
                                    from (
                                        select
                                            side,
                                            price,
                                            case when cumulative_volume < %(target_liquidity)s then volume else coalesce(lag(remainder) over (partition by side), case when volume < %(target_liquidity)s then volume else %(target_liquidity)s end) end as fill
                                        from (select *, %(target_liquidity)s - cumulative_volume as remainder from orders) as alpha
                                    ) as beta
                                    where fill > 0
                                ),
                                weighted_average_quotes as (
                                    select
                                        case when sum(case when side = 'buy' then fill end) = %(target_liquidity)s then sum(case when side = 'buy' then price * fill end) / %(target_liquidity)s end as weighted_average_bid,
                                        case when sum(case when side = 'sell' then fill end) = %(target_liquidity)s then sum(case when side = 'sell' then price * fill end) / %(target_liquidity)s end as weighted_average_ask
                                    from fills
                                ),
                                spreads as (
                                    select
                                        weighted_average_bid,
                                        weighted_average_ask,
                                        weighted_average_ask - weighted_average_bid as absolute_spread,
                                        ((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 1e2 as relative_spread
                                    from weighted_average_quotes
                                )
                            select
                                %(market)s as market,
                                %(account)s as account,
                                weighted_average_bid,
                                weighted_average_ask,
                                absolute_spread,
                                relative_spread,
                                absolute_spread is not null as active,
                                relative_spread <= %(target_spread)s as compliant,
                                %(slot)s as slot,
                                %(created_at)s as created_at
                            from spreads
                        """, {
                            'account': account,
                            'market': market,
                            'target_liquidity': target_liquidity,
                            'target_spread': target_spread,
                            'slot': lag['slot'],
                            'created_at': lag['timestamp']
                        })

                        cur.execute("""
                            insert into liquidity
                            select
                                %(market)s as market,
                                %(account)s as account,
                                sum(price * size) filter (where side = 'buy') as buy,
                                sum(price * size) filter (where side = 'sell') as sell,
                                %(slot)s as slot,
                                %(created_at)s as created_at
                            from mango.orders
                            where market = %(market)s and account = %(account)s
                        """, {
                            'account': account,
                            'market': market,
                            'slot': lag['slot'],
                            'created_at': lag['timestamp']
                        })

                if entry['type'] == 'l3snapshot':
                    cur.execute('delete from mango.orders where market = %s', [entry['market']])

                    for side in {'bids', 'asks'}:
                        for order in entry[side]:                            cur.execute(
                                'insert into mango.orders values (%s, %s, %s, %s, %s, %s, %s, %s)',
                                [entry['market'], order['account'], order['side'], order['orderId'], order['clientId'], order['price'], order['size'], order['eventTimestamp']]
                            )

                    conn.commit()

                if entry['type'] == 'open':
                    cur.execute(
                        'insert into mango.orders values (%s, %s, %s, %s, %s, %s, %s, %s)',
                        [entry['market'], entry['account'], entry['side'], entry['orderId'], entry['clientId'], entry['price'], entry['size'], entry['eventTimestamp']]
                    )

                    conn.commit()

                if entry['type'] == 'done':
                    cur.execute(
                        'delete from mango.orders where market = %s and side = %s and id = %s',
                        [entry['market'], entry['side'], entry['orderId']]
                    )

                    conn.commit()

                trail[entry['market']] = entry
        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())