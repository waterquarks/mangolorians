import sqlite3
import psycopg2
import psycopg2.extras


def historical_liquidity(instrument, account=None):
    state = sqlite3.connect(':memory:')

    state.row_factory = sqlite3.Row

    state.execute('drop table if exists orders')

    state.execute("""
        create table if not exists orders (
            orderId text,
            clientId text,
            side text,
            price numeric,
            size numeric,
            account text,
            accountSlot numeric,
            eventTimestamp text,
            primary key (side, orderId)
        ) without rowid
    """)

    state.execute('drop table if exists liquidity')

    state.execute("""
        create table if not exists liquidity (
            market text,
            slot integer,
            timestamp text,
            buy real default 0,
            sell real default 0,
            primary key (market, slot, timestamp)
        ) without rowid
    """)

    db = psycopg2.connect('dbname=mangolorians', cursor_factory=psycopg2.extras.RealDictCursor)

    cur = db.cursor()

    cur.execute("""
        with
            var(market, account) as (
                values(%s, %s)
            ),
            snapshots as (
                select
                    market,
                    slot,
                    "timestamp",
                    coalesce(
                            (
                                select
                                    json_agg(element)
                                from json_array_elements(message::json->'bids') element
                                where element->>'account' = (select account from var)
                            ), json_build_array()
                        ) as bids,
                    coalesce(
                            (
                                select
                                    json_agg(element)
                                from json_array_elements(message::json->'asks') element
                                where element->>'account' = (select account from var)
                            ), json_build_array()
                        ) as asks
                from alt
                where market = (select market from var)
                  and type = 'l3snapshot'
                order by slot
            ),
            deltas as (
                select
                    market,
                    slot,
                    "timestamp",
                    json_agg(message::json) as messages
                from alt
                where market = (select market from var)
                  and type in ('open', 'done')
                  and account = (select account from var)
                group by market, slot, "timestamp"
                order by slot
            )
        select
            market,
            slot,
            "timestamp",
            json_build_array(json_build_object('type', 'l3snapshot', 'bids', bids, 'asks', asks)) as messages
        from snapshots
        union all select market, slot, "timestamp", messages from deltas
        order by slot;
    """, [instrument, account])

    for row in cur:
        for message in row['messages']:
            if message['type'] == 'l3snapshot':
                state.execute('delete from orders')

                for side in ['bids', 'asks']:
                    for order in message[side]:
                        keys = ['orderId', 'clientId', 'side', 'price', 'size', 'account', 'accountSlot', 'eventTimestamp']

                        order = {key: order[key] for key in keys}

                        try:
                            state.execute('insert into orders values (?, ?, ?, ?, ?, ?, ?, ?)', list(order.values()))
                        except sqlite3.DatabaseError as error:
                            print(error)

                            print(list(order.values()))


            if message['type'] == 'open':
                keys = ['orderId', 'clientId', 'side', 'price', 'size', 'account', 'accountSlot', 'eventTimestamp']

                order = {key: message[key] for key in keys}

                try:
                    state.execute('insert into orders values (?, ?, ?, ?, ?, ?, ?, ?)', list(order.values()))
                except sqlite3.DatabaseError as error:
                    print(error)

                    print(list(order.values()))

            if message['type'] == 'done':
                state.execute('delete from orders where side = ? and orderId = ?', [message['side'], message['orderId']])
        else:
            liquidity = {
                'market': row['market'],
                'slot': row['slot'],
                'timestamp': row['timestamp'],
                'buy': 0,
                'sell': 0,
            }

            for entry in state.execute('select side, sum(price * size) as liquidity from orders group by side'):
                liquidity[entry['side']] = entry['liquidity']

            state.execute("insert into liquidity (market, slot, timestamp, buy, sell) values (:market, :slot, :timestamp, :buy, :sell)", liquidity)
    else:
        return list(map(dict, state.execute("""
            select
                   market,
                   strftime('%Y-%m-%dT%H:%M:00Z', "timestamp") as minute,
                   round(avg(buy)) as buy,
                   round(avg(sell)) as sell
            from liquidity
            group by minute;
        """)));

