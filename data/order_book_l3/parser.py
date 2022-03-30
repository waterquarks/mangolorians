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
        select
            market,
            slot,
            timestamp,
            messages
        from order_book_l3_blocks
        where market = 'BTC-PERP'
        order by market, slot, timestamp;
    """, [instrument])

    for row in cur:
        for message in row['messages']:
            if message['type'] == 'l3snapshot':
                state.execute('delete from orders')

                for side in ['bids', 'asks']:
                    for order in message[side]:
                        if (account is not None):
                            if order['account'] != account:
                                continue

                        keys = ['orderId', 'clientId', 'side', 'price', 'size', 'account', 'accountSlot', 'eventTimestamp']

                        order = {key: order[key] for key in keys}

                        state.execute('insert into orders values (?, ?, ?, ?, ?, ?, ?, ?)', list(order.values()))

            if message['type'] == 'open':
                keys = ['orderId', 'clientId', 'side', 'price', 'size', 'account', 'accountSlot', 'eventTimestamp']

                order = {key: message[key] for key in keys}

                if account is not None:
                    if order['account'] != account:
                        continue

                state.execute('insert into orders values (?, ?, ?, ?, ?, ?, ?, ?)', list(order.values()))

            if message['type'] == 'done':
                if account is not None:
                    if message['account'] != account:
                        continue

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
        """)))


def main():
    entries = []

    for entry in historical_liquidity('GR8wiP7NfHR8nihLjiADRoRM7V7aUvtTfUnkBy8Zkd5T'):
        entries.append(dict(entry))

    return entries


if __name__ == '__main__':
    main()