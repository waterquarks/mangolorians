from lib import streams
import asyncio
import psycopg2


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    markets = [
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
        'SRM-PERP',
        'GMT-PERP'
    ]

    cur.execute("""
        create table if not exists native.messages (
            exchange text,
            channel text,
            message jsonb,
            local_timestamp timestamptz
        ) partition by list (exchange);
    """)

    cur.execute("""
        create table if not exists "native.messages_mango-markets"
            partition of native.messages for values in ('mango-markets')
            partition by list (channel);
    """)

    cur.execute("""
        create table if not exists "native.messages_mango-markets_l2"
            partition of "native.messages_mango-markets" for values in ('l2')
            partition by range (local_timestamp);
    """)

    cur.execute("""
        create table if not exists "native.messages_mango-markets_l2"
            partition of "native.messages_mango-markets" for values in ('l2')
            partition by range (local_timestamp);
    """)


    cur.execute("""
        create table if not exists "native.messages"
    """)

    async for message in streams.mango_markets_l2_order_book(markets):
        print(message)


if __name__ == '__main__':
    asyncio.run(main())