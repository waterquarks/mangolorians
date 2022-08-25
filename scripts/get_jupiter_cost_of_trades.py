import asyncio
import itertools

import aiohttp
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('create schema if not exists raw')

    cur.execute("""
        create table if not exists raw.jupiter_cost_of_trades (
            url text,
            status numeric,
            response jsonb,
            local_timestamp timestamptz
        )
    """)

    conn.commit()

    symbols = ["SOL", "MSOL", "SRM", "BNB", "ETH", "BTC", "RAY", "GMT", "USDT", "FTT", "MNGO"]

    order_sizes = [
        1000,
        10000,
        25000,
        50000,
        100000
    ]

    product = itertools.product(symbols, order_sizes)

    url = 'https://price.jup.ag/v1/price'

    queries = [{'id': symbol, 'vsAmount': order_size} for symbol, order_size in product]

    async with aiohttp.ClientSession() as session:
        responses = await asyncio.gather(*[session.get(url, params=params) for params in queries])

        local_timestamp = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')

        data = [[str(response.url), response.status, await response.text(), local_timestamp] for response in responses]

        cur.executemany('insert into raw.jupiter_cost_of_trades values (%s, %s, %s, %s)', data)

        conn.commit()


if __name__ == '__main__':
    asyncio.run(main())
