from lib import streams
import asyncio
import psycopg2


async def main():
    db = psycopg2.connect('dbname=mangolorians')

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

    async for message in streams.mango_markets_l2_order_book(markets):
        db.execute('insert into nati')


if __name__ == '__main__':
    asyncio.run(main())