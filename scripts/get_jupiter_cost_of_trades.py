import asyncio
import itertools

import aiohttp
import psycopg2


async def main():
    symbols = ["SOL", "MSOL", "AVAX", "SRM", "BNB", "ETH", "BTC", "RAY", "GMT", "USDT", "FTT", "MNGO"]

    order_sizes = [1, 1000, 10000, 25000]

    product = list(itertools.product(symbols, order_sizes))

    urls = [f"https://price.jup.ag/v1/price?id={symbol}&vsAmount={order_size}" for symbol, order_size in product][0:3]

    async with aiohttp.ClientSession() as session:
        async def fetch(url):
            response = await session.get(url)

            data = await response.json()

            print(data)





if __name__ == '__main__':
    asyncio.run(main())