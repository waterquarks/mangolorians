import psycopg2
from psycopg2 import sql
from pathlib import Path
import sys
import os


def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor('cur')

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
        'SRM-PERP'
    ]

    for market in markets:
        path = Path(__file__).parent.parent / 'data' / 'liquidations'

        path.mkdir(parents=True, exist_ok=True)

        with open(path / f"{market}.csv", 'w') as file:
            cur.copy_expert(sql.SQL("copy (select * from perp_liquidations where market = {market}) to stdout csv header").format(market=sql.Literal(market)), file)

if __name__ == '__main__':
    main()