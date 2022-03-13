import sqlite3
from threading import Thread

import mango
from mango.perpmarket import PerpMarket


def snapshot_funding_rates(context, market: PerpMarket):
    db = sqlite3.connect('/Users/ioaquine/Downloads/copper/dev.db')

    funding_rate = market.fetch_funding(context)

    funding_rate = {
        'exchange': 'Mango Markets',
        'symbol': funding_rate.symbol,
        'rate': str(funding_rate.rate),
        'open_interest': str(funding_rate.open_interest),
        'from': funding_rate.from_,
        'to': funding_rate.to
    }

    db.execute("""
        insert into
        funding_rates (
            exchange, symbol, rate, open_interest, "from", "to"
        )
        values (
            :exchange, :symbol, :rate, :open_interest, :from, :to
        )
    """, funding_rate)

    db.commit()

if __name__ == '__main__':
    with mango.ContextBuilder.build(cluster_name='mainnet') as context:
        markets = [mango.market(context, market) for market in [
            'BTC-PERP',
            'SOL-PERP',
            'MNGO-PERP',
            'ADA-PERP',
            'AVAX-PERP',
            'BNB-PERP',
            'ETH-PERP',
            'FTT-PERP',
            'LUNA-PERP',
            'RAY-PERP',
            'SRM-PERP'
        ]]

        [Thread(target=snapshot_funding_rates, args=(context, market)).start() for market in markets]
