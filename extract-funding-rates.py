import csv
import os
from threading import Thread
from time import mktime

import mango
from mango.perpmarket import PerpMarket


def snapshot_funding_rates(context, market: PerpMarket):
    funding_rate = market.fetch_funding(context)

    file_path = f"/Users/ioaquine/Downloads/copper/funding_rates/{market.symbol}.csv"

    file_exists = os.path.exists(file_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'a') as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow(['exchange', 'symbol', 'funding_rate', 'open_interest', 'from', 'to'])

        row = [
            'Mango Markets',
            funding_rate.symbol,
            funding_rate.rate,
            funding_rate.open_interest,
            int(mktime(funding_rate.from_.timetuple()) * 1e6),
            int(mktime(funding_rate.to.timetuple()) * 1e6)
        ]

        print(row)

        writer.writerow(row)


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
            'MNGO-PERP',
            'RAY-PERP',
            'SRM-PERP'
        ]]

        [Thread(target=snapshot_funding_rates, args=(context, market)).start() for market in markets]
