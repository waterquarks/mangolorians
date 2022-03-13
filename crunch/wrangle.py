import sqlite3
from datetime import datetime
from typing import Literal, Dict, Optional
from time import time


def reconstruct_order_book(symbol, timestamp=int(time() * 1e6), depth=None):
    db = sqlite3.connect('mangolorians.db')

    db.row_factory = sqlite3.Row

    cur = db.cursor()

    cur.execute(
        """select * from order_book where symbol = ? and "timestamp" <= ? order by local_timestamp""",
        (symbol, timestamp)
    )

    order_book = {
        'exchange': 'Mango Markets',
        'symbol': symbol,
        'timestamp': datetime.utcfromtimestamp(timestamp / 1e6).isoformat(),
        'bids': {},
        'asks': {}
    }

    previous = None

    for current in cur.fetchall():
        if previous is not None:
            if current["is_snapshot"] and not previous["is_snapshot"]:
                order_book['bids'] = {}
                order_book['asks'] = {}

        side = {
            'bid': 'bids',
            'ask': 'asks'
        }[current['side']]

        if current['amount'] == 0:
            del order_book[side][current['price']]
        else:
            order_book[side][current['price']] = current['amount']

        previous = current

    order_book['bids'] = [list(order) for order in sorted(list(order_book['bids'].items()), reverse=True)]

    order_book['asks'] = [list(order) for order in sorted(list(order_book['asks'].items()))]

    if depth is not None:
        order_book['bids'] = order_book['bids'][:depth]

        order_book['asks'] = order_book['asks'][:depth]

    return order_book


def calculate_slippage(
        order_book,
        order_size: int,
        order_side: Literal['buy', 'sell'],
        method: Literal['average_fill_price', 'impact_price']
) -> Optional[float]:
    """
    Calculate slippage for an order size of an order book's side, by a method specified.
    :param order_size: Can only be positive - the function will return None otherwise.
    :param order_side: Either 'buy' or 'sell'.
    :param method: 'average_fill_price' or 'impact_price'
    :param order_book:
    :return: None if the order size isn't positive or exceeds available liquidity, else returns corresponding slippage.
    """
    orders = {
        'buy': order_book["asks"],
        'sell': order_book["bids"]
    }[order_side]

    if order_size <= 0:
        return None

    fills: Dict[float, float] = {}

    cumulative_fills = 0

    for price, quantity in orders:
        fill = price * quantity

        to_be_filled: float = float(order_size - cumulative_fills)

        cumulative_fills += fill

        if cumulative_fills < order_size:
            fills[price] = fill
        else:
            fills[price] = to_be_filled

            break

    if cumulative_fills < order_size:
        return None

    best_bid_price = order_book["bids"][0][0]

    best_ask_price = order_book["asks"][0][0]

    mid_price = (best_bid_price + best_ask_price) / 2

    average_fill_price = sum([key * value for key, value in fills.items()]) / order_size

    impact_price = [*fills.keys()][-1]

    slippage = {
        'average_fill_price': {
            'buy': (average_fill_price - mid_price) / mid_price,
            'sell': (mid_price - average_fill_price) / average_fill_price
        }[order_side],
        'impact_price': {
            'buy': (impact_price - mid_price) / mid_price,
            'sell': (mid_price - impact_price) / impact_price
        }[order_side]
    }[method] * 100

    return slippage