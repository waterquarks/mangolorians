import sqlite3
from typing import Literal, Optional, Dict
import logging


def calculate(
        order_book,
        order_size: int,
        order_side: Literal['buy', 'sell'],
        method: Literal['average_fill_price', 'impact_price']
) -> Optional[float]:
    """
    Calculate slippage for an order size of an order book's side, by a method specified.
    :param order_book:
    :param order_size: Can only be positive - the function will return None otherwise.
    :param order_side: Either 'buy' or 'sell'.
    :param method: 'average_fill_price' or 'impact_price'
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


def transform(order_book):
    return {
        'exchange': order_book['exchange'],
        'symbol': order_book['symbol'],
        'buy_50K': calculate(order_book, 50000, 'buy', 'average_fill_price'),
        'buy_100K': calculate(order_book, 100000, 'buy', 'average_fill_price'),
        'buy_200K': calculate(order_book, 200000, 'buy', 'average_fill_price'),
        'buy_500K': calculate(order_book, 500000, 'buy', 'average_fill_price'),
        'buy_1M': calculate(order_book, 1000000, 'buy', 'average_fill_price'),
        'sell_50K': calculate(order_book, 50000, 'sell', 'average_fill_price'),
        'sell_100K': calculate(order_book, 100000, 'sell', 'average_fill_price'),
        'sell_200K': calculate(order_book, 200000, 'sell', 'average_fill_price'),
        'sell_500K': calculate(order_book, 500000, 'sell', 'average_fill_price'),
        'sell_1M': calculate(order_book, 1000000, 'sell', 'average_fill_price'),
        'timestamp': order_book['timestamp']
    }


def load(entry):
    query = """
        insert into slippages(
            exchange,
            symbol,
            buy_50K,
            buy_100K,
            buy_200K,
            buy_500K, 
            buy_1M,
            sell_50K,
            sell_100K,
            sell_200K,
            sell_500K,
            sell_1M,
            timestamp
        ) values (
            :exchange,
            :symbol,
            :buy_50K,
            :buy_100K,
            :buy_200K,
            :buy_500K,
            :buy_1M,
            :sell_50K,
            :sell_100K,
            :sell_200K,
            :sell_500K,
            :sell_1M,
            :timestamp
        )
    """

    try:
        db = sqlite3.connect('dev.db')

        db.row_factory = sqlite3.Row

        db.execute(query, entry)

        db.commit()
    except sqlite3.DatabaseError as error:
        logging.error(f"{error}: {query} | {entry}")
