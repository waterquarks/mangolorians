import json
import csv
import sqlite3
from calendar import timegm
from datetime import datetime
from time import time
from io import StringIO, BytesIO

from flask import Flask, jsonify, request, abort, render_template, redirect, send_file
from flask_cors import CORS

from crunch.wrangle import reconstruct_order_book, calculate_slippage

app = Flask(__name__)

app.config['JSON_SORT_KEYS'] = False

CORS(app)

@app.route('/')
def index():
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

    return render_template('./index.html', markets=markets)

@app.route('/analytics/', defaults={'market': None})
@app.route('/analytics/<market>')
def analytics(market):
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

    if market is None:
        return redirect('/analytics/SOL-PERP')

    db = sqlite3.connect('dev.db')
    db.row_factory = sqlite3.Row

    slippages = []

    for row in db.execute("""
        select
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
        from latest_slippages
    """):
        slippages.append(dict(row))

    return render_template('./analytics.html', markets=markets, market=market, slippages=slippages)

@app.route('/historical_data')
def historical_data():
    return render_template('./historical_data.html')

@app.route('/historical_trades')
def historical_trades():
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    io = StringIO()

    headers = [
        'exchange',
        'symbol',
        'timestamp',
        'local_timestamp',
        'taker',
        'taker_order',
        'taker_client_order_id',
        'maker',
        'maker_order',
        'maker_client_order_id',
        'side',
        'price',
        'amount',
        'taker_fee',
        'maker_fee'
    ]

    writer = csv.DictWriter(io, headers)

    for trade in db.execute("""
        select * from trades limit 1000
    """).fetchall():
        writer.writerow(dict(trade))

    mem = BytesIO()

    mem.write(io.getvalue().encode())

    mem.seek(0)

    return send_file(mem, attachment_filename='trades.csv', as_attachment=True, mimetype='text/csv')


@app.route('/markets')
def markets():
    with open('markets.json', 'r') as file:
        return jsonify(json.load(file))

@app.route('/liquidity')
def liquidity():
    symbol = request.args.get('symbol')

    if symbol is None:
        return (
            jsonify({'error': {'message': 'Please enter a symbol query parameter.'}}),
            400
        )

    db = sqlite3.connect('dev.db')
    db.row_factory = sqlite3.Row

    results = []

    for row in db.execute("""
        select
            exchange,
            symbol,
            buy,
            sell,
            timestamp
        from average_liquidity_per_minute
        where symbol = :symbol
    """, {'symbol': symbol}):
        results.append(dict(row))

    return jsonify(results)

@app.route('/slippages')
def slippages():
    symbol = request.args.get('symbol')

    if symbol is None:
        return (
            jsonify({'error': {'message': 'Please enter a symbol query parameter.'}}),
            400
        )

    db = sqlite3.connect('dev.db')
    db.row_factory = sqlite3.Row

    results = []

    for row in db.execute("""
        select
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
        from average_slippages_per_minute
        where symbol = :symbol
    """, {'symbol': symbol}):
        results.append(dict(row))

    return jsonify(results)

@app.route('/latest_slippages')
def latest_slippages():
    db = sqlite3.connect('dev.db')
    db.row_factory = sqlite3.Row

    results = []

    for row in db.execute("""
        select
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
        from latest_slippages
    """):
        results.append(dict(row))

    return jsonify(results)

@app.route('/order_book/<market>')
def order_book(market):
    timestamp = request.args.get('timestamp')

    timestamp = int(time() * 1e6) if timestamp is None else int(
        timegm(datetime.fromisoformat(timestamp).timetuple()) * 1e6)

    depth = request.args.get('depth')

    depth = None if depth is None else int(request.args.get('depth'))

    order_book = reconstruct_order_book(market, timestamp, depth)

    return jsonify(order_book)

if __name__ == '__main__':
    app.run(host='0.0.0.0')

# @app.route('/slippages/<market>')
# def slippage(market):
#     timestamp = request.args.get('timestamp')
#
#     timestamp = int(time() * 1e6) if timestamp is None else int(
#         timegm(datetime.fromisoformat(timestamp).timetuple()) * 1e6)
#
#     order_sizes = request.args.get('order_sizes')
#
#     order_sizes = [50000, 100000, 200000, 500000, 1000000] if order_sizes is None else list(map(int, order_sizes.split(',')))
#
#     order_book = reconstruct_order_book(timestamp, market)
#
#     return {
#         'symbol': market,
#         'timestamp': datetime.utcfromtimestamp(timestamp / 1e6).isoformat(),
#         'available_liquidity': {
#             'buy': sum(map(lambda order: order[0] * order[1], order_book['bids'])),
#             'sell': sum(map(lambda order: order[0] * order[1], order_book['asks']))
#         },
#         'slippages': {
#             'buy': [[order_size, calculate_slippage(order_book, order_size, 'buy', 'average_fill_price')] for order_size in order_sizes],
#             'sell': [[order_size, calculate_slippage(order_book, order_size, 'buy', 'average_fill_price')] for order_size in order_sizes]
#         }
#     }
#
# @app.route('/slippages')
# def slippages():
#     markets = request.args.get('markets')
#
#     markets = [
#         'BTC-PERP',
#         'SOL-PERP',
#         'MNGO-PERP',
#         'ADA-PERP',
#         'AVAX-PERP',
#         'BNB-PERP',
#         'ETH-PERP',
#         'FTT-PERP',
#         'LUNA-PERP',
#         'RAY-PERP',
#         'SRM-PERP'
#     ] if markets is None else markets.split(',')
#
#     timestamp = request.args.get('timestamp')
#
#     timestamp = int(time() * 1e6) if timestamp is None else int(timegm(datetime.fromisoformat(timestamp).timetuple()) * 1e6)
#
#     order_books = [reconstruct_order_book(timestamp, market) for market in markets]
#
#     order_sizes = [50000, 100000, 200000, 500000, 1000000]
#
#     def calculate_slippages(order_book, order_sizes):
#         return {
#             'symbol': order_book['symbol'],
#             'timestamp': order_book['timestamp'],
#             'buy': [[order_size, calculate_slippage(order_book, order_size, 'buy', 'average_fill_price')] for order_size in order_sizes],
#             'sell': [[order_size, calculate_slippage(order_book, order_size, 'sell', 'average_fill_price')] for order_size in order_sizes]
#         }
#
#     slippages = [calculate_slippages(order_book, order_sizes) for order_book in order_books]
#
#     return jsonify(slippages)
