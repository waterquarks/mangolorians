import decimal
import mango
import base58
from datetime import datetime, timedelta, timezone

from solana.publickey import PublicKey

wallet = mango.Wallet(base58.b58decode('4ajZZfUgLjUrfmk5zk7cv3HCQwKjZxnCRLWQPqodTSimwEfzwsQD98LYEiHhZv3ughKR1dZ3CotMmPgwRYRWWGFb'))

with mango.ContextBuilder.build(cluster_name="mainnet") as context:
    group = mango.Group.load(context)

    accounts = mango.Account.load_all_for_owner(context, wallet.address, group)

    account = mango.Account.load(context, PublicKey('FodFJ9k7UCMPYtfUMkv7jqrQvUrLnn5T7KxfdnKTW6fG'), group)

    market_operations = mango.operations(context, wallet, account, "SOL-PERP", dry_run=False)

    # Try to buy 2 SOL for $1.
    order = mango.Order.from_values(
        side=mango.Side.BUY,
        price=decimal.Decimal(1),
        quantity=decimal.Decimal(100),
        order_type=mango.OrderType.LIMIT,
        expiration=datetime.now(timezone.utc) + timedelta(seconds=15),
    )

    print("Placing order:", order)
    placed_order_signatures = market_operations.place_order(order)

    print("Waiting for place order transaction to confirm...\n", placed_order_signatures)
    mango.WebSocketTransactionMonitor.wait_for_all(context.client.cluster_ws_url, placed_order_signatures, commitment="processed")

print("Example complete.")