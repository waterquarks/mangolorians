import mango

from IPython.display import display

from solana.publickey import PublicKey

with mango.ContextBuilder.build(cluster_name="devnet") as context:
    group = mango.Group.load(context)
    cache: mango.Cache = mango.Cache.load(context, group.cache)

    account = mango.Account.load(context, PublicKey("HhepjyhSzvVP7kivdgJH9bj32tZFncqKUwWidS1ja4xL"), group)
    open_orders = account.load_all_spot_open_orders(context)
    frame = account.to_dataframe(group, open_orders, cache)
    display(frame)
    print(f"Init Health: {account.init_health(frame)}")
    print(f"Maint Health: {account.maint_health(frame)}")
    print(f"Total Value: {account.total_value(frame)}") # <-- Account value
    print(f"Leverage: {account.leverage(frame):,.2f}x")

print("Example complete.")