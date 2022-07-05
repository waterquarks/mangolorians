import mango

# Use our hard-coded devnet wallet for DeekipCw5jz7UgQbtUbHQckTYGKXWaPQV4xY93DaiM6h.
# For real-world use you'd load the bytes from the environment or a file.
wallet = mango.Wallet(bytes([60, 108, 131, 174, 114, 37, 141, 185, 219, 41, 173, 74, 191, 1, 227, 228, 119, 44, 76, 84, 36, 9, 190, 109, 198, 125, 60, 122, 31, 77, 27, 165, 35, 167, 119, 64, 58, 255, 216, 95, 204, 219, 59, 244, 48, 101, 109, 233, 41, 56, 181, 22, 242, 30, 66, 226, 220, 13, 187, 216, 152, 240, 238, 92]))

# Create a 'devnet' Context
with mango.ContextBuilder.build(cluster_name="devnet") as context:
    # Mango accounts are per-Group, so we need to load the Group first.
    group = mango.Group.load(context)

    # Get all the Wallet's accounts for that Group
    accounts = mango.Account.load_all_for_owner(context, wallet.address, group)

    for account in accounts:
        print(account)

print("Example complete.")