import sys

import mango
import csv

with mango.ContextBuilder.build(cluster_name='mainnet') as context:
    group = mango.Group.load(context)

    accounts = mango.Account.load_all(context, group)

    writer = csv.writer(sys.stdout)

    headers = ['address', 'signer', 'delegate', 'advanced_orders']

    writer.writerow(headers)

    for account in accounts:
        data = [account.address, account.owner, account.delegate, account.advanced_orders]

        writer.writerow(data)