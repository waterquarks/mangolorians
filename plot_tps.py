import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime, timezone
import numpy as np

def main():
    db = sqlite3.connect('./tps.db')

    tpm = db.execute("""
        with
            tps_per_minute as (
                select
                    strftime('%Y-%m-%dT%H:%M:00.00Z', timestamp) as minute,
                    cast(avg(committed_transactions_count) as integer) as committed_transactions_count
                from tps
                where "timestamp" between '2022-04-25T12:00' and '2022-04-26T12:00'
                group by "minute"
                order by "minute"
            )
        select minute, committed_transactions_count from tps_per_minute;
    """).fetchall()

    x = [datetime.strptime(t[0], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc) for t in tpm]

    y = [t[1] for t in tpm]

    plt.figure(figsize=(20, 10))

    plt.title('Solana average committed transactions per minute')

    plt.xlabel('Minute')

    plt.ylabel('Transactions')

    plt.plot(x, y)

    plt.gcf().autofmt_xdate()

    plt.show()



if __name__ == '__main__':
    main()