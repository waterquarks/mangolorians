import {MangoClient, Config} from "@blockworks-foundation/mango-client";
import {Connection} from "@solana/web3.js";

const main = async () => {
    const config = Config.ids()

    const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

    const group = config.getGroup('mainnet', 'mainnet.1')

    if (!group) {
        console.log('Invalid group specified.')

        return
    }

    const headers = ['mango_account', 'signer', 'delegate', 'advanced_orders']

    process.stdout.write(headers.join(',') + '\n')

    const client = new MangoClient(connection, group.mangoProgramId)

    const mangoGroup = await client.getMangoGroup(group.publicKey)

    const accounts = await client.getAllMangoAccounts(mangoGroup)

    for (const account of accounts) {
        // @ts-ignore
        const entry = [
            account.publicKey,
            account.owner,
            account.delegate.toString() !== '11111111111111111111111111111111' ? account.delegate : null,
            account.advancedOrders
        ]

        process.stdout.write(entry.join(',') + '\n')
    }
}

main()