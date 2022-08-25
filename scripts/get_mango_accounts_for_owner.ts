import {MangoClient, Config, PublicKey} from "@blockworks-foundation/mango-client";
import {Connection} from "@solana/web3.js";

const main = async () => {
    const { OWNER } = process.env

    if (!OWNER) {
        console.log('Enter an `owner` environment variable')

        return
    }

    const config = Config.ids()

    const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

    const group = config.getGroup('mainnet', 'mainnet.1')

    if (!group) {
        console.log('Invalid group specified.')

        return
    }

    const client = new MangoClient(connection, group.mangoProgramId)

    const mangoGroup = await client.getMangoGroup(group.publicKey)

    const accounts = await client.getMangoAccountsForOwner(mangoGroup, new PublicKey(OWNER))

    console.log(accounts.map(account => account.publicKey.toString()))
}

main()