import {Config, MangoClient, PublicKey} from "@blockworks-foundation/mango-client";
import {Connection} from "@solana/web3.js";

const main = async () => {
    if (!process.env.MANGO_ACCOUNT) {
        return
    }

    const config = Config.ids()

    const groupConfig = config.getGroup('mainnet', 'mainnet.1')

    if (!groupConfig) {
        return
    }

    const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

    const mangoClient = new MangoClient(connection, groupConfig.mangoProgramId)

    const mangoGroup = await mangoClient.getMangoGroup(groupConfig.publicKey)

    const account = await mangoClient.getMangoAccount(new PublicKey(process.env.MANGO_ACCOUNT), mangoGroup.dexProgramId)

    const mangoCache = await mangoGroup.loadCache(connection)

    console.log(account.toPrettyString(groupConfig, mangoGroup, mangoCache))
}

main()