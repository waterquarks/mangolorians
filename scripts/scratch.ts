import {Connection} from "@solana/web3.js";
import {Config, MangoClient, PublicKey} from "@blockworks-foundation/mango-client";
import {zip} from 'lodash'

async function main() {
    const config = Config.ids()

    const mangoGroupConfig = config.getGroupWithName('mainnet.1')

    if (!mangoGroupConfig) {
        return
    }

    const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

    const mangoClient = new MangoClient(connection, mangoGroupConfig.mangoProgramId)

    const mangoGroup = await mangoClient.getMangoGroup(mangoGroupConfig.publicKey)

    const perpMarketInfos = mangoGroupConfig.perpMarkets.map(perpMarketInfo => ({
        name: perpMarketInfo.name,
        publicKey: perpMarketInfo.publicKey.toString(),
        baseSymbol: perpMarketInfo.baseSymbol,
        baseDecimals: perpMarketInfo.baseDecimals,
        quoteDecimals: perpMarketInfo.quoteDecimals,
        marketIndex: perpMarketInfo.marketIndex,
        bidsKey: perpMarketInfo.bidsKey.toString(),
        asksKey: perpMarketInfo.asksKey.toString(),
        eventsKey: perpMarketInfo.eventsKey.toString()
    }))

    const meta = mangoGroup.perpMarkets.filter(perpMarketInfo => !perpMarketInfo.perpMarket.equals(PublicKey.default)).map(perpMarketInfo => ({
        publicKey: perpMarketInfo.perpMarket.toJSON(),
        baseLotSize: perpMarketInfo.baseLotSize.toNumber(),
        quoteLotSize: perpMarketInfo.quoteLotSize.toNumber(),
    }))

    const perpMarkets = zip(perpMarketInfos, meta)

    console.log(JSON.stringify(perpMarkets.map(([a, b]) => ({...a, ...b}))))
}

main()