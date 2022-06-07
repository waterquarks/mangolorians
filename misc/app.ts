import {
    IDS,
    Config,
    GroupConfig,
    getMarketByBaseSymbolAndKind,
    BookSide,
    BookSideLayout, MangoClient
} from '@blockworks-foundation/mango-client'
import {Connection} from "@solana/web3.js";

const main = async () => {
    const config = new Config(IDS)

    const groupConfig = config.getGroupWithName('mainnet.1') as GroupConfig

    const connection = new Connection(
        config.cluster_urls[groupConfig.cluster],
        'processed'
    )

    const mangoClient = new MangoClient(connection, groupConfig.mangoProgramId)

    const mangoGroup = await mangoClient.getMangoGroup(groupConfig.publicKey)

    const marketConfig = getMarketByBaseSymbolAndKind(groupConfig, 'SOL', 'perp')

    const perpMarket = await mangoGroup.loadPerpMarket(connection, marketConfig.marketIndex, marketConfig.baseDecimals, marketConfig.quoteDecimals)

    connection.onAccountChange(marketConfig.bidsKey, (accountInfo, context) => {
        // @ts-ignore
        const bookSide = new BookSide(null, perpMarket, BookSideLayout.decode(accountInfo.data), undefined, 100000)

        let idx = 0

        for (const { owner, price, size } of bookSide.items()) {
            console.log(owner.toString(), price, size)

            idx++

            if (idx > 25) {
                console.log()

                break
            }
        }
    })

    // const connection = new Connection(
    //     config.cluster_urls[]
    // )
}

main()