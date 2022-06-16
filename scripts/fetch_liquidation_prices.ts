import {Config, getMarketByPublicKey, MangoClient, PublicKey} from "@blockworks-foundation/mango-client";
import {Connection} from "@solana/web3.js";
import {zip} from 'lodash'

const main = async () => {
    const config = Config.ids()

    const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

    const group = config.getGroup('mainnet', 'mainnet.1')

    if (!group) {
        console.log('Invalid group specified.')

        return
    }

    const timestamp = new Date().toISOString()

    const headers = ['account', 'equity', 'assets', 'liabilities', 'leverage', 'init_health_ratio', 'maint_health_ratio', 'market', 'positionSize', 'timestamp', 'position_notional_size', 'liquidation_price', 'index_price']

    process.stdout.write(headers.join(',') + '\n')

    const pubkeys = [
        '39Fb59ftxuQUUsjD4boTEKMij1Gzw6xB1aKqSyGGgAbb'
    ]

    const client = new MangoClient(connection, group.mangoProgramId)

    const mangoGroup = await client.getMangoGroup(group.publicKey)

    const mangoCache = await mangoGroup.loadCache(connection)

    for (const pubkey of pubkeys) {
        const account = await client.getMangoAccount(new PublicKey(pubkey), mangoGroup.dexProgramId)

        const equity = account.computeValue(mangoGroup, mangoCache)

        const assets = account.getAssetsVal(mangoGroup, mangoCache)

        const liabilities = account.getLiabsVal(mangoGroup, mangoCache)

        const leverage = account.getLeverage(mangoGroup, mangoCache)

        const initHealthRatio = account.getHealthRatio(mangoGroup, mangoCache, 'Init')

        const maintHealthRatio = account.getHealthRatio(mangoGroup, mangoCache, 'Maint')

        const perpPositions = zip(account.perpAccounts, mangoGroup.perpMarkets)

        for (const [index, [perpAccount, perpMarketInfo]] of perpPositions.entries()) {
            const perpMarket = getMarketByPublicKey(group, perpMarketInfo!.perpMarket)

            if (!perpMarket) {
                continue
            }

            const positionSize = account.getBasePositionUiWithGroup(index, mangoGroup)

            if (positionSize == 0) {
                continue
            }

            const liqPrice = account.getLiquidationPrice(mangoGroup, mangoCache, index)

            const indexPrice = mangoGroup.getPriceUi(index, mangoCache)

            const entry = [account.publicKey, equity, assets, liabilities, leverage, initHealthRatio, maintHealthRatio, perpMarket.name, positionSize, timestamp, positionSize * indexPrice, liqPrice, indexPrice]

            process.stdout.write(entry.join(',') + '\n')
        }
    }
}

main()