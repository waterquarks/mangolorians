import {Connection} from "@solana/web3.js";
import {Config, getMarketByPublicKey, MangoClient} from '@blockworks-foundation/mango-client';
import {zip} from "lodash";


const main = async () => {
    const config = Config.ids()

    const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

    const group = config.getGroup('mainnet', 'mainnet.1')

    if (!group) {
        console.log('Invalid group specified.')

        return
    }

    const timestamp = new Date().toISOString()

    const headers = ['account', 'equity', 'assets', 'liabilities', 'leverage', 'init_health_ratio', 'maint_health_ratio', 'market', 'positionSize', 'timestamp', 'position_notional_size', 'liquidation_price', 'market_per_move_to_liquidation']

    process.stdout.write(headers.join(',') + '\n')

    const client = new MangoClient(connection, group.mangoProgramId)

    const mangoGroup = await client.getMangoGroup(group.publicKey)

    const mangoCache = await mangoGroup.loadCache(connection)

    const accounts = await client.getAllMangoAccounts(mangoGroup)

    for (const account of accounts) {
        const equity = account.computeValue(mangoGroup, mangoCache)

        const assets = account.getAssetsVal(mangoGroup, mangoCache)

        const liabilities = account.getLiabsVal(mangoGroup, mangoCache)

        const leverage = account.getLeverage(mangoGroup, mangoCache)

        const initHealthRatio = account.getHealthRatio(mangoGroup, mangoCache, 'Init')

        const maintHealthRatio = account.getHealthRatio(mangoGroup, mangoCache, 'Maint')

        const priceMoveToLiquidation = account.getPriceMoveToLiquidate(mangoGroup, mangoCache)

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

            const entry = [account.publicKey, equity, assets, liabilities, leverage, initHealthRatio, maintHealthRatio, perpMarket.name, positionSize, timestamp, positionSize * indexPrice, liqPrice, priceMoveToLiquidation]

            process.stdout.write(entry.join(',') + '\n')
        }
    }
}

main()