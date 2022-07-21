import WebSocket from "ws"
import {Connection} from "@solana/web3.js";
import {
    Config,
    getPerpMarketByBaseSymbol,
    getSpotMarketByBaseSymbol,
    getTokenBySymbol,
    MangoClient, PublicKey
} from "@blockworks-foundation/mango-client";

async function trackSpotPosition() {
    const config = Config.ids()

    const mangoGroupConfig = config.getGroupWithName('mainnet.1')

    if (!mangoGroupConfig) {
        return
    }

    const [token, perpMarketConfig, spotMarketConfig] = [
        getTokenBySymbol(mangoGroupConfig, 'SOL'),
        getPerpMarketByBaseSymbol(mangoGroupConfig, 'SOL'),
        getSpotMarketByBaseSymbol(mangoGroupConfig, 'SOL')
    ]

    const connection = new Connection(config.cluster_urls[mangoGroupConfig.cluster], 'processed')

    const mangoClient = new MangoClient(connection, mangoGroupConfig.mangoProgramId)

    const mangoGroup = await mangoClient.getMangoGroup(mangoGroupConfig.publicKey)

    const mangoAccount = await mangoClient.getMangoAccount(new PublicKey('3EBQE1C1L91L7Xe9h24pyQAriJDedtTyvDygvGmC8FxW'), mangoGroup.dexProgramId)

    console.log(mangoAccount.spotOpenOrdersAccounts.map(spotOpenOrdersAccount => [spotOpenOrdersAccount?.address.toString(), spotOpenOrdersAccount?.market.toString()]))

    const ws = new WebSocket('ws://mangolorians.com:8900/v1/ws')

    ws.onopen = () => {
        console.log(`[+] Websocket Open. Subscribing to trades.`)
        // subscribe both to trades and level2 real-time channels
        const subscribeTrades = {
            op: 'subscribe',
            channel: 'trades',
            markets: ['SOL/USDC']
        }

        ws.send(JSON.stringify(subscribeTrades))
    }

    ws.onmessage = async (message) => {
        const { data } = message

        const event = JSON.parse(data.toString())

        if (event.type !== 'trade') {
            return
        }

        console.log(event)
    }
}

trackSpotPosition()