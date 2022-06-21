import {Config, MangoClient} from "@blockworks-foundation/mango-client";
import {Connection, PublicKey} from "@solana/web3.js";

async function main() {
  const config = Config.ids()

  const mangoGroupConfig = config.getGroup('mainnet', 'mainnet.1')

  if (!mangoGroupConfig) {
    return
  }

  const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

  const mangoClient = new MangoClient(connection, mangoGroupConfig.mangoProgramId)

  const mangoGroup = await mangoClient.getMangoGroup(mangoGroupConfig.publicKey)

  const mangoCache = await mangoGroup.loadCache(connection)

  // const accounts = await mangoClient.getAllMangoAccounts(mangoGroup)

  const accounts = [await mangoClient.getMangoAccount(new PublicKey('F3TTrgxjrkAHdS9zEidtwU5VXyvMgr5poii4HYatZheH'), mangoGroup.dexProgramId)]

  const headers = ['mango_account', 'percentage_move_to_liquidation']

  console.log(headers.join(',') + '\n')

  for (const account of accounts) {
    const priceMoveToLiquidate = await account.getPriceMoveToLiquidate(mangoGroup, mangoCache)

    const entry = [account.publicKey, priceMoveToLiquidate]

    console.log(entry.join(','))
  }
}

main()