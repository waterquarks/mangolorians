import {
  BookSide,
  BookSideLayout,
  Config,
  getMarketByBaseSymbolAndKind,
  GroupConfig,
  MangoClient,
} from '@blockworks-foundation/mango-client';
import { Commitment, Connection } from '@solana/web3.js';
import configFile from './ids.json';

async function subscribeToOrderbook() {
  const config = new Config(configFile);

  const groupConfig = config.getGroupWithName('mainnet.1') as GroupConfig;

  const connection = new Connection(
    config.cluster_urls[groupConfig.cluster],
    'processed' as Commitment,
  );

  const client = new MangoClient(connection, groupConfig.mangoProgramId);

  // load group & market
  const perpMarketConfig = getMarketByBaseSymbolAndKind(
    groupConfig,
    'BTC',
    'perp',
  );

  const mangoGroup = await client.getMangoGroup(groupConfig.publicKey);

  const perpMarket = await mangoGroup.loadPerpMarket(
    connection,
    perpMarketConfig.marketIndex,
    perpMarketConfig.baseDecimals,
    perpMarketConfig.quoteDecimals,
  );

  // subscribe to bids
  connection.onAccountChange(perpMarketConfig.bidsKey, (accountInfo) => {
    const bids = new BookSide(
      perpMarketConfig.bidsKey,
      perpMarket,
      BookSideLayout.decode(accountInfo.data),
    );

    for (const order of bids) {
      console.log(
        order.owner.toBase58(),
        order.orderId.toString(),
        order.price,
        order.size,
        order.side, // 'buy' or 'sell'
      );
    }
  });
}

subscribeToOrderbook();