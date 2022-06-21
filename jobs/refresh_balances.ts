import {
  Config,
  getTokenBySymbol,
  I80F48,
  MangoClient,
  nativeI80F48ToUi,
  nativeToUi,
  QUOTE_INDEX
} from "@blockworks-foundation/mango-client";
import {Connection} from "@solana/web3.js";
import {sumBy} from 'lodash'

const i80f48ToPercent = (value: I80F48) =>
  value.mul(I80F48.fromNumber(100))

const main = async () => {
  const config = Config.ids()

  const connection = new Connection(config.cluster_urls['mainnet'], 'processed')

  const mangoGroupConfig = config.getGroup('mainnet', 'mainnet.1')

  if (!mangoGroupConfig) {
      console.log('Invalid group specified.')

      return
  }

  const mangoClient = new MangoClient(connection, mangoGroupConfig.mangoProgramId)

  const mangoGroup = await mangoClient.getMangoGroup(mangoGroupConfig.publicKey)

  const mangoCache = await mangoGroup.loadCache(connection)

  await mangoGroup.loadRootBanks(connection)

  const mangoAccounts = await mangoClient.getAllMangoAccounts(mangoGroup)

  const headers = ['asset', 'account', 'deposits', 'borrows', 'unsettled', 'net', 'value', 'equity', 'assets', 'liabilities', 'leverage', 'init_health_ratio', 'maint_health_ratio', 'timestamp', 'market_percentage_move_to_liquidation']

  console.log(headers.join(','))

  const timestamp = new Date().toISOString()

  for (const mangoAccount of mangoAccounts) {
    const equity = mangoAccount.computeValue(mangoGroup, mangoCache)

    const assets = mangoAccount.getAssetsVal(mangoGroup, mangoCache)

    const liabilities = mangoAccount.getLiabsVal(mangoGroup, mangoCache)

    const leverage = mangoAccount.getLeverage(mangoGroup, mangoCache)

    const initHealthRatio = mangoAccount.getHealthRatio(mangoGroup, mangoCache, 'Init')

    const maintHealthRatio = mangoAccount.getHealthRatio(mangoGroup, mangoCache, 'Maint')

    const balances: any[] = []

    for (const { marketIndex, baseSymbol, name } of mangoGroupConfig.spotMarkets) {
      if (!mangoAccount || !mangoGroup || !mangoCache) {
        return []
      }

      const openOrders: any = mangoAccount.spotOpenOrdersAccounts[marketIndex]
      const quoteCurrencyIndex = QUOTE_INDEX

      let nativeBaseFree = 0
      let nativeQuoteFree = 0
      let nativeBaseLocked = 0
      let nativeQuoteLocked = 0
      if (openOrders) {
        nativeBaseFree = openOrders.baseTokenFree.toNumber()
        nativeQuoteFree = openOrders.quoteTokenFree
          .add(openOrders['referrerRebatesAccrued'])
          .toNumber()
        nativeBaseLocked = openOrders.baseTokenTotal
          .sub(openOrders.baseTokenFree)
          .toNumber()
        nativeQuoteLocked = openOrders.quoteTokenTotal
          .sub(openOrders.quoteTokenFree)
          .toNumber()
      }

      const tokenIndex = marketIndex

      const net = (nativeBaseLocked: number, tokenIndex: number) => {
        const amount = mangoAccount
          .getUiDeposit(
            mangoCache.rootBankCache[tokenIndex],
            mangoGroup,
            tokenIndex
          )
          .add(
            nativeI80F48ToUi(
              I80F48.fromNumber(nativeBaseLocked),
              mangoGroup.tokens[tokenIndex].decimals
            ).sub(
              mangoAccount.getUiBorrow(
                mangoCache.rootBankCache[tokenIndex],
                mangoGroup,
                tokenIndex
              )
            )
          )

        return amount
      }

      const value = (nativeBaseLocked: number, tokenIndex: number) => {
        const amount = mangoGroup
          .getPrice(tokenIndex, mangoCache)
          .mul(net(nativeBaseLocked, tokenIndex))

        return amount
      }

      const marketPair = [
        {
          market: null,
          key: `${baseSymbol}${name}`,
          symbol: baseSymbol,
          deposits: mangoAccount.getUiDeposit(
            mangoCache.rootBankCache[tokenIndex],
            mangoGroup,
            tokenIndex
          ),
          borrows: mangoAccount.getUiBorrow(
            mangoCache.rootBankCache[tokenIndex],
            mangoGroup,
            tokenIndex
          ),
          orders: nativeToUi(
            nativeBaseLocked,
            mangoGroup.tokens[tokenIndex].decimals
          ),
          unsettled: nativeToUi(
            nativeBaseFree,
            mangoGroup.tokens[tokenIndex].decimals
          ),
          net: net(nativeBaseLocked, tokenIndex),
          value: value(nativeBaseLocked, tokenIndex),
          depositRate: i80f48ToPercent(mangoGroup.getDepositRate(tokenIndex)),
          borrowRate: i80f48ToPercent(mangoGroup.getBorrowRate(tokenIndex)),
          decimals: mangoGroup.tokens[tokenIndex].decimals,
        },
        {
          market: null,
          key: `${name}`,
          symbol: mangoGroupConfig.quoteSymbol,
          deposits: mangoAccount.getUiDeposit(
            mangoCache.rootBankCache[quoteCurrencyIndex],
            mangoGroup,
            quoteCurrencyIndex
          ),
          borrows: mangoAccount.getUiBorrow(
            mangoCache.rootBankCache[quoteCurrencyIndex],
            mangoGroup,
            quoteCurrencyIndex
          ),
          orders: nativeToUi(
            nativeQuoteLocked,
            mangoGroup.tokens[quoteCurrencyIndex].decimals
          ),
          unsettled: nativeToUi(
            nativeQuoteFree,
            mangoGroup.tokens[quoteCurrencyIndex].decimals
          ),
          net: net(nativeQuoteLocked, quoteCurrencyIndex),
          value: value(nativeQuoteLocked, quoteCurrencyIndex),
          depositRate: i80f48ToPercent(mangoGroup.getDepositRate(tokenIndex)),
          borrowRate: i80f48ToPercent(mangoGroup.getBorrowRate(tokenIndex)),
          decimals: mangoGroup.tokens[quoteCurrencyIndex].decimals,
        },
      ]
      balances.push(marketPair)
    }

    const baseBalances = balances.map((b) => b[0])
    const quoteBalances = balances.map((b) => b[1])
    const quoteMeta = quoteBalances[0]
    const quoteInOrders = sumBy(quoteBalances, 'orders')
    const unsettled = sumBy(quoteBalances, 'unsettled')

    if (!mangoGroup || !mangoCache) {
      return []
    }

      const net: I80F48 = quoteMeta.deposits
    .add(I80F48.fromNumber(unsettled))
    .sub(quoteMeta.borrows)
    .add(I80F48.fromNumber(quoteInOrders))

    const token = getTokenBySymbol(mangoGroupConfig, quoteMeta.symbol)
    const tokenIndex = mangoGroup.getTokenIndex(token.mintKey)

    const value = net.mul(mangoGroup.getPrice(tokenIndex, mangoCache))

    const depositRate = i80f48ToPercent(mangoGroup.getDepositRate(tokenIndex))

    const borrowRate = i80f48ToPercent(mangoGroup.getBorrowRate(tokenIndex))

    const summary = [
      {
        market: null,
        key: `${quoteMeta.symbol}${quoteMeta.symbol}`,
        symbol: quoteMeta.symbol,
        deposits: quoteMeta.deposits,
        borrows: quoteMeta.borrows,
        orders: quoteInOrders,
        unsettled,
        net,
        value,
        depositRate,
        borrowRate,
        decimals: mangoGroup.tokens[QUOTE_INDEX].decimals,
      },
    ].concat(baseBalances)

    const priceMoveToLiquidation = mangoAccount.getPriceMoveToLiquidate(mangoGroup, mangoCache)

    for (const balance of summary) {
      if (balance.deposits == 0 && balance.borrows == 0) {
        continue
      }

      const values = [balance.symbol, mangoAccount.publicKey, balance.deposits, balance.borrows, balance.unsettled, balance.net, balance.value, equity, assets, liabilities, leverage, initHealthRatio, maintHealthRatio, timestamp, priceMoveToLiquidation]

      console.log(values.join(','))
    }
  }
}

main()
