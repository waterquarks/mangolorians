"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const mango_client_1 = require("@blockworks-foundation/mango-client");
const web3_js_1 = require("@solana/web3.js");
const ids_json_1 = __importDefault(require("./ids.json"));
function subscribeToOrderbook() {
    return __awaiter(this, void 0, void 0, function* () {
        const config = new mango_client_1.Config(ids_json_1.default);
        const groupConfig = config.getGroupWithName('mainnet.1');
        const connection = new web3_js_1.Connection(config.cluster_urls[groupConfig.cluster], 'processed');
        const client = new mango_client_1.MangoClient(connection, groupConfig.mangoProgramId);
        // load group & market
        const perpMarketConfig = (0, mango_client_1.getMarketByBaseSymbolAndKind)(groupConfig, 'BTC', 'perp');
        const mangoGroup = yield client.getMangoGroup(groupConfig.publicKey);
        const perpMarket = yield mangoGroup.loadPerpMarket(connection, perpMarketConfig.marketIndex, perpMarketConfig.baseDecimals, perpMarketConfig.quoteDecimals);
        // subscribe to bids
        connection.onAccountChange(perpMarketConfig.bidsKey, (accountInfo) => {
            const bids = new mango_client_1.BookSide(perpMarketConfig.bidsKey, perpMarket, mango_client_1.BookSideLayout.decode(accountInfo.data));
            for (const order of bids) {
                console.log(order.owner.toBase58(), order.orderId.toString(), order.price, order.size, order.side);
            }
        });
    });
}
subscribeToOrderbook();
