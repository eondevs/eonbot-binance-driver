const Koa = require('koa');
const Router = require('koa-router');
const auth = require('basic-auth');
const error = require('koa-json-error');
const bodyParser = require('koa-bodyparser');
const fs = require('fs');
const minify = require('node-json-minify');
const Binance = require('binance-api-node').default;
const config = require('./setup.js');

const app = new Koa();
const router = new Router({
    sensitive: true,
});

const intervals = new Map([
    [60, '1m'],
    [60 * 3, '3m'],
    [60 * 5, '5m'],
    [60 * 15, '15m'],
    [60 * 30, '30m'],
    [60 * 60, '1h'],
    [60 * 60 * 2, '2h'],
    [60 * 60 * 4, '4h'],
    [60 * 60 * 6, '6h'],
    [60 * 60 * 8, '8h'],
    [60 * 60 * 12, '12h'],
    [60 * 60 * 24, '1d'],
    [60 * 60 * 24 * 3, '3d'],
    [60 * 60 * 24 * 7, '1w'],
    [60 * 60 * 24 * 31, '1M'],
]);

const maxOrderHist = 1000; // max amount of orders allowed by binance
const maxCandles = 1000; // max amount of candles allowed by binance

const driverData = {
    clients: [], // exchange clients with different api keys/secrets
    clientIndex: 0,
    cooldownState: {
        active: false,
        start: 0,
        end: 0,
    },
    rateLimits: {},
    pairs: {},
    tickers: {},
    candles: {},
    balances: {},
    balancesWSTime: 0, // balances ws event time
    openOrders: {},
    orderHistory: {},
    ordersWSTime: 0, // order history ws event time
};

function setupClients(apiInfo) {
    if (!Array.isArray(apiInfo)) {
        throw new Error('API info must be array');
    }

    apiInfo.forEach((creds) => {
        driverData.clients.push(Binance({
            apiKey: creds.key,
            apiSecret: creds.secret,
        }));
    });
    driverData.clientIndex = 0;
}

// retrieve client and increment current index for next use (i.e. rotate client).
// this is not really needed for binance, because api keys have no restrictions, but
// to avoid confusion and keep everything in sync with eonbot specifications
// this can be still here.
function getClient() {
    if (driverData.clients.length == 0) {
        throw new Error('API info is not found');
    }

    const client = driverData.clients[driverData.clientIndex];

    // rotate api credentials/client for next use
    driverData.apiIndex++;
    if (driverData.clientIndex > driverData.clients.length) {
        driverData.clientIndex = 0;
    }

    return client;
}

// convert to format required by binance.
function formatPair(pair) {
    const [base, counter] = pair.split('_');
    return base + counter;
}

function isValidPair(pair) {
    return pair !== undefined && pair !== '' && pair.includes('_');
}

// convert pair returned from binance to BASE_COUNTER format.
function toNormalPair(pair) {
    for (const p in driverData.pairs) {
        if (pair === formatPair(p)) {
            return p;
        }
    }
    return '';
}

// rounds number with given step
function roundStep(number, stepSize) {
    const precision = stepSize.toString().split('.')[1].length || 0;
    return (Math.round(number / stepSize) * stepSize).toFixed(precision);
}

function activateCooldown(end) {
    driverData.cooldownState.active = true;
    driverData.cooldownState.start = Date.now();
    driverData.cooldownState.end = end;
}

function deactivateCooldown() {
    if (driverData.cooldownState.active) {
        if (Date.now() > driverData.cooldownState.end) {
            driverData.cooldownState.active = false;
            driverData.cooldownState.start = 0;
            driverData.cooldownState.end = 0;
            return true;
        }
        return false;
    }
    return true;
}

function cooldownTimer(interval, incr) {
    interval = interval.toUpperCase();
    let d = new Date();
    switch (interval) {
    case 'SECOND':
        d = d.setSeconds(d.getSeconds() + incr);
        break;
    case 'DAY':
        d = d.setHours(d.getHours() + 24 * incr);
        break;
    case 'MINUTE':
    default:
        d = d.setMinutes(d.getMinutes() + incr);
    }
    return d;
}

function incrCooldown(limitType, weight, ctx) {
    if (!deactivateCooldown()) {
        if (ctx) {
            ctx.throw(429, 'Cooldown state is active');
        }
        throw new Error('Cooldown state is active');
    }

    const limits = driverData.rateLimits[limitType];

    for (let i = 0; i < limits.length; i++) {
        const limit = limits[i];
        if (Date.now() >= limit.resetTime) { // update/reset limit data
            limit.resetTime = cooldownTimer(limit.interval, 1);
            limit.current = 0;
        }

        limit.current += weight;

        if (limit.current > limit.limit) {
            activateCooldown(cooldownTimer(limit.interval, 1));
            if (ctx) {
                ctx.throw(429, 'Cooldown state is active');
            }
            throw new Error('Cooldown state is active');
        }
        limits[i] = limit;
    }

    driverData.rateLimits[limitType] = limits;
}

// error codes reference: https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
function exchangeError(ctx, err) {
    switch (err.code) {
    case -1003: { // too many requests
        // default minute interval from error message: "Too many requests; current limit is %s requests per minute."
        activateCooldown(cooldownTimer('', 2));
        ctx.throw(429, 'Cooldown state is active');
        break;
    }
    case -1015: { // too many orders
        // try to get interval from error message: "Too many new orders; current limit is %s orders per %s."
        let data = err.message.split('orders per ');
        data = data[1].split('.');
        activateCooldown(cooldownTimer(data[0] || '', 2));
        ctx.throw(429, 'Cooldown state is active');
        break;
    }
    case -2013: { // order does not exist
        ctx.throw(404, err);
        break;
    }
    default:
        ctx.throw(400, err);
    }
}

async function prepareExchangeInfo() {
    // api keys are not needed
    const info = await getClient().exchangeInfo();

    info.symbols.forEach((pair) => {
        const value = pair.filters.find(v => v.filterType == 'MIN_NOTIONAL').minNotional;
        const rate = pair.filters.find(v => v.filterType == 'PRICE_FILTER');
        const amount = pair.filters.find(v => v.filterType == 'LOT_SIZE');
        driverData.pairs[`${pair.baseAsset}_${pair.quoteAsset}`] = {
            basePrecision: pair.baseAssetPrecision,
            counterPrecision: pair.quotePrecision,
            minValue: value,
            minRate: rate.minPrice,
            maxRate: rate.maxPrice,
            rateStep: rate.tickSize,
            minAmount: amount.minQty,
            maxAmount: amount.maxQty,
            amountStep: amount.stepSize,
        };
    });

    info.rateLimits.forEach((limit) => {
        if (driverData.rateLimits[limit.rateLimitType] === undefined) {
            driverData.rateLimits[limit.rateLimitType] = [];
        }

        driverData.rateLimits[limit.rateLimitType].push({
            interval: limit.interval,
            limit: limit.limit,
            current: 0,
            resetTime: 0,
        });
    });
    incrCooldown('REQUEST_WEIGHT', 1);
}

async function handleTickers() {
    incrCooldown('REQUEST_WEIGHT', 1);
    const tickers = await getClient().dailyStats();
    tickers.forEach((tick) => {
        driverData.tickers[toNormalPair(tick.symbol)] = {
            lastPrice: tick.lastPrice,
            askPrice: tick.askPrice,
            bidPrice: tick.bidPrice,
            baseVolume: tick.volume,
            counterVolume: tick.quoteVolume,
            dayPercentChange: tick.priceChangePercent,
        };
    });

    getClient().ws.allTickers((ticks) => {
        ticks.forEach((tick) => {
            driverData.tickers[toNormalPair(tick.symbol)] = {
                lastPrice: tick.curDayClose,
                askPrice: tick.bestAsk,
                bidPrice: tick.bestBid,
                baseVolume: tick.volume,
                counterVolume: tick.volumeQuote,
                dayPercentChange: tick.priceChangePercent,
            };
        });
    });
}

async function handleUserData() {
    incrCooldown('REQUEST_WEIGHT', 5);
    const acc = await getClient().accountInfo({ useServerTime: true });

    acc.balances.forEach((balance) => {
        driverData.balances[balance.asset] = balance.free;
    });

    await getClient().ws.user((info) => {
        switch (info.eventType) {
        case 'account': {
            if (info.eventTime < driverData.balancesWSTime) {
                return;
            }

            driverData.balancesWSTime = info.eventTime;
            for (const asset in info.balances) {
                const balance = info.balances[asset];
                driverData.balances[asset] = balance.available;
            }
            break;
        }
        case 'executionReport': {
            if (info.eventTime < driverData.ordersWSTime) {
                return;
            }

            driverData.ordersWSTime = info.eventTime;
            const pair = toNormalPair(info.symbol);
            const removeFromOpen = (ordID) => {
                if (driverData.openOrders[pair]) {
                    driverData.openOrders[pair].forEach((order, index) => {
                        if (order.orderID === ordID) {
                            driverData.openOrders[pair].splice(index, 1);
                        }
                    });
                }
            };

            if (info.orderStatus == 'NEW' && driverData.openOrders[pair]) {
                driverData.openOrders[pair].push({
                    timestamp: new Date(info.orderTime).toISOString(),
                    orderID: info.orderId.toString(),
                    isFilled: false,
                    amount: info.quantity,
                    rate: info.price,
                    side: info.side.toLowerCase(),
                });
            } else if (info.orderStatus == 'CANCELED') {
                removeFromOpen(info.orderId.toString());
            } else if (info.orderStatus == 'FILLED') {
                removeFromOpen(info.orderId.toString());
                if (driverData.orderHistory[pair]) {
                    driverData.orderHistory[pair].push({
                        timestamp: new Date(info.orderTime).toISOString(),
                        orderID: info.orderId.toString(),
                        isFilled: true,
                        side: info.side.toLowerCase(),
                        amount: info.quantity,
                        rate: roundStep(parseFloat(info.totalQuoteTradeQuantity) / parseFloat(info.totalTradeQuantity), driverData.pairs[pair].rateStep),
                    });

                    if (driverData.orderHistory[pair].length > maxOrderHist) {
                        driverData.orderHistory[pair].splice(0, 1);
                    }
                }
            }
            break;
        }
        }
    });
}

router.use((ctx, next) => {
    // basic auth
    const user = auth(ctx.req);

    if (config.username == '' && config.password == ''
        || user && user.name == config.username && user.pass == config.password) {
        return next();
    }
    ctx.throw(401, 'Unauthorized');
});

router.post('/api-info', (ctx) => {
    if (!ctx.request.body || !Array.isArray(ctx.request.body) || ctx.request.body.length === 0) {
        ctx.throw(400, 'API info array cannot be empty');
    }

    try {
        fs.writeFileSync(config.apiInfoFile, JSON.stringify(ctx.request.body, null, 4));
    } catch (err) {
        ctx.throw(500, err);
    }

    setupClients(ctx.request.body);
    ctx.status = 200;
});

router.get('/api-info', (ctx) => {
    let apiInfo;
    try {
        apiInfo = fs.readFileSync(config.apiInfoFile);
    } catch (err) {
        if (err.code == 'ENOENT') {
            ctx.throw(404, 'API info file not found');
        }
        ctx.throw(err);
    }
    ctx.body = minify(apiInfo.toString());
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.get('/ping', async (ctx) => {
    try {
        await getClient().ping();
    } catch (err) {
        ctx.throw(400, err);
    }
    ctx.status = 200;
});

router.get('/cooldown-info', (ctx) => {
    deactivateCooldown(); // try disabling cooldown
    const data = {
        active: driverData.cooldownState.active,
    };

    if (data.active) {
        const start = new Date(driverData.cooldownState.start);
        const end = new Date(driverData.cooldownState.end);
        data.start = start.toISOString();
        data.end = end.toISOString();
    }

    ctx.body = JSON.stringify(data);
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.get('/intervals', (ctx) => {
    ctx.body = JSON.stringify(Array.from(intervals.keys()));
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.get('/pairs', (ctx) => {
    ctx.body = JSON.stringify(driverData.pairs);
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.get('/ticker', async (ctx) => {
    let data = {};

    if (isValidPair(ctx.query.pair)) {
        const ticker = driverData.tickers[ctx.query.pair];
        ctx.assert(ticker, 404, 'Pair ticker not found');
        data = ticker;
    } else {
        data = driverData.tickers;
    }

    ctx.body = JSON.stringify(data);
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.get('/candles', async (ctx) => {
    ctx.assert(isValidPair(ctx.query.pair), 400, 'Pair is invalid');
    ctx.assert(ctx.query.interval, 400, 'Interval is invalid');

    let candles = [];
    const binancePair = formatPair(ctx.query.pair);
    const interval = intervals.get(parseInt(ctx.query.interval, 10));

    const resp = (candlesData) => {
        const limit = ctx.query.limit || maxCandles;
        const start = candlesData.length - limit;
        ctx.assert(start >= 0, 400, 'Limit value exceeds max allowed amount of candles');
        ctx.body = JSON.stringify(candlesData.slice(start));
        ctx.type = 'application/json';
        ctx.status = 200;
    };

    if (!ctx.query.end && driverData.candles[ctx.query.pair] && driverData.candles[ctx.query.pair][interval]) {
        resp(driverData.candles[ctx.query.pair][interval]);
        return;
    }

    // on first request (when end timestamp is not provided) to this endpoint gather all data and subscribe to ws

    incrCooldown('REQUEST_WEIGHT', 1, ctx);

    try {
        candles = await getClient().candles({
            symbol: binancePair,
            interval,
            endTime: Date.parse(ctx.query.end) || Date.now(),
            limit: maxCandles,
        });
    } catch (err) {
        exchangeError(ctx, err);
    }

    const data = [];
    candles.forEach((candle) => {
        data.push({
            timestamp: new Date(candle.openTime).toISOString(),
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            baseVolume: candle.baseAssetVolume,
            counterVolume: candle.quoteAssetVolume,
        });
    });

    if (!ctx.query.end) {
        // if end timestamp was not provided (i.e. current date was used), cache all candles
        // and subscribe to ws to update latest candle.
        driverData.candles[ctx.query.pair] = {};
        driverData.candles[ctx.query.pair][interval] = data;
        getClient().ws.candles(binancePair, interval, (candle) => {
            const driverCandles = driverData.candles[ctx.query.pair][interval];
            const lastTime = Date.parse(driverCandles[driverCandles.length - 1].timestamp);
            if (candle.startTime > lastTime) {
                // add new candle and remove oldest
                driverCandles.push({
                    timestamp: new Date(candle.startTime).toISOString(),
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    baseVolume: candle.volume,
                    counterVolume: candle.quoteVolume,
                });
                driverCandles.splice(0, 1);
            } else if (candle.startTime === lastTime) {
                // update latest candle
                driverCandles[driverCandles.length - 1] = {
                    timestamp: new Date(candle.startTime).toISOString(),
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    baseVolume: candle.volume,
                    counterVolume: candle.quoteVolume,
                };
            }
            driverData.candles[ctx.query.pair][interval] = driverCandles;
        });
    }

    resp(data);
});

router.get('/balances', async (ctx) => {
    ctx.body = JSON.stringify(driverData.balances);
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.post('/buy', async (ctx) => {
    ctx.assert(ctx.request.body, 400, 'Order data not specified');
    ctx.assert(isValidPair(ctx.request.body.pair), 400, 'Pair is invalid');
    ctx.assert(ctx.request.body.amount, 400, 'Amount is invalid');
    ctx.assert(ctx.request.body.rate, 400, 'Rate is invalid');

    incrCooldown('REQUEST_WEIGHT', 1, ctx);
    incrCooldown('ORDERS', 1, ctx);

    let order;
    try {
        order = await getClient().order({
            symbol: formatPair(ctx.request.body.pair),
            side: 'BUY',
            quantity: ctx.request.body.amount,
            price: ctx.request.body.rate,
            newOrderRespType: 'ACK',
            useServerTime: true,
        });
    } catch (err) {
        exchangeError(ctx, err);
    }

    ctx.body = `{"id": "${order.orderId}"}`;
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.post('/sell', async (ctx) => {
    ctx.assert(ctx.request.body, 400, 'Order data not specified');
    ctx.assert(isValidPair(ctx.request.body.pair), 400, 'Pair is invalid');
    ctx.assert(ctx.request.body.amount, 400, 'Amount is invalid');
    ctx.assert(ctx.request.body.rate, 400, 'Rate is invalid');

    incrCooldown('REQUEST_WEIGHT', 1, ctx);
    incrCooldown('ORDERS', 1, ctx);

    let order;
    try {
        order = await getClient().order({
            symbol: formatPair(ctx.request.body.pair),
            side: 'SELL',
            quantity: ctx.request.body.amount,
            price: ctx.request.body.rate,
            newOrderRespType: 'ACK',
            useServerTime: true,
        });
    } catch (err) {
        exchangeError(ctx, err);
    }

    ctx.body = `{"id": "${order.orderId}"}`;
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.post('/cancel', async (ctx) => {
    ctx.assert(ctx.request.body, 400, 'Order data not specified');
    ctx.assert(isValidPair(ctx.request.body.pair), 400, 'Pair is invalid');
    ctx.assert(ctx.request.body.id, 400, 'ID is invalid');

    incrCooldown('REQUEST_WEIGHT', 1, ctx);

    try {
        await getClient().cancelOrder({
            symbol: formatPair(ctx.request.body.pair),
            orderId: ctx.request.body.id,
            useServerTime: true,
        });
    } catch (err) {
        exchangeError(ctx, err);
    }

    ctx.status = 200;
});

router.get('/order', async (ctx) => {
    ctx.assert(isValidPair(ctx.query.pair), 400, 'Pair is invalid');
    ctx.assert(ctx.query.id, 400, 'ID is invalid');

    incrCooldown('REQUEST_WEIGHT', 1, ctx);

    let order;
    try {
        order = await getClient().getOrder({
            symbol: formatPair(ctx.query.pair),
            orderId: ctx.query.id,
            useServerTime: true,
        });
    } catch (err) {
        exchangeError(ctx, err);
    }

    const data = {
        timestamp: new Date(order.time).toISOString(),
        orderID: order.orderId.toString(),
        isFilled: order.status === 'FILLED',
        amount: order.origQty,
        rate: order.price,
        side: order.side.toLowerCase(),
    };

    ctx.body = JSON.stringify(data);
    ctx.type = 'application/json';
    ctx.status = 200;
});

router.get('/open-orders', async (ctx) => {
    ctx.assert(isValidPair(ctx.query.pair), 400, 'Pair is invalid');

    const resp = () => {
        ctx.body = JSON.stringify(driverData.openOrders[ctx.query.pair]);
        ctx.type = 'application/json';
        ctx.status = 200;
    };

    if (driverData.openOrders[ctx.query.pair]) {
        resp();
        return;
    }

    incrCooldown('REQUEST_WEIGHT', 1, ctx);

    let openOrders;
    try {
        openOrders = await getClient().openOrders({
            symbol: formatPair(ctx.query.pair),
            useServerTime: true,
        });
    } catch (err) {
        exchangeError(ctx, err);
    }

    driverData.openOrders[ctx.query.pair] = [];
    openOrders.forEach((order) => {
        driverData.openOrders[ctx.query.pair].push({
            timestamp: new Date(order.time).toISOString(),
            orderID: order.orderId.toString(),
            isFilled: false,
            amount: order.origQty,
            rate: order.price,
            side: order.side.toLowerCase(),
        });
    });
    resp();
});

router.get('/order-history', async (ctx) => {
    ctx.assert(isValidPair(ctx.query.pair), 400, 'Pair is invalid');

    const resp = (ordersData) => {
        // since binance does not allow start and end times interval
        // to be more than 24hrs, we have to ask for biggest number of trades
        // possible and then filter them by their dates manually.
        const startTime = Date.parse(ctx.query.start);
        const data = startTime ? ordersData.filter(ord => Date.parse(ord.timestamp) >= startTime) : ordersData;

        ctx.body = JSON.stringify(data);
        ctx.type = 'application/json';
        ctx.status = 200;
    };

    if (!ctx.query.end && driverData.orderHistory[ctx.query.pair]) {
        resp(driverData.orderHistory[ctx.query.pair]);
        return;
    }

    incrCooldown('REQUEST_WEIGHT', 5, ctx);

    let tradeHist;
    try {
        // 'myTrades' endpoint is being used, because
        // 'allOrders' endpoint returns all closed, open
        // and *cancelled* orders.
        tradeHist = await getClient().myTrades({
            symbol: formatPair(ctx.query.pair),
            endTime: Date.parse(ctx.query.end) || Date.now(),
            limit: maxOrderHist,
            useServerTime: true,
        });
    } catch (err) {
        exchangeError(ctx, err);
    }

    const pairRateStep = driverData.pairs[ctx.query.pair].rateStep;
    const pairAmountStep = driverData.pairs[ctx.query.pair].amountStep;

    const data = [];

    // combine all trades that have the same order id.
    let order;
    const rates = []; // for rate averaging
    tradeHist.forEach((trade) => {
        if (order != undefined) {
            if (trade.orderId == order.orderID) {
                const ordAmount = parseFloat(trade.qty);
                order.timestamp = new Date(trade.time).toISOString();
                order.amount += ordAmount;
                rates.push(parseFloat(trade.price) * ordAmount);
                return;
            }

            if (rates.length > 0) {
                order.rate = roundStep(rates.reduce((p, c) => p + c) / order.amount, pairRateStep);
                order.amount = roundStep(order.amount, pairAmountStep);
                rates.length = 0;
            }

            data.push(order);
            order = undefined;
        }

        if (order == undefined) {
            order = {
                timestamp: new Date(trade.time).toISOString(),
                orderID: trade.orderId.toString(),
                isFilled: true,
                side: trade.isBuyer ? 'buy' : 'sell',
                amount: parseFloat(trade.qty),
            };
            rates.push(parseFloat(trade.price) * order.amount);
        }
    });

    // add final order
    if (rates.length > 0) {
        order.rate = roundStep(rates.reduce((p, c) => p + c) / order.amount, pairRateStep);
        order.amount = roundStep(order.amount, pairAmountStep);
    }
    data.push(order);

    if (!ctx.query.end) {
        // if end timestamp was not provided (i.e. current date was used), cache all orders
        // and wait for new orders from ws.
        driverData.orderHistory[ctx.query.pair] = data;
    }

    resp(data);
});

app.use(error(err => ({ error: err.message })));
app.use(bodyParser());
app.use(router.routes());
app.use(router.allowedMethods({ throw: true }));
app.listen(config.port, async () => {
    try {
        setupClients(JSON.parse(fs.readFileSync(config.apiInfoFile)));
        await prepareExchangeInfo();
        await handleTickers();
        await handleUserData();
    } catch (err) {
        if (err.code == 'ENOENT') {
            console.error('API info file not found');
        } else {
            console.error(err.message, err.code);
        }
        process.exit(1);
    }

    console.log('Binance driver is running...');
});
