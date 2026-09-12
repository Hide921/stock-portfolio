// 価格取得の順次表示・期限・再試行を外部通信なしで確認する。
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
function source(name) {
  const match = new RegExp(`(?:async )?function ${name}\\(`).exec(html);
  return html.slice(match.index, html.indexOf('\n}', match.index) + 2);
}
function context(extra = {}) {
  const ctx = vm.createContext({
    AbortController, DOMException, setTimeout, clearTimeout,
    API_BASE: '', isFundTicker: () => false, ...extra,
  });
  for (const name of ['mapWithConcurrency', 'normalizeBackendPrice', 'runPriceRequest', 'collectPrices']) {
    vm.runInContext(source(name), ctx);
  }
  return ctx;
}
const response = data => ({ ok: true, json: async () => data });
const flush = () => new Promise(resolve => setImmediate(resolve));

test('古いサーバー保存値は表示しつつ代替取得で更新する', async () => {
  const events = [];
  const ctx = context({
    fetch: async url => {
      assert.ok(url.includes('mode=cached'));
      return response({ A: { price: 10, stale: true } });
    },
    getCurrentPriceViaCors: async () => ({ price: 12 }),
  });
  const result = await ctx.collectPrices(['A'], batch => events.push(batch.A.price));
  assert.deepEqual(events, [10, 12]);
  assert.equal(result.prices.A.price, 12);
});

test('進捗表示は更新済み・保存値・未取得を区別する', () => {
  const nodes = {};
  const ctx = context({ document: { getElementById: id => nodes[id] ||= {} } });
  vm.runInContext(source('updatePriceProgress'), ctx);
  ctx.updatePriceProgress(['A','B','C'], { A:{price:10}, B:{price:9,stale:true} }, true, true);
  assert.equal(nodes.priceProgressCount.textContent, '1 / 3 銘柄更新');
  assert.match(nodes.priceProgressDetail.textContent, /更新済み 1 ・ 保存値 1 ・ 未取得 1/);
  assert.match(nodes.priceProgressDetail.textContent, /30秒後/);
  assert.equal(nodes.priceProgressBar.value, 1);
});

test('バックエンドの成功分を代替取得の完了前に通知する', async () => {
  let finish;
  const events = [];
  const ctx = context({
    fetch: async () => response({ A: { price: 10, currency: 'USD' } }),
    getCurrentPriceViaCors: () => new Promise(resolve => { finish = resolve; }),
  });
  const pending = ctx.collectPrices(['A', 'B'], batch => events.push(Object.keys(batch)));
  await flush();
  assert.deepEqual(events[0], ['A']);
  finish({ price: 20 });
  const result = await pending;
  assert.equal(result.prices.B.price, 20);
  assert.deepEqual(events[1], ['B']);
});

test('全体期限で戻り、中断を無視した遅延応答は反映しない', async () => {
  let finish;
  let signal;
  const events = [];
  const ctx = context({
    fetch: async () => response({ A: { price: 10 } }),
    getCurrentPriceViaCors: (_, s) => { signal = s; return new Promise(resolve => { finish = resolve; }); },
  });
  const result = await ctx.collectPrices(['A', 'B'], batch => events.push(Object.keys(batch)), 80, 40);
  assert.equal(result.timedOut, true);
  assert.equal(signal.aborted, true);
  assert.equal(result.prices.A.price, 10);
  finish({ price: 999 });
  await flush();
  assert.equal(result.prices.B, undefined);
  assert.equal(events.length, 1);
});

test('バックエンドが停止しても代替取得に切り替わる', async () => {
  let backendSignal;
  const ctx = context({
    fetch: (_, options) => { backendSignal = options.signal; return new Promise(() => {}); },
    getCurrentPriceViaCors: async () => ({ price: 30 }),
  });
  const result = await ctx.collectPrices(['A'], () => {}, 500, 20);
  assert.equal(backendSignal.aborted, true);
  assert.equal(result.prices.A.price, 30);
  assert.equal(result.timedOut, false);
});

test('代替取得は成功済み銘柄を再取得しない', async () => {
  const requested = [];
  const ctx = context({
    fetch: async () => response({ A: { price: 10 } }),
    getCurrentPriceViaCors: async ticker => { requested.push(ticker); return { price: 20 }; },
  });
  await ctx.collectPrices(['A', 'B', 'C'], () => {});
  assert.deepEqual(requested.sort(), ['B', 'C']);
});

test('先行表示は未取得銘柄の価格やエラーを変更しない', () => {
  const ctx = context({ document: { getElementById: () => ({}) }, render() {}, renderWatchlist() {} });
  vm.runInContext(`
    let stocks = [{id:'a',yahooTicker:'A',currentPrice:1}, {id:'b',yahooTicker:'B',currentPrice:2,fetchError:'以前のエラー'}];
    let watchlist = [], usdJpyRate = 150;
  `, ctx);
  vm.runInContext(source('showPartialPrices'), ctx);
  ctx.showPartialPrices({ A: { price: 10 } });
  assert.equal(vm.runInContext('stocks[0].currentPrice', ctx), 10);
  assert.equal(vm.runInContext('stocks[1].currentPrice', ctx), 2);
  assert.equal(vm.runInContext('stocks[1].fetchError', ctx), '以前のエラー');
});

test('30秒後の再試行は未取得分だけに限定し、再試行を繰り返さない', async () => {
  const timers = [], calls = [], cacheWrites = [];
  const ctx = context({
    console, document: { getElementById: () => ({ style: {} }) },
    localStorage: { setItem() {} },
    setTimeout: (callback, ms) => { timers.push({ callback, ms }); return timers.length; },
    clearTimeout() {}, showPartialPrices() {}, updatePriceProgress() {}, renderPriceIssues() {},
    loadPriceCache: () => ({ prices: { OLD: { price: 7 } }, ts: Date.now() }),
    savePriceCache: prices => cacheWrites.push(prices), cacheAgeText: () => '',
    recordDailyLog: () => false, savePriceState() {}, render() {}, renderWatchlist() {},
    renderPieChart() {}, renderLineChart() {}, toast() {}, resetAutoRefreshSchedule() {},
  });
  vm.runInContext(`
    let stocks = [{id:'a',yahooTicker:'A',currentPrice:1},{id:'b',yahooTicker:'B',currentPrice:2}];
    let watchlist = [], priceRetryTimer = null, isFetching = false, usdJpyRate = 150;
    let _flashMap = {}, apiOnline = false, lastPriceFetchTs = 0, _sb = null;
  `, ctx);
  ctx.collectPrices = async tickers => {
    calls.push([...tickers]);
    return { prices: calls.length === 1 ? { A: { price: 10 }, 'USDJPY=X': { price: 150 } } : {}, timedOut: true };
  };
  vm.runInContext(source('fetchAllPrices'), ctx);
  await ctx.fetchAllPrices();
  assert.equal(timers.length, 1);
  assert.equal(timers[0].ms, 30000);
  assert.equal(cacheWrites[0].OLD.price, 7);
  await timers[0].callback();
  assert.deepEqual(calls[1], ['B']);
  assert.equal(timers.length, 1);
  assert.equal(vm.runInContext('stocks[0].fetchError', ctx), null);
  assert.equal(vm.runInContext('stocks[0].currentPrice', ctx), 10);
  assert.equal(vm.runInContext('isFetching', ctx), false);
});
