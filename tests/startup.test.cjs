// 起動時に不要な通信待ちが入らないことを検証する。
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.join(__dirname, '..');

test('保存銘柄のない初回はサンプルの価格取得を始めない', () => {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  const start = html.indexOf('function startInitialPrices(');
  const code = html.slice(start, html.indexOf('\n}', start) + 2);
  let requests = 0, saved = null;
  const ctx = vm.createContext({
    LOCAL_ONLY_MODE: false, localStorage: { getItem: () => saved },
    document: { getElementById: () => ({}) }, checkAPI: () => { requests++; },
  });
  vm.runInContext(code, ctx);
  ctx.startInitialPrices();
  assert.equal(requests, 0);
  saved = '[]';
  ctx.startInitialPrices();
  assert.equal(requests, 1);
});

test('取得中に同期で銘柄が変わった場合は再取得を予約する', () => {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  const start = html.indexOf('function requestPortfolioPrices(');
  const code = html.slice(start, html.indexOf('\n}', start) + 2);
  let calls = 0;
  const ctx = vm.createContext({ clearTimeout() {}, fetchAllPrices: () => { calls++; } });
  vm.runInContext('let isFetching = true, priceRefreshQueued = false, priceRetryTimer = null;', ctx);
  vm.runInContext(code, ctx);
  ctx.requestPortfolioPrices();
  assert.equal(calls, 0);
  assert.equal(vm.runInContext('priceRefreshQueued', ctx), true);
});

test('初回同期は1リクエストで読み、要求順に行を返す', async () => {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  const start = html.indexOf('async function _sbGetAll(');
  const code = html.slice(start, html.indexOf('\n}', start) + 2);
  let requests = 0;
  const ctx = vm.createContext({
    AbortSignal,
    _sb: { from: () => { requests++; return {
      select: () => ({ in: () => ({ abortSignal: async signal => {
        assert.equal(signal.aborted, false);
        return { data: [{ key:'b', value:2 }, { key:'a', value:1 }], error:null };
      } }) }),
    }; } },
  });
  vm.runInContext(code, ctx);
  const rows = await ctx._sbGetAll(['a','b','missing']);
  assert.equal(requests, 1);
  assert.equal(rows[0].value, 1);
  assert.equal(rows[1].value, 2);
  assert.equal(rows[2], null);
});

for (const host of ['hide921.github.io', 'stock-portfolio-1-2rnh.onrender.com']) {
  test(`${host}: 起動時は通信が止まっていても保存済み画面を返す`, async () => {
    const handlers = {};
    const saved = { html: '保存済み画面' };
    const ctx = vm.createContext({
      URL, self: { addEventListener: (event, handler) => { handlers[event] = handler; } },
      caches: { open: async () => ({ match: async () => saved }) },
      fetch: () => new Promise(() => {}),
    });
    vm.runInContext(fs.readFileSync(path.join(root, 'sw.js'), 'utf8'), ctx);
    let result;
    handlers.fetch({
      request: { method: 'GET', mode: 'navigate', url: `https://${host}/` },
      waitUntil() {}, respondWith: promise => { result = promise; },
    });
    assert.ok(result);
    assert.equal(await result, saved);
  });
}

test('グラフ未読込でも同期を一度だけ即開始する', () => {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  const start = html.indexOf('function bootLibsReady()');
  const code = html.slice(start, html.indexOf('\n}', start) + 2);
  let loads = 0;
  const channel = { on: () => channel, subscribe() {} };
  const sdk = { createClient: () => ({ channel: () => channel }) };
  const ctx = vm.createContext({
    window: { supabase: sdk }, supabase: sdk, URLSearchParams,
    location: { search: '' }, setInterval() {}, updateSyncIcon() {},
    pollCloudChanges() {}, sbLoad: () => { loads++; },
  });
  vm.runInContext("let syncBootStarted = false, _sb; const LOCAL_ONLY_MODE = false, _SB_URL = '', _SB_KEY = '';", ctx);
  vm.runInContext(code, ctx);
  ctx.bootLibsReady();
  ctx.bootLibsReady();
  assert.equal(loads, 1);
});
