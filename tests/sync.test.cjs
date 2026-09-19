// 外部通信や実データを使わず、同期処理を検証する。
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function source(name) {
  const match = new RegExp(`(?:async )?function ${name}\\(`).exec(html);
  assert.ok(match, name);
  return html.slice(match.index, html.indexOf('\n}', match.index) + 2);
}

function context(extra = {}) {
  const values = new Map();
  const ctx = vm.createContext({
    console, Date, render: () => {}, ...extra,
    localStorage: { getItem: k => values.get(k) ?? null, setItem: (k, v) => values.set(k, v) },
  });
  vm.runInContext(`
    let stocks = [], portfolioLog = [], collateralEntries = [], transactions = [], watchlist = [];
    let watchlistBudgetJPY = 0, transactionDeletions = {}, _lastSynced = {}, lastCloudUpdatedAt = null;
    const LOCAL_UPDATED_AT_KEY = 'updated';
    const TX_DELETIONS_KEY = 'deletions', TRANSACTIONS_KEY = 'transactions';
  `, ctx);
  for (const name of ['mergeTransactionDeletions', 'recordTransactionDeletions', 'mergeTransactions',
    '_canonicalStocks', '_canonicalWatchlist', '_syncPayloads', '_hasUnsyncedChanges', '_doSave', '_sbGet']) {
    vm.runInContext(source(name), ctx);
  }
  if (!extra.useRealRead) ctx._sbGet = extra.readCloud || (async () => null);
  return ctx;
}

const tx = { id: 'a', date: '2026-09-12', type: 'sell', ticker: 'AAA', quantity: 10, realizedPLJPY: 100 };

test('インラインJavaScriptの構文', () => {
  for (const match of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) new vm.Script(match[1]);
});

test('同条件の別約定は保持し、同じIDだけ統合する', () => {
  const ctx = context();
  const result = ctx.mergeTransactions([tx, { ...tx, id: 'b' }], [tx]);
  assert.equal(result.length, 2);
});

test('削除は古い端末にも適用され、新しい取り消しで復元できる', () => {
  const ctx = context();
  ctx.recordTransactionDeletions([tx], []);
  const deleted = vm.runInContext('transactionDeletions', ctx);
  assert.equal(ctx.mergeTransactions([tx], [], deleted).length, 0);
  ctx.recordTransactionDeletions([], [tx]);
  const restored = vm.runInContext('transactionDeletions', ctx);
  const merged = ctx.mergeTransactionDeletions({ a: { deleted: true, updatedAt: 1 } }, restored);
  assert.equal(ctx.mergeTransactions([], [tx], merged).length, 1);
});

test('保存失敗時に同期済み状態と日時を変更しない', async () => {
  const ctx = context({ _sb: { from: () => ({ upsert: async () => ({ error: new Error('失敗') }) }) } });
  await assert.rejects(vm.runInContext('_doSave()', ctx), /失敗/);
  assert.equal(vm.runInContext('Object.keys(_lastSynced).length', ctx), 0);
  assert.equal(ctx.localStorage.getItem('updated'), null);
  assert.equal(vm.runInContext('_hasUnsyncedChanges()', ctx), true);
});

test('読込エラーをデータなしとして扱わない', async () => {
  const ctx = context({ useRealRead: true, _sb: { from: () => ({ select: () => ({ eq: () => ({
    maybeSingle: async () => ({ data: null, error: new Error('読込失敗') }),
  }) }) }) } });
  await assert.rejects(ctx._sbGet('sp_stocks'), /読込失敗/);
});

test('関連データを一括保存し、通信中の編集は未同期として残す', async () => {
  let finish;
  let rows;
  const ctx = context({ _sb: { from: () => ({ upsert: data => {
    rows = data;
    return new Promise(resolve => { finish = resolve; });
  } }) } });
  const saving = vm.runInContext('_doSave()', ctx);
  // 保存前のクラウド確認を完了させる。
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(rows.length, 7);
  assert.equal(new Set(rows.map(r => r.updated_at)).size, 1);
  vm.runInContext("transactions.push({id:'new'}); localStorage.setItem('updated','newer')", ctx);
  finish({ error: null });
  await saving;
  assert.equal(rows.find(r => r.key === 'sp_transactions').value.length, 0);
  assert.equal(vm.runInContext('_hasUnsyncedChanges()', ctx), true);
  assert.equal(ctx.localStorage.getItem('updated'), 'newer');
});

test('ポーリング前の保存でも別端末の削除を保持する', async () => {
  let rows;
  const ctx = context({
    readCloud: async key => ({ value: key === 'sp_transactions' ? [] : { a: { deleted: true, updatedAt: 100 } } }),
    _sb: { from: () => ({ upsert: async data => { rows = data; return { error: null }; } }) },
  });
  ctx.oldTx = tx;
  vm.runInContext('transactions = [oldTx]', ctx);
  await vm.runInContext('_doSave()', ctx);
  assert.equal(rows.find(r => r.key === 'sp_transactions').value.length, 0);
  assert.equal(rows.find(r => r.key === 'sp_transaction_deletions').value.a.deleted, true);
});

test('保存済みの空の銘柄一覧をサンプルで置き換えない', () => {
  const ctx = context({ migrateStock: x => x, defaultStocks: () => [{ id: 'sample' }] });
  ctx.localStorage.setItem('stocks', '[]');
  const init = html.slice(html.indexOf("const raw = JSON.parse(localStorage.getItem('stocks')"));
  vm.runInContext(init.slice(0, init.indexOf('\nportfolioLog =')), ctx);
  assert.equal(vm.runInContext('stocks.length', ctx), 0);
});

function loadContext(readCloud) {
  const ctx = context({
    readCloud, console: { error() {} },
    portfolioTickerSignature: () => "", requestPortfolioPrices() {},
    document: { getElementById: () => ({}) },
    normalizePortfolioLog: x => x, mergePortfolioLogs: (a, b) => b,
    migrateStock: x => x, migrateWatchlistItem: x => x,
    renderWatchlist() {}, renderPieChart() {}, renderLineChart() {},
    recordDailyLog() {}, updateSyncIcon() {}, toast() {},
    setTimeout() {}, clearTimeout() {}, localizeJapaneseNames() {}, scheduleSyncSave() {},
  });
  vm.runInContext(`
    let isSyncing = false, initialLoadDone = false, syncTimer = null;
    const PORTFOLIO_LOG_KEY = 'log', COLLATERAL_KEY = 'collateral';
    const WATCHLIST_KEY = 'watchlist', WATCHLIST_BUDGET_KEY = 'budget';
  `, ctx);
  ctx._sbGetAll = keys => Promise.all(keys.map(readCloud));
  vm.runInContext(source('sbLoad'), ctx);
  ctx._doSave = async () => { throw new Error('読み込むべき状態で保存が呼ばれた'); };
  return ctx;
}

test('別端末で全削除した保有銘柄を件数ガードで復活させない', async () => {
  const ctx = loadContext(async key => key === 'sp_stocks'
    ? { value: [], updated_at: '2026-09-12T02:00:00.000Z' } : null);
  ctx.localStorage.setItem('stocks', '[{"id":"old"}]');
  ctx.localStorage.setItem('updated', '2026-09-12T01:00:00.000Z');
  vm.runInContext('stocks = [{id:"old"}]', ctx);
  await ctx.sbLoad(true);
  assert.equal(vm.runInContext('stocks.length', ctx), 0);
  assert.equal(vm.runInContext('initialLoadDone', ctx), true);
});

test('初回読込失敗では自動アップロードを有効にしない', async () => {
  const ctx = loadContext(async () => { throw new Error('読込失敗'); });
  await ctx.sbLoad(true);
  assert.equal(vm.runInContext('initialLoadDone', ctx), false);
});

test('初回クラウド読込で保有銘柄が揃ったら価格取得を開始する', async () => {
  const ctx = loadContext(async key => key === 'sp_stocks'
    ? { value: [{id:'actual',yahooTicker:'REAL'}], updated_at:'2026-09-19T00:00:00Z' } : null);
  let requested = 0;
  ctx.requestPortfolioPrices = () => { requested++; };
  await ctx.sbLoad(true);
  assert.equal(requested, 1);
  assert.equal(vm.runInContext('stocks[0].yahooTicker', ctx), 'REAL');
});
