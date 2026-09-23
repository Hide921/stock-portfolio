const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
function source(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.notEqual(start, -1);
  return html.slice(start, html.indexOf('\n}', start) + 2);
}

test('旧サンプルの誤コードだけを正しいオルカンのコードへ補正する', () => {
  const ctx = vm.createContext({});
  vm.runInContext(source('migrateStock'), ctx);
  const sample = {
    market: 'FUND', ticker: '0131103C', yahooTicker: '0131103C.T',
    name: 'eMAXIS Slim 全世界株式', currency: 'JPY', quantity: 1000000, avgPrice: 1.8,
    currentPrice: 0.877309, prevClose: 0.8774, _perKuchi: true,
  };
  const corrected = ctx.migrateStock(sample);
  assert.equal(corrected.ticker, '0331418A');
  assert.equal(corrected.yahooTicker, '0331418A.T');
  assert.equal(corrected.currentPrice, null);
  assert.equal(corrected.quantity, 1000000);
  assert.equal(corrected.avgPrice, 1.8);

  const actualFund = { ...sample, ticker: '0131103C', yahooTicker: '0131103C.T', name: '米欧債券･インカムオープン' };
  assert.equal(ctx.migrateStock(actualFund).ticker, '0131103C');
});

test('新しいサンプルは取得可能なコードを使い、古い価格を表示しない', () => {
  const ctx = vm.createContext({ uid: () => 'id' });
  vm.runInContext(source('defaultStocks'), ctx);
  const fund = ctx.defaultStocks().find(s => s.market === 'FUND');
  assert.equal(fund.ticker, '0331418A');
  assert.equal(fund.currentPrice, null);
});
