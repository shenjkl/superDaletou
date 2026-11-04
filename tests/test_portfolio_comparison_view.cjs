// Pure rendering and async-state tests; not a browser/layout verification.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const moneyStart = source.indexOf('function decimalYuan(');
const moneyEnd = source.indexOf('\nconst tag=', moneyStart);
const start = source.indexOf('function actualPortfolioComparison()');
const end = source.indexOf('\nfunction comparison()', start);
assert.ok(start >= 0 && end > start && moneyStart >= 0 && moneyEnd > moneyStart);

function setup() {
  const handlers = [];
  const c = {backend: {ready: true}, state: {page: 'comparison'}, connectionEpoch: 1,
    document: {body: {dataset: {serverStatus: 'ready'}}, addEventListener: (type, fn) => handlers.push({type, fn})},
    crossPortfolio: {options: [], selected: [], loaded: true, loading: false, ready: false,
      busy: false, error: '', result: null, epoch: 0},
    generationMethods: {hot: '热号加权', cold: '冷号加权'}, structureNames: {singles: '分散单式'},
    escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
    tag: text => text, pct: value => `${(value*100).toFixed(2)}%`, el: () => null,
    toast: () => {}, queueMicrotask: () => {}, URLSearchParams,
    redrawCrossPanel: () => {}, updateCrossResult: () => {},
    fetchCheckedConnection: async (url, options) => {
      const response = await c.fetch(url, options);
      return {response, json: () => response.json()};
    },
    fetch: async () => { throw new Error('unexpected test fetch'); }};
  vm.runInNewContext(source.slice(moneyStart, moneyEnd) + '\n' + source.slice(start, end), c);
  c.handlers = handlers;
  return c;
}

function record(key, pairKey, cost, method = 'hot') {
  const metric = {estimate: 0.1, successes: 100, samples: 1000, interval_95: [0.08, 0.12]};
  const value = {cost_yuan: String(cost/100), any_prize: metric, break_even: metric, profit: metric,
    jackpot_coverage: {probability: 0.000001}, mean_gross_tax_before: {yuan: '1.25', fraction: [5, 4]},
    net_quantiles_yuan: {p05: '-4', p50: '-4', p95: '1'}};
  const portfolio = {cost_fen: cost, remaining_fen: 0, count: cost/200};
  return {request_key: key, pair_key: pairKey,
    request: {samples: 1000, seed: 7, scenario: {source_issue: '26123', source_dependency: {snapshot_id: 'a'.repeat(64)}}},
    pair: {config: {method, structure: 'singles', target_issue: '26124', budget_fen: cost,
      base_price_fen: 200, additional_price_fen: 300}, candidate: portfolio, control: portfolio},
    result: {portfolios: {candidate: value, control: value}}};
}

test('saved evaluation options identify pair, source and actual cost', () => {
  const c = setup();
  c.crossPortfolio.options = [{request_key: 'eval-123', pair_key: 'pair-123', status: 'research_draft',
    source_issue: '26123', snapshot_id: 'a'.repeat(64), target_issue: '26124', method: 'hot',
    structure: 'singles', budget_fen: 600, cost_fen: 400, samples: 1000, seed: 7},
    {request_key: 'bad', pair_key: 'bad-pair', status: 'review_required', reason: '来源冲突'}];
  const html = c.actualPortfolioComparison();
  assert.match(html, /已保存组合对照/);
  assert.match(html, /草稿 pair-123/);
  assert.match(html, /快照 aaaaaaaa/);
  assert.match(html, /预算 6\.00 元 \/ 投入 4\.00 元/);
  assert.match(html, /来源冲突/);
  assert.match(html, /data-cross-evaluation="bad"\s+disabled/);
});

test('saved pair comparison shows costs and assumptions without ranking', () => {
  const c = setup();
  c.crossPortfolio.selected = ['one', 'two'];
  c.crossPortfolio.ready = true;
  c.crossPortfolio.result = {entries: [record('one', 'p1', 400), record('two', 'p2', 600, 'cold')]};
  const html = c.crossPortfolioResult();
  assert.match(html, /实际投入不同/);
  assert.match(html, /不进行收益排名/);
  assert.match(html, /热号加权/);
  assert.match(html, /冷号加权/);
  assert.match(html, /4\.00 元/);
  assert.match(html, /6\.00 元/);
  assert.match(html, /不参与购买/);
  assert.match(html, /不重算新增中奖者分奖或限额/);
});

test('cross-portfolio comparison renders exact large mean and quantiles', () => {
  const c = setup(), first = record('one', 'p1', 400), second = record('two', 'p2', 600);
  first.result.portfolios.candidate.mean_gross_tax_before = {fraction: ['1234567890123456789012345678', '100']};
  first.result.portfolios.candidate.net_quantiles_yuan.p95 = '12345678901234567890123456.78';
  c.crossPortfolio.selected = ['one', 'two']; c.crossPortfolio.ready = true;
  c.crossPortfolio.result = {entries: [first, second]};
  const html = c.crossPortfolioResult();
  assert.match(html, /12345678901234567890123456\.78 元/);
  assert.match(html, /四舍五入显示/);
  assert.doesNotMatch(html, /1\.2345678901234568e\+25/);
});

test('successful comparison rechecks selected keys and mismatched response stays hidden', async () => {
  const c = setup();
  c.crossPortfolio.selected = ['one', 'two'];
  let request;
  c.fetch = async url => {request = url; return {ok: true, json: async () => ({entries: [record('one', 'p1', 400), record('two', 'p2', 600)]})};};
  await c.runCrossComparison();
  assert.match(request, /key=one&key=two/);
  assert.equal(c.crossPortfolio.ready, true);
  assert.match(c.crossPortfolioResult(), /已保存组合比较/);
  c.crossPortfolio.result.entries.reverse();
  assert.match(c.crossPortfolioResult(), /与选择不一致/);
});

test('failed recheck and late response do not show old comparison', async () => {
  const c = setup();
  c.crossPortfolio.selected = ['one', 'two'];
  c.crossPortfolio.ready = true;
  c.crossPortfolio.result = {entries: [record('one', 'p1', 400), record('two', 'p2', 600)]};
  c.fetch = async () => ({ok: false, json: async () => ({error: '来源需要复核'})});
  await c.runCrossComparison(true);
  assert.equal(c.crossPortfolio.ready, false);
  assert.match(c.crossPortfolioResult(), /来源需要复核/);
  assert.doesNotMatch(c.crossPortfolioResult(), /已保存组合比较/);
  let resolve;
  c.fetch = () => new Promise(done => {resolve = done;});
  const pending = c.runCrossComparison();
  c.crossPortfolio.selected = ['one'];
  c.crossPortfolio.epoch++;
  c.crossPortfolio.busy = false;
  resolve({ok: true, json: async () => ({entries: [record('one', 'p1', 400), record('two', 'p2', 600)]})});
  await pending;
  assert.equal(c.crossPortfolio.ready, false);
  assert.doesNotMatch(c.crossPortfolioResult(), /已保存组合比较/);
});

test('selection disallows two evaluations of the same pair', () => {
  const c = setup();
  c.crossPortfolio.options = [{request_key: 'one', pair_key: 'p1'}, {request_key: 'other', pair_key: 'p1'}];
  c.crossPortfolio.selected = ['one'];
  const input = {checked: true, dataset: {crossEvaluation: 'other'}, closest: () => input};
  const handler = c.handlers.find(item => item.type === 'change');
  handler.fn({target: input});
  assert.equal(input.checked, false);
  assert.deepEqual(c.crossPortfolio.selected, ['one']);
});

test('refreshing options cancels an in-flight comparison without leaving it busy', async () => {
  const c = setup();
  c.crossPortfolio.selected = ['one', 'two'];
  let resolveCompare;
  c.fetch = url => url.includes('comparison-options')
    ? Promise.resolve({ok: true, json: async () => ({options: []})})
    : new Promise(resolve => {resolveCompare = resolve;});
  const pending = c.runCrossComparison();
  assert.equal(c.crossPortfolio.busy, true);
  await c.loadCrossOptions();
  assert.equal(c.crossPortfolio.busy, false);
  resolveCompare({ok: true, json: async () => ({entries: [record('one', 'p1', 400), record('two', 'p2', 600)]})});
  await pending;
  assert.equal(c.crossPortfolio.ready, false);
  assert.doesNotMatch(c.crossPortfolioResult(), /已保存组合比较/);
});

test('cross-pair comparison marks user-specified payouts as research assumptions', () => {
  const c = setup(), first = record('one', 'p1', 400), second = record('two', 'p2', 600);
  for (const item of [first, second]) item.request.scenario = {kind: 'custom_payout_v1', source_issue: 'custom', custom_input: {label: '<假设>'}};
  c.crossPortfolio.selected = ['one', 'two']; c.crossPortfolio.ready = true;
  c.crossPortfolio.result = {entries: [first, second]};
  assert.match(c.crossPortfolioResult(), /自定义：&lt;假设&gt;（研究假设）/);
  assert.doesNotMatch(c.crossPortfolioResult(), /第 custom 期/);
  c.crossPortfolio.options = [{request_key:'one',pair_key:'p1',status:'research_draft',scenario_kind:'custom_payout_v1',scenario_label:'<假设>',source_issue:'custom',target_issue:'26124',method:'hot',structure:'singles',budget_fen:400,cost_fen:400,samples:1000,seed:7}];
  assert.match(c.actualPortfolioComparison(), /自定义：&lt;假设&gt;（研究假设）/);
});
