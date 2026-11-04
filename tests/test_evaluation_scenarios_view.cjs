// Pure rendering and state checks; this does not exercise a real browser.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const moneyStart = source.indexOf('function decimalYuan(');
const moneyEnd = source.indexOf('\nconst tag=', moneyStart);
const begin = source.indexOf('function evaluationSources()');
const end = source.indexOf('function readLiveEvaluation()', begin);
assert.ok(begin >= 0 && end > begin && moneyStart >= 0 && moneyEnd > moneyStart);

function setup() {
  const context = {
    backend: {snapshots: [{id: 'a'.repeat(64), issues: ['26123', '26122', '26013']}],
      liveResult: {request_key: 'pair', pair: {pair_sha256: 'pair-hash', config: {snapshot_id: 'a'.repeat(64)}}}, liveEval: null,
      liveEvalCfg: null, liveEvalDirty: false, liveEvalHistory: [], evalCompareKeys: [],
      evalCompareReady: false, evalCompareBusy: false, evalCompareError: '', evalHistoryEpoch: 0, ready: true},
    escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
    tag: text => `<span>${text}</span>`, el: () => null,
    document: {addEventListener() {}, body: {dataset: {serverStatus: ''}}},
    state: {page: 'generation'}, connectionEpoch: 1, refreshEvaluationHistory() {}, pct: value => String(value),
  };
  vm.runInNewContext(source.slice(moneyStart, moneyEnd) + '\n' + source.slice(begin, end), context);
  return context;
}

test('evaluation form offers supported historical periods and makes assumptions explicit', () => {
  const c = setup(), html = c.liveEvaluation();
  assert.match(html, /第 26123 期/);
  assert.match(html, /第 26122 期/);
  assert.doesNotMatch(html, /第 26013 期/);
  assert.match(html, /不是下一期奖金预测/);
  assert.doesNotMatch(html, /使用第 26123 期历史公布奖金/);
});

test('selecting historical evaluation restores its own scenario and review status', () => {
  const c = setup();
  const entry = {request_key: 'older', status: 'review_required', reason: '来源冲突',
    request: {scenario: {source_dependency: {snapshot_id: 'a'.repeat(64), issue: '26122'}}, samples: 5000, seed: 2}};
  c.backend.liveEvalHistory = [entry];
  c.selectEvaluation('older');
  assert.equal(c.backend.liveEvalCfg.issue, '26122');
  assert.equal(c.backend.liveEvalCfg.seed, 2);
  assert.match(c.liveEvaluationResult(), /评估结果或其依赖需要复核/);
  assert.match(c.liveEvaluationHistory(), /第 26122 期历史奖金/);
  c.backend.liveEvalDirty = true;
  assert.match(c.liveEvaluationResult(), /请重新计算/);
});

test('unavailable saved scenario cannot silently select a different source', () => {
  const c = setup();
  c.backend.liveEvalCfg = {snapshot: 'b'.repeat(64), issue: '26122', samples: 1000, seed: 1};
  const html = c.liveEvaluation();
  assert.match(html, /已保存情景不可用/);
  assert.match(html, /selected disabled/);
  assert.match(html, /type="submit" disabled/);
});

test('a current result identifies its own historical issue and baseline', () => {
  const c = setup();
  const metric = {estimate: 0.1, successes: 100, samples: 1000, interval_95: [0.08, 0.12]};
  const item = {cost_yuan: '4', any_prize: metric, break_even: metric, profit: metric,
    mean_gross_tax_before: {yuan: '1.25', fraction: [5, 4]}, jackpot_coverage: {probability: 0.001},
    net_quantiles_yuan: {p05: '-4', p50: '-4', p95: '1'}};
  c.backend.liveEval = {status: 'research_draft', result: {scenario: {source_issue: '26122'},
    sampling: {samples: 1000, seed: 7}, portfolios: {candidate: item, control: item}}};
  const html = c.liveEvaluationResult();
  assert.match(html, /第 26122 期历史奖金/);
  assert.match(html, /不参与购买/);
  assert.match(html, /只反映模拟抽样误差/);
});

test('current and historical scenario displays preserve large exact values', () => {
  const c = setup(), first = entry('one', '26123'), second = entry('two', '26122');
  for (const selected of [first, second]) {
    const item = selected.result.portfolios.candidate;
    item.mean_gross_tax_before = {yuan: '12345678901234567890123456.78', fraction: ['1234567890123456789012345678', '100']};
    item.net_quantiles_yuan.p95 = '12345678901234567890123456.78';
  }
  c.backend.liveEval = first;
  assert.match(c.liveEvaluationResult(), /12345678901234567890123456\.78 元/);
  c.backend.evalCompareKeys = ['one', 'two']; c.backend.liveEvalHistory = [first, second]; c.backend.evalCompareReady = true;
  const html = c.liveScenarioComparison();
  assert.match(html, /12345678901234567890123456\.78 元/);
  assert.match(html, /四舍五入显示/);
  assert.doesNotMatch(html, /1\.2345678901234568e\+25/);
});

function entry(key, issue, status = 'research_draft') {
  const metric = {estimate: 0.1, successes: 100, samples: 1000, interval_95: [0.08, 0.12]};
  const item = {cost_yuan: '4', any_prize: metric, break_even: metric, profit: metric,
    mean_gross_tax_before: {yuan: '1.25', fraction: [5, 4]}, jackpot_coverage: {probability: 0.001},
    net_quantiles_yuan: {p05: '-4', p50: '-4', p95: '1'}};
  return {request_key: key, status,
    request: {scenario: {source_dependency: {snapshot_id: 'a'.repeat(64), issue}}, samples: 1000, seed: 7},
    result: {pair_sha256: 'pair-hash', scenario: {source_issue: issue},
      sampling: {samples: 1000, seed: 7}, portfolios: {candidate: item, control: item}}};
}

test('selected scenarios are checked before a side-by-side view appears', async () => {
  const c = setup();
  c.backend.evalCompareKeys = ['one', 'two'];
  c.backend.liveEvalHistory = [entry('one', '26123'), entry('two', '26122')];
  c.fetch = async () => ({ok: true, json: async () => ({evaluations: c.backend.liveEvalHistory})});
  assert.match(c.liveScenarioComparison(), /点击“核验并对照/);
  await c.compareSelectedEvaluations();
  const html = c.liveScenarioComparison();
  assert.match(html, /第 26123 期 \/ 候选方案/);
  assert.match(html, /第 26122 期 \/ 随机对照/);
  assert.match(html, /不参与购买/);
  assert.match(html, /不对情景或方案排名/);
  c.backend.liveEvalHistory[1].status = 'review_required';
  assert.match(c.liveScenarioComparison(), /对照结果已隐藏/);
  assert.doesNotMatch(c.liveScenarioComparison(), /第 26123 期 \/ 候选方案/);
  assert.match(c.liveEvaluationHistory(), /data-compare-evaluation="two" checked\s*>/);
  c.backend.liveEvalHistory[1].status = 'research_draft';
  c.backend.liveEvalHistory[1].request.scenario.source_dependency.issue = '26121';
  assert.match(c.liveScenarioComparison(), /对照结果已隐藏/);
});

test('a late comparison response does not restore a changed selection', async () => {
  const c = setup();
  c.backend.evalCompareKeys = ['one', 'two'];
  let resolveResponse;
  c.fetch = () => new Promise(resolve => { resolveResponse = resolve; });
  const pending = c.compareSelectedEvaluations();
  assert.equal(c.backend.evalCompareBusy, true);
  c.backend.evalCompareKeys = ['one'];
  resolveResponse({ok: true, json: async () => ({evaluations: [entry('one', '26123'), entry('two', '26122')]})});
  await pending;
  assert.equal(c.backend.evalCompareReady, false);
  assert.equal(c.backend.evalCompareBusy, false);
  assert.doesNotMatch(c.liveScenarioComparison(), /历史奖金情景对照/);
});

test('a failed recheck hides previous comparison values', async () => {
  const c = setup();
  c.backend.evalCompareKeys = ['one', 'two'];
  c.backend.liveEvalHistory = [entry('one', '26123'), entry('two', '26122')];
  c.backend.evalCompareReady = true;
  assert.match(c.liveScenarioComparison(), /历史奖金情景对照/);
  c.fetch = async () => { throw new Error('offline'); };
  await c.compareSelectedEvaluations();
  assert.equal(c.backend.evalCompareReady, false);
  assert.match(c.liveScenarioComparison(), /无法核验所选情景/);
  assert.doesNotMatch(c.liveScenarioComparison(), /第 26123 期 \/ 候选方案/);
});

test('custom payout form and saved result retain assumptions and escape user text', () => {
  const c = setup(), saved = entry('custom', '26123');
  saved.request.scenario = {kind: 'custom_payout_v1', source_issue: 'custom',
    custom_input: {label: '<研究>', payouts: Object.fromEntries(['101','201','301','401','501','601','701','801','901'].map(k => [k, '10.00']))}};
  saved.result.scenario.source_issue = 'custom';
  c.backend.liveEvalHistory = [saved];
  c.selectEvaluation('custom');
  assert.equal(c.backend.liveEvalCfg.mode, 'custom');
  assert.equal(c.backend.liveEvalCfg.custom.payouts['201'], '10.00');
  const form = c.liveEvaluation();
  assert.match(form, /自定义固定奖金/);
  assert.match(form, /追加奖金为在基本奖金之外增加的金额/);
  assert.match(form, /value="&lt;研究&gt;"/);
  assert.doesNotMatch(form, /value="<研究>"/);
  assert.match(c.liveEvaluationResult(), /自定义：&lt;研究&gt;/);
  assert.match(c.liveEvaluationHistory(), /自定义：&lt;研究&gt;/);
  assert.doesNotMatch(c.liveEvaluationResult(), /第 custom 期/);
});

test('custom and historical records remain distinctly labelled in scenario comparisons', () => {
  const c = setup(), custom = entry('custom', '26123'), historical = entry('history', '26122');
  custom.request.scenario = {kind: 'custom_payout_v1', source_issue: 'custom', custom_input: {
    label: '<假设>', payouts: Object.fromEntries(['101','201','301','401','501','601','701','801','901'].map(k => [k, '5.00']))}};
  custom.result.scenario.source_issue = 'custom';
  c.backend.liveEvalHistory = [custom, historical];
  c.backend.evalCompareKeys = ['custom', 'history']; c.backend.evalCompareReady = true;
  const html = c.liveScenarioComparison();
  assert.match(html, /自定义：&lt;假设&gt;/);
  assert.match(html, /第 26122 期/);
  custom.result.scenario.source_issue = '26123';
  assert.match(c.liveScenarioComparison(), /对照结果已隐藏/);
});
