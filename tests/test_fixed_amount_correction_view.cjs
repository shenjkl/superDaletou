// Pure rendering and mocked requests; not an interactive browser test.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const renderStart = source.indexOf('function fixedEvaluationReport(');
const renderEnd = source.indexOf('function reservationCards(', renderStart);
const actionStart = source.indexOf('async function recalculateFixedAmounts(');
const actionEnd = source.indexOf('async function reserveHoldout(', actionStart);
assert.ok(renderStart >= 0 && renderEnd > renderStart && actionStart >= 0 && actionEnd > actionStart);

function context() {
  const calls = [], notices = [];
  const c = {generationMethods: {hot: '热号'}, escape: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
    exactYuan: v => String(v ?? '暂无'), pairedDescription: () => '', document: {addEventListener() {}},
    backendPost: async (url, body) => {calls.push({url, body}); return {};},
    refreshReservations: async () => {calls.push('refresh');}, toast: message => notices.push(message)};
  vm.runInNewContext(source.slice(renderStart, renderEnd) + '\n' + source.slice(actionStart, actionEnd), c);
  return {c, calls, notices};
}

function report() {
  const amount = {gross: '10', tax: '2', cost: '4', net: '4'};
  return {methods: [{id: 'hot', result: {protocol: {candidate: {method: 'hot'}}, complete_periods: 1,
    planned_periods: 1, completeness: 'complete', descriptive_mean_fraction: [1, 1],
    periods: [{target: {issue: '26017'}, status: 'complete', mean_difference_fraction: [1, 1],
      replicas: [{replica: 'a', status: 'complete', candidate_settlement: amount, control_settlement: amount, difference_yuan: '0'}]}]}}]};
}

test('amount review offers an explicit recalculation and retains the old report', () => {
  const {c} = context();
  const html = c.fixedEvaluationReport({status: 'complete', validity: 'review_required', can_recalculate_amounts: true,
    validity_reason: '精确计算不一致', reservation_id: 'r123', report: report()});
  assert.match(html, /data-action="recalculate-fixed-amounts" data-reservation="r123"/);
  assert.match(html, /不重新训练或选择方法；原报告会保留/);
  assert.match(html, /查看历史结果/);
});

test('corrected report identifies both versions and exposes the original', () => {
  const {c} = context(), original = report();
  const html = c.fixedEvaluationReport({status: 'complete', validity: 'local_descriptive', report: report(), original_report: original,
    correction: {base_report_sha256: 'a'.repeat(64), report_sha256: 'b'.repeat(64)}});
  assert.match(html, /已更正金额/);
  assert.match(html, /aaaaaaaaaaaa/);
  assert.match(html, /bbbbbbbbbbbb/);
  assert.match(html, /查看更正前的报告/);
  assert.doesNotMatch(html, /data-action="recalculate-fixed-amounts"/);
});

test('click sends the fixed reservation, refreshes, and releases the control', async () => {
  const {c, calls, notices} = context(), button = {dataset: {reservation: 'r123'}, disabled: false, isConnected: true};
  await c.recalculateFixedAmounts(button);
  assert.equal(calls[0].url, '/api/fixed-evaluations/recalculate-amounts');
  assert.equal(calls[0].body.reservation_id, 'r123');
  assert.equal(calls[1], 'refresh');
  assert.equal(button.disabled, false);
  assert.match(notices[0], /原报告已保留/);
});

test('a rejected correction shows the reason and does not claim success', async () => {
  const {c, calls, notices} = context(), button = {dataset: {reservation: 'r123'}, disabled: false, isConnected: true};
  c.backendPost = async () => {throw new Error('奖金情景已变化');};
  await c.recalculateFixedAmounts(button);
  assert.equal(calls.length, 0);
  assert.equal(button.disabled, false);
  assert.match(notices[0], /金额重算未完成：奖金情景已变化/);
  assert.doesNotMatch(notices[0], /原报告已保留/);
});
