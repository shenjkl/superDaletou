// Rendering checks only; not a browser interaction or layout test.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('function updatePlanList()');
const end = source.indexOf('\nfunction ', start + 1);
assert.ok(start >= 0 && end > start);

function render(followup, error = null) {
  const context = {backend: {plans: [{id: 'plan123456', enabled: true,
    config: {interval_seconds: 3600, page_size: 20}, next_due: 'later',
    runs: [{status: 'success', started_at: 'now', error, followups: [followup]}]}]},
    escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    tag: text => `<span>${text}</span>`, runConfigHistory: () => '', planChangeHistory: () => ''};
  vm.runInNewContext(source.slice(start, end) + '\nresult=updatePlanList();', context);
  return context.result;
}

test('missing target and per-experiment errors are visible in an incomplete follow-up', () => {
  const html = render({status: 'incomplete', details: {
    first: {status: 'waiting_source', issue: '26017'},
    second: {status: 'error', reason: 'record damaged'},
  }});
  assert.match(html, /第 26017 期尚未查到/);
  assert.match(html, /检查异常：record damaged/);
  assert.doesNotMatch(html, /没有匹配奖期/);
});

test('completed follow-up still displays successful and blocked outcomes', () => {
  const html = render({status: 'complete', details: {
    first: {status: 'complete'}, second: {status: 'blocked', reason: 'rule missing'},
  }});
  assert.match(html, /计算完成/);
  assert.match(html, /阻断：rule missing/);
});

test('historical page failure does not look like complete fetching', () => {
  const html = render({status: 'incomplete', details: {first: {status: 'waiting_source', issue: '26017'}}},
    '历史第 2 页抓取失败：offline');
  assert.match(html, /最新页已更新，历史补查未完成/);
  assert.match(html, /历史第 2 页抓取失败：offline/);
  assert.match(html, /历史补查最多 10 页/);
});

test('plan setup explains resumed search and the minimum useful page budget', () => {
  const startForm = source.indexOf('function updatePlans()');
  const endForm = source.indexOf('\nfunction updateWorkerStatus()', startForm);
  assert.ok(startForm >= 0 && endForm > startForm);
  const context = {tag: text => text, updateWorkerStatus: () => '', updatePlanList: () => '', autostartControls: () => ''};
  vm.runInNewContext(source.slice(startForm, endForm) + '\nresult=updatePlans();', context);
  assert.match(context.result, /下次先核对末页/);
  assert.match(context.result, /至少设置 3 页/);
});
