// Render and simulate cancellation without a browser or network.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const listStart = source.indexOf('function updatePlanList()');
const listEnd = source.indexOf('\nfunction ', listStart + 1);
const cancelStart = source.indexOf('async function cancelUpdateRun(');
const cancelEnd = source.indexOf('\n', cancelStart);
assert.ok(listStart >= 0 && listEnd > listStart && cancelStart >= 0 && cancelEnd > cancelStart);
let messages = [];
const context = {backend: {plans: []},
  escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
  tag: text => text, runConfigHistory: () => '', planChangeHistory: () => '',
  refreshBackend: async () => {}, toast: text => messages.push(text)};
vm.runInNewContext(source.slice(listStart, listEnd) + '\n' + source.slice(cancelStart, cancelEnd), context);

function html(status, oneOff=false) {
  context.backend.plans = [{id: 'plan', enabled: !oneOff, config: {run_once: oneOff, interval_seconds: 60, page_size: 20},
    next_due: 'later', runs: [{id: 'run1', status, started_at: 'now', followups: []}]}];
  return context.updatePlanList();
}

test('only a running update offers cancellation; intermediate and final states are clear', () => {
  assert.match(html('running'), /取消本次运行/);
  assert.match(html('running', true), /取消本次运行/);
  assert.doesNotMatch(html('cancelling'), /取消本次运行/);
  assert.match(html('cancelling'), /取消中/);
  assert.match(html('cancelling'), /已提交的快照会保留/);
  assert.match(html('cancelled', true), /已取消/);
  assert.doesNotMatch(html('success'), /取消本次运行/);
});

test('cancel sends both ids and reports the actual competing terminal result', async () => {
  const button = {dataset: {plan: 'plan', run: 'run1'}, disabled: false, isConnected: true};
  context.backendPost = async (url, data) => {
    assert.equal(url, '/api/update-runs/cancel'); assert.equal(data.plan_id, 'plan'); assert.equal(data.run_id, 'run1');
    return {run: {status: 'success'}};
  };
  messages = [];
  await context.cancelUpdateRun(button);
  assert.match(messages[0], /本次运行已完成/);
  assert.doesNotMatch(messages[0], /已取消/);
  assert.equal(button.disabled, false);
  context.backendPost = async () => ({run: {status: 'cancelling'}});
  await context.cancelUpdateRun(button);
  assert.match(messages[1], /已请求取消/);
  assert.match(messages[1], /计划启停状态不变/);
});
