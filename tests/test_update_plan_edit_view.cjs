// Pure rendering and simulated submission; does not start a server or browser.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const listStart = source.indexOf('function updatePlanList()');
const formEnd = source.indexOf('async function saveUpdatePlan(', listStart);
assert.ok(listStart >= 0 && formEnd > listStart);
const cfg = {start_at: '2026-01-01T00:00:00+08:00', interval_seconds: 60,
  timezone_offset_minutes: 480, page_size: 20, timeout_seconds: 20, max_bytes: 100000,
  retry_count: 1, retry_wait_seconds: 30, max_pages: 10};
const root = {hidden: true, innerHTML: '', scrollIntoView() {}};
let notices = [];
const c = {backend: {plans: []},
  escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  tag: value => value, el: id => id === 'update-plan-editor' ? root : null,
  FormData: class {constructor(form) {this.values = form.values;} get(key) {return this.values[key];}},
  refreshBackend: async () => {}, toast: text => notices.push(text)};
vm.runInNewContext(source.slice(listStart, formEnd), c);
c.submitUpdateWrite=async(operation,request)=>{
  const result=await c.backendPost('/api/update-plans/edit',request);
  return {accepted:true,entry:{result}};
};

test('paused plan offers an inline editor and displays its retained history', () => {
  c.backend.plans = [{id: 'plan', enabled: false, config_sha256: 'a'.repeat(64), next_due: 'later',
    config: cfg, runs: [{status: 'failed', started_at: 'now', config_status: 'recorded', recorded_config: cfg}],
    changes: [{status: 'recorded', changed_at: 'then', old_config: cfg, new_config: {...cfg, interval_seconds: 120}}]}];
  let html = c.updatePlanList();
  assert.match(html, /编辑计划/); assert.match(html, /查看最近运行时的配置/);
  assert.match(html, /查看配置变更记录/); assert.match(html, /间隔 120 秒/);
  c.openUpdatePlanEditor('plan');
  assert.equal(root.hidden, false); assert.match(root.innerHTML, /保存后仍保持暂停/);
  assert.match(root.innerHTML, /2026-01-01T00:00:00\+08:00/);
  c.backend.plans[0].enabled = true;
  root.hidden = true;
  c.openUpdatePlanEditor('plan');
  assert.equal(root.hidden, true);
  html = c.updatePlanList(); assert.match(html, /disabled title="请先暂停计划"/);
});

test('submit carries displayed revision and handles stale write without closing editor', async () => {
  const button = {disabled: false, isConnected: true}, error = {textContent: ''};
  const form = {dataset: {plan: 'plan', sha: 'a'.repeat(64)}, isConnected: true,
    values: {start: cfg.start_at, interval: '120', offset: '480', page_size: '20', timeout: '20',
      max_bytes: '100000', retry_count: '1', retry_wait: '30', max_pages: '10'},
    querySelector: selector => selector === '[type=submit]' ? button : error};
  c.backendPost = async (url, data) => {
    assert.equal(url, '/api/update-plans/edit'); assert.equal(data.expected_sha256, 'a'.repeat(64));
    assert.equal(data.config.interval_seconds, 120);
    throw Error('计划配置已变化，请重新加载');
  };
  root.hidden = false;
  await c.saveEditedUpdatePlan(form);
  assert.match(error.textContent, /已变化/); assert.equal(root.hidden, false); assert.equal(button.disabled, false);
  c.backendPost = async () => ({edit: {changed: true}});
  await c.saveEditedUpdatePlan(form);
  assert.equal(root.hidden, true); assert.match(notices.at(-1), /仍处于暂停状态/);
});
