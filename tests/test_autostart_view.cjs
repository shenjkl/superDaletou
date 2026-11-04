// Exercise the controls without starting a browser, service or registry write.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('let autostartBusy=false;');
const end = source.indexOf('function updatePlans()', start);
assert.ok(start >= 0 && end > start);
const target = {innerHTML: ''};
let notices = [];
const context = {backend: {ready: true, autostart: null},
  location: {port: '8000'},
  escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
  el: id => id === 'autostart-controls' ? target : null,
  toast: message => notices.push(message)};
vm.runInNewContext(source.slice(start, end), context);
context.refreshBackend = async () => {context.backend.autostart = null;};
context.submitUpdateWrite = async (_operation, request) => {
  const result = await context.backendPost('/api/autostart', {enabled:request.enabled});
  context.backend.autostart = result.autostart;
  return {accepted:true};
};

test('disabled, enabled, conflict and missing program states are clear', () => {
  context.backend.autostart = {state: 'disabled', can_enable: true};
  assert.match(context.autostartControls(), /未开启/);
  assert.match(context.autostartControls(), /开启登录后启动/);
  context.backend.autostart = {state: 'enabled', can_enable: false, reason: '后台启动程序缺失'};
  assert.match(context.autostartControls(), /关闭登录后启动/);
  assert.match(context.autostartControls(), /程序缺失/);
  context.backend.autostart = {state: 'conflict', reason: '<unsafe>'};
  assert.doesNotMatch(context.autostartControls(), /data-action="toggle-autostart"/);
  assert.match(context.autostartControls(), /&lt;unsafe&gt;/);
  context.backend.autostart = {state: 'disabled', can_enable: false};
  assert.doesNotMatch(context.autostartControls(), /data-action="toggle-autostart"/);
});

test('toggle applies returned state and errors clear stale status', async () => {
  notices = [];
  context.backend.autostart = {state: 'disabled', can_enable: true};
  context.backendPost = async (url, body) => {
    assert.equal(url, '/api/autostart'); assert.equal(body.enabled, true);
    return {autostart: {state: 'enabled', can_enable: true}};
  };
  await context.toggleAutostart(true);
  assert.equal(context.backend.autostart.state, 'enabled');
  assert.match(target.innerHTML, /已开启/);
  assert.match(notices[0], /已设置/);
  context.backendPost = async () => {throw Error('保存失败');};
  await context.toggleAutostart(false);
  assert.equal(context.backend.autostart, null);
  assert.match(target.innerHTML, /连接本机服务/);
  assert.match(notices[1], /结果待确认/);
});
