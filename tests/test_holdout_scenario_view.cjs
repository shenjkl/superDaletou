// Readable labels for the local audit; no browser or service is started.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('function holdoutAuditContent(');
const end = source.indexOf('\n', start);
assert.ok(start >= 0 && end > start);
const context = {escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')};
vm.runInNewContext(source.slice(start, end), context);

test('historical and unverified scenario uses are identified in the audit', () => {
  const report = {status: 'known_use', as_of_utc: '2026-11-01T00:00:00+00:00', issues: ['26122'], unresolved: [],
    findings: [
      {kind: 'portfolio_scenario_evaluation', status: 'saved', record_key: 'one', input_issues: [], target_issues: ['26122']},
      {kind: 'portfolio_scenario_unverified', status: 'saved', record_key: 'two', input_issues: [], target_issues: ['26122']}
    ]};
  const html = context.holdoutAuditContent(report);
  assert.match(html, /历史奖金情景评估/);
  assert.match(html, /本人声明或未核验的奖金期号/);
  assert.match(html, /目标期或奖金情景期/);
  assert.match(html, /独立性仍未验证/);
});

test('unattributed custom payouts explicitly block an unused-range claim', () => {
  const report = {status:'unresolved',as_of_utc:'2026-11-01T00:00:00+00:00',issues:['26122'],findings:[],
    unresolved:[{kind:'custom_scenario_origin',record_key:'old-custom',reason:'custom payout origin not declared'}]};
  const html = context.holdoutAuditContent(report);
  assert.match(html,/自定义奖金来源未声明/);
  assert.match(html,/无法判断是否涉及本范围/);
  assert.match(html,/可在下方旧记录补录本人声明/);
  assert.doesNotMatch(html,/未发现已记录的使用/);
});
