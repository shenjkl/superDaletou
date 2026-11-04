// Render-only checks; the interactive browser workflow still needs UI QA.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const cards = source.slice(source.indexOf('function reservationCards('), source.indexOf('async function refreshReservations('));
const editor = source.slice(source.indexOf('function prepareEvaluationSuccessor('), source.indexOf('async function submitEvaluationSuccessor('));
const submitter = source.slice(source.indexOf('async function submitEvaluationSuccessor('), source.indexOf('async function startFixedEvaluation('));
assert.ok(cards.startsWith('function reservationCards(') && editor.startsWith('function prepareEvaluationSuccessor('));

function render({superseded = false, stale = false, child = false, blocked = false} = {}) {
  const id = 'a'.repeat(64), next = 'b'.repeat(64), root = {innerHTML: '', scrollIntoView() {}};
  const reservation = {id: child ? next : id, superseded_by: superseded ? next : null,
    content: {start_issue: '26122', end_issue: '26123', issues: ['26122', '26123'],
      ...(child ? {supersedes: id, supersession_reason: '系统升级'} : {})}};
  const evaluation = {reservation_id: id, status: 'pending', superseded_by: superseded ? next : null,
    environment_matches: !stale, can_supersede: stale && !superseded && !blocked, attempts: [],
    protocol: {hypothesis: '原假设', methods: [{id: 'hot', protocol: {candidate: {method: 'hot', budget_fen: 400}}}]}};
  const context = {holdoutReservations: [reservation], fixedEvaluations: child ? [{...evaluation, reservation_id: next}] : [evaluation],
    generationMethods: {hot: '热号'}, escape: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    tag: label => `<span>${label}</span>`, fixedEvaluationReport: () => '', el: () => root};
  vm.runInNewContext(cards+'\n'+editor+'\nresult = reservationCards();', context);
  return {html: context.result, editor: () => {context.prepareEvaluationSuccessor(id); return root.innerHTML;}};
}

test('stale unused protocol offers explicit successor instead of running', () => {
  const result = render({stale: true});
  assert.match(result.html, /创建关联新版本/);
  assert.doesNotMatch(result.html, /运行固定评估/);
  const form = result.editor();
  assert.match(form, /旧协议被替代后不能运行/);
  assert.match(form, /新版本原样继承方法、预算、种子/);
  assert.match(form, /旧协议从未运行/);
  assert.match(form, /原假设/);
});

test('superseded protocol remains visible but cannot run or create another successor', () => {
  const {html} = render({stale: true, superseded: true});
  assert.match(html, /已由新版本替代/);
  assert.doesNotMatch(html, /运行固定评估/);
  assert.doesNotMatch(html, /创建关联新版本/);
});

test('new successor identifies its predecessor and retains the fixed protocol', () => {
  const {html} = render({child: true});
  assert.match(html, /新版本，继承 aaaaaaaa/);
  assert.match(html, /创建原因：系统升级/);
  assert.match(html, /运行固定评估/);
  assert.doesNotMatch(html, /配置固定评估/);
});

test('unchanged pending protocol remains runnable', () => {
  const {html} = render();
  assert.match(html, /运行固定评估/);
  assert.doesNotMatch(html, /创建关联新版本/);
});

test('stale protocol without an eligible successor does not offer unsafe actions', () => {
  const {html} = render({stale: true, blocked: true});
  assert.match(html, /记录状态无法满足新版本条件/);
  assert.doesNotMatch(html, /运行固定评估/);
  assert.doesNotMatch(html, /创建关联新版本/);
});

test('successor submit sends explicit confirmation and refreshes the linked version', async () => {
  const calls = [], notices = [], button = {disabled: false, isConnected: true};
  const form = {dataset: {reservation: 'old-id'}, querySelector: () => button};
  const context = {FormData: class {get(key) {return key === 'reason' ? '  upgrade  ' : 'on';}},
    backendPost: async (url, body) => {calls.push({url, body}); return {reservation: {id: 'new-id'}};},
    refreshReservations: async () => calls.push('refresh'), toast: text => notices.push(text), el: () => null};
  vm.runInNewContext(submitter, context);
  await context.submitEvaluationSuccessor(form);
  assert.equal(calls[0].url, '/api/fixed-evaluations/supersede');
  assert.equal(calls[0].body.reservation_id, 'old-id');
  assert.equal(calls[0].body.reason, 'upgrade');
  assert.equal(calls[0].body.confirmed, true);
  assert.equal(calls[1], 'refresh');
  assert.match(notices[0], /保留原协议/);
  assert.equal(button.disabled, false);
});

test('successor submit shows server rejection and re-enables the action', async () => {
  const error = {textContent: ''}, button = {disabled: false, isConnected: true};
  const form = {dataset: {reservation: 'old-id'}, querySelector: () => button};
  const context = {FormData: class {get(key) {return key === 'reason' ? 'upgrade' : 'on';}},
    backendPost: async () => {throw Error('已有运行记录');}, refreshReservations: async () => {throw Error('unexpected refresh');},
    toast: () => {}, el: id => id === 'supersede-fixed-eval-error' ? error : null};
  vm.runInNewContext(submitter, context);
  await context.submitEvaluationSuccessor(form);
  assert.equal(error.textContent, '已有运行记录');
  assert.equal(button.disabled, false);
});
