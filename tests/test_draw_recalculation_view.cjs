// Pure rendering and mocked interactions; this is not a browser/layout test.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('function singleDrawRetry(');
const retry = source.indexOf('async function retryDrawCalculation(', start);
const end = source.indexOf('\n', retry);
assert.ok(start >= 0 && retry > start && end > retry);

function setup() {
  const calls = [], target = {hidden: true, innerHTML: ''};
  const c = {drawReviewSequence: 3, activeDrawReview: {issue: '26017'},
    escape: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
    tag: text => text, el: id => id === 'draw-review-outcomes' ? target : null,
    getPaperWriteJournal: () => ({
      begin: async (_operation, request) => {calls.push({url:'/api/paper-results/recalculate-issue', body:request}); return {phase:'accepted'};},
      result: async () => ({recalculations: {exp: {status: 'complete'}}}),
    }),
    refreshBackend: async () => {}, toast: () => {}, reviewError: text => text,
  };
  vm.runInNewContext(source.slice(start, end), c);
  return {c, calls, target};
}

function review(snapshots, resolution = null) {
  return {issue: '26017', resolution, variants: [{row: {}, row_sha256: 'a'.repeat(64), snapshots}]};
}

test('single version offers a retry per retained snapshot', () => {
  const {c} = setup();
  const html = c.drawReviewMarkup(review([{snapshot_id: '0123456789abcdef', observations: []}]));
  assert.match(html, /当前只有一个开奖版本/);
  assert.match(html, /data-action="retry-draw-calculation" data-issue="26017" data-snapshot="0123456789abcdef"/);
  assert.match(html, /重算本期 · 0123456789ab/);
  assert.match(html, /来源、规则或截止条件不满足时仍会阻断/);
});

test('missing snapshots do not offer an unusable retry', () => {
  const {c} = setup();
  const html = c.drawReviewMarkup(review([]));
  assert.match(html, /没有可用于重算的留存快照/);
  assert.doesNotMatch(html, /data-action="retry-draw-calculation"/);
});

test('resolved draw has only one retry entry', () => {
  const {c} = setup();
  const html = c.drawReviewMarkup(review([{snapshot_id: '0123456789abcdef', observations: []}],
    {selected_snapshot: '0123456789abcdef', selected_row_sha256: 'a'.repeat(64), reviewed_at: 'now', note: 'checked'}));
  assert.equal((html.match(/data-action="retry-draw-calculation"/g) || []).length, 1);
});

test('retry sends the chosen issue and snapshot and displays completion', async () => {
  const {c, calls, target} = setup();
  await c.retryDrawCalculation('26017', 'snapshot-id');
  assert.equal(calls[0].url, '/api/paper-results/recalculate-issue');
  assert.equal(calls[0].body.issue, '26017');
  assert.equal(calls[0].body.snapshot_id, 'snapshot-id');
  assert.equal(target.hidden, false);
  assert.match(target.innerHTML, /已计算/);
});
