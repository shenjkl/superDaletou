// Pure rendering checks; browser layout and interactions require separate QA.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const helpers = source.slice(source.indexOf('function rationalYuan('), source.indexOf('function outcomeTable('));
const renderers = source.slice(source.indexOf('function backtestResult('), source.indexOf('async function submitBacktest('));
assert.ok(helpers.startsWith('function rationalYuan(') && renderers.startsWith('function backtestResult('));

function render(status, corrupt = false, reason = '') {
  const amount = {gross: '7', tax: '1', cost: '4', net: '2'};
  const report = {planned_periods: 1, complete_periods: 1, incomplete_issues: [], completeness: 'complete',
    descriptive_mean_fraction: [2, 1], periods: [{target: {issue: '26017'}, status: 'complete',
      mean_difference_fraction: [2, 1], replicas: [{replica: 'a', status: 'complete',
        candidate_settlement: amount, control_settlement: amount, difference_yuan: '0'}]}]};
  const description = {included: 1, planned: 1, positive: 1, zero: 0, negative: 0,
    min_fraction: [2, 1], p25_fraction: [2, 1], median_fraction: [2, 1], p75_fraction: [2, 1],
    max_fraction: [2, 1], largest_absolute_issue: '26017', largest_absolute_share_fraction: [1, 1]};
  if (corrupt) report.periods[0].replicas[0].candidate_settlement = null;
  const context = {
    backend: {backtests: [{request: {protocol: {candidate: {method: 'hot', budget_fen: 400},
      targets: ['26017'], replicas: ['a']}}, result: {report, status, reason, paired_description: description},
      status: 'complete', created_at: '2026-01-01'}]},
    generationMethods: {hot: '热号'},
    escape: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    tag: label => `<span>${label}</span>`,
  };
  vm.runInNewContext(helpers + '\n' + renderers + '\nresult = backtestJobs();', context);
  return context.result;
}

test('valid report displays observed distribution and its non-inference limitation', () => {
  const html = render('research_only');
  assert.match(html, /已观察配对差额的分布与敏感性/);
  assert.match(html, /不是置信区间/);
  assert.match(html, /平均税后净收益配对差额 2\.00 元/);
});

test('review-required report keeps old mean inside closed historical details', () => {
  const html = render('review_required');
  const closedHistory = '<details class="section-space"><summary class="text-button">查看历史报告</summary>';
  const boundary = html.indexOf(closedHistory);
  assert.ok(boundary >= 0);
  assert.match(html.slice(0, boundary), /当前统计不可用/);
  assert.doesNotMatch(html.slice(0, boundary), /平均税后净收益配对差额/);
  assert.match(html.slice(boundary), /平均税后净收益配对差额 2\.00 元/);
  assert.doesNotMatch(html, /已观察配对差额的分布与敏感性/);
});

test('damaged historical settlement remains inspectable without crashing the list', () => {
  const html = render('review_required', true);
  assert.match(html, /当前统计不可用/);
  assert.match(html, /查看历史报告/);
  assert.match(html, /候选 金额缺失/);
  assert.doesNotMatch(html, /已观察配对差额的分布与敏感性/);
});

test('precision review explains the cause before the collapsed historical report', () => {
  const html = render('review_required', false, '金额不一致 <重新运行>');
  const boundary = html.indexOf('<details class="section-space"><summary class="text-button">查看历史报告</summary>');
  assert.ok(boundary > 0);
  assert.match(html.slice(0, boundary), /结果待复核/);
  assert.match(html.slice(0, boundary), /金额不一致 &lt;重新运行&gt;/);
  assert.doesNotMatch(html.slice(0, boundary), /平均税后净收益配对差额/);
});
