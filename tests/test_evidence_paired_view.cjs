// Render-only check; does not replace interactive browser verification.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const helpers = source.slice(source.indexOf('function rationalYuan('), source.indexOf('function outcomeTable('));
const decisionEntry = source.slice(source.indexOf('function researchDecisionEntry('), source.indexOf('function decisionEvidenceSummary('));
const renderer = source.slice(source.indexOf('function evidenceFixedPanel('), source.indexOf('function refreshEvidencePanel('));
assert.ok(helpers.startsWith('function rationalYuan(') && renderer.startsWith('function evidenceFixedPanel('));

function render(validity) {
  const description = {included: 1, planned: 2, positive: 1, zero: 0, negative: 0,
    min_fraction: [2, 1], p25_fraction: [2, 1], median_fraction: [2, 1], p75_fraction: [2, 1],
    max_fraction: [2, 1], largest_absolute_issue: '26017', largest_absolute_share_fraction: [1, 1]};
  const result = {protocol: {candidate: {method: 'hot'}}, complete_periods: 1, planned_periods: 2,
    descriptive_mean_fraction: [2, 1]};
  const context = {fixedEvaluations: [{reservation_id: 'reservation-1', status: 'complete', validity,
    protocol: {hypothesis: '研究假设', methods: [{id: 'hot', protocol: {candidate: {method: 'hot'},
      targets: ['26017', '26018']}}]}, report: {methods: [{id: 'hot', result}]},
    paired_descriptions: {hot: description}}], generationMethods: {hot: '热号'},
    escape: v => String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    tag: label => `<span>${label}</span>`, go: () => '', exactYuan: () => '2.00 元'};
  vm.runInNewContext(helpers+'\n'+decisionEntry+'\n'+renderer+'\nresult = evidenceFixedPanel();', context);
  return context.result;
}

test('valid fixed evaluation offers paired distribution with limits', () => {
  const html = render('local_descriptive');
  assert.match(html, /查看热号配对差额分布/);
  assert.match(html, /不是置信区间/);
  assert.match(html, /完整 1 \/ 计划 2 期/);
  assert.match(html, /个人判断与新假设/);
});

test('review-required fixed evaluation withholds the current distribution', () => {
  const html = render('review_required');
  assert.match(html, /结果或依赖需要复核/);
  assert.doesNotMatch(html, /查看热号配对差额分布/);
  assert.doesNotMatch(html, /期均配对差额 2\.00/);
});
