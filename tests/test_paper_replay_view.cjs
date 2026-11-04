// Pure rendering and mocked download checks; not a browser/layout test.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('function fixedExperimentList()');
const end = source.indexOf('async function saveSourceReview(', start);
assert.ok(start >= 0 && end > start);

function setup() {
  const links = [], messages = [];
  const c = {backend: {experiments: []},
    escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
    tag: text => text, toast: text => messages.push(text), atob, Blob,
    URL: {createObjectURL: () => 'blob:fixture', revokeObjectURL() {}},
    setTimeout: fn => fn(),
    document: {addEventListener() {}, body: {append(link) {links.push(link);}},
      createElement() {return {click() {this.clicked = true;}, remove() {this.removed = true;}};}},
    backendPost: async () => ({package_base64: Buffer.from('{}').toString('base64'), bundle_sha256: 'a'.repeat(64)}),
  };
  vm.runInNewContext(source.slice(start, end), c);
  c.links = links; c.messages = messages;
  return c;
}

test('only a currently available paper result offers replay download', () => {
  const c = setup();
  const base = {target_issue: '26017', hypothesis: 'test', replicas: 1, registered_at: 'now', status: 'locally_attested'};
  c.backend.experiments = [{...base, request_key: 'ready', paper_status: 'locally_attested'},
    {...base, request_key: 'waiting', paper_status: 'waiting'}];
  const html = c.fixedExperimentList();
  assert.match(html, /data-experiment="ready"\s*>下载结算重放包/);
  assert.match(html, /data-experiment="waiting" disabled>下载结算重放包/);
  assert.match(html, /包含号码与结算金额/);
});

test('download requests the selected experiment and restores its button', async () => {
  const c = setup(), calls = [];
  c.backendPost = async (url, body) => {calls.push({url, body}); return {package_base64: Buffer.from('{}').toString('base64'), bundle_sha256: 'a'.repeat(64)};};
  const button = {dataset: {experiment: 'exp'}, disabled: false, isConnected: true};
  await c.exportPaperReplay(button);
  assert.equal(calls[0].url, '/api/paper-replay');
  assert.equal(calls[0].body.experiment_key, 'exp');
  assert.equal(c.links[0].download, 'dlt-paper-replay-aaaaaaaaaaaa.json');
  assert.equal(c.links[0].clicked, true);
  assert.equal(button.disabled, false);
  assert.match(c.messages[0], /已下载/);
});

test('a rejected export does not initiate a download', async () => {
  const c = setup();
  c.backendPost = async () => {throw new Error('需要复核');};
  const button = {dataset: {experiment: 'exp'}, disabled: false, isConnected: true};
  await c.exportPaperReplay(button);
  assert.equal(c.links.length, 0);
  assert.equal(button.disabled, false);
  assert.match(c.messages[0], /需要复核/);
});
