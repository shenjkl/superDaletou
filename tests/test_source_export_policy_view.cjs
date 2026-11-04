// Pure rendering check; no browser or HTTP server is started.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('const sourceFileChecks=new WeakMap();');
const end = source.indexOf('function updateSourceCorrectionPreview(', start);
assert.ok(start >= 0 && end > start);
const c = {escape: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
  crypto: crypto.webcrypto, Uint8Array, document: {addEventListener() {}}};
vm.runInNewContext(source.slice(start, end), c);
const listStart = source.indexOf('function sourceReviewList()');
const listEnd = source.indexOf('\n', listStart);
assert.ok(listStart >= 0 && listEnd > listStart);
vm.runInNewContext(source.slice(listStart, listEnd), c);

test('source metadata retains the checksum without offering an unsanitized download', () => {
  const html = c.sourceDocumentLink({review_id: 'b'.repeat(64), document_sha256: 'a'.repeat(64), document_name: 'SECRET_NAME'});
  assert.match(html, /原文已留存在本机/);
  assert.match(html, /a{64}/);
  assert.match(html, /原样下载暂不可用/);
  assert.match(html, /文件只在浏览器内计算摘要，不上传/);
  assert.match(html, /查看留存资料/);
  assert.doesNotMatch(html, /SECRET_NAME|<a |download=|source-review-document/);
});

test('one saved source can be viewed without creating a correction', () => {
  c.backend = {reviews: [{review_id: 'b'.repeat(64), document_sha256: 'a'.repeat(64),
    kind: 'calendar', status: 'personally_reviewed'}]};
  const html = c.sourceReviewList();
  assert.match(html, /已保存资料（1）/);
  assert.match(html, /查看留存资料/);
  assert.match(html, /data-source-id="b{64}"/);
  c.backend.reviews[0].status = 'review_required';
  assert.doesNotMatch(c.sourceReviewList(), /查看留存资料/);
});

function fakePreviewPanel() {
  const status = {textContent: ''}, content = {textContent: '', hidden: true}, viewer = {hidden: true};
  const image = {hidden: true, src: '', removeAttribute(name) {if (name === 'src') this.src = '';}};
  const prev = {disabled: true, dataset: {}, isConnected: true, matches: () => true};
  const next = {disabled: true, dataset: {}, isConnected: true, matches: () => true};
  const fields = {'[data-source-viewer]': viewer, '[data-source-preview-status]': status,
    '[data-source-preview-text]': content, '[data-source-preview-image]': image,
    '[data-source-preview-prev]': prev, '[data-source-preview-next]': next};
  const panel = {dataset: {sourceId: 'b'.repeat(64)}, isConnected: true, querySelector: key => fields[key]};
  const open = {disabled: false, dataset: {offset: '0'}, isConnected: true, matches: () => false,
    closest: () => panel};
  prev.closest = next.closest = () => panel;
  return {panel, open, prev, next, status, content, image, viewer};
}

test('stored text is rendered as text, with Unicode-aware range and no download', async () => {
  const view = fakePreviewPanel(), text = 'A😀<script>alert(1)</script>';
  c.backendPost = async (url, request) => {
    assert.equal(url, '/api/source-review-preview');
    assert.equal(request.review_id, view.panel.dataset.sourceId);
    return {preview: {review_id: request.review_id, offset: 0, text,
      total_chars: Array.from(text).length, previous_offset: null, next_offset: null}};
  };
  await c.previewStoredSource(view.open);
  assert.equal(view.content.textContent, text);
  assert.equal(view.content.innerHTML, undefined);
  assert.match(view.status.textContent, /1—27 \/ 27 字/);
  assert.equal(view.prev.disabled, true);
  assert.equal(view.next.disabled, true);
  assert.equal(view.open.disabled, false);
});

test('preview navigation preserves disabled state at both ends', async () => {
  const view = fakePreviewPanel();
  c.backendPost = async (_, request) => ({preview: {review_id: request.review_id, offset: request.offset,
    text: request.offset ? '末' : '首', total_chars: 20001,
    previous_offset: request.offset ? 0 : null, next_offset: request.offset ? null : 20000}});
  await c.previewStoredSource(view.open);
  assert.equal(view.prev.disabled, true); assert.equal(view.next.disabled, false);
  await c.previewStoredSource(view.next);
  assert.equal(view.prev.disabled, false); assert.equal(view.next.disabled, true);
  await c.previewStoredSource(view.prev);
  assert.equal(view.prev.disabled, true); assert.equal(view.next.disabled, false);
});

test('PDF preview displays only PNG data and navigates by PDF page', async () => {
  const view = fakePreviewPanel();
  c.backendPost = async (_, request) => ({preview: {kind: 'pdf_page', review_id: request.review_id, offset: request.offset,
    total_pages: 2, width: 450, height: 600, image_base64: 'iVBORw0KGgoAAA==',
    previous_offset: request.offset ? 0 : null, next_offset: request.offset ? null : 1}});
  await c.previewStoredSource(view.open);
  assert.match(view.image.src, /^data:image\/png;base64,/);
  assert.equal(view.image.hidden, false); assert.equal(view.content.hidden, true);
  assert.match(view.status.textContent, /PDF 第 1 \/ 2 页/);
  assert.equal(view.prev.disabled, true); assert.equal(view.next.disabled, false);
  await c.previewStoredSource(view.next);
  assert.match(view.status.textContent, /PDF 第 2 \/ 2 页/);
  assert.equal(view.prev.disabled, false); assert.equal(view.next.disabled, true);
});

test('invalid image data clears the old preview without inserting unsafe markup', async () => {
  const view = fakePreviewPanel(); view.image.src = 'data:image/png;base64,old';
  c.backendPost = async (_, request) => ({preview: {kind: 'pdf_page', review_id: request.review_id, offset: 0,
    total_pages: 1, width: 100, height: 100, image_base64: '<script>', previous_offset: null, next_offset: null}});
  await c.previewStoredSource(view.open);
  assert.equal(view.image.src, ''); assert.equal(view.image.hidden, true);
  assert.match(view.status.textContent, /返回的 PDF 预览无效/);
});

test('unsupported-format error clears old text and displays the reason', async () => {
  const view = fakePreviewPanel(); view.content.textContent = 'old private content';
  c.backendPost = async () => {throw Error('该格式暂不支持文本查看');};
  await c.previewStoredSource(view.open);
  assert.equal(view.content.textContent, '');
  assert.match(view.status.textContent, /该格式暂不支持/);
  assert.equal(view.open.disabled, false);
});

test('invalid source checksum is not displayed as trusted', () => {
  const html = c.sourceDocumentLink({document_sha256: '<unsafe>'});
  assert.match(html, /待核验/);
  assert.doesNotMatch(html, /<unsafe>/);
});

test('locally selected bytes are compared with the retained digest without a request', async () => {
  const bytes = new TextEncoder().encode('synthetic document');
  const sha = crypto.createHash('sha256').update(bytes).digest('hex');
  const message = {textContent: ''};
  const input = {dataset: {sourceSha: sha}, files: [{size: bytes.length, arrayBuffer: async () => bytes.buffer}],
    isConnected: true, parentElement: {parentElement: {querySelector: () => message}}};
  await c.verifyLocalSourceFile(input);
  assert.match(message.textContent, /摘要一致/);
  assert.match(message.textContent, /不证明来源真实/);
  input.dataset.sourceSha = '0'.repeat(64);
  await c.verifyLocalSourceFile(input);
  assert.match(message.textContent, /摘要不一致/);
});

test('oversized files and detached inputs do not publish a misleading match', async () => {
  const message = {textContent: ''};
  const input = {dataset: {sourceSha: 'a'.repeat(64)}, files: [{size: 5*1024*1024+1}],
    isConnected: true, parentElement: {parentElement: {querySelector: () => message}}};
  await c.verifyLocalSourceFile(input);
  assert.match(message.textContent, /超过 5 MiB/);
  input.files = [{size: 1, arrayBuffer: async () => {input.isConnected = false; return new Uint8Array([1]).buffer;}}];
  await c.verifyLocalSourceFile(input);
  assert.match(message.textContent, /正在本地计算/);
});

test('a slower previous file cannot replace the latest result', async () => {
  const latest = new TextEncoder().encode('latest'), old = new TextEncoder().encode('old');
  const sha = crypto.createHash('sha256').update(latest).digest('hex');
  let release;
  const pending = new Promise(resolve => {release = () => resolve(old.buffer);});
  const message = {textContent: ''};
  const input = {dataset: {sourceSha: sha}, files: [{size: old.length, arrayBuffer: () => pending}],
    isConnected: true, parentElement: {parentElement: {querySelector: () => message}}};
  const first = c.verifyLocalSourceFile(input);
  input.files = [{size: latest.length, arrayBuffer: async () => latest.buffer}];
  await c.verifyLocalSourceFile(input);
  release(); await first;
  assert.match(message.textContent, /摘要一致/);
});
