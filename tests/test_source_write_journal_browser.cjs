const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const repository = path.resolve(__dirname, '..');
const journalPath = path.join(repository, 'prototype', 'source-write-journal.js');
const browserCandidates = [
  process.env.CHROME_PATH,
  path.join(process.env.ProgramFiles || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
  path.join(process.env['ProgramFiles(x86)'] || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
  path.join(process.env.ProgramFiles || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
].filter(Boolean);
const browser = browserCandidates.find(candidate => fs.existsSync(candidate));
const tempRoot = process.env.DLT_BROWSER_TEST_ROOT;

test('real browser retains and resolves a lost source-write response after reload', {
  skip: !browser || !tempRoot,
  timeout: 60000,
}, async () => {
  const resolvedRoot = path.resolve(tempRoot);
  const allowedRoot = path.resolve(repository, '.tmp');
  assert.ok(resolvedRoot.startsWith(allowedRoot + path.sep), 'browser profile root must be inside repository .tmp');
  fs.mkdirSync(resolvedRoot, { recursive: true });
  const profile = fs.mkdtempSync(path.join(resolvedRoot, 'source-write-browser-'));
  const source = fs.readFileSync(journalPath, 'utf8');
  const writes = new Map();
  let server;
  let child;
  let socket;

  try {
    server = http.createServer((request, response) => {
      const url = new URL(request.url, 'http://127.0.0.1');
      if (url.pathname === '/') {
        response.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
        response.end('<!doctype html><meta charset="utf-8"><title>Journal integration</title>'
          + '<script src="/source-write-journal.js"></script>');
        return;
      }
      if (url.pathname === '/source-write-journal.js') {
        response.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8', 'cache-control': 'no-store' });
        response.end(source);
        return;
      }
      if (url.pathname === '/_test/accepted' && request.method === 'POST') {
        let body = '';
        request.setEncoding('utf8');
        request.on('data', chunk => { body += chunk; });
        request.on('end', () => {
          const input = JSON.parse(body);
          const write = { status: 'accepted', operation: 'source_review', request_key: input.request_key,
            request_sha256: input.request_sha256, retry_of: input.retry_of || null,
            result: { review_id: 'browser-journal-review-1' } };
          writes.set(input.request_key, write);
          response.writeHead(200, { 'content-type': 'application/json' });
          response.end(JSON.stringify({ write }));
        });
        return;
      }
      if (url.pathname === '/_test/status') {
        const key = url.searchParams.get('request_key');
        const write = writes.get(key);
        response.writeHead(write ? 200 : 404, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ write: write || { status: 'unknown' } }));
        return;
      }
      response.writeHead(404);
      response.end();
    });
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
    const origin = `http://127.0.0.1:${server.address().port}`;

    child = spawn(browser, ['--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
      '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0',
      `--user-data-dir=${profile}`, origin], { windowsHide: true, stdio: 'ignore' });
    const activePortFile = path.join(profile, 'DevToolsActivePort');
    let devtoolsPort;
    for (let i = 0; i < 200; i++) {
      if (child.exitCode !== null) throw new Error(`browser exited before DevTools started (${child.exitCode})`);
      if (fs.existsSync(activePortFile)) {
        devtoolsPort = Number(fs.readFileSync(activePortFile, 'utf8').split(/\r?\n/)[0]);
        if (Number.isInteger(devtoolsPort) && devtoolsPort > 0) break;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.ok(devtoolsPort, 'Chrome did not expose its DevTools port');
    const targets = await (await fetch(`http://127.0.0.1:${devtoolsPort}/json/list`)).json();
    const page = targets.find(target => target.type === 'page' && target.url === origin + '/');
    assert.ok(page?.webSocketDebuggerUrl, 'Chrome page target was not found');

    socket = new WebSocket(page.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve, { once: true });
      socket.addEventListener('error', reject, { once: true });
    });
    let nextId = 1;
    const pending = new Map();
    socket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.id && pending.has(message.id)) {
        const handlers = pending.get(message.id);
        pending.delete(message.id);
        message.error ? handlers.reject(new Error(message.error.message)) : handlers.resolve(message.result);
      }
    });
    const send = (method, params = {}) => new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      socket.send(JSON.stringify({ id, method, params }));
    });
    const evaluate = async expression => {
      const result = await send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
      if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description || result.exceptionDetails.text);
      return result.result.value;
    };
    await send('Runtime.enable');
    const waitForPage = async () => {
      for (let i = 0; i < 100; i++) {
        if (await evaluate('document.readyState') === 'complete'
            && await evaluate('Boolean(window.SourceWriteJournal)')) return;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      throw new Error('journal module did not load in Chrome');
    };
    await waitForPage();

    const key = '00000000-0000-4000-8000-000000000001';
    const setup = `(() => {
      const api = {
        digest: async () => '${'a'.repeat(64)}',
        objectKey: (operation, request) => operation + ':' + request.document_sha256,
        async post(body) {
          await fetch('/_test/accepted', {method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify(body)});
          if (window.holdPost) {
            window.postStarted = true;
            await new Promise(resolve => { window.releasePost = resolve; });
          }
          throw new Error('simulated lost HTTP response after commit');
        },
        async status(request_key, request_sha256, operation) {
          const response = await fetch('/_test/status?request_key=' + encodeURIComponent(request_key));
          const data = await response.json();
          if (!response.ok) return {status:'unknown'};
          return data.write;
        },
        async fence() { return {status:'fenced'}; }
      };
      window.makeSourceJournal = () => SourceWriteJournal.coordinator({
        store: SourceWriteJournal.browserStore(indexedDB), locks: navigator.locks, api,
        uuid: () => '${key}'
      });
    })()`;
    await evaluate(setup);
    assert.equal(await evaluate('typeof window.makeSourceJournal'), 'function', 'journal factory was not initialized');
    const started = await evaluate(`(async () => {
      const journal = window.makeSourceJournal();
      const request = {kind:'rule', document_sha256:'${'b'.repeat(64)}', document_size:3};
      const body = {...request, document_base64:'YWJj', document_name:'review.txt'};
      window.holdPost = true;
      window.postStarted = false;
      const original = journal.begin('source_review', request, body);
      for (let i = 0; i < 100 && !window.postStarted; i++) await new Promise(resolve => setTimeout(resolve, 10));
      if (!window.postStarted) throw new Error('source write did not reach the held HTTP request');
      let lockError = '';
      try { await journal.begin('source_review', {...request, reviewer_note:'overlapping operation'}, body); }
      catch (caught) { lockError = caught.message; }
      window.holdPost = false;
      window.releasePost();
      let error = '';
      try { await original; } catch (caught) { error = caught.message; }
      const [entry] = await journal.list();
      return {error, entry, lockError, locks: Boolean(navigator.locks), idb: Boolean(indexedDB)};
    })()`);
    assert.match(started.error, /simulated lost HTTP response/);
    assert.match(started.lockError, /同一来源对象正在其他页面处理/);
    assert.equal(started.entry.phase, 'submitted');
    assert.equal(started.locks, true);
    assert.equal(started.idb, true);
    assert.equal(writes.has(key), true, 'mock server must have committed before losing the response');

    await send('Page.reload', { ignoreCache: true });
    await waitForPage();
    await evaluate(setup);
    const recovered = await evaluate(`(async () => {
      const journal = window.makeSourceJournal();
      const entries = await journal.list();
      const checked = await journal.check('${key}');
      await journal.dismiss('${key}');
      return {entries, checked, remaining: await journal.list()};
    })()`);
    assert.equal(recovered.entries.length, 1);
    assert.equal(recovered.entries[0].phase, 'submitted');
    assert.equal(recovered.checked.phase, 'accepted');
    assert.equal(recovered.checked.request_key, key);
    assert.equal(recovered.remaining.length, 0);
  } finally {
    try { socket?.close(); } catch {}
    if (child && child.exitCode === null) {
      child.kill();
      await Promise.race([
        new Promise(resolve => child.once('exit', resolve)),
        new Promise(resolve => setTimeout(resolve, 5000)),
      ]);
    }
    if (server?.listening) await new Promise(resolve => server.close(resolve));
    const finalProfile = path.resolve(profile);
    assert.ok(finalProfile.startsWith(resolvedRoot + path.sep), 'refuse to remove profile outside test root');
    fs.rmSync(finalProfile, { recursive: true, force: true });
  }
});
