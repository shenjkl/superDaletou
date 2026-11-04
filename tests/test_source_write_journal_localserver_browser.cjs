const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const test = require('node:test');

const repository = path.resolve(__dirname, '..');
const browser = [
  process.env.CHROME_PATH,
  path.join(process.env.ProgramFiles || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
  path.join(process.env['ProgramFiles(x86)'] || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
  path.join(process.env.ProgramFiles || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
].filter(Boolean).find(candidate => fs.existsSync(candidate));
const python = process.env.DLT_PYTHON || path.join(repository, '.venv', 'Scripts', 'python.exe');
const tempRoot = process.env.DLT_BROWSER_TEST_ROOT;

test('real Chrome restores a lost source-review response after LocalServer process restart', {
  skip: !browser || !tempRoot || !fs.existsSync(python),
  timeout: 90000,
}, async () => {
  const root = path.resolve(tempRoot);
  const allowed = path.resolve(repository, '.tmp');
  assert.ok(root.startsWith(allowed + path.sep), 'browser test directory must be inside repository .tmp');
  fs.mkdirSync(root, { recursive: true });
  const profile = fs.mkdtempSync(path.join(root, 'source-review-e2e-chrome-'));
  const dataDirectory = fs.mkdtempSync(path.join(root, 'source-review-e2e-data-'));
  let serverProcess;
  let chromeProcess;
  let socket;
  let secondSocket;
  let browserSocket;
  let serverOutput = '';
  try {
    const port = await new Promise((resolve, reject) => {
      const probe = net.createServer();
      probe.once('error', reject);
      probe.listen(0, '127.0.0.1', () => {
        const { port: available } = probe.address();
        probe.close(error => error ? reject(error) : resolve(available));
      });
    });
    const startServer = () => {
      serverOutput = '';
      serverProcess = spawn(python, [path.join(__dirname, 'browser_local_server.py'), dataDirectory, String(port)], {
        cwd: repository,
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...process.env, PYTHONPATH: `${repository};${path.join(repository, 'tests')}` },
      });
      serverProcess.stdout.setEncoding('utf8');
      serverProcess.stderr.setEncoding('utf8');
      serverProcess.stderr.on('data', chunk => { serverOutput += chunk; });
      return new Promise((resolve, reject) => {
        let stdout = '';
        const proc = serverProcess;
        const timer = setTimeout(() => reject(new Error(`LocalServer startup timed out: ${serverOutput}`)), 30000);
        proc.once('error', error => { clearTimeout(timer); reject(error); });
        proc.stdout.on('data', chunk => {
          stdout += chunk;
          const line = stdout.split(/\r?\n/)[0];
          if (line === String(port)) { clearTimeout(timer); resolve(proc); }
          else if (proc.exitCode !== null) { clearTimeout(timer); reject(new Error(`LocalServer exited: ${serverOutput}`)); }
        });
        proc.once('exit', code => {
          if (code !== 0) { clearTimeout(timer); reject(new Error(`LocalServer exited with ${code}: ${serverOutput}`)); }
        });
      });
    };
    const stopServer = async proc => {
      if (!proc || proc.exitCode !== null) return;
      proc.kill();
      await new Promise(resolve => {
        let timer;
        const done = () => { clearTimeout(timer); resolve(); };
        timer = setTimeout(done, 5000);
        proc.once('exit', done);
        if (proc.exitCode !== null) done();
      });
      assert.ok(proc.exitCode !== null || proc.signalCode !== null,
        `LocalServer process did not stop before restart (pid=${proc.pid}, killed=${proc.killed})`);
    };
    await startServer();
    const origin = `http://127.0.0.1:${port}`;

    chromeProcess = spawn(browser, ['--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
      '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0',
      `--user-data-dir=${profile}`, origin], { windowsHide: true, stdio: 'ignore' });
    const activePortFile = path.join(profile, 'DevToolsActivePort');
    let devtoolsPort;
    for (let i = 0; i < 300; i++) {
      if (chromeProcess.exitCode !== null) throw new Error(`Chrome exited before DevTools started (${chromeProcess.exitCode})`);
      if (fs.existsSync(activePortFile)) {
        devtoolsPort = Number(fs.readFileSync(activePortFile, 'utf8').split(/\r?\n/)[0]);
        if (Number.isInteger(devtoolsPort) && devtoolsPort > 0) break;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.ok(devtoolsPort, 'Chrome did not expose its DevTools port');
    const targets = await (await fetch(`http://127.0.0.1:${devtoolsPort}/json/list`)).json();
    const page = targets.find(target => target.type === 'page' && target.url === origin + '/');
    assert.ok(page?.webSocketDebuggerUrl, 'application page target was not found');

    const connectDevtools = async url => {
      const ws = new WebSocket(url);
      await new Promise((resolve, reject) => {
        ws.addEventListener('open', resolve, { once: true });
        ws.addEventListener('error', reject, { once: true });
      });
      let nextId = 1;
      const pending = new Map();
      ws.addEventListener('message', event => {
        const message = JSON.parse(event.data);
        if (!message.id || !pending.has(message.id)) return;
        const handlers = pending.get(message.id);
        pending.delete(message.id);
        message.error ? handlers.reject(new Error(message.error.message)) : handlers.resolve(message.result);
      });
      const send = (method, params = {}) => new Promise((resolve, reject) => {
        const id = nextId++;
        pending.set(id, { resolve, reject });
        ws.send(JSON.stringify({ id, method, params }));
      });
      return { socket: ws, send };
    };
    const firstConnection = await connectDevtools(page.webSocketDebuggerUrl);
    socket = firstConnection.socket;
    const send = firstConnection.send;
    let shouldDrop = false;
    let sourceReviewPostResponses = 0;
    let resolvePaused;
    const pausedResponse = new Promise(resolve => { resolvePaused = resolve; });
    socket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.method === 'Fetch.requestPaused') {
        const params = message.params;
        if (params.request.method === 'POST' && params.request.url.includes('/api/source-reviews')
            && params.responseStatusCode === 200) {
          sourceReviewPostResponses++;
          if (shouldDrop) {
            shouldDrop = false;
            resolvePaused(params);
            return;
          }
        }
        send('Fetch.continueRequest', { requestId: params.requestId }).catch(() => {});
      }
    });
    const evaluate = async expression => {
      const result = await send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
      if (result.exceptionDetails) throw new Error(result.result?.exception?.description || result.exceptionDetails.text);
      return result.result.value;
    };
    await send('Runtime.enable');
    await send('Fetch.enable', { patterns: [{ urlPattern: '*://127.0.0.1/*/api/source-reviews*', requestStage: 'Response' },
      { urlPattern: `${origin}/api/source-reviews*`, requestStage: 'Response' }] });
    const waitForPage = async (evaluatePage = evaluate) => {
      for (let i = 0; i < 200; i++) {
        if (await evaluatePage('document.readyState') === 'complete'
            && await evaluatePage('Boolean(window.SourceWriteJournal)')) return;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      throw new Error('application or source journal did not load in Chrome');
    };
    await waitForPage(evaluate);

    const version = await (await fetch(`http://127.0.0.1:${devtoolsPort}/json/version`)).json();
    const browserConnection = await connectDevtools(version.webSocketDebuggerUrl);
    browserSocket = browserConnection.socket;
    const { targetId } = await browserConnection.send('Target.createTarget', { url: `${origin}/#prospective` });
    let secondPage;
    for (let i = 0; i < 100; i++) {
      const currentTargets = await (await fetch(`http://127.0.0.1:${devtoolsPort}/json/list`)).json();
      secondPage = currentTargets.find(target => target.id === targetId && target.type === 'page');
      if (secondPage?.webSocketDebuggerUrl) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.ok(secondPage?.webSocketDebuggerUrl, 'second same-profile Chrome page was not created');
    const secondConnection = await connectDevtools(secondPage.webSocketDebuggerUrl);
    secondSocket = secondConnection.socket;
    const sendSecond = secondConnection.send;
    secondSocket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.method === 'Fetch.requestPaused') {
        const params = message.params;
        if (params.request.method === 'POST' && params.request.url.includes('/api/source-reviews')
            && params.responseStatusCode === 200) sourceReviewPostResponses++;
        sendSecond('Fetch.continueRequest', { requestId: params.requestId }).catch(() => {});
      }
    });
    const evaluateSecond = async expression => {
      const result = await sendSecond('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
      if (result.exceptionDetails) throw new Error(result.result?.exception?.description || result.exceptionDetails.text);
      return result.result.value;
    };
    await sendSecond('Runtime.enable');
    await sendSecond('Fetch.enable', { patterns: [{ urlPattern: '*://127.0.0.1/*/api/source-reviews*', requestStage: 'Response' },
      { urlPattern: `${origin}/api/source-reviews*`, requestStage: 'Response' }] });
    await waitForPage(evaluateSecond);
    for (let i = 0; i < 300; i++) {
      if (await evaluateSecond("Boolean(document.querySelector('#review-form') && document.body.dataset.serverStatus==='verified')")) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(await evaluateSecond("Boolean(document.querySelector('#review-form') && document.body.dataset.serverStatus==='verified')"), true,
      'second same-profile page did not connect to the local service');
    await evaluate(`document.querySelector('#navigation a[href="#prospective"]').click()`);
    for (let i = 0; i < 300; i++) {
      if (await evaluate("Boolean(document.querySelector('#review-form') && document.body.dataset.serverStatus==='verified')")) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(await evaluate("Boolean(document.querySelector('#review-form') && document.body.dataset.serverStatus==='verified')"), true,
      'source review form did not become ready against the local service');

    const documentPath = path.join(dataDirectory, 'browser-source.txt');
    const documentBytes = Buffer.from('synthetic browser integration document', 'utf8');
    fs.writeFileSync(documentPath, documentBytes);
    const documentTree = await send('DOM.getDocument', { depth: -1 });
    const fileInput = await send('DOM.querySelector', { nodeId: documentTree.root.nodeId, selector: '#review-file' });
    assert.ok(fileInput.nodeId, 'product source file input was not found');
    await send('DOM.setFileInputFiles', { nodeId: fileInput.nodeId, files: [documentPath] });
    const secondDocumentTree = await sendSecond('DOM.getDocument', { depth: -1 });
    const secondFileInput = await sendSecond('DOM.querySelector', { nodeId: secondDocumentTree.root.nodeId, selector: '#review-file' });
    assert.ok(secondFileInput.nodeId, 'second product source file input was not found');
    await sendSecond('DOM.setFileInputFiles', { nodeId: secondFileInput.nodeId, files: [documentPath] });
    const fillAndSubmitForm = `(() => {
      const form=document.querySelector('#review-form');
      form.closest('details').open=true;
      form.elements.kind.value='calendar';
      form.elements.url.value='https://www.sporttery.cn/synthetic-test';
      form.elements.extracted.value=JSON.stringify({synthetic:'only'});
      form.elements.note.value='synthetic browser integration only';
      form.elements.confirmed.checked=true;
      form.requestSubmit();
    })()`;
    shouldDrop = true;
    await evaluate(fillAndSubmitForm);
    let responseTimer;
    const responseTimedOut = new Promise((_, reject) => {
      responseTimer = setTimeout(() => reject(new Error('source review did not reach the real server')), 30000);
    });
    const paused = await Promise.race([pausedResponse, responseTimedOut]);
    clearTimeout(responseTimer);
    assert.equal(paused.responseStatusCode, 200);

    await evaluateSecond(fillAndSubmitForm);
    let secondWaiting = false;
    for (let i = 0; i < 100; i++) {
      secondWaiting = await evaluateSecond("document.querySelector('#review-form button[type=submit]')?.disabled===true");
      if (secondWaiting) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(secondWaiting, true, 'second tab did not remain inside the same-object browser lock');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.equal(sourceReviewPostResponses, 1, 'second tab sent a duplicate source-review POST while the first held the lock');

    await send('Fetch.failRequest', { requestId: paused.requestId, errorReason: 'ConnectionReset' });
    let secondFinished = false;
    for (let i = 0; i < 300; i++) {
      secondFinished = await evaluateSecond("document.querySelector('#review-form button[type=submit]')?.disabled===false");
      if (secondFinished) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(secondFinished, true, 'second tab did not finish after the first request released the lock');
    assert.equal(sourceReviewPostResponses, 1, 'same-object second tab did not reuse the original accepted request');

    let submitted;
    for (let i = 0; i < 300; i++) {
      submitted = await evaluate(`(async()=>({error:document.querySelector('#review-error')?.textContent||'',entries:await new Promise((resolve,reject)=>{
        const request=indexedDB.open('dlt-source-write-journal',1);
        request.onerror=()=>reject(request.error);
        request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
          get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}};
      })}))()`);
      if (submitted.entries.length === 1 && submitted.entries[0].phase === 'submitted') break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(submitted.entries.length, 1, 'product form did not persist the request in browser storage');
    assert.equal(submitted.entries[0].phase, 'submitted');
    assert.match(submitted.error, /fetch|network|连接/i);
    const { createHash } = require('node:crypto');
    const documentSha256 = createHash('sha256').update(documentBytes).digest('hex');

    const firstServerPid = serverProcess.pid;
    await stopServer(serverProcess);
    await startServer();
    assert.notEqual(serverProcess.pid, firstServerPid, 'LocalServer did not restart as a new OS process');
    await send('Page.reload', { ignoreCache: true });
    await waitForPage();
    await evaluate(`document.querySelector('#navigation a[href="#prospective"]')?.click()`);
    let recovered;
    for (let i = 0; i < 300; i++) {
      recovered = await evaluate(`(async()=>({
        ready:document.body.dataset.serverStatus==='verified',
        entries:await new Promise((resolve,reject)=>{const request=indexedDB.open('dlt-source-write-journal',1);
          request.onerror=()=>reject(request.error);request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
            get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}}),
        dismiss:!!document.querySelector('#source-write-journal [data-source-write="dismiss"]')
      }))()`);
      if (recovered.ready && recovered.entries.length === 1 && recovered.dismiss) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(recovered.entries.length, 1);
    assert.equal(recovered.entries[0].phase, 'accepted', 'product recovery UI did not persist the verified server receipt');
    assert.equal(recovered.dismiss, true, 'product recovery UI did not verify the accepted request');
    const key = recovered.entries[0].request_key;
    const state = await evaluate(`(async()=>fetch('/api/source-write-status?request_key='+encodeURIComponent('${key}')
      +'&request_sha256='+encodeURIComponent('${recovered.entries[0].request_sha256}'),{credentials:'same-origin'})
      .then(response=>response.json()))()`);
    assert.equal(state.write.status, 'accepted');
    const reviews = await evaluate("fetch('/api/source-reviews',{credentials:'same-origin'}).then(response=>response.json())");
    const matchingReviews = reviews.reviews.filter(review => review.review_id === state.write.result.review_id
      && review.document_sha256 === documentSha256);
    assert.equal(matchingReviews.length, 1);
    await evaluate(`document.querySelector('#source-write-journal [data-source-write="dismiss"]').click()`);
    let remainder;
    for (let i = 0; i < 100; i++) {
      remainder = await evaluate(`new Promise((resolve,reject)=>{const request=indexedDB.open('dlt-source-write-journal',1);
        request.onerror=()=>reject(request.error);request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
          get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}})`);
      if (remainder.length === 0) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(remainder.length, 0, 'product recovery UI did not clear the verified local copy');
  } finally {
    try { socket?.close(); } catch {}
    try { secondSocket?.close(); } catch {}
    try { browserSocket?.close(); } catch {}
    for (const process of [chromeProcess, serverProcess]) {
      if (process && process.exitCode === null) {
        process.kill();
        await new Promise(resolve => {
          let timer;
          const done = () => { clearTimeout(timer); resolve(); };
          timer = setTimeout(done, 5000);
          process.once('exit', done);
          if (process.exitCode !== null) done();
        });
      }
    }
    for (const target of [profile, dataDirectory]) {
      const absolute = path.resolve(target);
      assert.ok(absolute.startsWith(root + path.sep), 'refuse to remove a test directory outside .tmp test root');
      fs.rmSync(absolute, { recursive: true, force: true });
    }
  }
});
