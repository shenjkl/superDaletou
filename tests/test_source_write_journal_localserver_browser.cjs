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
    let shouldDropCorrection = false;
    let sourceReviewPostResponses = 0;
    let sourceCorrectionPostResponses = 0;
    let resolvePaused;
    let resolveCorrectionPaused;
    const pausedResponse = new Promise(resolve => { resolvePaused = resolve; });
    const pausedCorrectionResponse = new Promise(resolve => { resolveCorrectionPaused = resolve; });
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
        if (params.request.method === 'POST' && params.request.url.includes('/api/source-corrections')) {
          if (shouldDropCorrection) {
            sourceCorrectionPostResponses++;
            shouldDropCorrection = false;
            resolveCorrectionPaused(params);
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
    const sourceWritePatterns = ['source-reviews', 'source-corrections'].map(route => ({
      urlPattern: `${origin}/api/${route}*`, requestStage: 'Response',
    }));
    await send('Fetch.enable', { patterns: sourceWritePatterns });
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
    let browserConnection = await connectDevtools(version.webSocketDebuggerUrl);
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
    await sendSecond('Fetch.enable', { patterns: sourceWritePatterns });
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
      form.elements.kind.value='rule';
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

    const replacementBytes = Buffer.from('synthetic corrected source document', 'utf8');
    const replacementPath = path.join(dataDirectory, 'browser-source-replacement.txt');
    fs.writeFileSync(replacementPath, replacementBytes);
    const replacementTree = await send('DOM.getDocument', { depth: -1 });
    const replacementInput = await send('DOM.querySelector', { nodeId: replacementTree.root.nodeId, selector: '#review-file' });
    await send('DOM.setFileInputFiles', { nodeId: replacementInput.nodeId, files: [replacementPath] });
    await evaluate(`(() => {
      const form=document.querySelector('#review-form');
      form.closest('details').open=true;
      form.elements.kind.value='rule';
      form.elements.url.value='https://www.sporttery.cn/synthetic-correction';
      form.elements.extracted.value=JSON.stringify({synthetic:'corrected'});
      form.elements.note.value='synthetic replacement source for correction flow';
      form.elements.confirmed.checked=true;
      form.requestSubmit();
    })()`);
    const replacementSha256 = createHash('sha256').update(replacementBytes).digest('hex');
    let replacementReview;
    for (let i = 0; i < 300; i++) {
      const current = await evaluate("fetch('/api/source-reviews',{credentials:'same-origin'}).then(response=>response.json())");
      replacementReview = current.reviews.find(review => review.document_sha256 === replacementSha256);
      if (replacementReview && sourceReviewPostResponses === 2) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.ok(replacementReview?.review_id, 'product form did not save the replacement source review');
    assert.equal(sourceReviewPostResponses, 2, 'replacement source review was not posted exactly once');
    const originalReview = matchingReviews[0];
    const replacementJournalEntry = await evaluate(`new Promise((resolve,reject)=>{const request=indexedDB.open('dlt-source-write-journal',1);
      request.onerror=()=>reject(request.error);request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
        get.onsuccess=()=>{db.close();resolve(get.result.find(item=>item.operation==='source_review'&&item.request.document_sha256==='${replacementSha256}'))};
        get.onerror=()=>{db.close();reject(get.error)}}})`);
    assert.equal(replacementJournalEntry?.phase, 'accepted');
    let replacementDismissReady = false;
    for (let i = 0; i < 100; i++) {
      replacementDismissReady = await evaluate(`Boolean(document.querySelector('#source-write-journal [data-source-write="dismiss"][data-request="${replacementJournalEntry.request_key}"]'))`);
      if (replacementDismissReady) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(replacementDismissReady, true, 'product UI did not render the accepted replacement review action');
    await evaluate(`document.querySelector('#source-write-journal [data-source-write="dismiss"][data-request="${replacementJournalEntry.request_key}"]').click()`);
    let replacementRemainder;
    for (let i = 0; i < 100; i++) {
      replacementRemainder = await evaluate(`new Promise((resolve,reject)=>{const request=indexedDB.open('dlt-source-write-journal',1);
        request.onerror=()=>reject(request.error);request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
          get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}})`);
      if (!replacementRemainder.length) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(replacementRemainder.length, 0);

    await evaluate(`document.querySelector('#source-correction-section [data-action="prepare-source-correction"]').click()`);
    for (let i = 0; i < 100; i++) {
      if (await evaluate("Boolean(document.querySelector('#source-correction-form'))")) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    shouldDropCorrection = true;
    await evaluate(`(() => {
      const form=document.querySelector('#source-correction-form');
      form.elements.old.value='${originalReview.review_id}';
      form.elements.old.dispatchEvent(new Event('change',{bubbles:true}));
      form.elements.new.value='${replacementReview.review_id}';
      form.elements.new.dispatchEvent(new Event('change',{bubbles:true}));
      form.elements.scope.value='global';
      form.elements.note.value='synthetic end-to-end source correction';
      form.elements.confirmed.checked=true;
      form.requestSubmit();
    })()`);
    const correctionFormState = await evaluate(`(() => {
      const form=document.querySelector('#source-correction-form');
      return {exists:!!form,valid:form?.checkValidity(),old:form?.elements.old.value,new:form?.elements.new.value,
        scope:form?.elements.scope.value,note:form?.elements.note.value,confirmed:form?.elements.confirmed.checked,
        error:form?.querySelector('#correction-error')?.textContent||'',ready:document.body.dataset.serverStatus,
        submitDisabled:form?.querySelector('button[type="submit"]')?.disabled,
        oldOptions:[...(form?.elements.old.options||[])].map(option=>option.value),
        newOptions:[...(form?.elements.new.options||[])].map(option=>option.value)};
    })()`);
    assert.equal(correctionFormState.valid, true, `product correction form is invalid: ${JSON.stringify(correctionFormState)}`);
    assert.equal(correctionFormState.submitDisabled, true, `product correction submit handler rejected or ignored the request: ${JSON.stringify(correctionFormState)}`);
    let correctionTimer;
    const correctionTimedOut = new Promise((_, reject) => {
      correctionTimer = setTimeout(() => reject(new Error('source correction did not reach the real server')), 10000);
    });
    const correctionPaused = await Promise.race([pausedCorrectionResponse, correctionTimedOut]);
    clearTimeout(correctionTimer);
    if (correctionPaused.responseStatusCode !== 200) {
      const body = await send('Fetch.getResponseBody', { requestId: correctionPaused.requestId });
      throw new Error(`source correction POST returned HTTP ${correctionPaused.responseStatusCode}: ${body.base64Encoded
        ? Buffer.from(body.body, 'base64').toString('utf8') : body.body}`);
    }
    await send('Fetch.failRequest', { requestId: correctionPaused.requestId, errorReason: 'ConnectionReset' });
    let correctionPending;
    for (let i = 0; i < 300; i++) {
      correctionPending = await evaluate(`(async()=>({
        error:document.querySelector('#correction-error')?.textContent||'',
        entries:await new Promise((resolve,reject)=>{const request=indexedDB.open('dlt-source-write-journal',1);
          request.onerror=()=>reject(request.error);request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
            get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}})
      }))()`);
      if (correctionPending.entries.some(item => item.operation === 'source_correction' && item.phase === 'submitted')) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    const pendingCorrection = correctionPending.entries.find(item => item.operation === 'source_correction');
    assert.equal(pendingCorrection?.phase, 'submitted', 'product correction form did not retain the lost-response request');
    assert.match(correctionPending.error, /fetch|network|连接/i);
    assert.equal(sourceCorrectionPostResponses, 1);

    const correctionServerPid = serverProcess.pid;
    await stopServer(serverProcess);
    await startServer();
    assert.notEqual(serverProcess.pid, correctionServerPid, 'LocalServer did not restart before correction recovery');
    const firstBrowserPid = chromeProcess.pid;
    try { secondSocket?.close(); } catch {}
    try { socket?.close(); } catch {}
    await browserConnection.send('Browser.close');
    await new Promise(resolve => {
      let timer;
      const done = () => { clearTimeout(timer); resolve(); };
      timer = setTimeout(() => {
        if (chromeProcess.exitCode === null && chromeProcess.signalCode === null) chromeProcess.kill();
        setTimeout(done, 3000);
      }, 10000);
      chromeProcess.once('exit', done);
      if (chromeProcess.exitCode !== null || chromeProcess.signalCode !== null) done();
    });
    assert.ok(chromeProcess.exitCode !== null || chromeProcess.signalCode !== null,
      'Chrome browser process did not exit before persistent profile restart');
    fs.rmSync(activePortFile, { force: true });
    chromeProcess = spawn(browser, ['--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
      '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0',
      `--user-data-dir=${profile}`, `${origin}/#prospective`], { windowsHide: true, stdio: 'ignore' });
    let restartedDevtoolsPort;
    for (let i = 0; i < 300; i++) {
      if (chromeProcess.exitCode !== null) throw new Error(`Chrome exited before restart DevTools started (${chromeProcess.exitCode})`);
      if (fs.existsSync(activePortFile)) {
        restartedDevtoolsPort = Number(fs.readFileSync(activePortFile, 'utf8').split(/\r?\n/)[0]);
        if (Number.isInteger(restartedDevtoolsPort) && restartedDevtoolsPort > 0) break;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.ok(restartedDevtoolsPort, 'restarted Chrome did not expose its DevTools port');
    const restartedTargets = await (await fetch(`http://127.0.0.1:${restartedDevtoolsPort}/json/list`)).json();
    const restartedPage = restartedTargets.find(target => target.type === 'page' && target.url.startsWith(origin));
    assert.ok(restartedPage?.webSocketDebuggerUrl, 'restarted Chrome application page was not found');
    const restartedConnection = await connectDevtools(restartedPage.webSocketDebuggerUrl);
    socket = restartedConnection.socket;
    const sendRestarted = restartedConnection.send;
    const evaluateRestarted = async expression => {
      const result = await sendRestarted('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
      if (result.exceptionDetails) throw new Error(result.result?.exception?.description || result.exceptionDetails.text);
      return result.result.value;
    };
    await sendRestarted('Runtime.enable');
    await evaluateRestarted(`location.hash='#prospective'`);
    let browserReady = false;
    for (let i = 0; i < 300; i++) {
      browserReady = await evaluateRestarted("Boolean(document.querySelector('#review-form') && document.body.dataset.serverStatus==='verified')");
      if (browserReady) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(browserReady, true, 'new Chrome process did not restore the prospective page and service connection');
    assert.notEqual(chromeProcess.pid, firstBrowserPid, 'Chrome process was not restarted');
    let recoveredCorrection;
    for (let i = 0; i < 300; i++) {
      recoveredCorrection = await evaluateRestarted(`(async()=>(
        {ready:document.body.dataset.serverStatus==='verified',
         entries:await new Promise((resolve,reject)=>{const request=indexedDB.open('dlt-source-write-journal',1);
           request.onerror=()=>reject(request.error);request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
             get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}}),
         dismiss:!!document.querySelector('#source-write-journal [data-source-write="dismiss"][data-request="${pendingCorrection.request_key}"]')})
      )()`);
      if (recoveredCorrection.ready && recoveredCorrection.dismiss) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    const correctionEntry = recoveredCorrection.entries.find(item => item.request_key === pendingCorrection.request_key);
    assert.equal(correctionEntry?.phase, 'accepted', 'product recovery UI did not persist the correction receipt');
    assert.equal(recoveredCorrection.dismiss, true);
    const correctionState = await evaluateRestarted(`fetch('/api/source-write-status?request_key='+encodeURIComponent('${pendingCorrection.request_key}')
      +'&request_sha256='+encodeURIComponent('${pendingCorrection.request_sha256}'),{credentials:'same-origin'}).then(response=>response.json())`);
    assert.equal(correctionState.write.status, 'accepted');
    assert.equal(correctionState.write.operation, 'source_correction');
    const corrections = await evaluateRestarted("fetch('/api/source-corrections',{credentials:'same-origin'}).then(response=>response.json())");
    const matchingCorrections = corrections.corrections.filter(item => item.seq === correctionState.write.result.correction_id
      && item.old_review_id === originalReview.review_id && item.new_review_id === replacementReview.review_id);
    assert.equal(matchingCorrections.length, 1,
      `server did not retain exactly one correction for the selected sources: ${JSON.stringify({corrections, write:correctionState.write, old:originalReview.review_id, new:replacementReview.review_id})}`);
    await evaluateRestarted(`document.querySelector('#source-write-journal [data-source-write="dismiss"][data-request="${pendingCorrection.request_key}"]').click()`);
    let finalJournal;
    for (let i = 0; i < 100; i++) {
      finalJournal = await evaluateRestarted(`new Promise((resolve,reject)=>{const request=indexedDB.open('dlt-source-write-journal',1);
        request.onerror=()=>reject(request.error);request.onsuccess=()=>{const db=request.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
          get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}})`);
      if (!finalJournal.length) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(finalJournal.length, 0, 'correction recovery UI did not clear the verified local request');
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
