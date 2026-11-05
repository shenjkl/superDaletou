const assert = require('node:assert/strict');
const { spawn, spawnSync } = require('node:child_process');
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

test('Chrome recovers single and series closure writes across LocalServer restarts', {
  skip: !browser || !tempRoot || !fs.existsSync(python),
  timeout: 180000,
}, async () => {
  const root = path.resolve(tempRoot);
  const allowed = path.resolve(repository, '.tmp');
  assert.ok(root.startsWith(allowed + path.sep), 'browser test directory must be inside repository .tmp');
  fs.mkdirSync(root, { recursive: true });
  const profile = fs.mkdtempSync(path.join(root, 'closure-e2e-chrome-'));
  const dataDirectory = fs.mkdtempSync(path.join(root, 'closure-e2e-data-'));
  const database = path.join(dataDirectory, 'browser-test.sqlite3');
  const stageFile = path.join(root, 'closure-e2e-stage.txt');
  let stage = 'seed database';
  const mark = value => { stage = value; fs.writeFileSync(stageFile, value); };
  mark(stage);
  const seeded = spawnSync(python, [path.join(__dirname, 'seed_closure_browser.py'), database], {
    cwd: repository,
    encoding: 'utf8',
    windowsHide: true,
    env: { ...process.env, PYTHONPATH: `${repository};${path.join(repository, 'tests')}` },
  });
  if (seeded.status !== 0) fs.writeFileSync(path.join(root, 'closure-e2e-last-error.txt'), `seed database\n${seeded.stderr}`);
  assert.equal(seeded.status, 0, `could not seed closure database: ${seeded.stderr}`);
  const seriesKey = seeded.stdout.trim().split(/\r?\n/).at(-1);
  if (!/^[0-9a-f-]{36}$/i.test(seriesKey)) fs.writeFileSync(path.join(root, 'closure-e2e-last-error.txt'), `invalid series key\n${seeded.stdout}`);
  assert.match(seriesKey, /^[0-9a-f-]{36}$/i, 'seed helper did not return a series UUID');

  let serverProcess;
  let chromeProcess;
  let pageSocket;
  let serverOutput = '';
  mark('server startup');
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
    assert.ok(proc.exitCode !== null || proc.signalCode !== null, 'LocalServer did not stop before restart');
  };
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
          if (stdout.split(/\r?\n/)[0] === String(port)) { clearTimeout(timer); resolve(proc); }
          else if (proc.exitCode !== null) { clearTimeout(timer); reject(new Error(`LocalServer exited: ${serverOutput}`)); }
        });
        proc.once('exit', code => {
          if (code !== 0) { clearTimeout(timer); reject(new Error(`LocalServer exited with ${code}: ${serverOutput}`)); }
        });
      });
    };
    await startServer();
    mark('LocalServer started');
    mark('Chrome startup');
    const origin = `http://127.0.0.1:${port}`;
    chromeProcess = spawn(browser, ['--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
      '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0',
      `--user-data-dir=${profile}`, `${origin}/#prospective`], { windowsHide: true, stdio: 'ignore' });
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
    mark('Chrome DevTools ready');
    const connectDevtools = async url => {
      const ws = new WebSocket(url);
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('DevTools WebSocket open timed out')), 10000);
        ws.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
        ws.addEventListener('error', error => { clearTimeout(timer); reject(error); }, { once: true });
      });
      let nextId = 1;
      const pending = new Map();
      ws.addEventListener('message', event => {
        const message = JSON.parse(event.data);
        if (!message.id || !pending.has(message.id)) return;
        const handlers = pending.get(message.id);
        pending.delete(message.id);
        clearTimeout(handlers.timer);
        message.error ? handlers.reject(new Error(message.error.message)) : handlers.resolve(message.result);
      });
      ws.addEventListener('close', () => {
        for (const [id, handlers] of pending) {
          clearTimeout(handlers.timer);
          handlers.reject(new Error(`DevTools socket closed while waiting for command ${id}`));
        }
        pending.clear();
      });
      const send = (method, params = {}) => new Promise((resolve, reject) => {
        const id = nextId++;
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`DevTools command ${method} timed out`));
        }, 15000);
        pending.set(id, { resolve, reject, timer });
        ws.send(JSON.stringify({ id, method, params }));
      });
      return { socket: ws, send };
    };
    const targets = await (await fetch(`http://127.0.0.1:${devtoolsPort}/json/list`)).json();
    const page = targets.find(target => target.type === 'page' && target.url === `${origin}/#prospective`);
    assert.ok(page?.webSocketDebuggerUrl, 'prospective page target was not found');
    let connection = await connectDevtools(page.webSocketDebuggerUrl);
    pageSocket = connection.socket;
    mark('connected to prospective page');
    const evaluate = async expression => {
      let result;
      try { result = await connection.send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }); }
      catch (error) { throw new Error(`${error.message}; expression=${expression.slice(0, 150)}`); }
      if (result.exceptionDetails) throw new Error(result.result?.exception?.description || result.exceptionDetails.text);
      return result.result.value;
    };
    await connection.send('Runtime.enable');
    let dropOperation;
    let resolvePaused;
    const armResponseDrop = operation => {
      dropOperation = operation;
      return new Promise(resolve => { resolvePaused = resolve; });
    };
    const waitForDroppedResponse = async pausedResponse => {
      let timer;
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('closure POST did not reach the real server')), 15000);
      });
      try { return await Promise.race([pausedResponse, timeout]); }
      finally { clearTimeout(timer); }
    };
    const postCounts = { experiment_close: 0, series_close: 0 };
    pageSocket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.method !== 'Fetch.requestPaused') return;
      const params = message.params;
      const operation = params.request.url.includes('/api/experiments/close') ? 'experiment_close'
        : params.request.url.includes('/api/experiment-series/close') ? 'series_close' : null;
      if (params.request.method === 'POST' && operation) {
        postCounts[operation]++;
        if (dropOperation === operation && params.responseStatusCode === 200) {
          dropOperation = null;
          const resolve = resolvePaused;
          resolvePaused = null;
          resolve(params);
          return;
        }
      }
      connection.send('Fetch.continueRequest', { requestId: params.requestId }).catch(() => {});
    });
    await connection.send('Fetch.enable', { patterns: [
      { urlPattern: `${origin}/api/experiments/close*`, requestStage: 'Response' },
      { urlPattern: `${origin}/api/experiment-series/close*`, requestStage: 'Response' },
    ] });

    const waitFor = async (expression, description) => {
      for (let i = 0; i < 300; i++) {
        const value = await evaluate(expression);
        if (value) return value;
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      throw new Error(`${description} timed out`);
    };
    await waitFor("document.body.dataset.serverStatus==='verified' && Boolean(document.querySelector('#series-list'))",
      'prospective page did not become ready against LocalServer');
    mark('series page loaded');

    const submitAndDrop = async ({ operation, formSelector, reason }) => {
      const selector = JSON.stringify(formSelector);
      const ready = await evaluate(`(() => {const form=document.querySelector(${selector});if(!form)return null;
        const details=form.closest('details');if(details)details.open=true;const input=form.querySelector('[name="reason"]');
        input.value=${JSON.stringify(reason)};input.dispatchEvent(new Event('input',{bubbles:true}));
        return {valid:form.checkValidity(),object:form.dataset.series||form.dataset.experiment};})()`);
      assert.ok(ready, `${operation} product form was not rendered`);
      assert.equal(ready.valid, true, `${operation} form is invalid`);
      const pausedPromise = armResponseDrop(operation);
      await evaluate(`document.querySelector(${selector}).requestSubmit()`);
      const paused = await waitForDroppedResponse(pausedPromise);
      assert.equal(paused.responseStatusCode, 200, `${operation} was not accepted by LocalServer`);
      await connection.send('Fetch.failRequest', { requestId: paused.requestId, errorReason: 'ConnectionReset' });
      const submitted = await waitFor(`(async()=>{const r=indexedDB.open('dlt-prospective-closure-write-journal',1);return await new Promise((resolve,reject)=>{
        r.onerror=()=>reject(r.error);r.onsuccess=()=>{const db=r.result,tx=db.transaction('writes','readonly'),q=tx.objectStore('writes').getAll();
          q.onsuccess=()=>{db.close();resolve(q.result.find(entry=>entry.operation===${JSON.stringify(operation)}&&entry.phase==='submitted')||null)};
          q.onerror=()=>{db.close();reject(q.error)}}})})()`, `${operation} request was not retained in IndexedDB`);
      assert.equal(submitted.request.object_key, ready.object);
      return submitted;
    };
    const recoverAfterRestart = async (entry, operation) => {
      const oldPid = serverProcess.pid;
      await stopServer(serverProcess);
      await startServer();
      assert.notEqual(serverProcess.pid, oldPid, 'LocalServer was not restarted as a new process');
      await connection.send('Page.reload', { ignoreCache: true });
      await waitFor("document.readyState==='complete' && document.body.dataset.serverStatus==='verified' && Boolean(window.ProspectiveClosureWriteJournal)",
        'application did not reload after closure server restart');
      const escapedKey = JSON.stringify(entry.request_key);
      const dismissSelector = JSON.stringify(`#prospective-closure-write-journal [data-prospective-closure-action="dismiss"][data-request="${entry.request_key}"]`);
      const dismissal = await waitFor(`!!document.querySelector(${dismissSelector})`,
        `${operation} recovery UI did not expose verified cleanup`);
      assert.equal(dismissal, true);
      const status = await evaluate(`fetch('/api/prospective-closure-writes?'+new URLSearchParams({request_key:${escapedKey},operation:${JSON.stringify(operation)},request_sha256:${JSON.stringify(entry.request_sha256)}}),
        {credentials:'same-origin'}).then(response=>response.json())`);
      assert.equal(status.write.status, 'accepted', `${operation} status API did not recover accepted receipt`);
      assert.equal(status.write.object_key, entry.request.object_key);
      await evaluate(`window.confirm=()=>true;document.querySelector(${dismissSelector}).click()`);
      const remaining = await waitFor(`new Promise((resolve,reject)=>{const r=indexedDB.open('dlt-prospective-closure-write-journal',1);r.onerror=()=>reject(r.error);
        r.onsuccess=()=>{const db=r.result,tx=db.transaction('writes','readonly'),q=tx.objectStore('writes').getAll();
          q.onsuccess=()=>{db.close();resolve(q.result.every(x=>x.request_key!==${escapedKey}))};q.onerror=()=>{db.close();reject(q.error)}}})`,
        `${operation} verified IndexedDB entry was not cleaned from the product UI`);
      assert.equal(remaining, true);
    };

    const seriesEntry = await submitAndDrop({ operation: 'series_close',
      formSelector: `form.series-close-form[data-series="${seriesKey}"]`, reason: 'browser series closure recovery' });
    mark('series close response dropped');
    assert.equal(postCounts.series_close, 1, 'product UI did not send exactly one series close');
    mark('series close recovery');
    await recoverAfterRestart(seriesEntry, 'series_close');
    mark('series close recovered');
    const seriesState = await evaluate(`fetch('/api/experiment-series?key='+encodeURIComponent(${JSON.stringify(seriesKey)}),
      {credentials:'same-origin'}).then(response=>response.json())`);
    assert.equal(seriesState.series.closure?.reason, 'browser series closure recovery');

    await evaluate(`document.querySelector('#fixed-experiment-list [data-action="view-fixed-experiment"][data-experiment="closure-browser-single"]').click()`);
    await waitFor("Boolean(document.querySelector('#fixed-detail-closure-browser-single .close-experiment-form'))",
      'fixed experiment detail did not render its close form');
    mark('single experiment detail loaded');
    const experimentEntry = await submitAndDrop({ operation: 'experiment_close',
      formSelector: 'form.close-experiment-form[data-experiment="closure-browser-single"]',
      reason: 'browser single closure recovery' });
    mark('single close response dropped');
    assert.equal(postCounts.experiment_close, 1, 'product UI did not send exactly one single-experiment close');
    mark('single experiment close recovery');
    await recoverAfterRestart(experimentEntry, 'experiment_close');
    mark('single experiment close recovered');
    const experimentState = await evaluate("fetch('/api/experiments?key=closure-browser-single',{credentials:'same-origin'}).then(response=>response.json())");
    assert.equal(experimentState.experiment.status, 'closed');
    assert.equal(experimentState.experiment.closure?.reason, 'browser single closure recovery');

    const verified = spawnSync(python, [path.join(__dirname, 'seed_closure_browser.py'), database, '--verify'], {
      cwd: repository,
      encoding: 'utf8',
      windowsHide: true,
      env: { ...process.env, PYTHONPATH: `${repository};${path.join(repository, 'tests')}` },
    });
    assert.equal(verified.status, 0, `could not verify closure rows: ${verified.stderr}`);
    const rows = JSON.parse(verified.stdout.trim());
    assert.deepEqual(rows.single, [1, 'browser single closure recovery']);
    assert.deepEqual(rows.series, [1, 'browser series closure recovery']);
  } catch (error) {
    fs.writeFileSync(stageFile, `failed at: ${stage}`);
    fs.writeFileSync(path.join(root, 'closure-e2e-last-error.txt'), `${stage}\n${error?.stack || error}\n${serverOutput}`);
    console.error(error?.stack || error);
    throw error;
  } finally {
    mark(`cleanup start (chrome=${chromeProcess?.exitCode}, server=${serverProcess?.exitCode})`);
    try {
      if (pageSocket && pageSocket.readyState === WebSocket.OPEN) pageSocket.close();
      if (chromeProcess && chromeProcess.exitCode === null) {
        chromeProcess.kill();
        const exited = await Promise.race([
          new Promise(resolve => chromeProcess.once('exit', () => resolve(true))),
          new Promise(resolve => setTimeout(() => resolve(false), 5000)),
        ]);
        if (!exited && chromeProcess.pid) {
          spawnSync(path.join(process.env.SystemRoot || 'C:\\Windows', 'System32', 'taskkill.exe'),
            ['/T', '/F', '/PID', String(chromeProcess.pid)], { windowsHide: true, stdio: 'ignore', timeout: 5000 });
        }
      }
      if (serverProcess && serverProcess.exitCode === null) await stopServer(serverProcess);
      mark('cleanup complete');
    } catch (error) {
      fs.writeFileSync(path.join(root, 'closure-e2e-last-error.txt'), `cleanup: ${error?.stack || error}\n${serverOutput}`);
      throw error;
    }
  }
});
