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

test('real Chrome recovers a lost update-plan response after LocalServer restart', {
  skip: !browser || !tempRoot || !fs.existsSync(python),
  timeout: 180000,
}, async () => {
  const root = path.resolve(tempRoot);
  const allowed = path.resolve(repository, '.tmp');
  assert.ok(root.startsWith(allowed + path.sep), 'browser test directory must be inside repository .tmp');
  fs.mkdirSync(root, { recursive: true });
  const profile = fs.mkdtempSync(path.join(root, 'update-plan-e2e-chrome-'));
  const dataDirectory = fs.mkdtempSync(path.join(root, 'update-plan-e2e-data-'));
  let serverProcess;
  let chromeProcess;
  let pageSocket;
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
          if (stdout.split(/\r?\n/)[0] === String(port)) { clearTimeout(timer); resolve(proc); }
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
      assert.ok(proc.exitCode !== null || proc.signalCode !== null, 'LocalServer did not stop before restart');
    };
    await startServer();
    const origin = `http://127.0.0.1:${port}`;
    chromeProcess = spawn(browser, ['--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
      '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0',
      `--user-data-dir=${profile}`, `${origin}/#data`], { windowsHide: true, stdio: 'ignore' });
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
    const page = targets.find(target => target.type === 'page' && target.url === `${origin}/#data`);
    assert.ok(page?.webSocketDebuggerUrl, 'data and rules page target was not found');
    const connection = await connectDevtools(page.webSocketDebuggerUrl);
    pageSocket = connection.socket;
    const evaluate = async expression => {
      let result;
      try { result = await connection.send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }); }
      catch (error) { throw new Error(`${error.message}; expression=${expression.slice(0, 140)}`); }
      if (result.exceptionDetails) throw new Error(result.result?.exception?.description || result.exceptionDetails.text);
      return result.result.value;
    };
    await connection.send('Runtime.enable');
    let dropResponse = false;
    let resolvePaused;
    const pausedResponse = new Promise(resolve => { resolvePaused = resolve; });
    let postResponses = 0;
    pageSocket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.method !== 'Fetch.requestPaused') return;
      const params = message.params;
      if (params.request.method === 'POST' && params.request.url.includes('/api/update-plans')) {
        postResponses++;
        if (dropResponse && params.responseStatusCode === 200) {
          dropResponse = false;
          resolvePaused(params);
          return;
        }
      }
      connection.send('Fetch.continueRequest', { requestId: params.requestId }).catch(() => {});
    });
    await connection.send('Fetch.enable', { patterns: [{ urlPattern: `${origin}/api/update-plans*`, requestStage: 'Response' }] });
    let formReady = false;
    for (let i = 0; i < 300; i++) {
      formReady = await evaluate("Boolean(document.querySelector('#update-plan-form') && document.body.dataset.serverStatus==='verified')");
      if (formReady) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(formReady, true, 'update-plan form did not become ready against LocalServer');
    const filled = await evaluate(`(() => {
      const form=document.querySelector('#update-plan-form');form.closest('details').open=true;
      document.querySelector('#plan-start').value='2099-01-01T00:00';
      document.querySelector('#plan-interval').value='3600';
      document.querySelector('#plan-size').value='1';
      document.querySelector('#plan-timeout').value='1';
      document.querySelector('#plan-max').value='1048576';
      document.querySelector('#plan-retries').value='0';
      document.querySelector('#plan-wait').value='1';
      document.querySelector('#plan-pages').value='3';
      return {valid:form.checkValidity(),values:Object.fromEntries(new FormData(form))};
    })()`);
    assert.equal(filled.valid, true, `synthetic plan form is invalid: ${JSON.stringify(filled)}`);
    dropResponse = true;
    await evaluate("document.querySelector('#update-plan-form').requestSubmit()");
    let responseTimer;
    const responseTimeout = new Promise((_, reject) => {
      responseTimer = setTimeout(() => reject(new Error('update-plan POST did not reach the real server')), 15000);
    });
    const paused = await Promise.race([pausedResponse, responseTimeout]);
    clearTimeout(responseTimer);
    assert.equal(paused.responseStatusCode, 200, 'server did not accept the update-plan creation');
    await connection.send('Fetch.failRequest', { requestId: paused.requestId, errorReason: 'ConnectionReset' });
    let submitted;
    for (let i = 0; i < 300; i++) {
      submitted = await evaluate(`(async()=>({error:document.querySelector('#plan-error')?.textContent||'',entries:
        await new Promise((resolve,reject)=>{const r=indexedDB.open('dlt-update-write-journal',1);r.onerror=()=>reject(r.error);
          r.onsuccess=()=>{const db=r.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
            get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}})}))()`);
      if (submitted.entries.some(entry => entry.operation === 'update_plan_create' && entry.phase === 'submitted')
          && submitted.error) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    const pendingEntry = submitted.entries.find(entry => entry.operation === 'update_plan_create');
    assert.equal(pendingEntry?.phase, 'submitted', 'product UI did not retain the lost-response request');
    assert.match(submitted.error, /fetch|network|连接/i);
    assert.equal(postResponses, 1, 'product UI did not send exactly one update-plan POST');

    const firstServerPid = serverProcess.pid;
    await stopServer(serverProcess);
    await startServer();
    assert.notEqual(serverProcess.pid, firstServerPid, 'LocalServer was not restarted as a new process');
    await connection.send('Page.reload', { ignoreCache: true });
    let reloaded = false;
    for (let i = 0; i < 300; i++) {
      reloaded = await evaluate("document.readyState==='complete' && Boolean(window.UpdateWriteJournal)");
      if (reloaded) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(reloaded, true, 'application did not reload after LocalServer restart');
    let recovered;
    for (let i = 0; i < 300; i++) {
      recovered = await evaluate(`(async()=>({ready:document.body.dataset.serverStatus==='verified',entries:
        await new Promise((resolve,reject)=>{const r=indexedDB.open('dlt-update-write-journal',1);r.onerror=()=>reject(r.error);
          r.onsuccess=()=>{const db=r.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
            get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}}),
        dismiss:!!document.querySelector('#update-write-journal [data-update-write-action="dismiss"][data-request="${pendingEntry.request_key}"]')}))()`);
      if (recovered.ready && recovered.dismiss) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    const accepted = recovered.entries.find(entry => entry.request_key === pendingEntry.request_key);
    assert.equal(accepted?.phase, 'accepted', 'product recovery did not persist the verified receipt');
    assert.equal(recovered.dismiss, true, 'product recovery UI did not expose verified cleanup');
    const status = await evaluate(`fetch('/api/update-write-requests?key='+encodeURIComponent('${pendingEntry.request_key}'),
      {credentials:'same-origin'}).then(response=>response.json())`);
    assert.equal(status.write.status, 'accepted');
    assert.equal(status.write.operation, 'update_plan_create');
    const data = await evaluate("fetch('/api/update-plans',{credentials:'same-origin'}).then(response=>response.json())");
    const plans = data.plans.filter(plan => plan.id === status.write.result.id);
    assert.equal(plans.length, 1, 'server did not retain exactly one update plan');
    assert.equal(plans[0].enabled, false, 'synthetic recovery plan must remain paused');
    await evaluate(`window.confirm=()=>true;document.querySelector('#update-write-journal [data-update-write-action="dismiss"][data-request="${pendingEntry.request_key}"]').click()`);
    let remainder;
    for (let i = 0; i < 100; i++) {
      remainder = await evaluate(`new Promise((resolve,reject)=>{const r=indexedDB.open('dlt-update-write-journal',1);r.onerror=()=>reject(r.error);
        r.onsuccess=()=>{const db=r.result,tx=db.transaction('writes','readonly'),get=tx.objectStore('writes').getAll();
          get.onsuccess=()=>{db.close();resolve(get.result)};get.onerror=()=>{db.close();reject(get.error)}}})`);
      if (!remainder.length) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(remainder.length, 0, 'product UI did not clear the verified local request');
    const version = await (await fetch(`http://127.0.0.1:${devtoolsPort}/json/version`)).json();
    const browserConnection = await connectDevtools(version.webSocketDebuggerUrl);
    browserSocket = browserConnection.socket;
    await browserConnection.send('Browser.close');
    await new Promise(resolve => {
      let timer;
      const done = () => { clearTimeout(timer); resolve(); };
      timer = setTimeout(() => { if (chromeProcess.exitCode === null) chromeProcess.kill(); setTimeout(done, 2000); }, 8000);
      chromeProcess.once('exit', done);
      if (chromeProcess.exitCode !== null || chromeProcess.signalCode !== null) done();
    });
  } finally {
    try { pageSocket?.close(); } catch {}
    try { browserSocket?.close(); } catch {}
    for (const proc of [chromeProcess, serverProcess]) {
      if (proc && proc.exitCode === null) {
        proc.kill();
        await new Promise(resolve => {
          const timer = setTimeout(resolve, 3000);
          proc.once('exit', () => { clearTimeout(timer); resolve(); });
        });
      }
    }
    for (const target of [profile, dataDirectory]) {
      const absolute = path.resolve(target);
      assert.ok(absolute.startsWith(root + path.sep), 'refuse to remove test data outside .tmp test root');
      fs.rmSync(absolute, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
    }
  }
});
