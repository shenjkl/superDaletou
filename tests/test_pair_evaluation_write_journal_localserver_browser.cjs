const assert = require('node:assert/strict');
const { spawn, spawnSync } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const test = require('node:test');

const repository = path.resolve(__dirname, '..');
const browser = [process.env.CHROME_PATH,
  path.join(process.env.ProgramFiles || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
  path.join(process.env['ProgramFiles(x86)'] || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
  path.join(process.env.ProgramFiles || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe')]
  .filter(Boolean).find(candidate => fs.existsSync(candidate));
const python = process.env.DLT_PYTHON || path.join(repository, '.venv', 'Scripts', 'python.exe');
const tempRoot = process.env.DLT_BROWSER_TEST_ROOT;
const seedScript = path.join(__dirname, 'seed_pair_evaluation_browser.py');

test('Chrome recovers pair creation, model selection, and evaluation writes through the product UI', {
  skip: !browser || !tempRoot || !fs.existsSync(python),
  timeout: 300000,
}, async () => {
  const root = path.resolve(tempRoot);
  const allowed = path.resolve(repository, '.tmp');
  assert.ok(root.startsWith(allowed + path.sep), 'browser test directory must be inside repository .tmp');
  fs.mkdirSync(root, { recursive: true });
  const profile = fs.mkdtempSync(path.join(root, 'pair-eval-e2e-chrome-'));
  const dataDirectory = fs.mkdtempSync(path.join(root, 'pair-eval-e2e-data-'));
  const stageFile = path.join(root, 'pair-eval-e2e-stage.txt');
  let stage = 'seed model database';
  const mark = value => { stage = value; fs.writeFileSync(stageFile, value); };
  mark(stage);
  const seeded = spawnSync(python, [seedScript, dataDirectory], {
    cwd: repository, encoding: 'utf8', windowsHide: true, timeout: 120000,
    env: { ...process.env, PYTHONPATH: repository + ';' + path.join(repository, 'tests') },
  });
  if (seeded.status !== 0) fs.writeFileSync(path.join(root, 'pair-eval-e2e-last-error.txt'), 'seed database\n' + seeded.stderr);
  assert.equal(seeded.status, 0, 'could not seed model database: ' + seeded.stderr);
  const metadata = JSON.parse(seeded.stdout.trim().split(/\r?\n/).at(-1));

  let serverProcess;
  let chromeProcess;
  let pageSocket;
  let serverOutput = '';
  let connection;
  let dropOperation;
  let resolvePaused;
  const postCounts = { pair_create: 0, selection_decision: 0, evaluation: 0 };
  mark('LocalServer startup');
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
        const available = probe.address().port;
        probe.close(error => error ? reject(error) : resolve(available));
      });
    });
    const startServer = () => {
      serverOutput = '';
      serverProcess = spawn(python, [path.join(__dirname, 'browser_local_server.py'), dataDirectory, String(port)], {
        cwd: repository, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...process.env, PYTHONPATH: repository + ';' + path.join(repository, 'tests') },
      });
      serverProcess.stdout.setEncoding('utf8');
      serverProcess.stderr.setEncoding('utf8');
      serverProcess.stderr.on('data', chunk => { serverOutput += chunk; });
      return new Promise((resolve, reject) => {
        let stdout = '';
        const proc = serverProcess;
        const timer = setTimeout(() => reject(new Error('LocalServer startup timed out: ' + serverOutput)), 30000);
        proc.once('error', error => { clearTimeout(timer); reject(error); });
        proc.stdout.on('data', chunk => {
          stdout += chunk;
          if (stdout.split(/\r?\n/)[0] === String(port)) { clearTimeout(timer); resolve(proc); }
          else if (proc.exitCode !== null) { clearTimeout(timer); reject(new Error('LocalServer exited: ' + serverOutput)); }
        });
        proc.once('exit', code => {
          if (code !== 0) { clearTimeout(timer); reject(new Error('LocalServer exited with ' + code + ': ' + serverOutput)); }
        });
      });
    };
    await startServer();
    const origin = 'http://127.0.0.1:' + port;
    mark('Chrome startup');
    chromeProcess = spawn(browser, ['--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
      '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0',
      '--user-data-dir=' + profile, origin + '/#training'], { windowsHide: true, stdio: 'ignore' });
    const activePortFile = path.join(profile, 'DevToolsActivePort');
    let devtoolsPort;
    for (let i = 0; i < 300; i++) {
      if (chromeProcess.exitCode !== null) throw new Error('Chrome exited before DevTools started');
      if (fs.existsSync(activePortFile)) {
        devtoolsPort = Number(fs.readFileSync(activePortFile, 'utf8').split(/\r?\n/)[0]);
        if (Number.isInteger(devtoolsPort) && devtoolsPort > 0) break;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.ok(devtoolsPort, 'Chrome did not expose DevTools');
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
        const handlers = pending.get(message.id); pending.delete(message.id); clearTimeout(handlers.timer);
        message.error ? handlers.reject(new Error(message.error.message)) : handlers.resolve(message.result);
      });
      ws.addEventListener('close', () => {
        for (const [id, handlers] of pending) { clearTimeout(handlers.timer); handlers.reject(new Error('DevTools socket closed at command ' + id)); }
        pending.clear();
      });
      const send = (method, params = {}) => new Promise((resolve, reject) => {
        const id = nextId++; const timer = setTimeout(() => { pending.delete(id); reject(new Error('DevTools command timed out: ' + method)); }, 15000);
        pending.set(id, { resolve, reject, timer }); ws.send(JSON.stringify({ id, method, params }));
      });
      return { socket: ws, send };
    };
    const targets = await (await fetch('http://127.0.0.1:' + devtoolsPort + '/json/list')).json();
    const page = targets.find(target => target.type === 'page' && target.url === origin + '/#training');
    assert.ok(page && page.webSocketDebuggerUrl, 'training page target was not found');
    connection = await connectDevtools(page.webSocketDebuggerUrl);
    pageSocket = connection.socket;
    const evaluate = async expression => {
      const result = await connection.send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
      if (result.exceptionDetails) throw new Error(result.result && result.result.exception ? result.result.exception.description : result.exceptionDetails.text);
      return result.result.value;
    };
    await connection.send('Runtime.enable');
    const armResponseDrop = operation => {
      dropOperation = operation;
      return new Promise(resolve => { resolvePaused = resolve; });
    };
    const waitForDroppedResponse = async promise => {
      let timer;
      const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('write did not reach LocalServer')), 30000); });
      try { return await Promise.race([promise, timeout]); } finally { clearTimeout(timer); }
    };
    pageSocket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.method !== 'Fetch.requestPaused') return;
      const params = message.params;
      const url = params.request.url;
      const operation = url.includes('/api/model-selection-decisions') ? 'selection_decision'
        : url.includes('/api/evaluations') ? 'evaluation'
          : url.includes('/api/pairs') ? 'pair_create' : null;
      if (params.request.method === 'POST' && operation) {
        postCounts[operation]++;
        if (dropOperation === operation && params.responseStatusCode === 200) {
          dropOperation = null;
          const resolve = resolvePaused; resolvePaused = null;
          resolve(params); return;
        }
      }
      connection.send('Fetch.continueRequest', { requestId: params.requestId }).catch(() => {});
    });
    await connection.send('Fetch.enable', { patterns: [
      { urlPattern: origin + '/api/pairs*', requestStage: 'Response' },
      { urlPattern: origin + '/api/model-selection-decisions*', requestStage: 'Response' },
      { urlPattern: origin + '/api/evaluations*', requestStage: 'Response' },
    ] });
    const waitFor = async (expression, description, attempts = 300) => {
      for (let i = 0; i < attempts; i++) {
        const value = await evaluate(expression);
        if (value) return value;
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      throw new Error(description + ' timed out');
    };
    await waitFor("document.body.dataset.serverStatus==='verified' && Boolean(document.querySelector('[data-action=\"use-trained-model\"]'))",
      'trained artifact did not appear in the product UI');
    mark('select trained artifact in product UI');
    const trainedSelector = '[data-action="use-trained-model"][data-artifact="' + metadata.artifact_id + '"]';
    await evaluate('document.querySelector(' + JSON.stringify(trainedSelector) + ').click()');
    await waitFor("document.body.dataset.serverStatus==='verified' && Boolean(document.querySelector('#live-generation-form'))",
      'generation form did not open from trained model');
    const prepared = await evaluate("(() => { const f=document.querySelector('#live-generation-form'); const set=(s,v)=>{const x=f.querySelector(s);if(!x)return false;x.value=v;x.dispatchEvent(new Event('input',{bubbles:true}));return true;}; set('[name=target]'," + JSON.stringify(metadata.target) + "); set('[name=date]'," + JSON.stringify(metadata.target_date) + "); set('[name=window]','6'); set('[name=selection_reason]','browser recovery acceptance'); set('[name=external_attempts_note]','无'); return {valid:f.checkValidity(),method:f.querySelector('[name=method]').value,artifact:f.querySelector('[name=artifact]').value,first:f.querySelector('[name=first]').value,cutoff:f.querySelector('[name=cutoff]').value,attempts:f.querySelectorAll('[name=considered_attempts]:checked').length}; })()");
    assert.ok(prepared.valid, 'product generation form is invalid: ' + JSON.stringify(prepared));
    assert.equal(prepared.method, 'tree'); assert.equal(prepared.artifact, metadata.artifact_id); assert.equal(prepared.attempts, 1);

    const idbEntry = async (operation, requestKey) => {
      const op = JSON.stringify(operation); const key = requestKey ? JSON.stringify(requestKey) : 'null';
      return evaluate("(async()=>{const r=indexedDB.open('dlt-pair-evaluation-journal',1);return await new Promise((resolve,reject)=>{r.onerror=()=>reject(r.error);r.onsuccess=()=>{const db=r.result,tx=db.transaction('writes','readonly'),q=tx.objectStore('writes').getAll();q.onsuccess=()=>{db.close();const all=q.result.filter(x=>x.operation===" + op + ");resolve(" + key + "?all.find(x=>x.request_key===" + key + ")||null:all.at(-1)||null)};q.onerror=()=>{db.close();reject(q.error)}}})})()");
    };
    const waitForIdbEntry = async operation => {
      for (let i = 0; i < 200; i++) {
        const value = await idbEntry(operation);
        if (value) return value;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      throw new Error(operation + ' write was not retained in IndexedDB');
    };
    const submitAndDrop = async (operation, submitExpression) => {
      const pausedPromise = armResponseDrop(operation);
      await evaluate(submitExpression);
      const paused = await waitForDroppedResponse(pausedPromise);
      assert.equal(paused.responseStatusCode, 200, operation + ' did not commit successfully');
      await connection.send('Fetch.failRequest', { requestId: paused.requestId, errorReason: 'ConnectionReset' });
      const record = await waitForIdbEntry(operation);
      assert.ok(['submitted','accepted'].includes(record.phase), operation + ' was not kept for recovery: ' + record.phase);
      return record;
    };
    const journalSelector = entry => '#pair-evaluation-journal [data-pair-eval=\"dismiss\"][data-request=\"' + entry.request_key + '\"]';
    const statusFor = (operation, key) => evaluate("fetch('/api/pair-evaluation-writes?'+new URLSearchParams({operation:" + JSON.stringify(operation) + ",key:" + JSON.stringify(key) + "}),{credentials:'same-origin'}).then(async r=>({status:r.status,data:await r.json()}))");
    const recoverAfterRestart = async (entry, operation) => {
      const priorPid = serverProcess.pid;
      await stopServer(serverProcess); await startServer();
      assert.notEqual(serverProcess.pid, priorPid, 'LocalServer was not restarted');
      await connection.send('Page.reload', { ignoreCache: true });
      await waitFor("document.readyState==='complete' && document.body.dataset.serverStatus==='verified' && Boolean(document.querySelector('#pair-evaluation-journal'))",
        'generation page did not reload after LocalServer restart');
      const selector = JSON.stringify(journalSelector(entry));
      await waitFor('!!document.querySelector(' + selector + ')', operation + ' recovered UI did not expose confirmation cleanup');
      const receipt = await statusFor(operation, entry.request_key);
      assert.equal(receipt.status, 200, JSON.stringify(receipt.data));
      assert.equal(receipt.data.write.status, 'accepted');
      assert.equal(receipt.data.write.request_key, entry.request_key);
      await evaluate('window.confirm=()=>true;document.querySelector(' + selector + ').click()');
      await waitFor("(async()=>{const r=indexedDB.open('dlt-pair-evaluation-journal',1);return await new Promise((resolve,reject)=>{r.onerror=()=>reject(r.error);r.onsuccess=()=>{const db=r.result,q=db.transaction('writes','readonly').objectStore('writes').get(" + JSON.stringify(entry.request_key) + ");q.onsuccess=()=>{db.close();resolve(!q.result)};q.onerror=()=>{db.close();reject(q.error)}}})})()",
        operation + ' verified IndexedDB copy was not cleaned through the product UI');
    };

    mark('generate model pair from actual product form');
    const pairEntry = await submitAndDrop('pair_create', "document.querySelector('#live-generation-form').requestSubmit()");
    const pairKey = pairEntry.request_key;
    assert.equal(postCounts.pair_create, 1);
    await recoverAfterRestart(pairEntry, 'pair_create');
    const pairRead = await evaluate("fetch('/api/pairs?key='+encodeURIComponent(" + JSON.stringify(pairKey) + "),{credentials:'same-origin'}).then(r=>r.json())");
    assert.equal(pairRead.request.artifact_id, metadata.artifact_id);
    assert.equal(pairRead.pair.model_artifact_id, metadata.artifact_id);

    mark('append model selection decision from product UI');
    const savedPairSelector = '[data-action="load-saved-pair"][data-pair-key="' + pairKey + '"]';
    await evaluate('document.querySelector(' + JSON.stringify(savedPairSelector) + ').click()');
    await waitFor("Boolean(document.querySelector('[data-action=\"append-model-selection\"]'))", 'saved model draft did not expose its selection decision action');
    const decisionEntry = await submitAndDrop('selection_decision', "document.querySelector('[data-action=\"append-model-selection\"]').click()");
    const decisionKey = decisionEntry.request_key;
    assert.equal(postCounts.selection_decision, 1);
    await recoverAfterRestart(decisionEntry, 'selection_decision');
    const selectedPair = await evaluate("fetch('/api/pairs?key='+encodeURIComponent(" + JSON.stringify(pairKey) + "),{credentials:'same-origin'}).then(r=>r.json())");
    assert.ok(selectedPair.selection_history.length >= 2, 'appended model selection history was not retained');
    assert.equal(selectedPair.selection_history.at(-1).request_key, decisionKey);

    mark('evaluate model pair through product form');
    await evaluate('document.querySelector(' + JSON.stringify(savedPairSelector) + ').click()');
    await waitFor("Boolean(document.querySelector('#live-evaluation-form'))", 'saved model pair did not render the evaluation form');
    const evaluationPrepared = await evaluate("(() => {const f=document.querySelector('#live-evaluation-form');const source=f.querySelector('[name=scenario]');const wanted=" + JSON.stringify(metadata.snapshot_id + '|26123') + ";if([...source.options].some(o=>o.value===wanted))source.value=wanted;f.querySelector('[name=samples]').value='1000';f.querySelector('[name=samples]').dispatchEvent(new Event('input',{bubbles:true}));f.querySelector('[name=seed]').value='4242';f.querySelector('[name=seed]').dispatchEvent(new Event('input',{bubbles:true}));source.dispatchEvent(new Event('change',{bubbles:true}));return {valid:f.checkValidity(),source:source.value,samples:f.querySelector('[name=samples]').value};})()");
    assert.ok(evaluationPrepared.valid, 'product evaluation form is invalid: ' + JSON.stringify(evaluationPrepared));
    const evaluationEntry = await submitAndDrop('evaluation', "document.querySelector('#live-evaluation-form').requestSubmit()");
    const evaluationKey = evaluationEntry.request_key;
    assert.equal(postCounts.evaluation, 1);
    await recoverAfterRestart(evaluationEntry, 'evaluation');
    const evalRead = await evaluate("fetch('/api/evaluations?pair='+encodeURIComponent(" + JSON.stringify(pairKey) + "),{credentials:'same-origin'}).then(r=>r.json())");
    assert.ok(evalRead.evaluations.some(x=>x.request_key===evaluationKey), 'accepted evaluation was absent from product history');
    mark('verify business rows and receipts');
    await stopServer(serverProcess);
    const verified = spawnSync(python, [seedScript, dataDirectory, '--verify', pairKey, decisionKey, evaluationKey], {
      cwd: repository, encoding: 'utf8', windowsHide: true,
      env: { ...process.env, PYTHONPATH: repository + ';' + path.join(repository, 'tests') },
    });
    assert.equal(verified.status, 0, 'SQLite verification failed: ' + verified.stderr);
    const rows = JSON.parse(verified.stdout.trim());
    assert.deepEqual(rows, { pairs: 1, pair_receipts: 1, decisions: 1, decision_receipts: 1, evaluations: 1, evaluation_receipts: 1 });
  } catch (error) {
    fs.writeFileSync(stageFile, 'failed at: ' + stage);
    fs.writeFileSync(path.join(root, 'pair-eval-e2e-last-error.txt'), stage + '\n' + (error && error.stack || error) + '\n' + serverOutput);
    console.error(error && error.stack || error);
    throw error;
  } finally {
    mark('cleanup start (chrome=' + (chromeProcess && chromeProcess.exitCode) + ', server=' + (serverProcess && serverProcess.exitCode) + ')');
    try {
      if (pageSocket && pageSocket.readyState === WebSocket.OPEN) pageSocket.close();
      if (chromeProcess && chromeProcess.exitCode === null) {
        chromeProcess.kill();
        await Promise.race([new Promise(resolve => chromeProcess.once('exit', resolve)), new Promise(resolve => setTimeout(resolve, 5000))]);
        if (chromeProcess.exitCode === null && chromeProcess.pid) {
          spawnSync(path.join(process.env.SystemRoot || 'C:\\Windows', 'System32', 'taskkill.exe'),
            ['/T','/F','/PID',String(chromeProcess.pid)], { windowsHide: true, stdio: 'ignore', timeout: 5000 });
        }
      }
      await stopServer(serverProcess);
    } finally { mark('cleanup complete'); }
  }
});
