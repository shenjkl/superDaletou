const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
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

test('real Chrome restores a lost source-review response against LocalServer after reload', {
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
  let serverOutput = '';
  try {
    serverProcess = spawn(python, [path.join(__dirname, 'browser_local_server.py'), dataDirectory], {
      cwd: repository,
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, PYTHONPATH: `${repository};${path.join(repository, 'tests')}` },
    });
    serverProcess.stdout.setEncoding('utf8');
    serverProcess.stderr.setEncoding('utf8');
    serverProcess.stderr.on('data', chunk => { serverOutput += chunk; });
    const port = await new Promise((resolve, reject) => {
      let stdout = '';
      const timer = setTimeout(() => reject(new Error(`LocalServer startup timed out: ${serverOutput}`)), 30000);
      serverProcess.once('error', error => { clearTimeout(timer); reject(error); });
      serverProcess.stdout.on('data', chunk => {
        stdout += chunk;
        const line = stdout.split(/\r?\n/)[0];
        if (/^\d+$/.test(line)) { clearTimeout(timer); resolve(Number(line)); }
        else if (serverProcess.exitCode !== null) { clearTimeout(timer); reject(new Error(`LocalServer exited: ${serverOutput}`)); }
      });
      serverProcess.once('exit', code => {
        if (code !== 0) { clearTimeout(timer); reject(new Error(`LocalServer exited with ${code}: ${serverOutput}`)); }
      });
    });
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

    socket = new WebSocket(page.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve, { once: true });
      socket.addEventListener('error', reject, { once: true });
    });
    let nextId = 1;
    const pending = new Map();
    const send = (method, params = {}) => new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      socket.send(JSON.stringify({ id, method, params }));
    });
    let shouldDrop = false;
    let resolvePaused;
    const pausedResponse = new Promise(resolve => { resolvePaused = resolve; });
    socket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.id && pending.has(message.id)) {
        const handlers = pending.get(message.id);
        pending.delete(message.id);
        message.error ? handlers.reject(new Error(message.error.message)) : handlers.resolve(message.result);
      }
      if (message.method === 'Fetch.requestPaused') {
        const params = message.params;
        if (shouldDrop && params.request.method === 'POST' && params.request.url.includes('/api/source-reviews')
            && params.responseStatusCode === 200) {
          shouldDrop = false;
          resolvePaused(params);
        } else {
          send('Fetch.continueRequest', { requestId: params.requestId }).catch(() => {});
        }
      }
    });
    const evaluate = async expression => {
      const result = await send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
      if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description || result.exceptionDetails.text);
      return result.result.value;
    };
    await send('Runtime.enable');
    await send('Fetch.enable', { patterns: [{ urlPattern: '*://127.0.0.1/*/api/source-reviews*', requestStage: 'Response' },
      { urlPattern: `${origin}/api/source-reviews*`, requestStage: 'Response' }] });
    const waitForPage = async () => {
      for (let i = 0; i < 200; i++) {
        if (await evaluate('document.readyState') === 'complete'
            && await evaluate('Boolean(window.SourceWriteJournal)')) return;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      throw new Error('application or source journal did not load in Chrome');
    };
    await waitForPage();

    const key = '00000000-0000-4000-8000-000000000031';
    const setup = `(() => {
      const api = {
        async digest(request) {
          const bytes = new TextEncoder().encode(SourceWriteJournal.canonical(request));
          const hash = await crypto.subtle.digest('SHA-256', bytes);
          return Array.from(new Uint8Array(hash), value => value.toString(16).padStart(2, '0')).join('');
        },
        objectKey(operation, request) { return request.kind + ':' + request.document_sha256; },
        async post(body) {
          const session = await fetch('/api/session', {credentials:'same-origin'}).then(response => response.json());
          return fetch('/api/source-reviews', {method:'POST', credentials:'same-origin',
            headers:{'content-type':'application/json','X-DLT-CSRF':session.csrf}, body:JSON.stringify(body)})
            .then(async response => { const data=await response.json(); if(!response.ok)throw new Error(data.error||'source write failed'); return data; });
        },
        async status(request_key, request_sha256) {
          const response = await fetch('/api/source-write-status?request_key='+encodeURIComponent(request_key)
            +'&request_sha256='+encodeURIComponent(request_sha256), {credentials:'same-origin'});
          const data = await response.json(); if(!response.ok)throw new Error(data.error||'status failed'); return data.write;
        },
        async fence(body) {
          const session = await fetch('/api/session', {credentials:'same-origin'}).then(response => response.json());
          const response = await fetch('/api/source-writes/fence', {method:'POST',credentials:'same-origin',
            headers:{'content-type':'application/json','X-DLT-CSRF':session.csrf},body:JSON.stringify(body)});
          const data=await response.json(); if(!response.ok&&response.status!==409)throw new Error(data.error||'fence failed'); return data.write;
        }
      };
      window.makeSourceJournal = () => SourceWriteJournal.coordinator({store:SourceWriteJournal.browserStore(indexedDB),
        locks:navigator.locks,api,uuid:()=> '${key}'});
    })()`;
    await evaluate(setup);
    const startedPromise = evaluate(`(async () => {
      const text = 'synthetic browser integration document';
      const bytes = new TextEncoder().encode(text);
      const hash = await crypto.subtle.digest('SHA-256', bytes);
      const document_sha256 = Array.from(new Uint8Array(hash), value => value.toString(16).padStart(2,'0')).join('');
      const request = {kind:'calendar',source_url:'https://www.sporttery.cn/synthetic-test',document_sha256,
        document_size:bytes.length,document_name:'browser-source.txt',extracted:{synthetic:'only'},
        reviewer_note:'synthetic browser integration only',confirmed:true};
      const binary = String.fromCharCode(...bytes);
      const body = {...request,document_base64:btoa(binary)};
      let error='';
      try { await window.makeSourceJournal().begin('source_review',request,body); } catch (caught) { error=caught.message; }
      const [entry] = await window.makeSourceJournal().list();
      return {error,entry,document_sha256};
    })()`);
    shouldDrop = true;
    let responseTimer;
    const responseTimedOut = new Promise((_, reject) => {
      responseTimer = setTimeout(() => reject(new Error('source review did not reach the real server')), 30000);
    });
    const paused = await Promise.race([pausedResponse, responseTimedOut]);
    clearTimeout(responseTimer);
    assert.equal(paused.responseStatusCode, 200);
    await send('Fetch.failRequest', { requestId: paused.requestId, errorReason: 'ConnectionReset' });
    const started = await startedPromise;
    assert.match(started.error, /Failed to fetch|NetworkError|fetch/);
    assert.equal(started.entry.phase, 'submitted');

    await send('Page.reload', { ignoreCache: true });
    await waitForPage();
    await evaluate(setup);
    const recovered = await evaluate(`(async () => {
      const journal = window.makeSourceJournal();
      const entries = await journal.list();
      const checked = await journal.check('${key}');
      const stateResponse = await fetch('/api/source-write-status?request_key='+encodeURIComponent('${key}')
        +'&request_sha256='+encodeURIComponent(entries[0].request_sha256),{credentials:'same-origin'});
      const state = await stateResponse.json();
      const reviews = await fetch('/api/source-reviews',{credentials:'same-origin'}).then(response=>response.json());
      await journal.dismiss('${key}');
      return {entries,checked,state:state.write,reviews,remainder:await journal.list()};
    })()`);
    assert.equal(recovered.entries.length, 1);
    assert.equal(recovered.entries[0].phase, 'submitted');
    assert.equal(recovered.checked.phase, 'accepted');
    assert.equal(recovered.state.status, 'accepted');
    assert.equal(recovered.reviews.reviews.filter(review => review.review_id === recovered.state.result.review_id
      && review.document_sha256 === started.document_sha256).length, 1);
    assert.equal(recovered.remainder.length, 0);
  } finally {
    try { socket?.close(); } catch {}
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
