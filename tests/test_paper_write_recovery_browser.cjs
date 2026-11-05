const assert = require('node:assert/strict');
const { spawn, spawnSync } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const test = require('node:test');

const repo = path.resolve(__dirname, '..');
const chrome = [process.env.CHROME_PATH,
  path.join(process.env.ProgramFiles || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
  path.join(process.env['ProgramFiles(x86)'] || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
  path.join(process.env.ProgramFiles || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe')]
  .filter(Boolean).find(file => fs.existsSync(file));
const python = process.env.DLT_PYTHON || path.join(repo, '.tmp', 'python312-full', 'python.exe');
const testRoot = process.env.DLT_BROWSER_TEST_ROOT;

test('draw review recovers the accepted write after response loss and LocalServer restart', {
  skip: !chrome || !testRoot || !fs.existsSync(python), timeout: 180000,
}, async () => {
  const root = path.resolve(testRoot);
  assert.ok(root.startsWith(path.resolve(repo, '.tmp') + path.sep), 'test files must remain under repository .tmp');
  fs.mkdirSync(root, { recursive: true });
  const profile = fs.mkdtempSync(path.join(root, 'paper-recovery-chrome-'));
  const data = fs.mkdtempSync(path.join(root, 'paper-recovery-data-'));
  const dbPath = path.join(data, 'browser-test.sqlite3');
  const pythonPath = [path.join(repo, '.venv', 'Lib', 'site-packages'), repo, path.join(repo, 'tests')].join(';');
  const env = { ...process.env, PYTHONPATH: pythonPath, TEMP: path.join(repo, '.tmp', 'pytemp'), TMP: path.join(repo, '.tmp', 'pytemp') };
  let chromeProc, serverProc, pageSocket, browserSocket, serverLog = '';
  try {
    const seeded = spawnSync(python, [path.join(__dirname, 'seed_paper_conflict_browser.py'), dbPath],
      { cwd: repo, encoding: 'utf8', env });
    assert.equal(seeded.status, 0, `fixture seed failed: ${seeded.stderr}`);
    const [originalSnapshot, selectedSnapshot, originalGroup] = seeded.stdout.trim().split(/\s+/);
    assert.ok(originalSnapshot && selectedSnapshot && originalGroup, `bad seed output: ${seeded.stdout}`);
    const port = await new Promise((resolve, reject) => {
      const probe = net.createServer(); probe.once('error', reject);
      probe.listen(0, '127.0.0.1', () => { const value = probe.address().port; probe.close(e => e ? reject(e) : resolve(value)); });
    });
    const startServer = () => {
      serverLog = '';
      serverProc = spawn(python, [path.join(__dirname, 'browser_local_server.py'), data, String(port)], {
        cwd: repo, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...env, DLT_BROWSER_TEST_PAPER_CLOCK_OFFSET_DAYS: '5' },
      });
      serverProc.stderr.setEncoding('utf8'); serverProc.stderr.on('data', x => { serverLog += x; });
      return new Promise((resolve, reject) => {
        let out = ''; const child = serverProc;
        const timer = setTimeout(() => reject(new Error(`server startup timed out: ${serverLog}`)), 30000);
        child.once('error', e => { clearTimeout(timer); reject(e); });
        child.stdout.on('data', chunk => { out += chunk; if (out.split(/\r?\n/)[0] === String(port)) { clearTimeout(timer); resolve(child); } });
        child.once('exit', code => { if (code !== 0) { clearTimeout(timer); reject(new Error(`server exited ${code}: ${serverLog}`)); } });
      });
    };
    const stopServer = async child => {
      if (!child || child.exitCode !== null) return;
      child.kill(); await new Promise(resolve => { const timer=setTimeout(resolve,5000); child.once('exit',()=>{clearTimeout(timer);resolve();}); });
      assert.ok(child.exitCode !== null || child.signalCode !== null, 'server did not stop');
    };
    await startServer();
    const origin = `http://127.0.0.1:${port}`;
    chromeProc = spawn(chrome, ['--headless=new','--no-sandbox','--disable-gpu','--disable-dev-shm-usage',
      '--no-first-run','--no-default-browser-check','--remote-debugging-port=0',`--user-data-dir=${profile}`,`${origin}/#data`],
    { windowsHide:true, stdio:'ignore' });
    const activePortFile = path.join(profile,'DevToolsActivePort'); let devtoolsPort;
    for (let i=0;i<300;i++) {
      if (chromeProc.exitCode!==null) throw new Error(`Chrome exited (${chromeProc.exitCode})`);
      if (fs.existsSync(activePortFile)) { devtoolsPort=Number(fs.readFileSync(activePortFile,'utf8').split(/\r?\n/)[0]); if(devtoolsPort>0)break; }
      await new Promise(r=>setTimeout(r,50));
    }
    assert.ok(devtoolsPort,'Chrome DevTools port missing');
    const connect = async url => {
      const socket=new WebSocket(url);
      await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error('DevTools open timeout')),10000);
        socket.addEventListener('open',()=>{clearTimeout(timer);resolve();},{once:true});
        socket.addEventListener('error',e=>{clearTimeout(timer);reject(e);},{once:true});});
      let id=1; const pending=new Map();
      socket.addEventListener('message',event=>{const m=JSON.parse(event.data);if(!m.id||!pending.has(m.id))return;const p=pending.get(m.id);pending.delete(m.id);clearTimeout(p.timer);m.error?p.reject(new Error(m.error.message)):p.resolve(m.result);});
      socket.addEventListener('close',()=>{for(const p of pending.values()){clearTimeout(p.timer);p.reject(new Error('DevTools closed'));}pending.clear();});
      return {socket,send:(method,params={})=>new Promise((resolve,reject)=>{const n=id++,timer=setTimeout(()=>{pending.delete(n);reject(new Error(`${method} timeout`));},15000);pending.set(n,{resolve,reject,timer});socket.send(JSON.stringify({id:n,method,params}));})};
    };
    const targets=await(await fetch(`http://127.0.0.1:${devtoolsPort}/json/list`)).json();
    const page=targets.find(x=>x.type==='page'&&x.url===`${origin}/#data`); assert.ok(page?.webSocketDebuggerUrl);
    const c=await connect(page.webSocketDebuggerUrl); pageSocket=c.socket; await c.send('Runtime.enable');
    const evaluate=async expression=>{const r=await c.send('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(r.exceptionDetails)throw new Error(r.result?.exception?.description||r.exceptionDetails.text);return r.result.value;};
    let drop=false,postCount=0,resolveDrop; const dropped=new Promise(resolve=>{resolveDrop=resolve;});
    pageSocket.addEventListener('message',async event=>{const m=JSON.parse(event.data);if(m.method!=='Fetch.requestPaused')return;const p=m.params;
      if(p.request.method==='POST'&&p.request.url.includes('/api/draw-conflicts/resolve')){postCount++;if(drop&&p.responseStatusCode===200){drop=false;resolveDrop(p);return;}}
      c.send('Fetch.continueRequest',{requestId:p.requestId}).catch(()=>{});
    });
    await c.send('Fetch.enable',{patterns:[{urlPattern:`${origin}/api/draw-conflicts/resolve*`,requestStage:'Response'}]});
    let ready=false;for(let i=0;i<300;i++){ready=await evaluate("document.body.dataset.serverStatus==='verified'");if(ready)break;await new Promise(r=>setTimeout(r,100));}
    assert.equal(ready,true,`local app not ready: ${serverLog}`);
    await evaluate(`document.querySelector('[data-action="review-draw"][data-issue="26017"]')?.click()`);
    let formReady=false;for(let i=0;i<300;i++){formReady=await evaluate("Boolean(document.querySelector('#draw-review-form input[name=snapshot]'))");if(formReady)break;await new Promise(r=>setTimeout(r,100));}
    assert.equal(formReady,true,'conflict review form did not load');
    const form=await evaluate(`(()=>{const options=[...document.querySelectorAll('#draw-review-form input[name=snapshot]')];const selected=options.find(x=>x.value==='${selectedSnapshot}');if(!selected)throw Error('selected snapshot absent');selected.checked=true;document.querySelector('#draw-review-note').value='browser recovery review';document.querySelector('#draw-review-form input[name=confirmed]').checked=true;return {valid:document.querySelector('#draw-review-form').checkValidity(),count:options.length}})()`);
    assert.equal(form.valid,true);assert.ok(form.count>=2);
    drop=true;await evaluate("document.querySelector('#draw-review-form').requestSubmit()");
    const paused=await Promise.race([dropped,new Promise((_,reject)=>setTimeout(()=>reject(new Error(`accepted POST did not reach response interception; count=${postCount}; ${serverLog}`)),20000))]);
    assert.equal(paused.responseStatusCode,200);await c.send('Fetch.failRequest',{requestId:paused.requestId,errorReason:'ConnectionReset'});
    let pending;
    for(let i=0;i<200;i++){pending=await evaluate(`new Promise((resolve,reject)=>{const r=indexedDB.open('dlt-paper-write-journal',1);r.onerror=()=>reject(r.error);r.onsuccess=()=>{const d=r.result,t=d.transaction('writes','readonly'),q=t.objectStore('writes').getAll();q.onsuccess=()=>{d.close();resolve(q.result)};q.onerror=()=>reject(q.error)}})`);if(pending.some(x=>x.operation==='draw_resolve'&&x.phase==='submitted'))break;await new Promise(r=>setTimeout(r,100));}
    const request=pending.find(x=>x.operation==='draw_resolve');assert.equal(request?.phase,'submitted');assert.equal(postCount,1);
    const oldPid=serverProc.pid;await stopServer(serverProc);await startServer();assert.notEqual(serverProc.pid,oldPid);
    await c.send('Page.reload',{ignoreCache:true});let recovered;
    for(let i=0;i<300;i++){recovered=await evaluate(`new Promise((resolve,reject)=>{const r=indexedDB.open('dlt-paper-write-journal',1);r.onerror=()=>reject(r.error);r.onsuccess=()=>{const d=r.result,t=d.transaction('writes','readonly'),q=t.objectStore('writes').get('${request.request_key}');q.onsuccess=()=>{d.close();resolve(q.result)};q.onerror=()=>reject(q.error)}})`);if(documentReady(recovered))break;await new Promise(r=>setTimeout(r,100));}
    function documentReady(x){return x?.phase==='accepted';}
    assert.equal(recovered.phase,'accepted');
    const status=await evaluate(`fetch('/api/paper-results/write-status?request_key=${request.request_key}&operation=draw_resolve&request_sha256=${request.request_sha256}',{credentials:'same-origin'}).then(r=>r.json())`);
    assert.equal(status.write.request.snapshot_id,selectedSnapshot,JSON.stringify(status));assert.equal(status.write.result.recalculations.exp.status,'complete');
    assert.notEqual(status.write.result.recalculations.exp.group_id,originalGroup);
    const history=await evaluate("fetch('/api/experiment-history?key=exp',{credentials:'same-origin'}).then(r=>r.json())");
    assert.equal(history.history.publications.length,2);assert.equal(history.history.publications[0].group_id,originalGroup);assert.equal(history.history.publications[1].previous_group,originalGroup);assert.equal(postCount,1);
    await evaluate(`window.confirm=()=>true;document.querySelector('#paper-write-journal [data-paper-write="dismiss"][data-request="${request.request_key}"]').click()`);
    let remaining;for(let i=0;i<100;i++){remaining=await evaluate(`new Promise((resolve,reject)=>{const r=indexedDB.open('dlt-paper-write-journal',1);r.onerror=()=>reject(r.error);r.onsuccess=()=>{const d=r.result,t=d.transaction('writes','readonly'),q=t.objectStore('writes').getAll();q.onsuccess=()=>{d.close();resolve(q.result)};q.onerror=()=>reject(q.error)}})`);if(!remaining.length)break;await new Promise(r=>setTimeout(r,100));}assert.equal(remaining.length,0);
    const version=await(await fetch(`http://127.0.0.1:${devtoolsPort}/json/version`)).json();const bc=await connect(version.webSocketDebuggerUrl);browserSocket=bc.socket;await bc.send('Browser.close');
    await new Promise(resolve=>{const timer=setTimeout(()=>{if(chromeProc.exitCode===null)chromeProc.kill();resolve();},8000);chromeProc.once('exit',()=>{clearTimeout(timer);resolve();});});
  } finally {
    try{pageSocket?.close();}catch{}try{browserSocket?.close();}catch{}
    for(const child of [chromeProc,serverProc])if(child&&child.exitCode===null){child.kill();await new Promise(r=>{const timer=setTimeout(r,3000);child.once('exit',()=>{clearTimeout(timer);r();});});}
    for(const target of [profile,data]){const abs=path.resolve(target);assert.ok(abs.startsWith(root+path.sep),'refuse cleanup outside .tmp');fs.rmSync(abs,{recursive:true,force:true,maxRetries:5,retryDelay:200});}
  }
});
