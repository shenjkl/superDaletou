// Mocked connection lifecycle; does not exercise a real browser or HTTP server.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'..','prototype','app.js'),'utf8');
const css=fs.readFileSync(path.join(__dirname,'..','prototype','styles.css'),'utf8');
const start=source.indexOf('let connectionEpoch=');
const end=source.indexOf('async function importTrainingSnapshot(',start);
assert.ok(start>=0&&end>start);

function setup(){
  const notice={hidden:true,textContent:''},editor={value:'未提交的计划配置'};
  const nodes={'connection-notice':notice,'plan-start':editor,'update-plan-list':{innerHTML:'旧计划'},
    'update-worker-status':{innerHTML:'旧后台状态'},'autostart-controls':{innerHTML:'旧启动状态'}};
  const ctx={backend:{ready:true,csrf:'csrf',snapshots:[],jobs:[],plans:[],worker:null,autostart:null},
    state:{page:'data'},document:{body:{dataset:{}},getElementById:id=>nodes[id]||null},el:id=>nodes[id]||null,
    updatePlanList:()=> '核验后的计划',updateWorkerStatus:()=> '核验后的后台状态',autostartControls:()=> '核验后的启动状态',
    refreshSnapshotDetail:async()=>{},location:{protocol:'http:',hostname:'127.0.0.1'}};
  ctx.fetch=async url=>({ok:true,status:200,json:async()=>url==='/api/snapshots'?{snapshots:[]}:
    url==='/api/jobs'?{jobs:[]}:{plans:[],worker:{state:'running'},autostart:{}}});
  vm.runInNewContext(source.slice(start,end),ctx);
  return {ctx,notice,editor,nodes};
}

test('failed data refresh hides stale statuses and preserves an edited field',async()=>{
  const {ctx,notice,editor}=setup();
  ctx.fetch=async()=>{throw Error('offline');};
  await ctx.refreshBackend();
  assert.equal(ctx.backend.ready,false);
  assert.equal(ctx.document.body.dataset.serverStatus,'unverified');
  assert.equal(notice.hidden,false);assert.match(notice.textContent,/编辑内容保留/);
  assert.equal(editor.value,'未提交的计划配置');
  for(const id of ['update-worker-status','update-plan-list','autostart-controls','live-snapshots'])
    assert.match(css,new RegExp('data-server-status="unverified"\\] #'+id));
});

test('reconnection unmasks only after plans and snapshot details are checked',async()=>{
  const {ctx,notice,editor}=setup();
  ctx.markConnectionUncertain('offline');ctx.backend.ready=true;
  let release,entered;
  const waiting=new Promise(resolve=>{release=resolve;});
  const reached=new Promise(resolve=>{entered=resolve;});
  ctx.refreshSnapshotDetail=async()=>{entered();await waiting;};
  const pending=ctx.refreshBackend();await reached;
  assert.equal(ctx.document.body.dataset.serverStatus,'unverified');
  assert.equal(notice.hidden,false);assert.equal(editor.value,'未提交的计划配置');
  release();await pending;
  assert.equal(ctx.document.body.dataset.serverStatus,'verified');assert.equal(notice.hidden,true);
});

test('an older refresh cannot unmask after a newer disconnection',async()=>{
  const {ctx,notice}=setup();let release,entered;
  const waiting=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  ctx.refreshSnapshotDetail=async()=>{entered();await waiting;};
  const pending=ctx.refreshBackend();await reached;ctx.markConnectionUncertain('offline');
  release();await pending;
  assert.equal(ctx.document.body.dataset.serverStatus,'unverified');assert.equal(notice.hidden,false);
  assert.equal(ctx.backend.ready,false);
});

test('writes are blocked during revalidation and network errors mark the connection uncertain',async()=>{
  const {ctx}=setup();ctx.markConnectionChecking();
  await assert.rejects(ctx.backendPost('/api/test',{}),/尚未完成连接核验/);
  ctx.markConnectionVerified(1,true);
  ctx.fetch=async()=>{throw Error('offline');};
  await assert.rejects(ctx.backendPost('/api/test',{}),/offline/);
  assert.equal(ctx.backend.ready,false);assert.equal(ctx.document.body.dataset.serverStatus,'unverified');
});

test('a refresh that did not check the data page cannot clear its revalidation mask',()=>{
  const {ctx,notice}=setup();ctx.markConnectionChecking();
  ctx.markConnectionVerified(1,false);
  assert.equal(ctx.document.body.dataset.serverStatus,'unverified');assert.equal(notice.hidden,false);
  ctx.markConnectionVerified(1,true);
  assert.equal(ctx.document.body.dataset.serverStatus,'verified');assert.equal(notice.hidden,true);
});

test('backtest revalidates reports, reservations and origins before unmasking',async()=>{
  const {ctx,notice,editor}=setup();ctx.state.page='backtest';ctx.backend.backtests=[];
  ctx.invalidateHoldoutAudit=()=>{};ctx.markConnectionUncertain('offline');ctx.backend.ready=true;
  ctx.fetch=async url=>({ok:true,status:200,json:async()=>url==='/api/snapshots'?{snapshots:[]}:{jobs:[]}});
  let release,entered;const wait=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  let reservationForced,originsForced;
  ctx.refreshReservations=async force=>{reservationForced=force;entered();await wait;return true;};
  ctx.refreshLegacyOrigins=async force=>{originsForced=force;return true;};
  const pending=ctx.refreshBackend();await reached;
  assert.equal(ctx.document.body.dataset.serverStatus,'unverified');assert.equal(notice.hidden,false);
  release();await pending;
  assert.equal(ctx.document.body.dataset.serverStatus,'verified');assert.equal(editor.value,'未提交的计划配置');
  assert.equal(reservationForced,true);assert.equal(originsForced,true);
  assert.match(css,/data-server-status="unverified"\] #holdout-reservations/);
  assert.match(css,/data-server-status="unverified"\] #backtest-jobs/);
});

test('failure to revalidate a reservation keeps backtest status hidden',async()=>{
  const {ctx,notice}=setup();ctx.state.page='backtest';ctx.backend.backtests=[];
  ctx.invalidateHoldoutAudit=()=>{};ctx.markConnectionUncertain('offline');ctx.backend.ready=true;
  ctx.fetch=async url=>({ok:true,status:200,json:async()=>url==='/api/snapshots'?{snapshots:[]}:{jobs:[]}});
  ctx.refreshReservations=async()=>false;ctx.refreshLegacyOrigins=async()=>true;
  await ctx.refreshBackend();
  assert.equal(ctx.backend.ready,false);assert.equal(ctx.document.body.dataset.serverStatus,'unverified');
  assert.equal(notice.hidden,false);
});

test('evidence page stays unverified until all four inputs return',async()=>{
  const {ctx,notice,nodes}=setup();ctx.state.page='evidence';ctx.backend.backtests=[];ctx.backend.experiments=[];
  ctx.seriesItems=[];ctx.fixedEvaluations=[];nodes['live-evidence']={};let refreshed=0;ctx.refreshEvidencePanel=()=>{refreshed++;};
  ctx.markConnectionUncertain('offline');ctx.backend.ready=true;
  let release,entered;const wait=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  ctx.fetch=async url=>{if(url==='/api/fixed-evaluations'){entered();await wait;}
    return {ok:true,status:200,json:async()=>url==='/api/snapshots'?{snapshots:[]}:
      url==='/api/experiments'?{experiments:[]}:url==='/api/experiment-series'?{series:[]}:
      url==='/api/fixed-evaluations'?{evaluations:[]}:{jobs:[]}};};
  const pending=ctx.refreshBackend();await reached;
  assert.equal(ctx.document.body.dataset.serverStatus,'unverified');assert.equal(notice.hidden,false);
  release();await pending;
  assert.equal(ctx.document.body.dataset.serverStatus,'verified');assert.equal(refreshed,1);
  assert.match(css,/#evidence-connection-notice/);
});

test('evidence input failure does not release its stale-state warning',async()=>{
  const {ctx,notice}=setup();ctx.state.page='evidence';ctx.backend.backtests=[];ctx.backend.experiments=[];
  ctx.seriesItems=[];ctx.fixedEvaluations=[];ctx.markConnectionUncertain('offline');ctx.backend.ready=true;
  ctx.fetch=async url=>({ok:url!=='/api/experiment-series',status:200,json:async()=>url==='/api/snapshots'?{snapshots:[]}:{jobs:[]}});
  await ctx.refreshBackend();
  assert.equal(ctx.document.body.dataset.serverStatus,'unverified');assert.equal(notice.hidden,false);
});
