// Pure rendering and mocked async interaction; not a browser test.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname,'..','prototype','app.js'),'utf8');
const start = source.indexOf('function legacyOriginContent(');
const end = source.indexOf('function holdoutAuditPanel(', start);
assert.ok(start >= 0 && end > start);
function setup(){
  const audit={innerHTML:'old confirmed range'}, button={disabled:false,isConnected:true}, status={textContent:''};
  const data={kind:'historical',issue:'26122',reason:'本人回忆',confirm:'on'};
  const form={dataset:{evaluation:'legacy-1'},isConnected:true,querySelector:s=>s==='[type=submit]'?button:status};
  const ctx={escape:v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'),
    document:{addEventListener(){}},crypto:{randomUUID:()=> 'event-1'},
    FormData:class{get(key){return data[key];}},el:id=>id==='holdout-audit-result'?audit:null};
  vm.runInNewContext(source.slice(start,end),ctx);
  ctx.refreshLegacyOrigins=async()=>{};ctx.refreshReservations=async()=>{};
  return {ctx,audit,button,status,data,form};
}
test('legacy records show escaped inputs and complete declaration history',()=>{
  const {ctx}=setup();
  const html=ctx.legacyOriginContent([{status:'available',evaluation_key:'legacy',custom_input:{label:'<旧>',payouts:{101:'100'}},
    declarations:[{kind:'historical',issue:'26122',reason:'<本人说明>',created_at:'2026-11-01'}]}]);
  assert.match(html,/&lt;旧&gt;/);assert.match(html,/&lt;本人说明&gt;/);
  assert.match(html,/参考第 26122 期/);assert.match(html,/既有声明不会删除/);
  assert.doesNotMatch(html,/<本人说明>/);
});
test('submitting declaration invalidates prior audit and preserves id on retry',async()=>{
  const {ctx,audit,form,button}=setup();let payload;
  ctx.backendPost=async(_path,body)=>{payload=body;};
  await ctx.submitLegacyOrigin(form);
  assert.equal(payload.event_id,'event-1');assert.equal(payload.evaluation_key,'legacy-1');
  assert.equal(payload.issue,'26122');assert.equal(payload.confirmed,true);
  assert.match(audit.innerHTML,/请重新核对/);assert.equal(button.disabled,false);
});
test('failed declaration leaves previous audit untouched and reports error',async()=>{
  const {ctx,audit,form,status}=setup();
  ctx.backendPost=async()=>{throw Error('记录损坏');};
  await ctx.submitLegacyOrigin(form);
  assert.equal(audit.innerHTML,'old confirmed range');assert.equal(status.textContent,'记录损坏');
});

test('reloading origins retains a dirty form and reopens its details',async()=>{
  const form={dataset:{evaluation:'legacy-1',dirty:'true'},value:'仍在填写的说明',
    closest:()=>({open:true}),querySelector:()=>({disabled:false})};
  const details={open:false};let transplanted=null,html='old';
  const replacement={dataset:{evaluation:'legacy-1'},closest:()=>details,replaceWith:node=>{transplanted=node;}};
  const root={isConnected:true,get innerHTML(){return html;},set innerHTML(value){html=value;this.fresh=true;},
    querySelectorAll(){return this.fresh?[replacement]:[form];},querySelector:()=>form,insertAdjacentHTML(){},append(){}};
  const ctx={legacyOriginEpoch:0,legacyOriginTask:null,reservationTask:null,el:id=>id==='legacy-origin-list'?root:null,
    escape:String,document:{addEventListener(){}},fetch:async()=>({ok:true,json:async()=>({records:[{status:'available',evaluation_key:'legacy-1',custom_input:{label:'旧记录',payouts:{}},declarations:[]}]})})};
  vm.runInNewContext(source.slice(start,end),ctx);
  assert.equal(await ctx.refreshLegacyOrigins(),true);
  assert.equal(transplanted,form);assert.equal(transplanted.value,'仍在填写的说明');assert.equal(details.open,true);
});

test('origin refresh failure retains a dirty form',async()=>{
  const form={dataset:{dirty:'true'}},root={isConnected:true,textContent:'保留',querySelector:()=>form};
  const ctx={legacyOriginEpoch:0,legacyOriginTask:null,reservationTask:null,el:()=>root,
    document:{addEventListener(){}},fetch:async()=>{throw Error('offline');}};
  vm.runInNewContext(source.slice(start,end),ctx);
  assert.equal(await ctx.refreshLegacyOrigins(),false);assert.equal(root.textContent,'保留');
});

test('forced origin revalidation waits for an older request and fetches again',async()=>{
  let release,entered,calls=0;
  const waiting=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  const root={isConnected:true,innerHTML:'',querySelectorAll:()=>[],querySelector:()=>null};
  const ctx={legacyOriginEpoch:0,legacyOriginTask:null,reservationTask:null,el:()=>root,
    document:{addEventListener(){}},fetch:async()=>{calls++;if(calls===1){entered();await waiting;}return {ok:true,json:async()=>({records:[]})};}};
  vm.runInNewContext(source.slice(start,end),ctx);
  const old=ctx.refreshLegacyOrigins();await reached;
  const shared=ctx.refreshLegacyOrigins(),fresh=ctx.refreshLegacyOrigins(true);
  assert.equal(calls,1);release();
  assert.equal(await old,true);assert.equal(await shared,true);assert.equal(await fresh,true);
  assert.equal(calls,2);
});

test('a missing origin retains its draft but disables submission until the origin returns',async()=>{
  let records=[];const button={disabled:false,isConnected:true};
  const form={dataset:{evaluation:'legacy-1',dirty:'true'},isConnected:true,value:'未提交说明',
    closest:()=>null,querySelector:()=>button};
  const root={isConnected:true,forms:[form],set innerHTML(_value){
    this.forms=records.map(item=>({dataset:{evaluation:item.evaluation_key},closest:()=>({open:false}),
      replaceWith:node=>{this.forms=[node];}}));
  },querySelectorAll(){return this.forms;},querySelector(){return null;},insertAdjacentHTML(){},append(node){this.forms.push(node);}};
  const ctx={legacyOriginEpoch:0,legacyOriginTask:null,reservationTask:null,el:()=>root,escape:String,
    document:{addEventListener(){}},fetch:async()=>({ok:true,json:async()=>({records})})};
  vm.runInNewContext(source.slice(start,end),ctx);
  assert.equal(await ctx.refreshLegacyOrigins(),true);
  assert.equal(button.disabled,true);assert.equal(form.dataset.originUnavailable,'true');assert.equal(form.value,'未提交说明');
  records=[{status:'available',evaluation_key:'legacy-1',custom_input:{label:'旧记录',payouts:{}},declarations:[]}];
  assert.equal(await ctx.refreshLegacyOrigins(),true);
  assert.equal(root.forms[0],form);assert.equal(form.dataset.originUnavailable,undefined);assert.equal(button.disabled,false);
});

test('an unavailable origin cannot submit, including by a programmatic call',async()=>{
  const {ctx,form,status}=setup();let called=false;
  form.dataset.originUnavailable='true';ctx.backendPost=async()=>{called=true;};
  await ctx.submitLegacyOrigin(form);
  assert.equal(called,false);assert.match(status.textContent,/暂不可核验/);
});

test('new edits made while a declaration is being saved stay dirty',async()=>{
  const {ctx,form,data,button}=setup();let release,entered;
  const waiting=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  form.dataset.dirty='true';ctx.backendPost=async()=>{entered();await waiting;};
  const pending=ctx.submitLegacyOrigin(form);await reached;
  assert.equal(button.disabled,true);data.reason='保存期间继续填写';release();await pending;
  assert.equal(data.reason,'保存期间继续填写');assert.equal(form.dataset.dirty,'true');assert.equal(button.disabled,false);
});

test('disconnect invalidates an in-flight holdout audit before it can publish',async()=>{
  let release,entered;const waiting=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  const result={innerHTML:''},button={disabled:false,isConnected:true},form={isConnected:true,querySelector:()=>button};
  const ctx={el:id=>id==='holdout-audit-result'?result:null,document:{addEventListener(){}},
    FormData:class{get(){return '26122';}},backendPost:async()=>{entered();await waiting;return {audit:{status:'no_recorded_use'}};}};
  const auditStart=source.indexOf('let holdoutAuditSequence='),auditEnd=source.indexOf('async function refreshReservations(',auditStart);
  vm.runInNewContext(source.slice(auditStart,auditEnd),ctx);
  const pending=ctx.submitHoldoutAudit(form);await reached;ctx.invalidateHoldoutAudit();release();await pending;
  assert.match(result.innerHTML,/原范围核对已失效/);assert.doesNotMatch(result.innerHTML,/固定为探索排除范围/);
});
