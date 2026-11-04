// Mocked DOM and asynchronous responses; not a browser or HTTP test.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'..','prototype','app.js'),'utf8');
const start=source.indexOf('function researchStatus('),end=source.indexOf('function evidenceSeriesPanel()',start);
assert.ok(start>=0&&end>start);

function setup(sha='new-sha'){
  const handlers={},status={textContent:'',innerHTML:''};
  const details={isConnected:true,open:true,dataset:{kind:'series',key:'series-1'},querySelector(selector){
    if(selector==='.research-decision-content')return root;
    if(selector==='.research-decision-status')return status;
    if(selector==='form.research-decision-form')return root.forms.find(f=>f.kind==='decision');
    return null;
  }};
  function makeForm(kind,dataset,value=''){
    const form={kind,dataset,value,isConnected:true,matches:selector=>selector==='.research-decision-form'&&kind==='decision',
      closest:selector=>selector==='details'||selector==='.research-decisions'?details:null,
      querySelector:()=>({textContent:'',disabled:false,isConnected:true})};
    form.replaceWith=node=>{const i=root.forms.indexOf(form);root.forms[i]=node;};return form;
  }
  const old=makeForm('decision',{request:'old-request',sha:'old-sha',dirty:'true'},'用户输入');
  const root={forms:[old],html:'old',get innerHTML(){return this.html;},set innerHTML(value){
    this.html=value;this.forms=[makeForm('decision',{request:'new-request',sha})];
  },querySelector(selector){return selector==='form.research-decision-form'?this.forms.find(f=>f.kind==='decision'):null;},
    querySelectorAll(selector){return selector.includes('research-draft-form')&&selector.includes('research-decision-form')?this.forms:
      selector==='form.research-draft-form'?this.forms.filter(f=>f.kind==='draft'):[];},insertAdjacentHTML(){},append(form){this.forms.push(form);}};
  const ctx={connectionEpoch:0,backend:{ready:true},document:{body:{dataset:{serverStatus:'verified'}},addEventListener:(type,fn)=>{(handlers[type]||=[]).push(fn);}},crypto:{randomUUID:()=> 'new-id'},
    escape:String,decisionEvidenceSummary:()=>'<p>当前证据</p>',decisionRecords:()=>'',toast(){},
    fetch:async url=>({ok:true,json:async()=>url.includes('context')?{evidence:{limitations:[]},evidence_sha256:sha}:{decisions:[]}})};
  ctx.getResearchJournal=()=>({begin:async()=>{await ctx.backendPost();return {phase:'committed'};}});
  ctx.refreshResearchWrites=async()=>{};
  vm.runInNewContext(source.slice(start,end),ctx);
  return {ctx,details,root,status,old,handlers,makeForm};
}

test('changed evidence retains input and requires explicit acknowledgement',async()=>{
  const {ctx,details,root,status,old,handlers}=setup();await ctx.loadResearchDecision(details);
  assert.equal(root.forms[0],old);assert.equal(old.value,'用户输入');assert.equal(old.dataset.stale,'true');
  assert.equal(old.dataset.sha,'old-sha');assert.match(status.innerHTML,/证据已变化/);
  const button={closest:()=>details};handlers.click[0]({target:{closest:()=>button}});
  assert.equal(old.dataset.sha,'new-sha');assert.equal(old.dataset.stale,undefined);
  assert.match(status.textContent,/已确认使用当前证据/);
});

test('unchanged evidence preserves draft without demanding reconfirmation',async()=>{
  const {ctx,details,root,old}=setup('old-sha');await ctx.loadResearchDecision(details);
  assert.equal(root.forms[0],old);assert.equal(old.dataset.stale,undefined);assert.equal(details.dataset.needsReview,undefined);
});

test('an older evidence response cannot overwrite edits after a disconnect',async()=>{
  const {ctx,details,root,old}=setup();let release,entered;
  const wait=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  ctx.fetch=async url=>{if(url.includes('context')){entered();await wait;return {ok:true,json:async()=>({evidence:{limitations:[]},evidence_sha256:'new-sha'})};}
    return {ok:true,json:async()=>({decisions:[]})};};
  const pending=ctx.loadResearchDecision(details);await reached;old.value='断线后继续编辑';ctx.connectionEpoch++;release();await pending;
  assert.equal(root.forms[0],old);assert.equal(old.value,'断线后继续编辑');assert.equal(details.dataset.needsReview,'true');
});

test('failed evidence read leaves inputs and blocks saving',async()=>{
  const {ctx,details,root,old,status}=setup();ctx.fetch=async()=>{throw Error('offline');};
  await ctx.loadResearchDecision(details);
  assert.equal(root.forms[0],old);assert.equal(details.dataset.needsReview,'true');assert.match(status.textContent,/输入已保留/);
  assert.equal(ctx.researchFormAvailable(old,details),false);
});

test('current evidence cannot be acknowledged while the connection is unverified',async()=>{
  const {ctx,details,old,handlers}=setup();await ctx.loadResearchDecision(details);
  ctx.document.body.dataset.serverStatus='unverified';const button={closest:()=>details};
  handlers.click[0]({target:{closest:()=>button}});
  assert.equal(old.dataset.sha,'old-sha');assert.equal(old.dataset.stale,'true');
});

test('an unavailable prior decision keeps the unsaved new hypothesis but cannot submit',async()=>{
  const {ctx,details,root,makeForm}=setup();const draft=makeForm('draft',{decision:'missing-decision',dirty:'true'},'新的假设');
  root.forms.push(draft);await ctx.loadResearchDecision(details);
  assert.ok(root.forms.includes(draft));assert.equal(draft.value,'新的假设');assert.equal(draft.dataset.unavailable,'true');
  assert.equal(ctx.researchFormAvailable(draft,details),false);
});

test('edits made while a judgment is saving are retained with a new request key',async()=>{
  const {ctx,old}=setup();let release,entered,reloaded=false;
  const wait=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  old.values={choice:'continue',reason:'原理由',limitations:'限制'};
  ctx.FormData=class{constructor(form){this.form=form;}get(key){return this.form.values[key];}};
  ctx.backendPost=async()=>{entered();await wait;};ctx.loadResearchDecision=async()=>{reloaded=true;};
  const pending=ctx.submitResearchDecision(old);await reached;old.values.reason='保存期间修改';release();await pending;
  assert.equal(old.values.reason,'保存期间修改');assert.equal(old.dataset.dirty,'true');
  assert.equal(old.dataset.request,'new-id');assert.equal(reloaded,true);
});

test('panel refresh transplants an open editor and marks missing records unavailable',()=>{
  const helperStart=source.indexOf('function refreshEvidencePanel()'),helperEnd=source.indexOf('function rationalYuan(',helperStart);
  let attached=null,refreshed=0;
  const old={open:true,dataset:{kind:'series',key:'old'},querySelectorAll:()=>[],querySelector:()=>({textContent:''})};
  const replacement={dataset:{kind:'series',key:'old'},replaceWith:item=>{attached=item;}};
  let rendered=false;
  const root={set innerHTML(_value){rendered=true;},querySelectorAll(selector){
    if(selector==='details.research-decisions')return rendered?[replacement]:[old];return [];
  },insertAdjacentHTML(){},append(){}};
  const ctx={el:()=>root,liveEvidenceContent:()=>'',keepResearchForm:()=>true,researchStatus(){},
    queueMicrotask:callback=>callback(),loadResearchDecision:()=>{refreshed++;}};
  vm.runInNewContext(source.slice(helperStart,helperEnd),ctx);ctx.refreshEvidencePanel();
  assert.equal(attached,old);assert.equal(old.dataset.needsReview,'true');assert.equal(refreshed,1);
  rendered=false;attached=null;root.querySelectorAll=selector=>selector==='details.research-decisions'?(rendered?[]:[old]):[];
  root.append=item=>{attached=item;};ctx.refreshEvidencePanel();
  assert.equal(attached,old);assert.equal(old.dataset.unavailable,'true');assert.equal(refreshed,1);
});
