// DOM-free rendering and async state checks; this is not browser validation.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const part = source.slice(source.indexOf('function overviewDate('), source.indexOf('function calcKey('));
assert.ok(part.startsWith('function overviewDate('));

function makeContext() {
  const root = {innerHTML: ''};
  const context = {
    overviewState: {data: null, error: '', loading: false, generation: 0, rendered: ''},
    state: {page: 'overview'}, root,
    escape: value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    tag: label => `<span>${label}</span>`,
    fact: (label, value) => `<b>${label}: ${value}</b>`, go: label => `<a>${label}</a>`,
    heading: () => '', button: () => '', el: () => root,
    fetch: () => {throw new Error('unexpected request');},
  };
  vm.createContext(context);
  vm.runInContext(part, context);
  return context;
}

function input() {
  return {
    snapshots: [{id: 'a'.repeat(64), first_issue: '26001', last_issue: '26020', row_count: 20}],
    detail: {snapshot_id: 'a'.repeat(64), observation: {source:'本机来源 <test>', observed_at:'2026-01-01T00:00:00Z'},
      draws:[{issue:'26020',conflict:true}]},
    jobs: [{name:'树模型 <x>',status:'complete',artifact_status:'ready',artifact_id:'model-1',created_at:'2026-01-01T00:00:00Z'},
      {name:'失败任务',status:'failed',created_at:'2026-01-02T00:00:00Z'}],
    plans: [{enabled:true,runs:[{status:'success',ended_at:'2026-01-01T00:00:00Z'},
      {status:'failed',ended_at:'2026-01-03T00:00:00Z'}]}],worker:{state:'running'},
    experiments: [{request_key:'late',target_issue:'26019',target_date:'2026-02-02',hypothesis:'后期',replicas:3,status:'locally_attested',paper_status:'waiting'},
      {request_key:'early',target_issue:'26018',target_date:'2026-02-01',hypothesis:'早期',replicas:2,status:'locally_attested',paper_status:'waiting'},
      {request_key:'complete1',target_issue:'26017',target_date:'2026-01-31',hypothesis:'完成',replicas:2,status:'locally_attested',paper_status:'locally_attested'},
      {request_key:'complete2',target_issue:'26017',target_date:'2026-01-31',hypothesis:'完成2',replicas:5,status:'closed',paper_status:'locally_attested'},
      {request_key:'invalid',target_issue:'26016',target_date:null,hypothesis:'异常',replicas:1,status:'review_required',paper_status:'review_required'}],
    series:[{content:{manifest:{observation_issues:['26018','26019']}},periods:[{issue:'26018'}]},
      {content:{manifest:{observation_issues:['26020','26021']}},periods:[],closure:{reason:'已结束'}}],
  };
}

test('renders verified record states, calendar ordering, last success and distinct issues', () => {
  const c=makeContext(), html=c.overviewContent(input());
  assert.match(html,/最近创建快照: 第 26020 期/);
  assert.match(html,/可用模型产物: 1/);
  assert.match(html,/本人留存已计算奖期: 1/);
  assert.ok(html.indexOf('早期') < html.indexOf('后期'));
  assert.ok(html.indexOf('后期') < html.indexOf('异常'));
  assert.match(html,/最近结束：失败/);
  assert.match(html,/最近成功：2026/);
  assert.match(html,/来源冲突/);
  assert.match(html,/本机来源 &lt;test&gt;/);
  assert.match(html,/树模型 &lt;x&gt;/);
  assert.match(html,/尚未登记 1 期/);
  assert.doesNotMatch(html,/500 个|30 个有效奖期|尚未实际训练/);
});

test('empty and unavailable states do not invent observations or models', () => {
  const c=makeContext(),d=input();
  d.snapshots=[];d.detail=null;d.jobs=[];d.plans=[];d.experiments=[];d.series=[];
  const html=c.overviewContent(d);
  assert.match(html,/尚未载入本机数据快照/);
  assert.match(html,/尚无本机训练任务/);
  assert.match(html,/暂无等待处理的本机登记记录/);
  assert.doesNotMatch(html,/第 26124 期/);
  c.overviewState.data=d;c.invalidateOverview('无法连接');
  assert.equal(c.overviewState.data,null);
  assert.match(c.root.innerHTML,/无法连接/);
  assert.doesNotMatch(c.root.innerHTML,/26020/);
});

test('failed refresh removes stale results and later refresh can recover', async () => {
  const c=makeContext(),d=input();c.overviewState.data=d;
  c.fetch=async () => ({ok:false});
  await c.refreshOverview(d.snapshots,d.jobs);
  assert.equal(c.overviewState.data,null);
  assert.match(c.root.innerHTML,/暂时无法核验/);
  assert.doesNotMatch(c.root.innerHTML,/26020/);
  c.fetch=async url => ({ok:true,json:async()=>url.startsWith('/api/update-plans')?{plans:d.plans,worker:d.worker}:
    url.startsWith('/api/experiments')?{experiments:d.experiments}:url.startsWith('/api/experiment-series')?{series:d.series}:d.detail});
  await c.refreshOverview(d.snapshots,d.jobs);
  assert.match(c.root.innerHTML,/第 26020 期/);
});

test('late response from an earlier page generation is discarded', async () => {
  const c=makeContext(),d=input();let release;
  const response=new Promise(resolve=>{release=resolve;});
  c.fetch=()=>response;
  const pending=c.refreshOverview(d.snapshots,d.jobs);
  c.invalidateOverview('已离开');
  release({ok:false});
  await pending;
  assert.equal(c.overviewState.data,null);
  assert.match(c.root.innerHTML,/已离开/);
});
