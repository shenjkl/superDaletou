// Mocked DOM and async flow; not a real browser test.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const begin = source.indexOf('function readLiveEvaluation()');
const end = source.indexOf('\nfunction liveHistory()', begin);
assert.ok(begin >= 0 && end > begin);

function setup() {
  const values = {scenario:'custom', custom_label:'假设 A', custom_origin:'synthetic', samples:'1000', seed:'7'};
  for (const key of ['101','201','301','401','501','601','701','801','901']) values['payout_'+key] = '12.00';
  const submit = {disabled:false, isConnected:true, textContent:''};
  const fields = {hidden:true, disabled:true};
  const result = {innerHTML:'', scrollIntoView(){}};
  const form = {dataset:{}, querySelector(selector) {
    if (selector === '[name=scenario]') return {value:values.scenario};
    if (selector === '[name=custom_origin]') return {value:values.custom_origin};
    if (selector.startsWith('button')) return submit;
    return null;
  }};
  const nodes = {'live-evaluation-form':form, 'custom-payout-fields':fields, 'live-eval-result':result};
  const context = {backend:{liveResult:{request_key:'pair'},liveEval:null,liveEvalHistory:[],evalHistoryEpoch:0,liveDirty:false},
    state:{page:'generation'}, el:id=>nodes[id]||null, customPayoutFields:Object.keys(values).filter(k=>k.startsWith('payout_')).map(k=>[k.slice(7),k]),
    FormData:class {get(key){return values[key]??null;}}, document:{addEventListener(){}},
    crypto:{randomUUID:()=> 'request'}, evaluationSources:()=>[], refreshEvaluationHistory(){}, canonicalScenarioMoney:value=>String(value),
    customHitFields:Array.from({length:18},(_,i)=>[`${Math.floor(i/3)},${i%3}`,`前区 ${Math.floor(i/3)} + 后区 ${i%3}`]),
    liveEvaluationResult:()=> 'result', toast(){}};
  vm.runInNewContext(source.slice(begin,end), context);
  return {context, values, submit, fields, result};
}

test('custom inputs are enabled, collected, and cannot resubmit while a calculation is running', async () => {
  const {context:c, values, submit, fields, result} = setup();
  c.syncLiveEvaluation();
  assert.equal(fields.hidden, false); assert.equal(fields.disabled, false);
  assert.equal(c.backend.liveEvalCfg.custom.payouts['101'], '12.00');
  let resolve, sent;
  c.backendPost = (_url, payload) => {sent=payload;return new Promise(done=>{resolve=done;});};
  const pending = c.evaluateLive();
  assert.equal(sent.scenario_kind, 'custom_payout_v1');
  assert.equal(sent.custom_input.label, '假设 A');
  assert.equal(sent.custom_input.provenance.kind, 'synthetic');
  assert.equal(submit.disabled, true);
  values.payout_101 = '15.00';
  c.syncLiveEvaluation();
  assert.equal(submit.disabled, true);
  const beforeResponse = result.innerHTML;
  resolve({request_key:'request'});
  await pending;
  assert.equal(submit.disabled, false);
  assert.equal(result.innerHTML, beforeResponse);
  assert.equal(c.backend.liveEval, null);
  assert.equal(c.backend.liveEvalHistory.length, 1);
});

test('switching away disables the required custom controls and retains their in-page input', () => {
  const {context:c, values, fields} = setup();
  c.syncLiveEvaluation();
  values.scenario='a'.repeat(64)+'|26123';
  c.syncLiveEvaluation();
  assert.equal(fields.hidden, true); assert.equal(fields.disabled, true);
  assert.equal(c.backend.liveEvalCfg.issue, '26123');
  values.scenario='custom'; c.syncLiveEvaluation();
  assert.equal(c.backend.liveEvalCfg.custom.label, '假设 A');
  assert.equal(c.backend.liveEvalCfg.custom.payouts['101'], '12.00');
});

test('a declared historical source issue is carried in the evaluation request', () => {
  const {context:c, values} = setup();
  values.custom_origin = 'historical'; values.custom_origin_issue = '26122';
  c.syncLiveEvaluation();
  assert.equal(c.backend.liveEvalCfg.custom.provenance.kind, 'historical');
  assert.equal(c.backend.liveEvalCfg.custom.provenance.issue, '26122');
});
