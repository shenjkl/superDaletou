const test=require('node:test');
const assert=require('node:assert/strict');
const {coordinator}=require('../prototype/fixed-experiment-journal.js');

const digest='a'.repeat(64);
const key1='11111111-1111-4111-8111-111111111111';
const key2='22222222-2222-4222-8222-222222222222';
const protocol={target_issue:'26124',hypothesis:'synthetic only',replicas:[{replica:'a',pair_key:'pair-a'}]};
function fixture({loseCommitResponse=false,failBeforeCommit=false,failStore=false,hasLocks=true}={}){
  const entries=new Map(),states=new Map(),calls=[];
  const store={
    async add(entry){if(failStore)throw Error('storage unavailable');if(entries.has(entry.request_key))throw Error('duplicate');entries.set(entry.request_key,{...entry});},
    async put(entry){entries.set(entry.request_key,{...entry});},
    async get(key){const value=entries.get(key);return value&&{...value};},
    async list(){return [...entries.values()].map(value=>({...value}));},
    async remove(key){entries.delete(key);}
  };
  const api={
    async post(path,body){calls.push({path,body,phase:entries.get(body.request_key)?.phase});
      if(path.endsWith('/prepare')){
        const prior=states.get(body.request_key);
        if(body.retry_of&&states.get(body.retry_of)?.status!=='fenced')throw Error('retry must be fenced');
        if(prior&&prior.digest!==digest)throw Error('digest conflict');
        states.set(body.request_key,{digest,status:prior?.status||'unknown'});
        return {intent:{request_key:body.request_key,request_sha256:digest,status:states.get(body.request_key).status}};
      }
      if(path==='/api/experiments/fence'){
        const state=states.get(body.request_key);if(!state||state.digest!==body.request_sha256)throw Error('unprepared');
        if(state.status==='unknown')state.status='fenced';
        return {write:{request_key:body.request_key,request_sha256:digest,status:state.status}};
      }
      if(path==='/api/experiments'||path==='/api/experiment-series/periods'){
        if(failBeforeCommit){failBeforeCommit=false;throw Error('connection lost before commit');}
        const state=states.get(body.request_key);if(!state||state.status==='fenced')throw Error('fenced');
        state.status='committed';
        const result={write:{request_key:body.request_key,request_sha256:digest,status:'committed'}};
        if(loseCommitResponse){loseCommitResponse=false;throw Error('connection lost');}
        return result;
      }
      throw Error('unexpected route '+path);
    },
    async status(key,hash){const state=states.get(key);return {request_key:key,request_sha256:hash,status:state?.status||'unknown'};}
  };
  const locks=hasLocks?{async request(_name,_options,work){return work({});}}:null;
  let next=0;const ids=[key1,key2];
  return {journal:coordinator({store,locks,api,uuid:()=>ids[next++]}),entries,states,calls};
}

test('persists submitted phase before calling the business write',async()=>{
  const f=fixture();const result=await f.journal.begin(protocol);
  assert.equal(result.phase,'committed');
  const send=f.calls.find(call=>call.path==='/api/experiments');
  assert.equal(send.phase,'submitted');
  assert.equal(send.body.require_intent,true);
});

test('series period uses the same durable journal and request key',async()=>{
  const f=fixture();
  const result=await f.journal.begin(protocol,{requestKey:key1,seriesKey:'series-alpha'});
  assert.equal(result.phase,'committed');
  const preparation=f.calls.find(call=>call.path.endsWith('/prepare'));
  const send=f.calls.find(call=>call.path==='/api/experiment-series/periods');
  assert.equal(preparation.body.series_key,'series-alpha');
  assert.equal(send.body.series_key,'series-alpha');
  assert.equal(send.body.request_key,key1);
  assert.equal(send.body.require_intent,true);
});

test('lost response is resolved by original key without resending',async()=>{
  const f=fixture({loseCommitResponse:true});
  await assert.rejects(f.journal.begin(protocol),/connection lost/);
  const before=f.calls.filter(call=>call.path==='/api/experiments').length;
  const result=await f.journal.check(key1);
  assert.equal(result.phase,'committed');
  assert.equal(f.calls.filter(call=>call.path==='/api/experiments').length,before);
});

test('unknown request is fenced before a linked retry can be sent',async()=>{
  const f=fixture({failBeforeCommit:true});
  await assert.rejects(f.journal.begin(protocol),/connection lost before commit/);
  assert.equal((await f.journal.check(key1)).phase,'submitted');
  assert.equal((await f.journal.fence(key1)).phase,'fenced');
  const retry=await f.journal.retry(key1);
  assert.equal(retry.phase,'committed');
  assert.equal(retry.retry_of,key1);
  const retryPrepare=f.calls.findIndex(call=>call.path.endsWith('/prepare')&&call.body.request_key===key2);
  const retrySend=f.calls.findIndex(call=>call.path==='/api/experiments'&&call.body.request_key===key2);
  assert.ok(retryPrepare>f.calls.findIndex(call=>call.path==='/api/experiments/fence'));
  assert.ok(retrySend>retryPrepare);
});

test('storage failure and missing Web Locks prevent any request',async()=>{
  const noStorage=fixture({failStore:true});
  await assert.rejects(noStorage.journal.begin(protocol),/storage unavailable/);
  assert.equal(noStorage.calls.length,0);
  const noLock=fixture({hasLocks:false});
  await assert.rejects(noLock.journal.begin(protocol),/跨页面写入保护/);
  assert.equal(noLock.calls.length,0);
});

test('confirmed result can be cleared only after terminal status',async()=>{
  const f=fixture();await f.journal.begin(protocol);
  await f.journal.dismiss(key1);
  assert.equal(f.entries.has(key1),false);
  assert.equal(f.states.get(key1).status,'committed');
});
