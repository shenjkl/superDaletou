// In-memory storage and service doubles; browser IndexedDB and HTTP remain untested here.
const test=require('node:test');
const assert=require('node:assert/strict');
const {browserStore,coordinator}=require('../prototype/research-write-journal.js');

const keys=['00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-000000000002',
  '00000000-0000-4000-8000-000000000003'];
const payload={kind:'series',key:'series-1',choice:'continue',reason:'继续观察',limitations:'不足',evidence_sha256:'e'.repeat(64)};
const clone=x=>structuredClone(x);
function memoryStore(){
  const data=new Map();return {data,
    async get(slot){return clone(data.get(slot)||{});},async list(){return [...data.values()].map(x=>clone(x.entry));},
    async create(entry,content){if(data.has(entry.slot))throw Error('already exists');data.set(entry.slot,clone({entry,payload:content}));},
    async replace(entry,content){data.set(entry.slot,clone({entry,payload:content}));},
    async update(entry){const item=data.get(entry.slot);if(!item)throw Error('missing');item.entry=clone(entry);},
    async remove(slot){data.delete(slot);}};
}
function fakeLocks(){const busy=new Set();return {request:async(name,_options,callback)=>{
  if(busy.has(name))return callback(null);busy.add(name);try{return await callback({name});}finally{busy.delete(name);}
}};}
function server(){
  const intents=new Map(),records=new Map(),fences=new Set(),slots=new Map(),acks=new Map(),calls=[];let mode='success';
  const operationKey=x=>x.operation+':'+x.request_key;
  const objectKey=(operation,kind,key)=>JSON.stringify([operation,kind,key]);
  return {intents,records,fences,slots,acks,calls,set mode(value){mode=value;},
    async post(path,body){calls.push([path,clone(body)]);
      if(path.endsWith('/prepare')){
        const id=operationKey(body),prior=intents.get(id),obj=body.operation==='decision'?[body.request.kind,body.request.key]:['decision',body.request.decision_key],slot=objectKey(body.operation,obj[0],obj[1]);
        const occupied=slots.get(slot);
        if(!prior&&occupied&&body.retry_of!==occupied.request_key){const error=Error('object is occupied');error.code='object_occupied';
          error.write={status:'unknown',request_key:occupied.request_key};throw error;}
        const digest='a'.repeat(64),intent={operation:body.operation,request_key:body.request_key,object_kind:obj[0],object_key:obj[1],
          request_sha256:digest,retry_of:body.retry_of??null,status:records.has(id)?'committed':fences.has(id)?'fenced':'unknown'};
        if(prior&&JSON.stringify(prior.request)!==JSON.stringify(body.request))throw Error('conflicting input');
        if(body.retry_of&&!fences.has(body.operation+':'+body.retry_of))throw Error('old not fenced');
        if(!prior){const version=occupied?occupied.slot_version+1:1;slots.set(slot,{request_key:body.request_key,slot_version:version});intent.slot_version=version;}
        else intent.slot_version=occupied?.request_key===body.request_key?occupied.slot_version:prior.intent.slot_version;
        intents.set(id,{request:clone(body.request),intent});
        if(mode==='prepare-lost'){mode='success';throw Error('prepare response lost');}return {intent};
      }
      if(path.endsWith('/fence')){const id=operationKey(body),intent=intents.get(id);if(!intent||!body.require_intent)throw Error('missing intent');
        if(records.has(id))return {write:{...body,status:'committed',record:records.get(id),slot_version:intents.get(id).intent.slot_version}};
        fences.add(id);return {write:{...body,status:'fenced',slot_version:intents.get(id).intent.slot_version}};}
      if(path.endsWith('/acknowledge')){const id=operationKey(body),prior=acks.get(id),intent=intents.get(id);if(!intent)throw Error('missing matching intent');
        const record=records.get(id),status=record?'committed':fences.has(id)?'fenced':'unknown';if(prior)return {acknowledgment:prior};
        const slot=slots.get(objectKey(intent.intent.operation,intent.intent.object_kind,intent.intent.object_key));
        if(status==='unknown'||slot?.request_key!==body.request_key||slot.slot_version!==body.slot_version)throw Error('stale slot');
        const ack={status:'acknowledged',operation:body.operation,request_key:body.request_key,request_sha256:body.request_sha256,
          result_status:status,slot_version:body.slot_version,acknowledged_at:'now'};acks.set(id,ack);slots.delete(objectKey(intent.intent.operation,intent.intent.object_kind,intent.intent.object_key));
        return {acknowledgment:ack};}
      const operation=path.endsWith('decisions')?'decision':'draft',id=operation+':'+body.request_key;
      if(!intents.has(id)||!body.require_intent||fences.has(id))throw Error('not permitted');
      if(mode==='send-lost-before')throw Error('network before commit');
      const record={request_key:body.request_key};records.set(id,record);
      if(mode==='send-lost-after')throw Error('response lost after commit');
      return operation==='decision'?{decision:record}:{draft:record};
    },
    async status(query){calls.push(['status',clone(query)]);const id=operationKey(query),intent=intents.get(id);
      if(!intent||query.require_intent!=='1')throw Error('missing matching intent');
      return {write:{...query,status:records.has(id)?'committed':fences.has(id)?'fenced':'unknown',
        slot_version:slots.get(objectKey(intent.intent.operation,intent.intent.object_kind,intent.intent.object_key))?.slot_version??null,
        ...(records.has(id)?{record:records.get(id)}:{})}};},
    async current(query){const key=objectKey(query.operation,query.object_kind,query.object_key),slot=slots.get(key);if(!slot)return {status:'none'};
      const id=query.operation+':'+slot.request_key,intent=intents.get(id)?.intent;return {...intent,status:records.has(id)?'committed':fences.has(id)?'fenced':'unknown',slot_version:slot.slot_version};}
  };
}
function setup(){const store=memoryStore(),locks=fakeLocks(),api=server();let i=1;
  const create=()=>coordinator({store,locks,api,uuid:()=>keys[i++]});return {store,locks,api,create,journal:create()};}

test('writes journal before sending and retains data until confirmed cleanup',async()=>{
  const {journal,store,api}=setup();const result=await journal.begin('decision',payload,keys[0]);
  assert.equal(result.phase,'committed');assert.equal((await store.get(result.slot)).payload.reason,'继续观察');
  assert.deepEqual(api.calls.map(x=>x[0]),['/api/research-writes/prepare','/api/research-decisions']);
  await journal.dismiss(result.slot);assert.equal((await store.list()).length,0);assert.equal(api.slots.size,0);
});

test('storage or cross-tab lock failure prevents any network write',async()=>{
  const {journal,store,api}=setup();store.create=async()=>{throw Error('disk full');};
  await assert.rejects(journal.begin('decision',payload,keys[0]),/disk full/);assert.equal(api.calls.length,0);
  const unprotected=coordinator({store:memoryStore(),locks:null,api,uuid:()=>keys[1]});
  await assert.rejects(unprotected.begin('decision',payload,keys[0]),/跨页面写入保护/);assert.equal(api.calls.length,0);
});

test('a second browser is told another request owns the object and can only discard its unsubmitted copy',async()=>{
  const {journal,api}=setup();await journal.begin('decision',payload,keys[0]);
  const other=coordinator({store:memoryStore(),locks:fakeLocks(),api,uuid:()=>keys[1]});
  const blocked=await other.begin('decision',{...payload,reason:'另一浏览器输入'},keys[1]);
  assert.equal(blocked.phase,'blocked');assert.equal(blocked.conflict.request_key,keys[0]);
  assert.equal(api.calls.filter(x=>x[0]==='/api/research-decisions').length,1);
  assert.equal((await other.list()).length,1);
  await other.discardBlocked(blocked.slot);assert.equal((await other.list()).length,0);
  assert.equal(api.slots.values().next().value.request_key,keys[0]);
});

test('lost commit response remains pending across coordinator recreation',async()=>{
  const {journal,create,store,api}=setup();api.mode='send-lost-after';
  await assert.rejects(journal.begin('decision',payload,keys[0]),/response lost/);
  const [pending]=await store.list();assert.equal(pending.phase,'submitted');
  const recovered=await create().check(pending.slot);assert.equal(recovered.phase,'committed');
  await create().dismiss(pending.slot);assert.equal((await store.list()).length,0);
});

test('unknown request requires fencing before a linked retry',async()=>{
  const {journal,store,api}=setup();api.mode='send-lost-before';
  await assert.rejects(journal.begin('decision',payload,keys[0]),/before commit/);
  const [pending]=await store.list();assert.equal((await journal.check(pending.slot)).phase,'submitted');
  await assert.rejects(journal.dismiss(pending.slot),/尚未确认/);
  assert.equal((await journal.fence(pending.slot)).phase,'fenced');
  api.mode='success';const retry=await journal.retry(pending.slot);
  assert.equal(retry.phase,'committed');assert.equal(retry.retry_of,keys[0]);assert.equal(retry.request_key,keys[1]);
  const retryPrepare=api.calls.filter(x=>x[0].endsWith('/prepare')).at(-1)[1];assert.equal(retryPrepare.retry_of,keys[0]);
});

test('lost prepare response can resume without automatically sending the write',async()=>{
  const {journal,store,api}=setup();api.mode='prepare-lost';
  await assert.rejects(journal.begin('decision',payload,keys[0]),/prepare response lost/);
  const [pending]=await store.list();assert.equal(pending.phase,'preparing');
  assert.equal((await journal.check(pending.slot)).phase,'prepared');
  assert.equal(api.calls.filter(x=>x[0]==='/api/research-decisions').length,0);
  assert.equal((await journal.continuePrepared(pending.slot)).phase,'committed');
});

test('wrong database cannot release or fence a pending submitted operation',async()=>{
  const {journal,store,api,locks}=setup();api.mode='send-lost-before';
  await assert.rejects(journal.begin('decision',payload,keys[0]));const [pending]=await store.list();
  const wrong=coordinator({store,locks,api:server(),uuid:()=>keys[1]});
  await assert.rejects(wrong.check(pending.slot),/missing matching intent/);
  await assert.rejects(wrong.fence(pending.slot),/missing intent/);
  assert.equal((await store.list())[0].phase,'submitted');
});

test('a failed journal transition prevents sending the business operation',async()=>{
  const {journal,store,api}=setup(),update=store.update;
  store.update=async entry=>{if(entry.phase==='submitted')throw Error('storage failure');return update(entry);};
  await assert.rejects(journal.begin('decision',payload,keys[0]),/storage failure/);
  assert.equal(api.calls.filter(x=>x[0]==='/api/research-decisions').length,0);
  assert.equal((await store.list())[0].phase,'prepared');
});

test('another tab cannot submit or overwrite the same target during an in-flight request',async()=>{
  const {journal,create,store,api}=setup();let release,entered;
  const wait=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;}),post=api.post.bind(api);
  api.post=async(path,body)=>{if(path.endsWith('/prepare')){entered();await wait;}return post(path,body);};
  const first=journal.begin('decision',payload,keys[0]);await reached;
  await assert.rejects(create().begin('decision',{...payload,reason:'另一个页面'},keys[1]),/其他页面处理/);
  release();assert.equal((await first).phase,'committed');assert.equal((await store.list()).length,1);
  assert.equal(api.calls.filter(x=>x[0]==='/api/research-decisions').length,1);
});

test('a damaged local journal cannot be submitted or silently cleared',async()=>{
  const {journal,store,api}=setup();api.mode='send-lost-before';
  await assert.rejects(journal.begin('decision',payload,keys[0]));const [entry]=await store.list();
  store.data.get(entry.slot).payload.key='different-target';
  const count=api.calls.length;
  await assert.rejects(journal.check(entry.slot),/无法核验/);
  await assert.rejects(journal.dismiss(entry.slot),/无法核验/);
  assert.equal(api.calls.length,count);assert.equal((await store.list()).length,1);
});

test('a previously resolved state is checked against the current database again',async()=>{
  const {journal,store,locks}=setup();const entry=await journal.begin('decision',payload,keys[0]);
  const wrong=coordinator({store,locks,api:server(),uuid:()=>keys[1]});
  await assert.rejects(wrong.check(entry.slot),/missing matching intent/);
  await assert.rejects(wrong.dismiss(entry.slot),/missing matching intent/);
  assert.equal((await store.list())[0].phase,'committed');
});

test('browser storage refuses a write when strict durability is unavailable',async()=>{
  let writes=0;const db={transaction(){return {durability:'relaxed',abort(){},objectStore(){return {add(){writes++;}};}};},close(){}};
  const indexedDB={open(){const request={result:db};queueMicrotask(()=>request.onsuccess());return request;}};
  const store=browserStore(indexedDB);
  await assert.rejects(store.create({slot:'x'},payload),/可靠的本机持久写入/);
  assert.equal(writes,0);
});

test('later changes to the caller input cannot alter the submitted snapshot',async()=>{
  const {journal,store,api}=setup(),originalCreate=store.create;let release,entered;
  const wait=new Promise(resolve=>{release=resolve;}),reached=new Promise(resolve=>{entered=resolve;});
  store.create=async(entry,content)=>{entered();await wait;return originalCreate(entry,content);};
  const changing={...payload},pending=journal.begin('decision',changing,keys[0]);await reached;
  changing.reason='后来改的内容';release();const result=await pending;
  assert.equal((await store.get(result.slot)).payload.reason,'继续观察');
  assert.equal(api.calls[0][1].request.reason,'继续观察');
});

test('failure to persist the success state leaves the original request queryable',async()=>{
  const {journal,store,api}=setup(),update=store.update;let fail=true;
  store.update=async entry=>{if(entry.phase==='committed'&&fail){fail=false;throw Error('commit state not persisted');}return update(entry);};
  await assert.rejects(journal.begin('decision',payload,keys[0]),/commit state not persisted/);
  const [entry]=await store.list();assert.equal(entry.phase,'submitted');assert.equal(api.records.size,1);
  assert.equal((await journal.check(entry.slot)).phase,'committed');
});

test('a competing commit wins over a requested fence and is not reported as fenced',async()=>{
  const {journal,store,api}=setup();api.mode='send-lost-before';
  await assert.rejects(journal.begin('decision',payload,keys[0]));const [entry]=await store.list();
  api.records.set('decision:'+keys[0],{request_key:keys[0]});
  assert.equal((await journal.fence(entry.slot)).phase,'committed');
  assert.equal(api.fences.size,0);
});

test('a mismatched preparation response is rejected before the business request',async()=>{
  const {journal,store,api}=setup(),post=api.post.bind(api);
  api.post=async(path,body)=>{const result=await post(path,body);if(path.endsWith('/prepare'))result.intent.object_key='different';return result;};
  await assert.rejects(journal.begin('decision',payload,keys[0]),/准备记录无法核验/);
  assert.equal((await store.list())[0].phase,'preparing');
  assert.equal(api.calls.filter(x=>x[0]==='/api/research-decisions').length,0);
});
