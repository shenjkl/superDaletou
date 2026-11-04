/* Durable client journal for the two research-decision writes. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.ResearchWriteJournal=api;
})(globalThis,function(){
  'use strict';
  const phases=new Set(['preparing','blocked','prepared','submitted','committed','fenced']);
  const fields={decision:['kind','key','choice','reason','limitations','evidence_sha256'],draft:['decision_key','hypothesis']};
  const shaPattern=/^[0-9a-f]{64}$/;
  const uuidPattern=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  function target(operation,payload){
    if(!fields[operation]||!payload||typeof payload!=='object'||Array.isArray(payload)||
       Object.keys(payload).sort().join('|')!==fields[operation].slice().sort().join('|')||
       Object.values(payload).some(x=>typeof x!=='string'||x.length>4096))throw new Error('待确认请求内容无效');
    if(operation==='decision'&&!['series','fixed_evaluation'].includes(payload.kind))throw new Error('证据类型无效');
    const kind=operation==='decision'?payload.kind:'decision',key=operation==='decision'?payload.key:payload.decision_key;
    if(!key||key.length>128)throw new Error('待确认对象无效');
    return {kind,key,slot:JSON.stringify([operation,kind,key])};
  }
  function valid(entry,payload){
    if(!entry||entry.version!==1||!phases.has(entry.phase)||!uuidPattern.test(entry.request_key||'')||
       (entry.retry_of!==null&&!uuidPattern.test(entry.retry_of||'')))
      throw new Error('本机待确认记录无法核验');
    const t=target(entry.operation,payload);
    if(entry.slot!==t.slot||entry.object_kind!==t.kind||entry.object_key!==t.key||
       (['preparing','blocked'].includes(entry.phase)?entry.request_sha256!==null:!shaPattern.test(entry.request_sha256||''))||
       (['preparing','blocked'].includes(entry.phase)?entry.slot_version!==null:
         (['prepared','submitted'].includes(entry.phase)?(!Number.isSafeInteger(entry.slot_version)||entry.slot_version<1):
           (entry.slot_version!==null&&(!Number.isSafeInteger(entry.slot_version)||entry.slot_version<1)))))
      throw new Error('本机待确认记录无法核验');
    return entry;
  }
  function browserStore(indexedDB){
    if(!indexedDB)throw new Error('浏览器本机存储不可用，未发送请求');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;
        const req=indexedDB.open('dlt-research-write-journal',1);
        req.onupgradeneeded=()=>{const db=req.result;db.createObjectStore('index',{keyPath:'slot'});db.createObjectStore('content',{keyPath:'slot'});};
        req.onsuccess=()=>{if(abandoned){req.result.close();return;}req.result.onversionchange=()=>{req.result.close();opening=null;};resolve(req.result);};
        req.onerror=()=>{opening=null;reject(req.error||new Error('本机存储打开失败'));};
        req.onblocked=()=>{abandoned=true;opening=null;reject(new Error('本机存储被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,fn){
      const db=await open();return new Promise((resolve,reject)=>{
        let result;const transaction=mode==='readwrite'
          ?db.transaction(['index','content'],mode,{durability:'strict'})
          :db.transaction(['index','content'],mode);
        transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('本机存储操作失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){
          transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，未发送请求'));return;
        }
        try{fn(transaction.objectStore('index'),transaction.objectStore('content'),value=>{result=value;});}
        catch(error){transaction.abort();reject(error);}
      });
    }
    return {
      async create(entry,payload){return tx('readwrite',(index,content)=>{index.add(entry);content.add({slot:entry.slot,payload});});},
      async replace(entry,payload){return tx('readwrite',(index,content)=>{index.put(entry);content.put({slot:entry.slot,payload});});},
      async update(entry){return tx('readwrite',index=>{index.put(entry);});},
      async remove(slot){return tx('readwrite',(index,content)=>{index.delete(slot);content.delete(slot);});},
      async get(slot){return tx('readonly',(index,content,done)=>{let entry,payload;const a=index.get(slot),b=content.get(slot);
        a.onsuccess=()=>{entry=a.result;};b.onsuccess=()=>{payload=b.result?.payload;};
        done(()=>({entry,payload}));}).then(result=>result());},
      async list(){return tx('readonly',(index,_content,done)=>{const request=index.getAll();done(()=>request.result);}).then(result=>result());}
    };
  }
  function coordinator({store,locks,api,uuid}){
    if(!store||!api||typeof uuid!=='function')throw new Error('待确认写入配置无效');
    async function locked(slot,work){
      if(!locks||typeof locks.request!=='function')throw new Error('浏览器不支持跨页面写入保护，未发送请求');
      return locks.request('dlt-research-write:'+slot,{ifAvailable:true},async lock=>{
        if(!lock)throw new Error('该记录正在其他页面处理，请稍后重试');return work();
      });
    }
    async function get(slot){const value=await store.get(slot);if(!value?.entry||!value.payload)throw new Error('本机待确认记录不完整');valid(value.entry,value.payload);return value;}
    function checkIntent(entry,intent){
      if(!intent||intent.operation!==entry.operation||intent.request_key!==entry.request_key||
         intent.object_kind!==entry.object_kind||intent.object_key!==entry.object_key||
         intent.retry_of!==entry.retry_of||!shaPattern.test(intent.request_sha256||'')||
          !['unknown','committed','fenced'].includes(intent.status)||
          !Number.isSafeInteger(intent.slot_version)||intent.slot_version<1)throw new Error('服务端准备记录无法核验');
    }
    function checkState(entry,state){
      if(!state||state.operation!==entry.operation||state.request_key!==entry.request_key||
         state.request_sha256!==entry.request_sha256||!['unknown','committed','fenced'].includes(state.status))
        throw new Error('服务端返回的写入状态无法核验');
      if(state.status==='committed'&&state.record?.request_key!==entry.request_key)throw new Error('已保存记录与请求不符');
    }
    async function prepare(entry,payload){
      let response;
      try{response=await api.post('/api/research-writes/prepare',{operation:entry.operation,request_key:entry.request_key,request:payload,retry_of:entry.retry_of});}
      catch(error){
        if(error?.code!=='object_occupied')throw error;
        entry={...entry,phase:'blocked',conflict:error.write||null};await store.update(entry);return entry;
      }
      const intent=response.intent;checkIntent(entry,intent);
      entry={...entry,request_sha256:intent.request_sha256,slot_version:intent.slot_version,
             phase:intent.status==='unknown'?'prepared':intent.status};
      await store.update(entry);return entry;
    }
    async function send(entry,payload){
      if(entry.phase!=='prepared')throw new Error('当前状态不能发送，请先核对');
      entry={...entry,phase:'submitted',submission_state:null};await store.update(entry);
      const url=entry.operation==='decision'?'/api/research-decisions':'/api/research-drafts';
      let response;try{response=await api.post(url,{...payload,request_key:entry.request_key,require_intent:true});}catch(error){if(error.notSent===true){entry={...entry,phase:'prepared',submission_state:'not_sent'};await store.update(entry);}throw error;}
      const record=entry.operation==='decision'?response.decision:response.draft;
      if(record?.request_key!==entry.request_key)throw new Error('返回记录无法核验，请查询原请求');
      entry={...entry,phase:'committed'};await store.update(entry);return entry;
    }
    async function readState(entry){
      const state=await api.status({operation:entry.operation,request_key:entry.request_key,request_sha256:entry.request_sha256,require_intent:'1'});
      checkState(entry,state.write);return state.write;
    }
    return {
      target,
      async begin(operation,payload,requestKey){
        const t=target(operation,payload);if(!uuidPattern.test(requestKey||''))throw new Error('请求标识无效，未发送请求');
        const snapshot=Object.fromEntries(fields[operation].map(key=>[key,payload[key]]));
        return locked(t.slot,async()=>{
          if((await store.get(t.slot))?.entry)throw new Error('该对象有待核对的请求，请先处理');
          let entry={version:1,slot:t.slot,operation,object_kind:t.kind,object_key:t.key,request_key:requestKey,
                     request_sha256:null,retry_of:null,slot_version:null,phase:'preparing'};
          await store.create(entry,snapshot);entry=await prepare(entry,snapshot);
          if(entry.phase==='prepared')entry=await send(entry,snapshot);return entry;
        });
      },
      async list(){const entries=await store.list();return Promise.all(entries.map(async entry=>{const item=await get(entry.slot);return item;}));},
      async discover(operation,objectKind,objectKey){
        if(typeof api.current!=='function')throw new Error('服务端不支持按对象查询待确认写入');
        if((operation==='decision'&&!['series','fixed_evaluation'].includes(objectKind))||
           (operation==='draft'&&objectKind!=='decision')||!['decision','draft'].includes(operation)||
           typeof objectKey!=='string'||!objectKey||objectKey.length>128)throw new Error('研究对象无效');
        return api.current({operation,object_kind:objectKind,object_key:objectKey});
      },
      async check(slot){return locked(slot,async()=>{
        let {entry,payload}=await get(slot);
        if(['preparing','blocked'].includes(entry.phase))return prepare(entry,payload);
        const result=await readState(entry);
        if(['committed','fenced'].includes(entry.phase)&&result.status!==entry.phase)
          throw new Error('原请求的状态无法重新确认，请检查当前数据库');
        if(result.status!=='unknown'){
          entry={...entry,phase:result.status,
                 slot_version:Number.isSafeInteger(result.slot_version)?result.slot_version:entry.slot_version,
                 ...(result.acknowledged?{acknowledged:true}:{})};
          await store.update(entry);
        }
        return entry;
      });},
      async continuePrepared(slot){return locked(slot,async()=>{let {entry,payload}=await get(slot);
        if(['preparing','blocked'].includes(entry.phase))entry=await prepare(entry,payload);
        if(entry.phase!=='prepared')return entry;return send(entry,payload);
      });},
      async fence(slot){return locked(slot,async()=>{let {entry}=await get(slot);
        if(!['prepared','submitted'].includes(entry.phase))throw new Error('当前状态不能封止');
        const result=await api.post('/api/research-writes/fence',{operation:entry.operation,request_key:entry.request_key,request_sha256:entry.request_sha256,require_intent:true});
        checkState(entry,result.write);if(result.write.status==='unknown')throw new Error('封止结果无法确认');
        entry={...entry,phase:result.write.status,
               slot_version:Number.isSafeInteger(result.write.slot_version)?result.write.slot_version:entry.slot_version};
        await store.update(entry);return entry;
      });},
      async retry(slot){return locked(slot,async()=>{let {entry,payload}=await get(slot);
        if(entry.phase!=='fenced')throw new Error('只有已封止的请求可以关联重试');
        const original=await readState(entry);if(original.status!=='fenced')throw new Error('原请求封止状态无法确认');
        const key=uuid();if(!uuidPattern.test(key)||key===entry.request_key)throw new Error('新请求标识无效');
        entry={...entry,request_key:key,retry_of:entry.request_key,request_sha256:null,slot_version:null,phase:'preparing'};
        await store.replace(entry,payload);entry=await prepare(entry,payload);
        if(entry.phase==='prepared')entry=await send(entry,payload);return entry;
      });},
      async dismiss(slot){return locked(slot,async()=>{const {entry}=await get(slot);
        if(!['committed','fenced'].includes(entry.phase))throw new Error('结果尚未确认，不能清除');
        const slotVersion=entry.slot_version||1;
        const result=await api.post('/api/research-writes/acknowledge',{
          operation:entry.operation,request_key:entry.request_key,request_sha256:entry.request_sha256,
          slot_version:slotVersion});
        if(result.acknowledgment?.request_key!==entry.request_key||
           result.acknowledgment?.request_sha256!==entry.request_sha256||
           result.acknowledgment?.slot_version!==slotVersion||
           result.acknowledgment?.result_status!==entry.phase)throw new Error('服务端确认结果无法核验');
        await store.remove(slot);return entry;
      });},
      async discardBlocked(slot){return locked(slot,async()=>{const {entry}=await get(slot);
        if(entry.phase!=='blocked'||!entry.conflict||typeof api.current!=='function')throw new Error('当前请求不是可安全取消的对象占用冲突');
        const current=await api.current({operation:entry.operation,object_kind:entry.object_kind,object_key:entry.object_key});
        const sameHolder=entry.conflict.request_key
          ?current.request_key===entry.conflict.request_key&&current.status!=='none'
          :current.status==='recovery_required'&&JSON.stringify((current.pending||[]).map(x=>x.request_key).sort())===
            JSON.stringify((entry.conflict.pending||[]).map(x=>x.request_key).sort());
        if(!sameHolder)throw new Error('对象占用已经变化，请先重新查询原请求');
        await store.remove(slot);return entry;
      });}
    };
  }
  return {browserStore,coordinator,target};
});
