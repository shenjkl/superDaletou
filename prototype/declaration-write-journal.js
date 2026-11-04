/* Durable client journal for legacy custom-payout source declarations. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.DeclarationWriteJournal=api;
})(globalThis,function(){
  'use strict';
  const phases=new Set(['submitted','accepted','fenced','rejected']);
  const uuidPattern=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  const digestPattern=/^[0-9a-f]{64}$/;
  function stable(value){
    if(Array.isArray(value))return value.map(stable);
    if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])]));
    return value;
  }
  function equal(a,b){return JSON.stringify(stable(a))===JSON.stringify(stable(b));}
  async function digest(request,cryptoApi){
    if(!cryptoApi?.subtle||typeof TextEncoder!=='function')throw new Error('本机摘要能力不可用，未发送请求');
    const bytes=new TextEncoder().encode(JSON.stringify(stable(request)));
    return [...new Uint8Array(await cryptoApi.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join('');
  }
  function browserStore(indexedDB){
    if(!indexedDB)throw new Error('本机声明暂存不可用，未发送请求');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;const request=indexedDB.open('dlt-declaration-write-journal',1);
        request.onupgradeneeded=()=>request.result.createObjectStore('writes',{keyPath:'event_id'});
        request.onsuccess=()=>{if(abandoned){request.result.close();return;}request.result.onversionchange=()=>{request.result.close();opening=null;};resolve(request.result);};
        request.onerror=()=>{opening=null;reject(request.error||new Error('本机声明暂存打开失败'));};
        request.onblocked=()=>{abandoned=true;opening=null;reject(new Error('本机声明暂存被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,work){
      const db=await open();return new Promise((resolve,reject)=>{
        const transaction=mode==='readwrite'?db.transaction('writes',mode,{durability:'strict'}):db.transaction('writes',mode);
        let result;transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('本机声明暂存操作失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){
          transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，声明未提交'));return;
        }
        try{work(transaction.objectStore('writes'),value=>{result=value;});}
        catch(error){transaction.abort();reject(error);}
      });
    }
    return {
      add(entry){return tx('readwrite',store=>store.add(entry));},
      put(entry){return tx('readwrite',store=>store.put(entry));},
      remove(key){return tx('readwrite',store=>store.delete(key));},
      get(key){return tx('readonly',(store,done)=>{const request=store.get(key);request.onsuccess=()=>done(request.result||null);});},
      list(){return tx('readonly',(store,done)=>{const request=store.getAll();request.onsuccess=()=>done(request.result);});}
    };
  }
  function coordinator({store,locks,api,uuid,cryptoApi=globalThis.crypto}){
    if(!store||!api||typeof uuid!=='function')throw new Error('来源声明写入协调器配置无效');
    async function locked(key,work){
      if(!locks||typeof locks.request!=='function')throw new Error('浏览器不支持跨页面声明写入保护，未发送请求');
      return locks.request('dlt-declaration-write:'+key,{ifAvailable:true},async lock=>{
        if(!lock)throw new Error('该来源声明正在其他页面核对，请稍后重试');return work();
      });
    }
    function valid(entry){
      if(!entry||entry.version!==1||!uuidPattern.test(entry.event_id||'')||
         (entry.retry_of!==null&&!uuidPattern.test(entry.retry_of||''))||!phases.has(entry.phase)||
         !digestPattern.test(entry.request_sha256||'')||!entry.request||typeof entry.request!=='object'||Array.isArray(entry.request)||
         !equal(Object.keys(entry.request).sort(),['confirmed','evaluation_key','issue','kind','reason'])||
         entry.request.confirmed!==true||!['synthetic','historical'].includes(entry.request.kind)||
         typeof entry.request.evaluation_key!=='string'||!entry.request.evaluation_key.trim()||entry.request.evaluation_key.length>128||
         typeof entry.request.reason!=='string'||!entry.request.reason.trim()||entry.request.reason.length>1000||
         (entry.request.kind==='synthetic'&&entry.request.issue!==null)||
         (entry.request.kind==='historical'&&(typeof entry.request.issue!=='string'||!/^\d{5}$/.test(entry.request.issue))))
        throw new Error('本机待确认声明无法核验');
      return entry;
    }
    async function state(entry){
      const result=await api.status(entry.event_id,entry.request_sha256);
      if(!result||result.event_id!==entry.event_id||result.request_sha256!==entry.request_sha256||
         !['unknown','accepted','fenced'].includes(result.status))throw new Error('服务端声明状态无法核验');
      if(result.status==='accepted'&&result.record?.event_id!==entry.event_id)throw new Error('已保存声明与原请求不符');
      return result;
    }
    async function send(entry){
      entry={...entry,phase:'submitted',submission_state:null,error:null};await store.put(entry);
      try{
        const result=await api.submit({...entry.request,event_id:entry.event_id,retry_of:entry.retry_of});
        const write=result?.write;
        if(!write||write.event_id!==entry.event_id||write.request_sha256!==entry.request_sha256||write.status!=='accepted'||
           result.declaration?.event_id!==entry.event_id)throw new Error('来源声明提交结果无法核验，请查询原请求');
        entry={...entry,phase:'accepted',error:null};await store.put(entry);return entry;
      }catch(error){
        if(error.notSent===true){entry={...entry,submission_state:'not_sent',error:String(error.message||'请求未发送').slice(0,1000)};await store.put(entry);throw error;}
        if(error.write?.status==='fenced'&&error.write.event_id===entry.event_id&&error.write.request_sha256===entry.request_sha256){
          entry={...entry,phase:'fenced',error:null};await store.put(entry);return entry;
        }
        if(error.status===400||error.status===404||error.status===409){entry={...entry,phase:'rejected',error:String(error.message||'请求被拒绝').slice(0,1000)};await store.put(entry);}
        throw error;
      }
    }
    async function create(request,retryOf=null){
      const eventId=uuid();if(!uuidPattern.test(eventId)||eventId===retryOf)throw new Error('来源声明请求标识无效');
      const payload={evaluation_key:String(request.evaluation_key),kind:String(request.kind),
        issue:request.kind==='historical'?String(request.issue):null,reason:String(request.reason).trim(),confirmed:true};
      const requestSha=await digest(payload,cryptoApi);
      const entry=valid({version:1,event_id:eventId,retry_of:retryOf,request:payload,request_sha256:requestSha,
        phase:'submitted',created_at:new Date().toISOString(),error:null});
      await store.add(entry);
      return send(entry);
    }
    return {
      async begin(request){return locked('new',async()=>{
        const payload={evaluation_key:String(request.evaluation_key),kind:String(request.kind),
          issue:request.kind==='historical'?String(request.issue):null,reason:String(request.reason).trim(),confirmed:true};
        const existing=(await store.list()).map(valid).find(item=>equal(item.request,payload)&&
          ['submitted','accepted','fenced'].includes(item.phase));
        if(existing){
          if(existing.phase==='submitted')return this.continueOriginal(existing.event_id);
          return existing;
        }
        return create(payload);
      });},
      async list(){return (await store.list()).map(valid);},
      async check(eventId){return locked(eventId,async()=>{
        let entry=valid(await store.get(eventId));if(!entry)throw new Error('本机找不到这条来源声明');
        const result=await state(entry);
        if(result.status!=='unknown'){entry={...entry,phase:result.status,error:null};await store.put(entry);}
        return entry;
      });},
      async continueOriginal(eventId){return locked(eventId,async()=>{
        let entry=valid(await store.get(eventId));if(!entry||entry.phase!=='submitted')throw new Error('当前声明状态不能继续提交');
        const result=await state(entry);
        if(result.status!=='unknown'){entry={...entry,phase:result.status,error:null};await store.put(entry);return entry;}
        return send(entry);
      });},
      async fence(eventId){return locked(eventId,async()=>{
        let entry=valid(await store.get(eventId));if(!entry||!['submitted'].includes(entry.phase))throw new Error('当前声明状态不能封止');
        try{
          const result=await api.fence({...entry.request,event_id:entry.event_id,request_sha256:entry.request_sha256,retry_of:entry.retry_of});
          const write=result?.write;if(!write||write.event_id!==entry.event_id||write.request_sha256!==entry.request_sha256||!['accepted','fenced'].includes(write.status))throw new Error('来源声明封止结果无法核验');
          entry={...entry,phase:write.status,error:null};await store.put(entry);return entry;
        }catch(error){
          if(error.write?.event_id===entry.event_id&&error.write?.request_sha256===entry.request_sha256&&['accepted','fenced'].includes(error.write.status)){
            entry={...entry,phase:error.write.status,error:null};await store.put(entry);return entry;
          }
          throw error;
        }
      });},
      async retry(eventId){return locked(eventId,async()=>{
        const original=valid(await store.get(eventId));if(!original||original.phase!=='fenced')throw new Error('只有已封止声明可以关联重试');
        const stateResult=await state(original);if(stateResult.status!=='fenced')throw new Error('原声明封止状态无法确认');
        return create(original.request,original.event_id);
      });},
      async dismiss(eventId){return locked(eventId,async()=>{
        const entry=valid(await store.get(eventId));if(!entry||!['accepted','fenced','rejected'].includes(entry.phase))throw new Error('声明结果尚未确认，不能清理');
        if(entry.phase!=='rejected'){const result=await state(entry);if(result.status!==entry.phase)throw new Error('服务端声明状态与本机结果不符');}
        await store.remove(eventId);return entry;
      });}
    };
  }
  return {browserStore,coordinator};
});
