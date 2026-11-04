/* Durable browser journal for fixed evaluation commands. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.FixedEvaluationWriteJournal=api;
})(globalThis,function(){
  'use strict';
  const operations=new Set(['freeze','start','supersede','recalculate_amounts']);
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
    if(!cryptoApi?.subtle||typeof TextEncoder!=='function')throw new Error('本机摘要能力不可用，未发送固定评估请求');
    const bytes=new TextEncoder().encode(JSON.stringify(stable(request)));
    return [...new Uint8Array(await cryptoApi.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join('');
  }
  function browserStore(indexedDB){
    if(!indexedDB)throw new Error('本机固定评估暂存不可用，未发送请求');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;const request=indexedDB.open('dlt-fixed-evaluation-write-journal',1);
        request.onupgradeneeded=()=>request.result.createObjectStore('writes',{keyPath:'request_key'});
        request.onsuccess=()=>{if(abandoned){request.result.close();return;}request.result.onversionchange=()=>{request.result.close();opening=null;};resolve(request.result);};
        request.onerror=()=>{opening=null;reject(request.error||new Error('本机固定评估暂存打开失败'));};
        request.onblocked=()=>{abandoned=true;opening=null;reject(new Error('本机固定评估暂存被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,work){
      const db=await open();return new Promise((resolve,reject)=>{
        const transaction=mode==='readwrite'?db.transaction('writes',mode,{durability:'strict'}):db.transaction('writes',mode);
        let result;transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('本机固定评估暂存操作失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){
          transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，固定评估未提交'));return;
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
    if(!store||!api||typeof uuid!=='function')throw new Error('固定评估待确认协调器配置无效');
    async function locked(key,work){
      if(!locks||typeof locks.request!=='function')throw new Error('浏览器不支持跨页面固定评估写入保护，未发送请求');
      return locks.request('dlt-fixed-evaluation-write:'+key,{ifAvailable:true},async lock=>{
        if(!lock)throw new Error('该固定评估请求正在其他页面核对，请稍后重试');return work();
      });
    }
    function valid(entry){
      if(!entry||entry.version!==1||!uuidPattern.test(entry.request_key||'')||
         (entry.retry_of!==null&&!uuidPattern.test(entry.retry_of||''))||!operations.has(entry.operation)||
         !phases.has(entry.phase)||!digestPattern.test(entry.request_sha256||'')||
         !entry.request||typeof entry.request!=='object'||Array.isArray(entry.request)||
         !/^[0-9a-f]{64}$/.test(entry.request.reservation_id||''))
        throw new Error('本机固定评估待确认请求无法核验');
      if(entry.phase==='accepted'&&(!entry.result||typeof entry.result!=='object'))
        throw new Error('本机已接受固定评估结果无法核验');
      return entry;
    }
    async function checked(entry){
      entry=valid(entry);
      if(await digest(entry.request,cryptoApi)!==entry.request_sha256)throw new Error('本机固定评估请求摘要不符');
      return entry;
    }
    async function state(entry){
      const result=await api.status(entry.request_key,entry.operation,entry.request_sha256);
      if(!result||result.request_key!==entry.request_key||result.operation!==entry.operation||
         result.request_sha256!==entry.request_sha256||!['unknown','accepted','fenced'].includes(result.status))
        throw new Error('服务端固定评估请求状态无法核验');
      if(result.status==='accepted'&&(!result.result||typeof result.result!=='object'))
        throw new Error('服务端固定评估已接受结果无法核验');
      return result;
    }
    function sameAccepted(entry,result){
      if(entry.operation==='start'){
        const attempts=Array.isArray(result?.result?.attempts)?result.result.attempts:[];
        return result.result?.reservation_id===entry.request.reservation_id&&
          attempts.some(item=>item.request_key===entry.request_key&&item.retry_requested===Number(entry.request.retry));
      }
      return equal(result.result,entry.result);
    }
    function applyState(entry,result){
      if(result.status==='accepted'&&entry.phase==='accepted'&&!sameAccepted(entry,result))
        throw new Error('服务端固定评估结果与本机记录不符');
      return {...entry,phase:result.status,...(result.status==='accepted'?{result:result.result}:{}),error:null};
    }
    async function send(entry){
      entry={...entry,phase:'submitted',submission_state:null,error:null};await store.put(entry);
      try{
        const response=await api.submit(entry.operation,{...entry.request,request_key:entry.request_key,
          request_sha256:entry.request_sha256,retry_of:entry.retry_of});
        const write=response?.write;
        if(!write||write.request_key!==entry.request_key||write.operation!==entry.operation||
           write.request_sha256!==entry.request_sha256||write.status!=='accepted'||!write.result||
           typeof write.result!=='object')throw new Error('固定评估提交结果无法核验，请查询原请求');
        entry={...entry,phase:'accepted',result:write.result,error:null};await store.put(entry);return entry;
      }catch(error){
        if(error.notSent===true){entry={...entry,submission_state:'not_sent',error:String(error.message||'请求未发送').slice(0,1000)};await store.put(entry);throw error;}
        const write=error.write;
        if(write?.status==='fenced'&&write.request_key===entry.request_key&&
           write.operation===entry.operation&&write.request_sha256===entry.request_sha256){
          entry={...entry,phase:'fenced',error:null};await store.put(entry);return entry;
        }
        if(error.status===400){entry={...entry,phase:'rejected',error:String(error.message||'请求被拒绝').slice(0,1000)};await store.put(entry);}
        throw error;
      }
    }
    async function create(operation,request,retryOf=null){
      if(!operations.has(operation))throw new Error('固定评估操作类型无效');
      const requestKey=uuid();if(!uuidPattern.test(requestKey)||requestKey===retryOf)throw new Error('固定评估请求标识无效');
      const payload=stable(request),requestSha=await digest(payload,cryptoApi);
      const entry=await checked(valid({version:1,request_key:requestKey,retry_of:retryOf,operation,
        request:payload,request_sha256:requestSha,phase:'submitted',created_at:new Date().toISOString(),result:null,error:null}));
      await store.add(entry);
      return send(entry);
    }
    return {
      async begin(operation,request){
        if(!operations.has(operation))throw new Error('固定评估操作类型无效');
        const payload=stable(request),requestSha=await digest(payload,cryptoApi);
        return locked(operation+':'+requestSha,async()=>{
        const entries=(await store.list()).map(valid),matches=entries.filter(item=>
          item.operation===operation&&equal(item.request,payload)&&['submitted','accepted'].includes(item.phase));
        const existing=matches.find(item=>item.phase==='submitted')||matches.find(item=>item.phase==='accepted');
        if(existing){if(existing.phase==='submitted')return this.continueOriginal(existing.request_key);return this.check(existing.request_key);}
        const fenced=entries.filter(item=>item.operation===operation&&equal(item.request,payload)&&item.phase==='fenced')
          .sort((a,b)=>b.created_at.localeCompare(a.created_at))[0];
        if(fenced)return this.retry(fenced.request_key);
        return create(operation,payload);
      });},
      async list(){const entries=[];for(const item of await store.list())entries.push(await checked(item));return entries;},
      async check(requestKey){return locked(requestKey,async()=>{
        let entry=await checked(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条固定评估请求');
        const result=await state(entry);
        if(result.status==='unknown'&&['accepted','fenced'].includes(entry.phase))
          throw new Error('本机已有终态记录，但服务端当前无法确认；保留副本并检查数据库状态');
        if(result.status!=='unknown')entry=applyState(entry,result);
        await store.put(entry);return entry;
      });},
      async continueOriginal(requestKey){return locked(requestKey,async()=>{
        let entry=await checked(await store.get(requestKey));if(!entry||entry.phase!=='submitted')throw new Error('当前固定评估请求不能继续提交');
        const result=await state(entry);if(result.status!=='unknown'){
          entry=applyState(entry,result);await store.put(entry);return entry;
        }
        return send(entry);
      });},
      async fence(requestKey){return locked(requestKey,async()=>{
        let entry=await checked(await store.get(requestKey));if(!entry||entry.phase!=='submitted')throw new Error('当前固定评估请求不能封止');
        try{
          const response=await api.fence({request_key:entry.request_key,operation:entry.operation,
            request:entry.request,request_sha256:entry.request_sha256,retry_of:entry.retry_of});
          const write=response?.write;
          if(!write||write.request_key!==entry.request_key||write.operation!==entry.operation||
             write.request_sha256!==entry.request_sha256||!['accepted','fenced'].includes(write.status)||
             (write.status==='accepted'&&(!write.result||typeof write.result!=='object')))
            throw new Error('固定评估封止结果无法核验');
          entry={...entry,phase:write.status,...(write.status==='accepted'?{result:write.result}:{}),error:null};
          await store.put(entry);return entry;
        }catch(error){
          const write=error.write;
          if(write?.request_key===entry.request_key&&write?.operation===entry.operation&&
             write?.request_sha256===entry.request_sha256&&['accepted','fenced'].includes(write.status)&&
             (write.status==='fenced'||(write.result&&typeof write.result==='object'))){
            entry={...entry,phase:write.status,...(write.status==='accepted'?{result:write.result}:{}),error:null};
            await store.put(entry);return entry;
          }
          throw error;
        }
      });},
      async retry(requestKey){return locked(requestKey,async()=>{
        const original=await checked(await store.get(requestKey));
        if(!original||original.phase!=='fenced')throw new Error('只有已封止的固定评估请求可以关联重试');
        const result=await state(original);if(result.status!=='fenced')throw new Error('原固定评估封止状态无法确认');
        const prior=(await store.list()).map(valid).find(item=>item.retry_of===original.request_key);
        if(prior&&prior.phase==='submitted')return this.continueOriginal(prior.request_key);
        if(prior&&prior.phase!=='rejected')return this.check(prior.request_key);
        return create(original.operation,original.request,original.request_key);
      });},
      async dismiss(requestKey){return locked(requestKey,async()=>{
        const entry=await checked(await store.get(requestKey));if(!entry||!['accepted','rejected','fenced'].includes(entry.phase))
          throw new Error('固定评估结果尚未确认，不能清理本机副本');
        if(entry.phase!=='rejected'){const result=await state(entry);if(result.status!==entry.phase)
          throw new Error('服务端固定评估状态与本机结果不符');}
        await store.remove(requestKey);return entry;
      });}
    };
  }
  return {browserStore,coordinator};
});
