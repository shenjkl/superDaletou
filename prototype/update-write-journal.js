(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.UpdateWriteJournal=api;
})(globalThis,function(){
  'use strict';
  const operations=new Set(['update_plan_create','update_plan_edit','update_plan_set_enabled',
    'update_plan_run_now','update_run_cancel','autostart_set_enabled']);
  const phases=new Set(['submitted','accepted','fenced','rejected']);
  const uuidPattern=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  function copy(value){return JSON.parse(JSON.stringify(value));}
  function stable(value){
    if(Array.isArray(value))return value.map(stable);
    if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])]));
    return value;
  }
  function equal(a,b){return JSON.stringify(stable(a))===JSON.stringify(stable(b));}
  function browserStore(indexedDB){
    if(!indexedDB)throw new Error('本机计划请求暂存不可用，操作未提交');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;const request=indexedDB.open('dlt-update-write-journal',1);
        request.onupgradeneeded=()=>request.result.createObjectStore('writes',{keyPath:'request_key'});
        request.onsuccess=()=>{if(abandoned){request.result.close();return;}request.result.onversionchange=()=>{request.result.close();opening=null;};resolve(request.result);};
        request.onerror=()=>{opening=null;reject(request.error||new Error('本机计划请求暂存打开失败'));};
        request.onblocked=()=>{abandoned=true;opening=null;reject(new Error('本机计划请求暂存被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,work){
      const db=await open();return new Promise((resolve,reject)=>{
        const transaction=mode==='readwrite'?db.transaction('writes',mode,{durability:'strict'}):db.transaction('writes',mode);
        let result;transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('本机计划请求暂存失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){
          transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，操作未发送'));return;
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
  function coordinator({store,locks,api,uuid}){
    if(!store||!api||typeof uuid!=='function')throw new Error('更新计划写入协调器配置无效');
    async function locked(key,work){
      if(!locks||typeof locks.request!=='function')throw new Error('浏览器不支持跨页面计划写入保护，操作未发送');
      return locks.request('dlt-update-write:'+key,{ifAvailable:true},async lock=>{
        if(!lock)throw new Error('该计划正在其他页面核对，请稍后重试');return work();
      });
    }
    function valid(entry){
      if(!entry||entry.version!==1||!uuidPattern.test(entry.request_key||'')||
         !operations.has(entry.operation)||!phases.has(entry.phase)||!entry.request||
         typeof entry.request!=='object'||Array.isArray(entry.request)||
         (entry.retry_of!=null&&!uuidPattern.test(entry.retry_of)))throw new Error('本机计划请求记录无法核验');
      return entry;
    }
    function endpoint(operation){
      return ({update_plan_create:'/api/update-plans',update_plan_edit:'/api/update-plans/edit',
        update_plan_set_enabled:'/api/update-plans/set-enabled',update_plan_run_now:'/api/update-plans/run-now',
        update_run_cancel:'/api/update-runs/cancel',autostart_set_enabled:'/api/autostart'})[operation];
    }
    async function read(entry){
      const state=await api.status(entry.request_key);
      if(!state||!['unknown','accepted','fenced'].includes(state.status))throw new Error('更新计划请求状态无法核验');
      if(['accepted','fenced'].includes(state.status)&&(!state.write||state.write.request_key!==entry.request_key||
         state.write.operation!==entry.operation||!equal(state.write.request,entry.request)||
         (state.write.retry_of||null)!==(entry.retry_of||null)))
        throw new Error('同一请求标识对应了不同计划操作，本机记录已保留');
      return state;
    }
    async function send(entry){
      entry={...entry,phase:'submitted',submission_state:null,error:null,status_unverified:false};await store.put(entry);
      try{
        const write=await api.post(endpoint(entry.operation),{...entry.request,request_key:entry.request_key,
          ...(entry.retry_of?{retry_of:entry.retry_of}:{})});
        if(!write||write.status!=='accepted'||write.request_key!==entry.request_key||write.operation!==entry.operation||
           !equal(write.request,entry.request)||(write.retry_of||null)!==(entry.retry_of||null))
          throw new Error('计划操作结果无法核验，请查询原请求');
        entry={...entry,phase:'accepted',result:write.result,error:null,status_unverified:false};
        await store.put(entry);return entry;
      }catch(error){
        if(error.notSent===true){entry={...entry,submission_state:'not_sent',error:String(error.message||'请求未发送').slice(0,1000)};await store.put(entry);throw error;}
        if(error.write?.status==='fenced'&&error.write.request_key===entry.request_key&&
           error.write.operation===entry.operation&&equal(error.write.request,entry.request)&&
           (error.write.retry_of||null)===(entry.retry_of||null)){
          entry={...entry,phase:'fenced',result:null,error:'原请求已封止，服务端将拒绝迟到提交。'};
          await store.put(entry);return entry;
        }
        if(error.status===400||error.status===404||error.status===409){
          entry={...entry,phase:'rejected',error:String(error.message||'计划操作被拒绝').slice(0,1000)};
          await store.put(entry);
        }
        throw error;
      }
    }
    function applyState(entry,state){
      if(state.status==='accepted')return {...entry,phase:'accepted',result:state.write.result,error:null,status_unverified:false};
      if(state.status==='fenced')return {...entry,phase:'fenced',result:null,error:'原请求已封止，服务端将拒绝迟到提交。',status_unverified:false};
      if(['accepted','fenced'].includes(entry.phase))return {...entry,status_unverified:true};
      return entry;
    }
    return {
      browserStore,
      async begin(operation,request,{requestKey=uuid(),retryOf=null}={}){
        if(!operations.has(operation)||!uuidPattern.test(requestKey))throw new Error('更新计划操作或请求标识无效');
        const payload=copy(request),entry={version:1,operation,request_key:requestKey,request:payload,
          retry_of:retryOf,phase:'submitted',error:null,status_unverified:false,created_at:new Date().toISOString()};
        if(retryOf!==null&&!uuidPattern.test(retryOf))throw new Error('关联原请求标识无效');
        return locked('operation:'+operation,async()=>{
          const entries=(await store.list()).map(valid);
          if(!retryOf&&entries.some(item=>item.operation===operation&&equal(item.request,payload)&&item.phase==='fenced'))
            throw new Error('该操作已有已封止请求；如需重试，请从原记录发起关联重试');
          const same=entries.find(item=>item.operation===operation&&equal(item.request,payload)&&
            (item.retry_of||null)===(retryOf||null)&&['submitted','accepted'].includes(item.phase));
          if(same){const state=await read(same),updated=applyState(same,state);await store.put(updated);return updated;}
          const stored=await store.get(requestKey),existing=stored?valid(stored):null;
          if(existing){
            if(existing.operation!==operation||!equal(existing.request,payload)||
               (existing.retry_of||null)!==(retryOf||null))throw new Error('请求标识已用于不同计划操作，本机记录已保留');
            const state=await read(existing),updated=applyState(existing,state);await store.put(updated);return updated;
          }
          await store.add(entry);return send(entry);
        });
      },
      async list(){return (await store.list()).map(valid);},
      async check(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条更新计划请求');
        const updated=applyState(entry,await read(entry));await store.put(updated);return updated;
      });},
      async continueOriginal(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry||entry.phase!=='submitted'||entry.status_unverified)
          throw new Error('当前计划请求状态不能继续提交');
        const state=await read(entry);
        if(state.status!=='unknown'){const updated=applyState(entry,state);await store.put(updated);return updated;}
        return send(entry);
      });},
      async fenceOriginal(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry||entry.phase!=='submitted'||entry.status_unverified)
          throw new Error('当前计划请求状态不能封止');
        const state=await read(entry);
        if(state.status!=='unknown'){const updated=applyState(entry,state);await store.put(updated);return updated;}
        const write=await api.fence({request_key:entry.request_key,operation:entry.operation,
          request:entry.request,...(entry.retry_of?{retry_of:entry.retry_of}:{})});
        if(!write||!['accepted','fenced'].includes(write.status)||write.request_key!==entry.request_key||
           write.operation!==entry.operation||!equal(write.request,entry.request)||
           (write.retry_of||null)!==(entry.retry_of||null))throw new Error('封止结果无法核验，本机记录已保留');
        const updated=applyState(entry,{status:write.status,write});await store.put(updated);return updated;
      });},
      async retryFenced(requestKey){
        const original=valid(await store.get(requestKey));if(!original)throw new Error('本机找不到原更新计划请求');
        const current=await read(original),fenced=applyState(original,current);await store.put(fenced);
        if(current.status!=='fenced'||fenced.phase!=='fenced')throw new Error('只有服务端确认已封止的请求才能关联重试');
        return this.begin(fenced.operation,fenced.request,{requestKey:uuid(),retryOf:fenced.request_key});
      },
      async dismiss(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条更新计划请求');
        if(entry.phase==='rejected'){await store.remove(requestKey);return entry;}
        const state=await read(entry);if(!['accepted','fenced'].includes(state.status))throw new Error('更新计划请求仍未知，不能清理本机记录');
        await store.remove(requestKey);return entry;
      });}
    };
  }
  return {browserStore,coordinator};
});
