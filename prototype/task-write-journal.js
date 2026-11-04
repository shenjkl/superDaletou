(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.TaskWriteJournal=api;
})(globalThis,function(){
  'use strict';
  const phases=new Set(['submitted','accepted','rejected']);
  const operations=new Set(['training','backtest','training_cancel','backtest_cancel']);
  const uuidPattern=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  function copy(value){return JSON.parse(JSON.stringify(value));}
  function stable(value){
    if(Array.isArray(value))return value.map(stable);
    if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])]));
    return value;
  }
  function equal(a,b){return JSON.stringify(stable(a))===JSON.stringify(stable(b));}
  function browserStore(indexedDB){
    if(!indexedDB)throw new Error('本机任务暂存不可用，任务未提交');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;const request=indexedDB.open('dlt-task-write-journal',1);
        request.onupgradeneeded=()=>request.result.createObjectStore('writes',{keyPath:'request_key'});
        request.onsuccess=()=>{if(abandoned){request.result.close();return;}request.result.onversionchange=()=>{request.result.close();opening=null;};resolve(request.result);};
        request.onerror=()=>{opening=null;reject(request.error||new Error('本机任务暂存打开失败'));};
        request.onblocked=()=>{abandoned=true;opening=null;reject(new Error('本机任务暂存被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,work){
      const db=await open();return new Promise((resolve,reject)=>{
        const transaction=mode==='readwrite'?db.transaction('writes',mode,{durability:'strict'}):db.transaction('writes',mode);
        let result;transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('本机任务暂存操作失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){
          transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，任务未提交'));return;
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
    if(!store||!api||typeof uuid!=='function')throw new Error('任务写入协调器配置无效');
    async function locked(key,work){
      if(!locks||typeof locks.request!=='function')throw new Error('浏览器不支持跨页面任务写入保护，任务未提交');
      return locks.request('dlt-task-write:'+key,{ifAvailable:true},async lock=>{
        if(!lock)throw new Error('该任务正在其他页面核对，请稍后重试');return work();
      });
    }
    function valid(entry){
      if(!entry||entry.version!==1||!uuidPattern.test(entry.request_key||'')||
         !operations.has(entry.operation)||!phases.has(entry.phase)||!entry.request||
         typeof entry.request!=='object'||Array.isArray(entry.request)||
         (entry.operation.endsWith('_cancel')&&
          (typeof entry.request.target_request_key!=='string'||!entry.request.target_request_key.trim()||entry.request.target_request_key.length>128)))
        throw new Error('本机任务暂存记录无法核验');
      return entry;
    }
    function operationIsCancel(operation){return operation.endsWith('_cancel');}
    function path(operation){
      if(operation==='training')return '/api/jobs';
      if(operation==='backtest')return '/api/backtests';
      if(operation==='training_cancel')return '/api/jobs/cancel';
      return '/api/backtests/cancel';
    }
    function requestBody(entry){
      return operationIsCancel(entry.operation)
        ?{request_key:entry.request.target_request_key,cancel_request_key:entry.request_key}
        :{request_key:entry.request_key,...entry.request};
    }
    async function read(entry){
      const result=await api.status(entry.operation,entry.request_key);
      if(!result||!['unknown','accepted','rejected'].includes(result.status))throw new Error('服务端任务状态无法核验');
      if(result.status!=='unknown'&&(!result.record||result.record.request_key!==entry.request_key||
         !equal(result.record.request,entry.request)))throw new Error('同一任务标识对应了不同配置，本机记录已保留');
      return result;
    }
    async function send(entry){
      entry={...entry,phase:'submitted',submission_state:null,error:null};await store.put(entry);
      try{
        const result=await api.post(path(entry.operation),requestBody(entry));
        if(!result||result.request_key!==entry.request_key||!equal(result.request,entry.request))
          throw new Error('任务提交结果无法核验，请查询原任务');
        entry=result.status==='rejected'
          ?{...entry,phase:'rejected',last_status:'rejected',error:String(result.reason||'目标任务不存在').slice(0,1000)}
          :{...entry,phase:'accepted',last_status:result.target_status||result.status||'running',error:null};
        await store.put(entry);return entry;
      }catch(error){
        if(error.notSent===true){entry={...entry,submission_state:'not_sent',error:String(error.message||'请求未发送').slice(0,1000)};await store.put(entry);throw error;}
        if(error.status===400||error.status===404||error.status===409){
          entry={...entry,phase:'rejected',error:String(error.message||'请求被拒绝').slice(0,1000)};
          await store.put(entry);
        }
        throw error;
      }
    }
    return {
      browserStore,
      async begin(operation,request,{requestKey=uuid()}={}){
        if(!operations.has(operation)||!uuidPattern.test(requestKey))throw new Error('任务类型或标识无效');
        const payload=copy(request),entry={version:1,operation,request_key:requestKey,request:payload,
          phase:'submitted',error:null,created_at:new Date().toISOString()};
        return locked('submit:'+operation,async()=>{
          const same=(await store.list()).map(valid).find(item=>item.operation===operation&&equal(item.request,payload)&&
            ['submitted','accepted'].includes(item.phase));
          if(same){
            const state=await read(same);
            const existing=state.status==='accepted'
              ?{...same,phase:'accepted',last_status:state.record.target_status||state.record.status,error:null}
              :state.status==='rejected'?{...same,phase:'rejected',last_status:'rejected',error:state.record.reason||'请求被拒绝'}
              :same.phase==='accepted'?{...same,phase:'submitted',last_status:null}:same;
            await store.put(existing);return existing;
          }
          const stored=await store.get(requestKey),existing=stored?valid(stored):null;
          if(existing){
            if(existing.operation!==operation||!equal(existing.request,payload))throw new Error('任务标识已用于其他配置，本机记录已保留');
            const state=await read(existing);
            if(state.status==='accepted'){
              const updated={...existing,phase:'accepted',last_status:state.record.target_status||state.record.status,error:null};
              await store.put(updated);return updated;
            }
            if(state.status==='rejected'){
              const updated={...existing,phase:'rejected',last_status:'rejected',error:state.record.reason||'请求被拒绝'};
              await store.put(updated);return updated;
            }
            if(existing.phase==='accepted'){
              const updated={...existing,phase:'submitted',last_status:null};
              await store.put(updated);return updated;
            }
            return existing;
          }
          await store.add(entry);
          return send(entry);
        });
      },
      async list(){return (await store.list()).map(valid);},
      async check(requestKey){return locked(requestKey,async()=>{
        let entry=valid(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条待确认任务');
        const state=await read(entry);
        if(state.status==='accepted')entry={...entry,phase:'accepted',last_status:state.record.target_status||state.record.status,error:null};
        else if(state.status==='rejected')entry={...entry,phase:'rejected',last_status:'rejected',error:state.record.reason||'请求被拒绝'};
        else if(entry.phase==='accepted')entry={...entry,phase:'submitted',last_status:null};
        await store.put(entry);return entry;
      });},
      async continueOriginal(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry||entry.phase!=='submitted')
          throw new Error('当前任务状态不能继续提交');
        const state=await read(entry);
        if(state.status==='accepted'){
          const updated={...entry,phase:'accepted',last_status:state.record.target_status||state.record.status,error:null};
          await store.put(updated);return updated;
        }
        if(state.status==='rejected'){
          const updated={...entry,phase:'rejected',last_status:'rejected',error:state.record.reason||'请求被拒绝'};
          await store.put(updated);return updated;
        }
        return send(entry);
      });},
      async dismiss(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条任务记录');
        if(entry.phase==='rejected'){
          await store.remove(requestKey);return entry;
        }
        const state=await read(entry);
        if(!['accepted','rejected'].includes(state.status))throw new Error('任务结果仍未知，不能清理本机记录');
        await store.remove(requestKey);return entry;
      });},
      async discardNotSent(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));
        if(!entry||entry.operation!=='training'||entry.phase!=='submitted'||entry.submission_state!=='not_sent')
          throw new Error('只有确认未发送的训练请求可以清理');
        await store.remove(requestKey);return entry;
      });}
    };
  }
  return {browserStore,coordinator};
});
