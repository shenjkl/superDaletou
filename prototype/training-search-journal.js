(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.TrainingSearchJournal=api;
})(globalThis,function(){
  'use strict';
  const uuidPattern=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  const statuses=new Set(['queued','running','paused','complete','cancelled']);
  const childStatuses=new Set(['queued','running','complete','failed','cancelled','rejected','skipped']);
  const terminalChildStatuses=new Set(['complete','failed','cancelled','rejected','skipped']);
  function copy(value){return JSON.parse(JSON.stringify(value));}
  function stable(value){
    if(Array.isArray(value))return value.map(stable);
    if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])]));
    return value;
  }
  function sharedContext(request){
    const value=copy(request);
    if(!value||typeof value!=='object'||Array.isArray(value)||!value.config||typeof value.config!=='object'||
       Array.isArray(value.config)||!value.config.parameters||typeof value.config.parameters!=='object'||
       Array.isArray(value.config.parameters))throw new Error('本机参数搜索配置无法核验');
    delete value.config.parameters;
    return JSON.stringify(stable(value));
  }
  function browserStore(indexedDB){
    if(!indexedDB)throw new Error('参数搜索计划暂存不可用，未提交训练。');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;const request=indexedDB.open('dlt-training-search-journal',1);
        request.onupgradeneeded=()=>request.result.createObjectStore('searches',{keyPath:'id'});
        request.onsuccess=()=>{if(abandoned){request.result.close();return;}request.result.onversionchange=()=>{request.result.close();opening=null;};resolve(request.result);};
        request.onerror=()=>{opening=null;reject(request.error||new Error('参数搜索计划暂存打开失败'));};
        request.onblocked=()=>{abandoned=true;opening=null;reject(new Error('参数搜索计划暂存被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,work){
      const db=await open();return new Promise((resolve,reject)=>{
        const transaction=mode==='readwrite'?db.transaction('searches',mode,{durability:'strict'}):db.transaction('searches',mode);
        let result;transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('参数搜索计划暂存操作失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，未提交训练'));return;}
        try{work(transaction.objectStore('searches'),value=>{result=value;});}
        catch(error){transaction.abort();reject(error);}
      });
    }
    return {
      add(entry){return tx('readwrite',store=>store.add(entry));},
      put(entry){return tx('readwrite',store=>store.put(entry));},
      update(key,change){return tx('readwrite',(store,done)=>{const request=store.get(key);request.onsuccess=()=>{if(!request.result)throw new Error('找不到本机参数搜索计划');const updated=change(request.result);store.put(updated);done(updated);};});},
      get(key){return tx('readonly',(store,done)=>{const request=store.get(key);request.onsuccess=()=>done(request.result||null);});},
      list(){return tx('readonly',(store,done)=>{const request=store.getAll();request.onsuccess=()=>done(request.result);});}
    };
  }
  function coordinator({store,locks,api,uuid,now=()=>new Date().toISOString(),sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms))}){
    if(!store||typeof store.update!=='function'||!locks||typeof locks.request!=='function'||!api||typeof uuid!=='function')
      throw new Error('参数搜索协调器配置无效');
    function skipped(plan,from){
      for(let i=from;i<plan.children.length;i++)if(plan.children[i].status==='queued')
        plan.children[i]={...plan.children[i],status:'skipped',error:'搜索计划已停止，未提交此尝试',finished_at:now()};
      plan.next_index=plan.requests.length;plan.current_index=null;plan.status='cancelled';plan.error=null;
      return plan;
    }
    async function save(plan){
      return store.update(plan.id,latest=>{
        let merged={...plan,cancel_requested:!!(plan.cancel_requested||latest?.cancel_requested)};
        if(merged.cancel_requested&&merged.status==='complete')merged=skipped(merged,0);
        return merged;
      });
    }
    function valid(plan){
      if(!plan||plan.version!==1||!uuidPattern.test(plan.id||'')||typeof plan.trial_group_id!=='string'||
         !plan.trial_group_id.trim()||plan.trial_group_id.length>100||
         !statuses.has(plan.status)||!Array.isArray(plan.requests)||!Array.isArray(plan.children)||
         plan.requests.length<1||plan.requests.length>64||plan.children.length!==plan.requests.length||!Number.isInteger(plan.next_index)||
         plan.next_index<0||plan.next_index>plan.requests.length||
         !(plan.current_index===null||plan.current_index===plan.next_index)||
         (plan.next_index===plan.requests.length&&plan.current_index!==null)||
         typeof plan.created_at!=='string'||!plan.created_at||typeof plan.updated_at!=='string'||!plan.updated_at||
         !(plan.error===null||typeof plan.error==='string')||
         !(plan.cancel_requested===undefined||typeof plan.cancel_requested==='boolean')||
         (plan.status==='queued'&&(plan.next_index!==0||plan.current_index!==null||
           plan.children.some(child=>!child||child.status!=='queued')))||
         (['complete','cancelled'].includes(plan.status)&&(plan.next_index!==plan.requests.length||plan.current_index!==null||
           plan.children.some(child=>!child||!terminalChildStatuses.has(child.status)))))
        throw new Error('本机参数搜索记录无法核验');
      const keys=new Set();
      let context=null;
      for(let i=0;i<plan.requests.length;i++){
        const item=plan.requests[i],child=plan.children[i];
        if(!item||!uuidPattern.test(item.request_key||'')||!item.request||item.request.trial_group_id!==plan.trial_group_id||
           !Number.isInteger(item.ordinal)||item.ordinal!==i+1||!child||child.request_key!==item.request_key||
           !childStatuses.has(child.status)||keys.has(item.request_key)||
           (plan.status==='complete'&&child.status==='skipped'))throw new Error('本机参数搜索尝试清单无法核验');
        const itemContext=sharedContext(item.request);
        if(context!==null&&context!==itemContext)throw new Error('参数搜索尝试的快照、切分或公共配置不一致');
        context=itemContext;
        keys.add(item.request_key);
        if(i<plan.next_index&&!terminalChildStatuses.has(child.status)&&child.status!=='skipped')throw new Error('本机参数搜索进度与尝试状态不一致');
        if(i>plan.next_index&&child.status!=='queued'&&!(plan.status==='cancelled'&&child.status==='skipped'))throw new Error('本机参数搜索后续尝试状态不一致');
        if(i===plan.next_index&&plan.next_index<plan.requests.length&&
           !(child.status==='queued'||child.status==='running'||terminalChildStatuses.has(child.status)))
          throw new Error('本机参数搜索当前尝试状态不一致');
      }
      return plan;
    }
    function locked(work){return locks.request('dlt-training-parameter-search',{ifAvailable:true},lock=>{
      if(!lock)throw new Error('另一个页面正在推进参数搜索，请稍后刷新状态');return work();
    });}
    async function runLocked(id,onUpdate){
      let plan=valid(await store.get(id));if(!plan)throw new Error('找不到本机参数搜索计划');
      if(['complete','cancelled'].includes(plan.status))return plan;
      plan={...plan,status:'running',error:null,updated_at:now()};plan=await save(plan);onUpdate?.(copy(plan));
      while(plan.next_index<plan.requests.length){
        plan=valid(await store.get(id));
        if(plan.cancel_requested){
          try{
            const active=plan.current_index??plan.next_index;
            let child=plan.children[active];
            if(child?.status==='queued'){
              const recovered=await api.status(plan.requests[active].request_key);
              if(recovered&&['running','complete','failed','cancelled'].includes(recovered.status)){
                child={...child,status:recovered.status,started_at:now(),error:recovered.error||null,
                  artifact_id:recovered.artifact_id||null,finished_at:recovered.finished_at||null};
                plan.children[active]=child;
              }else if(!recovered){
                const local=await api.local(plan.requests[active].request_key);
                if(local&&local.phase!=='rejected'&&local.submission_state!=='not_sent')
                  throw new Error('子任务原提交仍待核实；计划已停止推进并保留原请求键');
                if(local?.phase==='submitted'&&local.submission_state==='not_sent')await api.abandon(plan.requests[active].request_key);
                if(local?.phase==='rejected'){
                  child={...child,status:'rejected',error:local.error||'训练请求被拒绝',finished_at:now()};
                  plan.children[active]=child;
                }
              }
            }
            if(child?.status==='running'){
              const cancellation=await api.cancel(plan.requests[active].request_key);
              if(cancellation?.phase!=='accepted')throw new Error('训练取消请求待核实，计划已停止推进并保留原请求键');
              let job=await api.status(plan.requests[active].request_key);
              while(job?.status==='running'){await sleep(1500);job=await api.status(plan.requests[active].request_key);}
              if(!job||!['complete','failed','cancelled'].includes(job.status))throw new Error('活动训练状态仍待核验，计划已暂停并保留原请求键');
              plan.children[active]={...child,status:job.status,error:job.error||null,artifact_id:job.artifact_id||null,finished_at:job.finished_at||now()};
            }
            plan=skipped(plan,plan.current_index===null?plan.next_index:plan.current_index+1);
            plan.updated_at=now();plan=await save(plan);onUpdate?.(copy(plan));return plan;
          }catch(error){
            plan={...plan,status:'paused',error:String(error?.message||error).slice(0,1000),updated_at:now()};
            plan=await save(plan);onUpdate?.(copy(plan));return plan;
          }
        }
        const index=plan.next_index,item=plan.requests[index];
        try{
          let submission;
          try{submission=await api.begin(item.request,item.request_key);}
          catch(error){
            const local=await api.local(item.request_key);
            if(local?.phase==='rejected')submission=local;
            else if(local?.phase==='submitted')submission=await api.continueOriginal(item.request_key);
            else throw error;
          }
          if(submission.phase==='rejected'){
            plan.children[index]={...plan.children[index],status:'rejected',error:submission.error||'训练请求被拒绝',finished_at:now()};
          }else{
            if(submission.phase==='submitted')submission=await api.continueOriginal(item.request_key);
            if(submission.phase==='rejected'){
              plan.children[index]={...plan.children[index],status:'rejected',error:submission.error||'训练请求被拒绝',finished_at:now()};
            }else{
              plan.children[index]={...plan.children[index],status:submission.last_status||'running',started_at:plan.children[index].started_at||now(),error:null};
              plan.current_index=index;plan.updated_at=now();plan=await save(plan);onUpdate?.(copy(plan));
              let job=await api.status(item.request_key);
              while(job?.status==='running'){
                await sleep(1500);
                const latest=valid(await store.get(id));
                if(latest.cancel_requested&&plan.children[index].cancel_sent!==true){
                  const cancellation=await api.cancel(item.request_key);
                  if(cancellation?.phase!=='accepted')throw new Error('训练取消请求待核实，计划已停止推进并保留原请求键');
                  plan.children[index]={...plan.children[index],cancel_sent:true};plan=await save(plan);
                }
                job=await api.status(item.request_key);
              }
              if(!job||!['complete','failed','cancelled'].includes(job.status))throw new Error('训练状态仍待核验，计划已暂停并保留原请求键');
              plan.children[index]={...plan.children[index],status:job.status,error:job.error||null,artifact_id:job.artifact_id||null,finished_at:job.finished_at||now()};
            }
          }
          plan={...plan,next_index:index+1,current_index:null,updated_at:now(),error:null};
          plan=await save(plan);onUpdate?.(copy(plan));
        }catch(error){
          plan={...plan,status:'paused',error:String(error?.message||error).slice(0,1000),updated_at:now()};
          plan=await save(plan);onUpdate?.(copy(plan));return plan;
        }
      }
      plan={...plan,status:'complete',current_index:null,error:null,updated_at:now()};
      plan=await save(plan);onUpdate?.(copy(plan));return plan;
    }
    return {
      browserStore,
      async create(requests,{trialGroupId=uuid(),id=uuid()}={}){
        if(!uuidPattern.test(id)||typeof trialGroupId!=='string'||!trialGroupId.trim()||trialGroupId.length>100||
           !Array.isArray(requests)||!requests.length||requests.length>64)
          throw new Error('参数搜索须包含 1 至 64 个有效尝试');
        const items=requests.map((request,index)=>{
          const payload=copy(request);
          if(!payload||typeof payload!=='object'||payload.trial_group_id!==trialGroupId)
            throw new Error('所有参数尝试必须属于同一个试验组');
          return {request_key:uuid(),request:payload,ordinal:index+1};
        });
        const stamp=now(),plan={version:1,id,trial_group_id:trialGroupId,status:'queued',requests:items,
          children:items.map(item=>({request_key:item.request_key,status:'queued'})),next_index:0,current_index:null,
          created_at:stamp,updated_at:stamp,error:null};
        await store.add(plan);return copy(plan);
      },
      async list(){return (await store.list()).map(valid).sort((a,b)=>b.created_at.localeCompare(a.created_at));},
      async cancel(id){
        let result=await store.update(id,latest=>{
          const plan=valid(latest);if(['complete','cancelled'].includes(plan.status))return plan;
          return {...plan,cancel_requested:true,updated_at:now()};
        });
        return copy(valid(result));
      },
      async run(id,onUpdate){return locked(async()=>{
        let plan=await runLocked(id,onUpdate);
        while(plan.status==='complete'){
          const pending=(await store.list()).map(valid).filter(item=>!['complete','cancelled'].includes(item.status)&&item.id!==plan.id)
            .sort((a,b)=>a.created_at.localeCompare(b.created_at));
          if(!pending.length)break;
          plan=await runLocked(pending[0].id,onUpdate);
        }
        return plan;
      });}
    };
  }
  return {browserStore,coordinator};
});
