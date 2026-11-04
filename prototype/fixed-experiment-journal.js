(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.FixedExperimentJournal=api;
})(globalThis,function(){
  'use strict';
  const phases=new Set(['preparing','prepared','submitted','committed','fenced']);
  const sha=/^[0-9a-f]{64}$/;
  const uuidPattern=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  function copy(value){return JSON.parse(JSON.stringify(value));}
  function stable(value){
    if(Array.isArray(value))return value.map(stable);
    if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])]));
    return value;
  }
  function seriesManifest(value){
    const manifest=copy(value);
    if(!manifest||typeof manifest!=='object'||Array.isArray(manifest)||!manifest.seeds||typeof manifest.seeds!=='object')
      throw new Error('多期实验版本内容无法核验');
    for(const schedule of Object.values(manifest.seeds))for(const [replica,seed] of Object.entries(schedule))schedule[replica]=String(seed);
    return JSON.stringify(stable(manifest));
  }
  function browserStore(indexedDB){
    if(!indexedDB)throw new Error('本机待确认存储不可用，未发送请求');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;const request=indexedDB.open('dlt-fixed-experiment-write-journal',1);
        request.onupgradeneeded=()=>request.result.createObjectStore('writes',{keyPath:'request_key'});
        request.onsuccess=()=>{if(abandoned){request.result.close();return;}request.result.onversionchange=()=>{request.result.close();opening=null;};resolve(request.result);};
        request.onerror=()=>{opening=null;reject(request.error||new Error('本机待确认存储打开失败'));};
        request.onblocked=()=>{abandoned=true;opening=null;reject(new Error('本机存储被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,work){
      const db=await open();return new Promise((resolve,reject)=>{
        const transaction=mode==='readwrite'?db.transaction('writes',mode,{durability:'strict'}):db.transaction('writes',mode);
        let result;transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('本机待确认存储操作失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){
          transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，未发送请求'));return;
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
    if(!store||!api||typeof uuid!=='function')throw new Error('固定实验待确认协调器配置无效');
    async function locked(key,work){
      if(!locks||typeof locks.request!=='function')throw new Error('浏览器不支持跨页面写入保护，未发送请求');
      return locks.request('dlt-fixed-experiment-write:'+key,{ifAvailable:true},async lock=>{
        if(!lock)throw new Error('该实验正在其他页面核对，请稍后重试');return work();
      });
    }
    function valid(entry){
      if(!entry||entry.version!==1||!uuidPattern.test(entry.request_key||'')||!phases.has(entry.phase)||
         (entry.retry_of!==null&&!uuidPattern.test(entry.retry_of||''))||!entry.protocol||typeof entry.protocol!=='object'||
         Array.isArray(entry.protocol)||entry.protocol.replicas?.length>100||
         (entry.operation!==undefined&&!['fixed_period','series_create'].includes(entry.operation))||
         (entry.series_key!==undefined&&entry.series_key!==null&&
           (typeof entry.series_key!=='string'||!entry.series_key.trim()||entry.series_key.length>128))||
         (entry.request_sha256!==null&&!sha.test(entry.request_sha256||'')))throw new Error('本机待确认实验记录无法核验');
      if(entry.operation==='series_create'&&(entry.series_key!==entry.request_key||entry.request_sha256!==null||
         (entry.retry_of!==null&&!uuidPattern.test(entry.retry_of||''))))
        throw new Error('本机待确认实验版本记录无法核验');
      return entry;
    }
    async function prepare(entry){
      if(entry.operation==='series_create'){
        entry={...entry,phase:'prepared'};await store.put(entry);return entry;
      }
      const result=await api.post('/api/experiments/prepare',{request_key:entry.request_key,protocol:entry.protocol,
        ...(entry.series_key?{series_key:entry.series_key}:{}),retry_of:entry.retry_of});
      const intent=result.intent;
      if(!intent||intent.request_key!==entry.request_key||!sha.test(intent.request_sha256||'')||
         !['unknown','committed','fenced'].includes(intent.status))throw new Error('服务端实验准备记录无法核验');
      entry={...entry,request_sha256:intent.request_sha256,
        phase:intent.status==='unknown'?'prepared':intent.status};
      await store.put(entry);return entry;
    }
    async function postWithState(entry,promise){try{return await promise;}catch(error){if(error.notSent===true){await store.put({...entry,submission_state:'not_sent'});}throw error;}}
    async function send(entry){
      if(entry.operation==='series_create'){
        if(!['prepared','submitted'].includes(entry.phase))throw new Error('当前实验版本请求不能提交');
        entry={...entry,phase:'submitted',submission_state:null};await store.put(entry);
        const result=await postWithState(entry,api.post('/api/experiment-series',{series_key:entry.series_key,manifest:entry.protocol,
          ...(entry.retry_of?{retry_of:entry.retry_of}:{})}));
        if(!result.series?.manifest||seriesManifest(result.series.manifest)!==seriesManifest(entry.protocol))
          throw new Error('实验版本提交结果无法核验，请查询原请求');
        entry={...entry,phase:'committed'};await store.put(entry);return entry;
      }
      if(entry.phase!=='prepared'||!entry.request_sha256)throw new Error('当前实验请求不能发送，请先核对状态');
      entry={...entry,phase:'submitted',submission_state:null};await store.put(entry);
      const path=entry.series_key?'/api/experiment-series/periods':'/api/experiments';
      const result=await postWithState(entry,api.post(path,{request_key:entry.request_key,protocol:entry.protocol,
        ...(entry.series_key?{series_key:entry.series_key}:{}),require_intent:true}));
      if(result.write?.request_key!==entry.request_key||result.write?.request_sha256!==entry.request_sha256||
         result.write?.status!=='committed')throw new Error('实验提交结果无法核验，请查询原请求');
      entry={...entry,phase:'committed'};await store.put(entry);return entry;
    }
    async function read(entry){
      const result=await api.status(entry.request_key,entry.request_sha256,entry);
      if(entry.operation==='series_create'){
        if(!result||result.request_key!==entry.request_key||!['unknown','committed','fenced'].includes(result.status))
          throw new Error('实验版本请求状态无法核验');
        if(result.status!=='unknown'&&(seriesManifest(result.request)!==seriesManifest(entry.protocol)||
           (result.retry_of||null)!==(entry.retry_of||null)))
          throw new Error('同一请求标识对应了不同实验版本，请保留本机记录');
        return result;
      }
      if(!result||result.request_key!==entry.request_key||result.request_sha256!==entry.request_sha256||
         !['unknown','committed','fenced'].includes(result.status))throw new Error('实验请求状态无法核验');
      return result;
    }
    return {
      browserStore,
      async begin(protocol,{requestKey=uuid(),seriesKey=null}={}){
        if(!uuidPattern.test(requestKey))throw new Error('无法生成有效的请求标识');
        if(seriesKey!==null&&(typeof seriesKey!=='string'||!seriesKey.trim()||seriesKey.length>128))throw new Error('实验版本标识无效');
        const entry={version:1,operation:'fixed_period',request_key:requestKey,request_sha256:null,retry_of:null,
          series_key:seriesKey,protocol:copy(protocol),phase:'preparing'};
        return locked(requestKey,async()=>{
          await store.add(entry);
          let current=await prepare(entry);
          if(current.phase==='prepared')current=await send(current);
          return current;
        });
      },
      async beginSeries(manifest,{seriesKey=uuid(),retryOf=null}={}){
        if(!uuidPattern.test(seriesKey))throw new Error('无法生成有效的实验版本标识');
        if(retryOf!==null&&(!uuidPattern.test(retryOf)||retryOf===seriesKey))throw new Error('系列关联重试标识无效');
        const request=copy(manifest),entry={version:1,operation:'series_create',request_key:seriesKey,
          request_sha256:null,retry_of:retryOf,series_key:seriesKey,protocol:request,phase:'prepared'};
        seriesManifest(request);
        return locked(seriesKey,async()=>{
          const stored=await store.get(seriesKey),existing=stored?valid(stored):null;
          if(existing){
            if(existing.operation!=='series_create'||seriesManifest(existing.protocol)!==seriesManifest(request))
              throw new Error('该请求标识已用于其他实验操作，本机记录已保留');
            const state=await read(existing);
            if(state.status==='committed'||state.status==='fenced')existing.phase=state.status;
            else existing.phase=existing.phase==='prepared'?'prepared':'submitted';
            await store.put(existing);
            return existing;
          }
          await store.add(entry);
          return send(entry);
        });
      },
      async list(){return (await store.list()).map(valid);},
      async check(requestKey){return locked(requestKey,async()=>{
        let entry=valid(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条待确认实验');
        if(entry.phase==='preparing')return prepare(entry);
        const state=await read(entry);
        if(state.status==='committed'||state.status==='fenced')entry={...entry,phase:state.status};
        else if(entry.phase==='committed'||entry.phase==='fenced')entry={...entry,phase:'submitted'};
        await store.put(entry);return entry;
      });},
      async continuePrepared(requestKey){return locked(requestKey,async()=>{
        let entry=valid(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条待确认实验');
        if(entry.phase==='preparing')entry=await prepare(entry);
        if(entry.operation==='series_create'){
          if(!['prepared','submitted'].includes(entry.phase))return entry;
          const state=await read(entry);
          if(state.status==='committed'){entry={...entry,phase:'committed'};await store.put(entry);return entry;}
          if(state.status==='fenced'){entry={...entry,phase:'fenced'};await store.put(entry);return entry;}
          return send({...entry,phase:'prepared'});
        }
        if(entry.phase!=='prepared')return entry;
        return send(entry);
      });},
      async fence(requestKey){return locked(requestKey,async()=>{
        let entry=valid(await store.get(requestKey));if(!entry||!['prepared','submitted'].includes(entry.phase))throw new Error('当前状态不能封止');
        let result;
        if(entry.operation==='series_create'){
          try{result=await api.post('/api/experiment-series/fence',{request_key:entry.request_key,manifest:entry.protocol,
            ...(entry.retry_of?{retry_of:entry.retry_of}:{})});}catch(error){if(error.write)result={write:error.write};else throw error;}
        }else result=await api.post('/api/experiments/fence',{request_key:entry.request_key,request_sha256:entry.request_sha256});
        const seriesWrite=entry.operation==='series_create';
        if(result.write?.request_key!==entry.request_key||!['committed','fenced'].includes(result.write.status)||
           (seriesWrite?(seriesManifest(result.write.request)!==seriesManifest(entry.protocol)||!sha.test(result.write.request_sha256||'')):
             result.write.request_sha256!==entry.request_sha256))throw new Error('服务端封止结果无法核验');
        entry={...entry,phase:result.write.status};await store.put(entry);return entry;
      });},
      async retry(requestKey){return locked(requestKey,async()=>{
        const original=valid(await store.get(requestKey));if(!original||original.phase!=='fenced')throw new Error('只有已封止的固定实验请求可以关联重试');
        if(original.operation==='series_create'){
          const state=await read(original);if(state.status!=='fenced')throw new Error('服务端尚未确认实验版本原请求已封止');
          return this.beginSeries(original.protocol,{seriesKey:uuid(),retryOf:requestKey});
        }
        const key=uuid();if(!uuidPattern.test(key)||key===requestKey)throw new Error('无法生成新的请求标识');
        const entry={...original,request_key:key,request_sha256:null,retry_of:requestKey,phase:'preparing'};
        await store.add(entry);let next=await prepare(entry);if(next.phase==='prepared')next=await send(next);return next;
      });},
      async dismiss(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry)throw new Error('结果待确认时不能清理本机记录');
        const state=await read(entry);if(!['committed','fenced'].includes(state.status))throw new Error('服务端未确认终态，不能清理本机记录');
        await store.remove(requestKey);return entry;
      });}
    };
  }
  return {browserStore,coordinator};
});
