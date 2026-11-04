/* Durable browser journal for fixing a local exploration exclusion range. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.HoldoutWriteJournal=api;
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
    if(!indexedDB)throw new Error('本机排除范围暂存不可用，未发送请求');
    let opening;
    function open(){
      if(!opening)opening=new Promise((resolve,reject)=>{
        let abandoned=false;const request=indexedDB.open('dlt-holdout-write-journal',1);
        request.onupgradeneeded=()=>request.result.createObjectStore('writes',{keyPath:'request_key'});
        request.onsuccess=()=>{if(abandoned){request.result.close();return;}request.result.onversionchange=()=>{request.result.close();opening=null;};resolve(request.result);};
        request.onerror=()=>{opening=null;reject(request.error||new Error('本机排除范围暂存打开失败'));};
        request.onblocked=()=>{abandoned=true;opening=null;reject(new Error('本机排除范围暂存被其他页面阻塞'));};
      });
      return opening;
    }
    async function tx(mode,work){
      const db=await open();return new Promise((resolve,reject)=>{
        const transaction=mode==='readwrite'?db.transaction('writes',mode,{durability:'strict'}):db.transaction('writes',mode);
        let result;transaction.oncomplete=()=>resolve(result);
        transaction.onabort=transaction.onerror=()=>reject(transaction.error||new Error('本机排除范围暂存操作失败'));
        if(mode==='readwrite'&&transaction.durability!=='strict'){
          transaction.abort();reject(new Error('浏览器不支持可靠的本机持久写入，排除范围未提交'));return;
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
    if(!store||!api||typeof uuid!=='function')throw new Error('排除范围待确认协调器配置无效');
    async function locked(key,work){
      if(!locks||typeof locks.request!=='function')throw new Error('浏览器不支持跨页面排除范围写入保护，未发送请求');
      return locks.request('dlt-holdout-write:'+key,{ifAvailable:true},async lock=>{
        if(!lock)throw new Error('该排除范围正在其他页面核对，请稍后重试');return work();
      });
    }
    function valid(entry){
      if(!entry||entry.version!==1||!uuidPattern.test(entry.request_key||'')||
         (entry.retry_of!==null&&!uuidPattern.test(entry.retry_of||''))||!phases.has(entry.phase)||
         !digestPattern.test(entry.request_sha256||'')||!entry.request||typeof entry.request!=='object'||Array.isArray(entry.request)||
         !equal(Object.keys(entry.request).sort(),['confirmed','end_issue','snapshot_id','start_issue'])||
         entry.request.confirmed!==true||!digestPattern.test(entry.request.snapshot_id||'')||
         !/^\d{5}$/.test(entry.request.start_issue||'')||!/^\d{5}$/.test(entry.request.end_issue||''))
        throw new Error('本机待确认排除范围无法核验');
      if(entry.phase==='accepted'&&(!entry.reservation||typeof entry.reservation.id!=='string'))
        throw new Error('本机已接受排除范围结果无法核验');
      return entry;
    }
    async function state(entry){
      const result=await api.status(entry.request_key,entry.request_sha256);
      if(!result||result.request_key!==entry.request_key||result.request_sha256!==entry.request_sha256||
         !['unknown','accepted','fenced'].includes(result.status))throw new Error('服务端排除范围状态无法核验');
      if(result.status==='accepted'&&result.reservation?.id!==entry.reservation?.id&&entry.phase==='accepted')
        throw new Error('服务端排除范围结果与本机记录不符');
      return result;
    }
    async function send(entry){
      entry={...entry,phase:'submitted',submission_state:null,error:null};await store.put(entry);
      try{
        const result=await api.submit({...entry.request,request_key:entry.request_key,retry_of:entry.retry_of});
        const write=result?.write;
        if(!write||write.request_key!==entry.request_key||write.request_sha256!==entry.request_sha256||
           write.status!=='accepted'||result.reservation?.id!==write.reservation?.id)
          throw new Error('排除范围提交结果无法核验，请查询原请求');
        entry={...entry,phase:'accepted',reservation:result.reservation,error:null};await store.put(entry);return entry;
      }catch(error){
        if(error.notSent===true){entry={...entry,submission_state:'not_sent',error:String(error.message||'请求未发送').slice(0,1000)};await store.put(entry);throw error;}
        if(error.write?.status==='fenced'&&error.write.request_key===entry.request_key&&
           error.write.request_sha256===entry.request_sha256){
          entry={...entry,phase:'fenced',error:null};await store.put(entry);return entry;
        }
        if(error.status===400){entry={...entry,phase:'rejected',error:String(error.message||'请求被拒绝').slice(0,1000)};await store.put(entry);}
        throw error;
      }
    }
    async function create(request,retryOf=null){
      const requestKey=uuid();if(!uuidPattern.test(requestKey)||requestKey===retryOf)throw new Error('排除范围请求标识无效');
      const payload={snapshot_id:String(request.snapshot_id),start_issue:String(request.start_issue),
        end_issue:String(request.end_issue),confirmed:true};
      const requestSha=await digest(payload,cryptoApi);
      const entry=valid({version:1,request_key:requestKey,retry_of:retryOf,request:payload,
        request_sha256:requestSha,phase:'submitted',created_at:new Date().toISOString(),reservation:null,error:null});
      await store.add(entry);
      return send(entry);
    }
    return {
      async begin(request){return locked('new',async()=>{
        const payload={snapshot_id:String(request.snapshot_id),start_issue:String(request.start_issue),
          end_issue:String(request.end_issue),confirmed:true};
        const matches=(await store.list()).map(valid).filter(item=>equal(item.request,payload)&&
          ['submitted','accepted','fenced'].includes(item.phase));
        const existing=matches.find(item=>item.phase==='submitted')||matches.find(item=>item.phase==='accepted')||
          matches.find(item=>item.phase==='fenced');
        if(existing){if(existing.phase==='submitted')return this.continueOriginal(existing.request_key);return this.check(existing.request_key);}
        return create(payload);
      });},
      async list(){return (await store.list()).map(valid);},
      async check(requestKey){return locked(requestKey,async()=>{
        let entry=valid(await store.get(requestKey));if(!entry)throw new Error('本机找不到这条排除范围请求');
        const result=await state(entry);
        if(result.status==='unknown'&&['accepted','fenced'].includes(entry.phase))throw new Error('本机已记录终态，但服务端当前无法确认；保留本机副本并检查数据库状态');
        if(result.status!=='unknown')entry={...entry,phase:result.status,
          ...(result.status==='accepted'?{reservation:result.reservation}:{}),error:null};
        await store.put(entry);return entry;
      });},
      async continueOriginal(requestKey){return locked(requestKey,async()=>{
        let entry=valid(await store.get(requestKey));if(!entry||entry.phase!=='submitted')throw new Error('当前排除范围请求不能继续提交');
        const result=await state(entry);if(result.status!=='unknown'){
          entry={...entry,phase:result.status,...(result.status==='accepted'?{reservation:result.reservation}:{}),error:null};
          await store.put(entry);return entry;
        }
        return send(entry);
      });},
      async fence(requestKey){return locked(requestKey,async()=>{
        let entry=valid(await store.get(requestKey));if(!entry||entry.phase!=='submitted')throw new Error('当前排除范围请求不能封止');
        try{
          const result=await api.fence({...entry.request,request_key:entry.request_key,
            request_sha256:entry.request_sha256,retry_of:entry.retry_of});
          const write=result?.write;
          if(!write||write.request_key!==entry.request_key||write.request_sha256!==entry.request_sha256||
             !['accepted','fenced'].includes(write.status))throw new Error('排除范围封止结果无法核验');
          entry={...entry,phase:write.status,...(write.status==='accepted'?{reservation:write.reservation}:{}),error:null};
          await store.put(entry);return entry;
        }catch(error){
          const write=error.write;
          if(write?.request_key===entry.request_key&&write?.request_sha256===entry.request_sha256&&
             ['accepted','fenced'].includes(write.status)){
            entry={...entry,phase:write.status,...(write.status==='accepted'?{reservation:write.reservation}:{}),error:null};
            await store.put(entry);return entry;
          }
          throw error;
        }
      });},
      async retry(requestKey){return locked(requestKey,async()=>{
        const original=valid(await store.get(requestKey));if(!original||original.phase!=='fenced')throw new Error('只有已封止排除范围请求可以关联重试');
        const result=await state(original);if(result.status!=='fenced')throw new Error('原请求封止状态无法确认');
        const prior=(await store.list()).map(valid).find(item=>item.retry_of===original.request_key);
        if(prior){if(prior.phase==='submitted')return this.continueOriginal(prior.request_key);
          if(prior.phase!=='rejected')return this.check(prior.request_key);
          throw new Error(prior.error||'该原请求已有一条明确拒绝的关联重试，请先核对服务端记录');}
        return create(original.request,original.request_key);
      });},
      async dismiss(requestKey){return locked(requestKey,async()=>{
        const entry=valid(await store.get(requestKey));if(!entry||!['accepted','fenced','rejected'].includes(entry.phase))
          throw new Error('排除范围结果尚未确认，不能清理');
        if(entry.phase!=='rejected'){const result=await state(entry);if(result.status!==entry.phase)
          throw new Error('服务端排除范围状态与本机结果不符');}
        await store.remove(requestKey);return entry;
      });}
    };
  }
  return {browserStore,coordinator};
});
