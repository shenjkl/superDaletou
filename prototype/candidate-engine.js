/* Deterministic research candidate construction. No model weights or forecast probabilities. */
(function(root){
'use strict';
function generate(p,history){
 const choose=(n,k)=>{let x=1;for(let i=1;i<=k;i++)x=x*(n-i+1)/i;return Math.round(x);};
 if(!['random','hot','cold'].includes(p.method))throw Error('模型权重未就绪，请先选择可用方法。');
 if(!/^\d{5}$/.test(String(p.target))||Number(p.target)!==Number(p.cutoff)+1)throw Error('当前快照仅支持数据截止后的下一目标期。');
 if(!Number.isInteger(p.budget)||p.budget<2||p.budget>360)throw Error('预算需为 2—360 元整数。');
 if(!Number.isInteger(p.seed)||p.seed<0||p.seed>4294967295)throw Error('种子需为 0—4294967295 整数。');
 if(![60,120].includes(p.window))throw Error('历史窗口需为 60 或 120 期。');
 if(!['singles','compound','dantuo'].includes(p.structure))throw Error('请选择有效组合结构。');
 if(!Number.isInteger(p.front)||p.front<5||p.front>10||!Number.isInteger(p.back)||p.back<2||p.back>5)throw Error('复式前区需为5—10个，后区需为2—5个。');
 const eligible=history.filter(r=>r.issue<=p.cutoff&&r.issue<p.target).sort((a,b)=>a.issue.localeCompare(b.issue));
 if(new Set(eligible.map(r=>r.issue)).size!==eligible.length)throw Error('历史数据存在重复期号。');
 if(eligible.length<p.window)throw Error('历史记录不足所选窗口。');
 const rows=eligible.slice(-p.window),counts=[Array(35).fill(0),Array(12).fill(0)];
 for(const row of rows)for(const [i,key] of ['front','back'].entries())for(const n of row[key])counts[i][n-1]++;
 const n=p.structure==='singles'?Math.floor(p.budget/(p.additional?3:2)):p.structure==='compound'?choose(p.front,5)*choose(p.back,2):18;
 if(n<1)throw Error('预算不足以生成一注，请增加预算。');
 const cost=n*(p.additional?3:2);if(cost>p.budget)throw Error(`所选结构需要 ${cost} 元，超过当前预算；请增加预算或减少选号。`);
 function rng(seed){let a=seed>>>0;return ()=>{a=(a+0x6D2B79F5)>>>0;let t=a;t=Math.imul(t^(t>>>15),t|1);t^=t+Math.imul(t^(t>>>7),t|61);return (t^(t>>>14))>>>0;};}
 function build(method,seed){const next=rng(seed);
  const uniform=n=>{const limit=Math.floor(4294967296/n)*n;let u;do{u=next();}while(u>=limit);return u%n;};
  function pick(size,k,region){const pool=Array.from({length:size},(_,i)=>i+1),result=[];
   while(result.length<k){let index;if(method==='random')index=uniform(pool.length);else{const weights=pool.map(x=>method==='hot'?counts[region][x-1]+1:1/(counts[region][x-1]+1));let u=(next()/4294967296)*weights.reduce((a,b)=>a+b,0);index=weights.length-1;for(let j=0;j<weights.length;j++){u-=weights[j];if(u<0){index=j;break;}}}result.push(pool.splice(index,1)[0]);}return result;
  }
  const sorted=a=>a.slice().sort((a,b)=>a-b),combinations=(a,k)=>{const out=[];function walk(start,v){if(v.length===k){out.push(v);return;}for(let i=start;i<=a.length-(k-v.length);i++)walk(i+1,[...v,a[i]]);}walk(0,[]);return out;};
  let tickets=[],pool=null;
  if(p.structure==='singles'){const seen=new Set();while(tickets.length<n){const front=sorted(pick(35,5,0)),back=sorted(pick(12,2,1)),key=front.join(',')+'|'+back.join(',');if(!seen.has(key)){seen.add(key);tickets.push({front,back});}}}
  else{const front=pick(35,p.structure==='compound'?p.front:10,0),back=sorted(pick(12,p.structure==='compound'?p.back:3,1));pool=p.structure==='compound'?{front:sorted(front),back}:{dan:sorted(front.slice(0,4)),drag:sorted(front.slice(4)),back};const fronts=p.structure==='compound'?combinations(sorted(front),5):pool.drag.map(x=>sorted([...pool.dan,x]));for(const f of fronts)for(const b of combinations(back,2))tickets.push({front:f,back:b});}
  const keys=tickets.map(t=>t.front.join(',')+'|'+t.back.join(','));if(new Set(keys).size!==n||tickets.length!==n)throw Error('组合校验失败。');
  return {method,seed:seed>>>0,tickets,pool,cost,count:n,remaining:p.budget-cost,frontCoverage:new Set(tickets.flatMap(t=>t.front)).size,backCoverage:new Set(tickets.flatMap(t=>t.back)).size};
 }
 const parameters={...p},candidate=build(p.method,p.seed),control=build('random',(p.seed^0x9e3779b9)>>>0);
 return {version:'candidate-builder-v1',methodVersion:p.method==='random'?'uniform-v1':p.method+'-laplace-v1',parameters,history:{first:rows[0].issue,last:rows.at(-1).issue,count:rows.length},candidate,control,evidence:'待评估',recommendation:'暂无证据支持优先采用'};
}
if(typeof module!=='undefined'&&module.exports)module.exports={generate};else root.generateDltCandidates=generate;
})(typeof window!=='undefined'?window:globalThis);
