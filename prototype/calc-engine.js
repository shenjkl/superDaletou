/* Local probability computation. Fixed historical payout scenario, no purchases or ledger writes. */
(function (root) {
'use strict';
function computeComparison(input, reportProgress) {
  const progress=reportProgress||function(){};
  const {budget,front,back,additional}=input;
  const samples=input.samples===undefined?200000:input.samples;
  const seed=input.seed===undefined?20261029:input.seed;
  if(!Number.isInteger(budget)||budget<2||budget>360)throw Error('预算须为2—360元整数');
  if(!Number.isInteger(front)||front<5||front>12||!Number.isInteger(back)||back<2||back>6)throw Error('选号范围不合法');
  if(typeof additional!=='boolean')throw Error('追加状态不合法');
  if(!Number.isInteger(samples)||samples<1000||samples>1000000)throw Error('模拟次数不合法');
  if(!Number.isInteger(seed)||seed<0||seed>0xffffffff)throw Error('随机种子不合法');
  const scenario=input.scenario;
  if(!scenario||!scenario.levels||!scenario.issue)throw Error('缺少已核验奖金情景');
  const mapping={'5,2':101,'5,1':301,'5,0':501,'4,2':501,'4,1':601,'4,0':701,'3,2':701,'3,1':801,'2,2':801,'3,0':901,'2,1':901,'1,2':901,'0,2':901};
  const base=new Float64Array(18),added=new Float64Array(18);
  for(let a=0;a<=5;a++)for(let b=0;b<=2;b++){
    const group=mapping[a+','+b];let amount=0;
    if(group){amount=scenario.levels[group];if(!Number.isFinite(amount)||amount<0)throw Error('基本奖金缺失');}
    base[a*3+b]=amount;added[a*3+b]=amount;
    if(group===101||group===301){const extra=scenario.levels[group+100];if(!Number.isFinite(extra)||extra<0)throw Error('实际追加奖金缺失，不使用估算值');added[a*3+b]+=extra;}
  }
  function C(n,k){if(k<0||k>n||n<0)return 0;let v=1;for(let i=1;i<=k;i++)v=v*(n-i+1)/i;return Math.round(v);}
  const total=C(35,5)*C(12,2);
  const net=g=>g>10000?g*.8:g;
  function summarize(states,cost,meta){let mass=0,gross=0,after=0,win=0,profit=0,even=0;
    for(const [w,g] of states){mass+=w;gross+=w*g;after+=w*net(g);if(g>0)win+=w;if(net(g)>cost+1e-8)profit+=w;if(net(g)>=cost-1e-8)even+=w;}
    if(mass!==total)throw Error('概率质量校验失败：'+mass);
    return {...meta,cost,method:'exact',pAny:win/total,pProfit:profit/total,pBreakEven:even/total,grossExpected:gross/total,afterTaxExpected:after/total,expectedNet:after/total-cost,outcomes:total};
  }
  let unitGross=0;
  for(let a=0;a<=5;a++)for(let b=0;b<=2;b++)unitGross+=C(5,a)*C(30,5-a)*C(2,b)*C(10,2-b)*base[a*3+b]/total;
  let addedUnitGross=0;
  for(let a=0;a<=5;a++)for(let b=0;b<=2;b++)addedUnitGross+=C(5,a)*C(30,5-a)*C(2,b)*C(10,2-b)*added[a*3+b]/total;
  const combinationCount=C(front,5)*C(back,2),compoundCost=combinationCount*(additional?3:2),compoundStates=[];
  for(let a=0;a<=5;a++)for(let b=0;b<=2;b++){
    const w=C(front,a)*C(35-front,5-a)*C(back,b)*C(12-back,2-b);if(!w)continue;
    let gross=0;
    for(let fh=0;fh<=5;fh++)for(let bh=0;bh<=2;bh++)gross+=C(a,fh)*C(front-a,5-fh)*C(b,bh)*C(back-b,2-bh)*(additional?added:base)[fh*3+bh];
    compoundStates.push([w,gross]);
  }
  const compound=summarize(compoundStates,compoundCost,{id:'compound',name:`前 ${front} 后 ${back} 复式${additional?'（追加）':''}`,tickets:combinationCount,pFirst:combinationCount/total});
  const danStates=[];
  for(let d=0;d<=4;d++)for(let t=0;t<=5-d;t++)for(let b=0;b<=2;b++){
    const w=C(4,d)*C(6,t)*C(25,5-d-t)*C(3,b)*C(9,2-b);if(!w)continue;
    let gross=0;
    for(let bh=0;bh<=2;bh++)gross+=(t*base[(d+1)*3+bh]+(6-t)*base[d*3+bh])*C(b,bh)*C(3-b,2-bh);
    danStates.push([w,gross]);
  }
  const dantuo=summarize(danStates,36,{id:'dantuo',name:'前区4胆6拖、后区3个',tickets:18,pFirst:18/total});
  const basicN=Math.floor(budget/2),additionalN=Math.floor(budget/3),repeatStates=[];
  for(let a=0;a<=5;a++)for(let b=0;b<=2;b++){const w=C(5,a)*C(30,5-a)*C(2,b)*C(10,2-b);if(w)repeatStates.push([w,base[a*3+b]*basicN]);}
  const repeat=summarize(repeatStates,basicN*2,{id:'repeat',name:`同号重复 ${basicN} 注`,tickets:basicN,uniqueTickets:1,pFirst:1/total});
  progress({percent:8,message:'复式、胆拖和重复投注精确计算完成'});
  // Reproducible uniform sampling, with rejection to avoid integer modulo bias.
  let randomState=seed>>>0;
  function u32(){let t=randomState=(randomState+0x6D2B79F5)>>>0;t=Math.imul(t^(t>>>15),t|1);t^=t+Math.imul(t^(t>>>7),t|61);return (t^(t>>>14))>>>0;}
  function integer(n){const limit=4294967296-(4294967296%n);let x;do{x=u32();}while(x>=limit);return x%n;}
  function draw(){let lo=0,hi=0,b=0,count=0;while(count<5){const x=integer(35);if(x<32){const bit=1<<x;if(lo&bit)continue;lo|=bit;}else{const bit=1<<(x-32);if(hi&bit)continue;hi|=bit;}count++;}count=0;while(count<2){const bit=1<<integer(12);if(b&bit)continue;b|=bit;count++;}return {lo,hi,b};}
  function pop(x){x=x-((x>>>1)&0x55555555);x=(x&0x33333333)+((x>>>2)&0x33333333);return (((x+(x>>>4))&0x0f0f0f0f)*0x01010101)>>>24;}
  const tickets=[],seen=new Set();while(tickets.length<Math.max(basicN,additionalN)){const t=draw(),key=t.lo+':'+t.hi+':'+t.b;if(!seen.has(key)){seen.add(key);tickets.push(t);}}
  const bStats={any:0,profit:0,even:0},aStats={any:0,profit:0,even:0};
  function addStats(s,g,cost){if(g>0)s.any++;const v=net(g);if(v>cost+1e-8)s.profit++;if(v>=cost-1e-8)s.even++;}
  for(let n=0;n<samples;n++){
    const d=draw();let bg=0,ag=0;
    for(let i=0;i<tickets.length;i++){const t=tickets[i];const key=(pop(d.lo&t.lo)+pop(d.hi&t.hi))*3+pop(d.b&t.b);if(i<basicN)bg+=base[key];if(i<additionalN)ag+=added[key];}
    addStats(bStats,bg,basicN*2);addStats(aStats,ag,additionalN*3);
    if(n%10000===0)progress({percent:8+Math.floor(90*n/samples),message:`分散单式模拟 ${(n/10000).toFixed(0)} / ${(samples/10000).toFixed(0)} 万次`});
  }
  function interval(k){const p=k/samples,z=1.959963984540054,z2=z*z,den=1+z2/samples;const c=(p+z2/(2*samples))/den,h=z*Math.sqrt(p*(1-p)/samples+z2/(4*samples*samples))/den;return [k===0?0:Math.max(0,c-h),k===samples?1:Math.min(1,c+h)];}
  function mc(s,n,add,id){if(n===0)return {id,name:'预算不足一注追加',tickets:0,cost:0,method:'exact',pAny:0,pProfit:0,pBreakEven:1,pFirst:0,grossExpected:0,afterTaxExpected:0,expectedNet:0};return {id,name:`分散单式 ${n} 注${add?'（追加）':''}`,tickets:n,cost:n*(add?3:2),method:'monte-carlo',samples,seed,pAny:s.any/samples,pProfit:s.profit/samples,pBreakEven:s.even/samples,anyInterval:interval(s.any),profitInterval:interval(s.profit),pFirst:n/total,grossExpected:n*(add?addedUnitGross:unitGross),afterTaxExpected:null,expectedNet:null};}
  progress({percent:100,message:'计算完成'});
  return {parameters:{budget,front,back,additional,samples,seed},scenarioIssue:scenario.issue,combinationCount,compoundCost,totalOutcomes:total,rows:[mc(bStats,basicN,false,'random-basic'),compound,dantuo,mc(aStats,additionalN,true,'random-additional'),repeat,{id:'no-purchase',name:'不参与购买',cost:0,tickets:0,method:'exact',pAny:0,pProfit:0,pBreakEven:1,pFirst:0,grossExpected:0,afterTaxExpected:0,expectedNet:0}],assumption:'按已公布单注奖金固定结算，未重算假设新增中奖注导致的浮动奖均分、奖池及限额变化。随机模拟只针对本次固定种子生成的组合；区间仅反映抽样误差。'};
}
root.computeDltComparison=computeComparison;
if(typeof module!=='undefined'&&module.exports)module.exports={computeComparison};
})(typeof window!=='undefined'?window:globalThis);
