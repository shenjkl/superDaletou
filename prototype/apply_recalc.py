from pathlib import Path
root=Path(__file__).resolve().parent
p=root/'app.js';s=p.read_text(encoding='utf-8')
s=s.replace("timer:null};", "timer:null,samples:200000,calcResult:null,calcResultKey:null,calcBusy:false,calcProgress:0,calcWorker:null,calcError:'',calcMessage:''};")
start=s.index('function comparison(){');end=s.index('function training(){',start)
s=s[:start]+(root/'recalc-view.inc.js').read_text(encoding='utf-8')+'\n'+s[end:]
s=s.replace("case'reset-budget':state.budget=36;navigate('comparison');break;", "case'recalculate':startCalculation();break;case'cancel-calculation':invalidateCalc();el('main').innerHTML=comparison();toast('已停止计算；没有生成新的结果。');break;case'sync-budget':{const amount=choose(state.front,5)*choose(state.back,2)*(state.additional?3:2);if(amount>=2&&amount<=360){invalidateCalc();state.budget=amount;el('main').innerHTML=comparison();toast('已同步预算，请点击重新计算。');}break;}case'reset-budget':invalidateCalc();state.budget=36;navigate('comparison');break;")
start=s.index("document.addEventListener('change',event=>{");end=s.index("document.addEventListener('input',event=>",start)
handler="""document.addEventListener('change',event=>{const t=event.target;let render=false;if(t.id==='budget'){const requested=t.value.trim()===''?36:Number(t.value);state.budget=Math.max(2,Math.min(360,Math.floor(Number.isFinite(requested)?requested:36)));if(state.budget!==requested)toast('预算已调整为支持范围内的整数：'+state.budget+' 元。');render=true;}if(t.id==='front-count'){state.front=Number(t.value);render=true;}if(t.id==='back-count'){state.back=Number(t.value);render=true;}if(t.id==='calc-additional'){state.additional=t.checked;render=true;}if(t.id==='calc-samples'){state.samples=Number(t.value);render=true;}if(render){invalidateCalc();let id=t.id;el('main').innerHTML=comparison();el(id)?.focus();}});\n"""
s=s[:start]+handler+s[end:];p.write_text(s,encoding='utf-8')
print('Installed live recalculation and synchronized comparison view.')
