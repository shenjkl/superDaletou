const fs=require('fs');
const {chromium}=require('C:/Users/申群辉/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:'C:/Program Files/Google/Chrome/Application/chrome.exe'});
 const fragment=fs.readFileSync('C:/Users/申群辉/.codex/visualizations/2026/10/29/01a1863d-3201-7063-b8df-a375e49faabf/dlt-budget-risk.html','utf8');
 let checks=[];
 for(const width of [320,736])for(const dark of [false,true]){
  const page=await browser.newPage({viewport:{width,height:900}});
  const fg=dark?'#eeeeee':'#222222',bg=dark?'#202020':'#ffffff';
  await page.setContent(`<style>:root{--foreground:${fg};--muted-foreground:${dark?'#bbbbbb':'#555555'};--viz-series-1:${dark?'#8ebcf3':'#235d9d'};--viz-series-2:${dark?'#efb171':'#975b18'};background:${bg};font:14px Arial,sans-serif}body{margin:8px}h3{font-size:16px}.text-small{font-size:12px}.text-muted{color:var(--muted-foreground)}</style>${fragment}`);
  const result=await page.evaluate(()=>({overflow:document.documentElement.scrollWidth>innerWidth,bars:document.querySelectorAll('.bar').length,root:!!document.getElementById('dlt-budget-risk')}));
  if(result.overflow||result.bars!==10||!result.root)throw Error(JSON.stringify(result));
  checks.push({width,dark,...result});
  if(width===736&&!dark)await page.screenshot({path:'D:/workSpace/daletou/research/risk_chart_preview.png',fullPage:true});
  await page.close();
 }
 await browser.close();fs.writeFileSync('D:/workSpace/daletou/research/visual_checks.json',JSON.stringify(checks,null,2));console.log(JSON.stringify(checks));
})().catch(e=>{console.error(e);process.exitCode=1});
