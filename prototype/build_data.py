"""Project reviewed research snapshots into the offline prototype; no research writes."""
from pathlib import Path
import json

root=Path(__file__).resolve().parent
research=root.parent/'research'
load=lambda p:json.loads(p.read_text(encoding='utf-8'))
results=load(research/'benchmark_results.json')
official=load(research/'data/official_page1.json')['value']['list']
packet=load(research/'prospective/v2/packets/26124/packet.json')
state=load(research/'prospective/v2/state_snapshot_20261029.json')
seal=load(research/'prospective/v2/packets/26124/seal.json')
data={'snapshotDate':'2026-10-29','issue':'26123','state':state,'seal':seal,'lock':load(research/'prospective/v2/protocol.lock.json'),'generatedAt':packet['generated_at_utc'],'cutoff':packet['training_cutoff_issue'],'historyFirst':packet['training_first_issue'],'quality':results['quality'],'historical':results['historical']['strategies'],'historicalIssues':[str(row['issue']) for row in results['historical']['periods']],'draws':[{'issue':r['lotteryDrawNum'],'date':r['lotteryDrawTime'],'front':list(map(int,r['lotteryDrawResult'].split()[:5])),'back':list(map(int,r['lotteryDrawResult'].split()[5:])),'sales':r['totalSaleAmount'],'pool':r['poolBalanceAfterdraw']} for r in official[:12]],'grossExpectedPerYuan':{'basic':.5663142549956921,'additional':.5124900155804628}}
data['payoutScenario']={'issue':official[0]['lotteryDrawNum'],'levels':{str(p['sort']):int(str(p['stakeAmount']).replace(',','')) for p in official[0]['prizeLevelList'] if str(p['stakeAmount']).replace(',','').isdigit()}}
history=official+load(research/'data/official_page2.json')['value']['list']
data['generationHistory']=[{'issue':r['lotteryDrawNum'],'front':list(map(int,r['lotteryDrawResult'].split()[:5])),'back':list(map(int,r['lotteryDrawResult'].split()[5:]))} for r in history]
import hashlib
data['generationHistorySha256']=hashlib.sha256(json.dumps(data['generationHistory'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
(root/'data.js').write_text('window.RESEARCH_DATA = '+json.dumps(data,ensure_ascii=False,separators=(',',':'))+';\n',encoding='utf-8')
print('Built data.js from reviewed snapshots.')
