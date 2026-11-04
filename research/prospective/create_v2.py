"""Preserve the frozen v1 packet and create a separate protocol version for timing guards."""
from pathlib import Path
import json
import shutil

root=Path(__file__).resolve().parent
dest=root/'v2'
dest.mkdir(exist_ok=False)
p=json.loads((root/'protocol.json').read_text(encoding='utf-8'))
p['experiment_id']='dlt-paper-v2';p['version']=2;p['seed_namespace']='dlt-paper-v2-frozen-20261029'
p['timing_eligibility']='Require generation before the actual official sale-end timestamp, source-receipt retrieval before generation, and actual draw date equal to planned date; otherwise retain as excluded anomaly. Missing official sale-end blocks settlement.'
(dest/'protocol.json').write_text(json.dumps(p,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
s=(root/'paper_lab.py').read_text(encoding='utf-8')
s=s.replace("if now.astimezone(ZONE).date()<datetime.fromisoformat(row['lotteryDrawTime']).date():raise ValueError('FUTURE_RESULT')", "if now.astimezone(ZONE).date()<datetime.fromisoformat(row['lotteryDrawTime']).date():raise ValueError('FUTURE_RESULT')\n    try:sale_end=datetime.strptime(row['lotterySaleEndtime'],'%Y-%m-%d %H:%M:%S').replace(tzinfo=ZONE)\n    except (KeyError,ValueError):raise ValueError('MISSING_OFFICIAL_SALE_END')\n    if now<sale_end:raise ValueError('SALE_NOT_ENDED')")
needle="    if result['status']=='SETTLED':\n        for port in packet['portfolios']:"
replacement="    generated=datetime.fromisoformat(packet['generated_at_utc'])\n    source_captured=datetime.fromisoformat(packet['source_receipt']['retrieved_at_utc'])\n    if result['status']=='SETTLED':\n        if source_captured>generated:result['status']='EXCLUDED_SOURCE_TIMING'\n        elif generated>=sale_end:result['status']='EXCLUDED_LATE_RECORD'\n        elif result['planned_date_changed']:result['status']='EXCLUDED_SCHEDULE_CHANGE'\n    if result['status']=='SETTLED':\n        for port in packet['portfolios']:"
assert needle in s;s=s.replace(needle,replacement)
s=s.replace("pending=[];excluded=[];by_strategy", "pending=[];excluded=[];excluded_records=[];by_strategy")
s=s.replace("if e['kind'] in ('SETTLED','EXCLUDED_PROMOTION')", "if e['kind']=='SETTLED' or e['kind'].startswith('EXCLUDED_')")
s=s.replace("if result['status']=='EXCLUDED_PROMOTION':excluded.append(issue);continue", "if result['status'].startswith('EXCLUDED_'):\n                excluded_records.append({'issue':issue,'reason':result['status']})\n                if result['status']=='EXCLUDED_PROMOTION':excluded.append(issue)\n                continue")
s=s.replace("'excluded_promotional_issues':excluded,'replicates'", "'excluded_promotional_issues':excluded,'excluded_records':excluded_records,'replicates'")
(dest/'paper_lab.py').write_text(s,encoding='utf-8')
t=(root/'test_paper_lab.py').read_text(encoding='utf-8')
t=t.replace("r['lotteryDrawTime']='2026-10-31';r['lotteryDrawStatus']", "r['lotteryDrawTime']='2026-10-31';r['lotterySaleEndtime']='2026-10-31 21:00:00';r['lotteryDrawStatus']")
extra='''    def test_schedule_change_excluded(self):
        self.prepare();source=self.fake_result();data=lab.load(source);data['value']['list'][0]['lotteryDrawTime']='2026-11-02';data['value']['list'][0]['lotterySaleEndtime']='2026-11-02 21:00:00';source.write_bytes(lab.canonical(data))
        receipt=lab.load(str(source)+'.receipt.json');receipt['sha256']=lab.digest(source.read_bytes());Path(str(source)+'.receipt.json').write_bytes(lab.canonical(receipt))
        answer=lab.settle(source,'26124',self.root,self.now+timedelta(days=6))
        self.assertEqual(answer['status'],'EXCLUDED_SCHEDULE_CHANGE')
        self.assertEqual(lab.status(self.root)['valid_settled_issues'],0)

    def test_actual_sale_end_blocks_late_record(self):
        self.prepare();source=self.fake_result();data=lab.load(source);data['value']['list'][0]['lotterySaleEndtime']='2026-10-29 00:00:00';source.write_bytes(lab.canonical(data))
        receipt=lab.load(str(source)+'.receipt.json');receipt['sha256']=lab.digest(source.read_bytes());Path(str(source)+'.receipt.json').write_bytes(lab.canonical(receipt))
        answer=lab.settle(source,'26124',self.root,self.now+timedelta(days=3))
        self.assertEqual(answer['status'],'EXCLUDED_LATE_RECORD')
        self.assertEqual(lab.status(self.root)['valid_settled_issues'],0)

'''
assert "if __name__=='__main__':" in t
t=t.replace("if __name__=='__main__':",extra+"if __name__=='__main__':")
(dest/'test_paper_lab.py').write_text(t,encoding='utf-8')
(dest/'bootstrap').mkdir();shutil.copyfile(root/'bootstrap/history.json',dest/'bootstrap/history.json')
(root/'superseded.json').write_text(json.dumps({'superseded_by':'v2','reason':'Add explicit official sale-end and schedule-change eligibility guards; v1 pending packet is archived and excluded from active research.','active_results_path':'v2'},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print('Created separate v2; original locked protocol/engine/packet preserved.')
