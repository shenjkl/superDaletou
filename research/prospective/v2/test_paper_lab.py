"""Isolated synthetic fixture checks; never writes the actual prospective ledger."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import paper_lab as lab

class PaperLabTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=lab.ROOT/'test_scratch')
        self.root=Path(self.tmp.name)
        self.p=lab.protocol();self.p['replicates']=3
        lab.exclusive(self.root/'protocol.json',self.p)
        shutil.copyfile(lab.ROOT/'paper_lab.py',self.root/'paper_lab.py')
        lab.lock_protocol(self.root)
        self.now=lab.utcnow()
        self.source=self.root/'source.json'
        data=lab.load(lab.ROOT/'bootstrap/history.json')
        lab.exclusive(self.source,data)
        lab.exclusive(str(self.source)+'.receipt.json',{'source':lab.HISTORY_URL,'retrieved_at_utc':self.now.isoformat(),'sha256':lab.digest(self.source.read_bytes())})

    def tearDown(self):self.tmp.cleanup()

    def prepare(self):return lab.prepare(self.source,'26124','2026-10-31',self.root,self.now)

    def fake_result(self,promotion=False):
        data=lab.load(self.source);r=deepcopy(data['value']['list'][0]);r['lotteryDrawNum']='26124';r['lotteryDrawTime']='2026-10-31';r['lotterySaleEndtime']='2026-10-31 21:00:00';r['lotteryDrawStatus']=20;r['lotteryPromotionFlag']=int(promotion)
        data['value']['list'].insert(0,r)
        path=self.root/'result.json';lab.exclusive(path,data)
        lab.exclusive(str(path)+'.receipt.json',{'source':lab.HISTORY_URL,'retrieved_at_utc':self.now.isoformat(),'sha256':lab.digest(path.read_bytes())})
        return path

    def test_replay_budget_and_matched_structure(self):
        self.prepare();packet=lab.verify_packet('26124',self.root)
        self.assertEqual(len(packet['portfolios']),27)
        for port in packet['portfolios']:
            lab.validate_portfolio(port['tickets'],self.p)
            s=port['strategy'];ts=port['tickets']
            if s.startswith('compound'):
                self.assertEqual(len({x for t in ts for x in t['front']}),6)
                self.assertEqual(len(ts),18)
            if s.startswith('dantuo'):
                self.assertEqual(len(set.intersection(*(set(t['front']) for t in ts))),4)
                self.assertEqual(len({x for t in ts for x in t['front']}),10)
        self.assertEqual(lab.status(self.root)['pending_issues'],['26124'])

    def test_refuses_overwrite(self):
        seal=self.prepare()
        with self.assertRaises(FileExistsError):self.prepare()
        self.assertEqual(lab.load(self.root/'packets/26124/seal.json'),seal)

    def test_refuses_late_generation(self):
        with self.assertRaisesRegex(ValueError,'FREEZE_DEADLINE'):lab.prepare(self.source,'26124','2026-10-31',self.root,self.now+timedelta(days=4))

    def test_refuses_past_target(self):
        with self.assertRaisesRegex(ValueError,'TARGET_OR_FUTURE'):lab.prepare(self.source,'26123','2026-10-31',self.root,self.now)

    def test_refuses_stale_snapshot(self):
        receipt=lab.load(str(self.source)+'.receipt.json');receipt['retrieved_at_utc']=(self.now-timedelta(days=2)).isoformat()
        Path(str(self.source)+'.receipt.json').write_bytes(lab.canonical(receipt))
        with self.assertRaisesRegex(ValueError,'STALE'):self.prepare()

    def test_detects_packet_tamper(self):
        self.prepare();path=self.root/'packets/26124/packet.json';packet=lab.load(path);packet['portfolios'][0]['tickets'][0]['front']=[1,2,3,4,5];path.write_bytes(lab.canonical(packet))
        with self.assertRaisesRegex(ValueError,'TAMPERED'):lab.verify_packet('26124',self.root)

    def test_detects_protocol_tamper(self):
        p=lab.protocol(self.root);p['history_window']=30;(self.root/'protocol.json').write_bytes(lab.canonical(p))
        with self.assertRaisesRegex(ValueError,'PROTOCOL_CHANGED'):lab.verify_lock(self.root)

    def test_detects_broken_ledger(self):
        self.prepare();path=self.root/'ledger.jsonl';e=json.loads(path.read_text(encoding='utf-8'));e['issue']='26125';path.write_bytes(lab.canonical(e)+b'\n')
        with self.assertRaisesRegex(ValueError,'LEDGER_CHAIN'):lab.status(self.root)

    def test_tax_is_aggregated_per_alternative(self):
        row={'lotteryDrawResult':'1 2 3 4 5 1 2','prizeLevelList':[{'sort':501,'stakeAmount':'6666'}]}
        ts=[lab.ticket([1,2,3,4,6],[1,2]),lab.ticket([1,2,3,4,7],[1,2])]
        result=lab.settle_portfolio(ts,row)
        self.assertEqual(result['after_tax_yuan'],'10665.6')
        self.assertEqual(result['tax_yuan'],'2666.4')

    def test_missing_additional_blocks_instead_of_estimate(self):
        row={'lotteryDrawResult':'1 2 3 4 5 1 2','prizeLevelList':[{'sort':101,'stakeAmount':'10000000'}]}
        with self.assertRaisesRegex(ValueError,'MISSING_MATCHED_ADDITIONAL'):lab.settle_portfolio([lab.ticket([1,2,3,4,5],[1,2],True)],row)

    def test_cannot_settle_undrawn_target(self):
        self.prepare()
        with self.assertRaisesRegex(ValueError,'TARGET_NOT_DRAWN'):lab.settle(self.source,'26124',self.root,self.now)

    def test_synthetic_settlement_and_duplicate_guard(self):
        self.prepare();source=self.fake_result();answer=lab.settle(source,'26124',self.root,self.now+timedelta(days=3))
        self.assertEqual(answer['portfolios_settled'],27)
        self.assertEqual(lab.status(self.root)['valid_settled_issues'],1)
        with self.assertRaises(FileExistsError):lab.settle(source,'26124',self.root,self.now+timedelta(days=3))

    def test_promotion_is_retained_but_excluded(self):
        self.prepare();source=self.fake_result(True);answer=lab.settle(source,'26124',self.root,self.now+timedelta(days=3))
        self.assertEqual(answer['status'],'EXCLUDED_PROMOTION')
        self.assertEqual(lab.status(self.root)['valid_settled_issues'],0)
        self.assertEqual(lab.status(self.root)['excluded_promotional_issues'],['26124'])

    def test_schedule_change_excluded(self):
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

if __name__=='__main__':
    (lab.ROOT/'test_scratch').mkdir(exist_ok=True)
    unittest.main(verbosity=2)
