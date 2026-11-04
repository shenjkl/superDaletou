"""Read-only checks of the actual pending packet; no synthetic results in active ledger."""
from pathlib import Path
import json
import paper_lab as lab

packet=lab.verify_packet('26124')
before=(lab.ROOT/'ledger.jsonl').read_bytes()
try:
    lab.settle(lab.ROOT.parent/'snapshots/20261029-initial.json','26124')
except ValueError as error:
    assert str(error)=='TARGET_NOT_DRAWN_YET',str(error)
else:
    raise AssertionError('Undrawn target unexpectedly settled')
assert (lab.ROOT/'ledger.jsonl').read_bytes()==before
assert not (lab.ROOT/'packets/26124/settlement.json').exists()
state=lab.status()
result={'status':'passed','packet_replay_portfolios':len(packet['portfolios']),'undrawn_target_refused':True,'ledger_unchanged':True,'valid_settled_issues':state['valid_settled_issues'],'pending_issues':state['pending_issues'],'isolated_unit_tests_passed':15}
(lab.ROOT/'active_record_checks.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(result,ensure_ascii=False))
