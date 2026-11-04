"""Economic invariants and official payout boundary checks, independent of backtest ranking."""
import json
from math import comb
import numpy as np
from benchmark import ROOT, TOTAL, payout_table, money, structured_portfolios, exact_states, tax

rows=sum([json.loads((ROOT/f'data/official_page{i}.json').read_text(encoding='utf-8'))['value']['list'] for i in (1,2)],[])
rows.sort(key=lambda r:r['lotteryDrawNum'])
regular=[r for r in rows if r['lotteryDrawNum'].startswith('26') and not r.get('lotteryPromotionFlag')]
prob=np.array([comb(5,a)*comb(30,5-a)*comb(2,b)*comb(10,2-b) for a in range(6) for b in range(3)])/TOTAL
assert abs(prob.sum()-1)<1e-12
assert np.array_equal(tax(np.array([9999,10000,10001,13332])), np.array([9999,10000,8000.8,10665.6]))
checked=0
for r in regular:
    if r['lotteryDrawNum']>='26014':
        table=payout_table(r)
        expected=(6666,380,200,18,7) if money(r['poolBalance'])>=800_000_000 else (5000,300,150,15,5)
        actual=tuple(table[a,b] for a,b in [(5,0),(4,1),(4,0),(3,1),(0,2)])
        assert actual==expected,(r['lotteryDrawNum'],actual,expected)
        assert table[4,2]==table[5,0] and table[3,2]==table[4,0]
        checked+=1
for name,(tickets,add) in structured_portfolios().items():
    states,weights=exact_states(tickets)
    assert int(weights.sum())==TOTAL
    # Jackpot probability counts unique full combinations, not multiplied duplicates.
    assert abs(weights[states[:,17]>0].sum()-len(set(tickets)))<1e-8
    for r in regular:
        t=payout_table(r,add).flatten()
        exact_mean=float((states@t)@weights/TOTAL)
        linear_mean=float(t@prob*len(tickets))
        assert abs(exact_mean-linear_mean)<1e-8,(name,r['lotteryDrawNum'])
r=json.loads((ROOT/'benchmark_results.json').read_text(encoding='utf-8'))
assert len(r['historical']['periods'])==106
assert len({x['issue'] for x in r['historical']['periods']})==106
assert not set(x['issue'] for x in r['historical']['periods']).intersection(r['quality']['promotional_issues'])
latest=rows[-1]
assert latest['lotteryDrawResult']=='13 18 19 20 28 09 11'
assert payout_table(latest)[5,2]==5750462 and payout_table(latest)[5,1]==47994
print(json.dumps({'status':'passed','current_rule_regular_periods_checked':checked,'portfolio_mean_invariance_checks':len(regular)*4,'exact_outcome_mass':TOTAL,'historical_periods':106,'latest_official_announcement_reconciliation':'passed','tax_boundary_checks':'passed'},ensure_ascii=False))
