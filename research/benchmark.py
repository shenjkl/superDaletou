"""Independent DLT benchmark: official-data audit, exact distributions, paired paper backtest.

Historical payouts are held at published amounts (small-player, price-taking scenario).
Promotional draws are excluded from returns. No predictions for future purchase.
"""
from pathlib import Path
from itertools import combinations
from collections import Counter
from math import comb, sqrt
import hashlib
import json
import numpy as np

ROOT = Path(__file__).resolve().parent
TOTAL = comb(35, 5) * comb(12, 2)
SEEDS = 500
MC = 500_000
COST = 36
NEW = {(5,2):101,(5,1):301,(5,0):501,(4,2):501,(4,1):601,(4,0):701,(3,2):701,(3,1):801,(2,2):801,(3,0):901,(2,1):901,(1,2):901,(0,2):901}
OLD = {(5,2):101,(5,1):301,(5,0):501,(4,2):601,(4,1):701,(3,2):801,(4,0):901,(3,1):1001,(2,2):1001,(3,0):1101,(2,1):1101,(1,2):1101,(0,2):1101}

def money(v):
    try:
        return float(str(v).replace(',', ''))
    except ValueError:
        return None

def tax(gross):
    return np.where(gross > 10000, gross * .8, gross)

def payout_table(r, additional=False):
    amounts = {int(p['sort']): money(p['stakeAmount']) for p in r['prizeLevelList']}
    mapping = NEW if r['lotteryDrawNum'] >= '26014' else OLD
    table = np.zeros((6,3),dtype=float)
    for hits, group in mapping.items():
        assert amounts[group] is not None, (r['lotteryDrawNum'],group)
        table[hits] = amounts[group]
    if additional:
        for hits, group in [((5,2),201),((5,1),401)]:
            # Published additional award absent when no additional winner: use floor(80%).
            extra = amounts.get(group)
            if extra is None:
                extra = float(int(table[hits] * .8))
            table[hits] += extra
    return table

def audit(rows, github):
    issues = [r['lotteryDrawNum'] for r in rows]
    valid = []
    for r in rows:
        n = list(map(int,r['lotteryDrawResult'].split()))
        if len(n)!=7 or len(set(n[:5]))!=5 or len(set(n[5:]))!=2 or not all(1<=x<=35 for x in n[:5]) or not all(1<=x<=12 for x in n[5:]):
            valid.append(r['lotteryDrawNum'])
    gh = {r['issue']:r for r in github}
    mismatches = {k:[] for k in ['numbers','date','sales','pool']}
    for r in rows:
        issue=r['lotteryDrawNum']
        if issue not in gh: continue
        g=gh[issue]; nums=list(map(int,r['lotteryDrawResult'].split()))
        if g['front']!=nums[:5] or g['back']!=nums[5:]: mismatches['numbers'].append(issue)
        if g['date']!=r['lotteryDrawTime']: mismatches['date'].append(issue)
        if g['sales']!=int(money(r['totalSaleAmount'])): mismatches['sales'].append(issue)
        if abs(g['pool']-money(r['poolBalanceAfterdraw']))>=1: mismatches['pool'].append(issue)
    year=[r for r in rows if r['lotteryDrawNum'].startswith('26')]
    return {'official_rows':len(rows),'duplicates':len(issues)-len(set(issues)),'invalid_numbers':valid,'first':issues[0],'last':issues[-1], 'year2026_rows':len(year),'missing_2026_issues':sorted(set(f'26{i:03}' for i in range(1,124))-set(issues)), 'github_comparison_rows':sum(i in gh for i in issues), 'github_mismatches':mismatches, 'promotional_issues':[r['lotteryDrawNum'] for r in year if r.get('lotteryPromotionFlag')], 'high_pool_regular_new_issues':[r['lotteryDrawNum'] for r in year if r['lotteryDrawNum']>='26014' and not r.get('lotteryPromotionFlag') and money(r['poolBalance'])>=800_000_000]}

def structured_portfolios():
    back3=list(combinations(range(1,4),2))
    compound=[(f,b) for f in combinations(range(1,7),5) for b in back3]
    dan=[(tuple(range(1,5))+(d,),b) for d in range(5,11) for b in back3]
    repeated=[(tuple(range(1,6)),(1,2))]*18
    add=[((1,2)+f,b) for f in combinations(range(3,7),3) for b in back3]
    return {'compound_6_3':(compound,False),'dantuo_4_6_3':(dan,False),'repeat_one_18x':(repeated,False),'additional_dantuo_2_4_3':(add,True)}

def exact_states(tickets):
    uf=sorted(set(x for f,b in tickets for x in f)); ub=sorted(set(x for f,b in tickets for x in b))
    assert len(uf)<=10 and len(ub)<=3
    states=[]; weights=[]
    for nf in range(6):
        if nf>len(uf) or 5-nf>35-len(uf): continue
        for f in combinations(uf,nf):
            fs=set(f); fw=comb(35-len(uf),5-nf)
            for nb in range(3):
                if nb>len(ub) or 2-nb>12-len(ub): continue
                for b in combinations(ub,nb):
                    bs=set(b); hist=np.zeros(18,dtype=np.int16)
                    for tf,tb in tickets: hist[len(fs.intersection(tf))*3+len(bs.intersection(tb))]+=1
                    states.append(hist);weights.append(fw*comb(12-len(ub),2-nb))
    assert sum(weights)==TOTAL
    return np.array(states),np.array(weights,dtype=float)

def summarize_distribution(gross, weights, cost=COST):
    weights=weights/weights.sum(); net=tax(gross)
    return {'expected_gross':float(gross@weights),'expected_after_tax':float(net@weights),'expected_net_profit':float(net@weights-cost),'after_tax_roi':float(net@weights/cost-1), 'p_any_prize':float(weights[gross>0].sum()),'p_break_even':float(weights[net>=cost].sum()),'p_profit':float(weights[net>cost].sum()),'p_zero':float(weights[gross==0].sum())}

def random_tickets(rng, count, samples, weights=None):
    wf=np.ones(35) if weights is None else weights[0]
    wb=np.ones(12) if weights is None else weights[1]
    f=np.argpartition(np.log(rng.random((samples,count,35)))/wf,-5,axis=2)[:,:,-5:]+1
    b=np.argpartition(np.log(rng.random((samples,count,12)))/wb,-2,axis=2)[:,:,-2:]+1
    # All tickets must be distinct within a sample/issue, equal spending/coverage.
    for i in range(1,count):
        for j in range(i):
            duplicate=(np.sort(f[:,i],axis=1)==np.sort(f[:,j],axis=1)).all(axis=1)&(np.sort(b[:,i],axis=1)==np.sort(b[:,j],axis=1)).all(axis=1)
            while duplicate.any():
                k=int(duplicate.sum()); f[duplicate,i],b[duplicate,i]=[v[:,0] for v in random_tickets(rng,1,k,weights)]
                duplicate=(np.sort(f[:,i],axis=1)==np.sort(f[:,j],axis=1)).all(axis=1)&(np.sort(b[:,i],axis=1)==np.sort(b[:,j],axis=1)).all(axis=1)
    return f,b

def hit_payout(f,b,actual_f,actual_b,table):
    fh=np.isin(f,actual_f).sum(axis=2); bh=np.isin(b,actual_b).sum(axis=2)
    return table[fh,bh].sum(axis=1)

def historical(rows):
    names=['random18','hot60_18','cold60_18','compound_6_3','dantuo_4_6_3','random12_additional','repeat_one_18x']
    returns={n:[] for n in names}; per_issue=[]
    for idx,r in enumerate(rows):
        if not r['lotteryDrawNum'].startswith('26') or r.get('lotteryPromotionFlag'):continue
        past=rows[max(0,idx-60):idx]
        if len(past)<60: continue
        freq_f=np.ones(35);freq_b=np.ones(12)
        for p in past:
            nums=list(map(int,p['lotteryDrawResult'].split()));freq_f[np.array(nums[:5])-1]+=1;freq_b[np.array(nums[5:])-1]+=1
        nums=list(map(int,r['lotteryDrawResult'].split())); af,ab=nums[:5],nums[5:]
        bt=payout_table(r); at=payout_table(r,True)
        seed=int.from_bytes(hashlib.sha256(('paper-v1-'+r['lotteryDrawNum']).encode()).digest()[:8],'big')
        for name in names:
            rng=np.random.default_rng(seed) # common random variates across weighted strategies
            if name in ['random18','hot60_18','cold60_18','random12_additional','repeat_one_18x']:
                count=12 if name=='random12_additional' else (1 if name=='repeat_one_18x' else 18)
                weights=(freq_f,freq_b) if name=='hot60_18' else ((1/freq_f,1/freq_b) if name=='cold60_18' else None)
                f,b=random_tickets(rng,count,SEEDS,weights)
                gross=hit_payout(f,b,af,ab,at if name=='random12_additional' else bt)
                if name=='repeat_one_18x':gross*=18
            else:
                pickf=np.argsort(np.log(rng.random((SEEDS,35)))/freq_f,axis=1)[:,::-1]+1
                pickb=np.argsort(np.log(rng.random((SEEDS,12)))/freq_b,axis=1)[:,::-1]+1
                if name=='compound_6_3':
                    inds=np.array(list(combinations(range(6),5))); f=pickf[:,inds]; backs=np.array(list(combinations(range(3),2)))
                    f=np.repeat(f,3,axis=1);b=np.tile(pickb[:,backs],(1,6,1))
                else:
                    f=np.concatenate([np.repeat(pickf[:,:4][:,None,:],6,axis=1),pickf[:,4:10,None]],axis=2)
                    f=np.repeat(f,3,axis=1);backs=np.array(list(combinations(range(3),2)));b=np.tile(pickb[:,backs],(1,6,1))
                gross=hit_payout(f,b,af,ab,bt)
            net=tax(gross);returns[name].append(net)
        per_issue.append({'issue':r['lotteryDrawNum'],'date':r['lotteryDrawTime']})
    result={}
    baseline=np.stack(returns['random18'],axis=1)
    for name, values in returns.items():
        a=np.stack(values,axis=1); total=a.sum(axis=1); roi=total/(a.shape[1]*COST)-1
        diff=a-baseline; draw_diff=diff.mean(axis=0)
        se=float(draw_diff.std(ddof=1)/sqrt(len(draw_diff)))
        result[name]={'issues':a.shape[1],'cost_per_run':a.shape[1]*COST,'runs':SEEDS,'median_roi':float(np.median(roi)), 'roi_p05_p95':list(map(float,np.quantile(roi,[.05,.95]))),'mean_roi':float(roi.mean()),'fraction_runs_profit':float((roi>0).mean()),'mean_prize_issue_rate':float((a>0).mean()),'mean_break_even_issue_rate':float((a>=COST).mean()), 'mean_paired_net_prize_difference_per_issue':float(draw_diff.mean()),'paired_issue_normal95_interval':[float(draw_diff.mean()-1.96*se),float(draw_diff.mean()+1.96*se)],'largest_single_draw_share_of_all_returns':float(a.max()/a.sum())}
    return {'scope':'historical descriptive sensitivity, not untouched validation; 500 randomized runs share the same actual draw sequence; intervals use draw-level paired differences, not 500 independent histories','periods':per_issue,'strategies':result}

def main():
    rows=sum([json.loads((ROOT/f'data/official_page{i}.json').read_text(encoding='utf-8'))['value']['list'] for i in (1,2)],[])
    rows.sort(key=lambda r:r['lotteryDrawNum'])
    github=json.loads((ROOT/'data/github_history.json').read_text(encoding='utf-8'))
    quality=audit(rows,github)
    assert not quality['duplicates'] and not quality['invalid_numbers'] and not quality['missing_2026_issues']
    regular=[r for r in rows if r['lotteryDrawNum'].startswith('26') and not r.get('lotteryPromotionFlag')]
    # Exact single-ticket theoretical response to each published payout schedule.
    counts=np.array([comb(5,a)*comb(30,5-a)*comb(2,b)*comb(10,2-b) for a in range(6) for b in range(3)])
    unit=[]
    for r in regular:
        bt=payout_table(r).flatten(); at=payout_table(r,True).flatten()
        unit.append({'issue':r['lotteryDrawNum'],'basic_gross_per_yuan':float(bt@counts/TOTAL/2),'additional_gross_per_yuan':float(at@counts/TOTAL/3),'basic_after_tax_per_yuan_single':float(tax(bt)@counts/TOTAL/2),'additional_after_tax_per_yuan_single':float(tax(at)@counts/TOTAL/3)})
    exact={}
    for name,(tickets,additional) in structured_portfolios().items():
        states,weights=exact_states(tickets); results=[]
        for r in regular:
            gross=states@payout_table(r,additional).flatten(); results.append(summarize_distribution(gross,weights))
        exact[name]={'n_tickets':len(tickets),'unique_tickets':len(set(tickets)),'cost':COST,'all_outcomes':TOTAL,'averaged_over_regular_2026_payout_schedules':{k:float(np.mean([v[k] for v in results])) for k in results[0]},'latest_schedule_26123':results[-1]}
    # Sample fixed random portfolios against IID uniform draws; rates only, rare-prize means omitted.
    rng=np.random.default_rng(20261029); tf,tb=random_tickets(rng,18,1);tf,tb=tf[0],tb[0]
    rate_counts={'random18':Counter(),'random12_additional':Counter()}
    latest=regular[-1]
    for start in range(0,MC,10000):
        n=min(10000,MC-start);df=np.argpartition(rng.random((n,35)),-5,axis=1)[:,-5:]+1;db=np.argpartition(rng.random((n,12)),-2,axis=1)[:,-2:]+1
        fh=(tf[None,:,:,None]==df[:,None,None,:]).any(axis=3).sum(axis=2); bh=(tb[None,:,:,None]==db[:,None,None,:]).any(axis=3).sum(axis=2)
        for name,k,add in [('random18',18,False),('random12_additional',12,True)]:
            gross=payout_table(latest,add)[fh[:,:k],bh[:,:k]].sum(axis=1);net=tax(gross)
            rate_counts[name].update({'any':int((gross>0).sum()),'break_even':int((net>=COST).sum()),'profit':int((net>COST).sum())})
    mc={name:{metric:{'rate':v/MC,'mc_standard_error':sqrt((v/MC)*(1-v/MC)/MC)} for metric,v in counts.items()} for name,counts in rate_counts.items()}
    report={'quality':quality,'unit_expectations':unit,'structured_exact':exact,'random_portfolio_mc':{'draws':MC,'seed':20261029,'scope':'one fixed random 18-ticket portfolio and its first 12 tickets, latest 26123 payout schedule; sampling uncertainty only; not an optimum','results':mc},'historical':historical(rows)}
    (ROOT/'benchmark_results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'quality':quality,'unit_mean':{k:float(np.mean([u[k] for u in unit])) for k in unit[0] if k!='issue'},'additional_better_gross_issues':sum(u['additional_gross_per_yuan']>u['basic_gross_per_yuan'] for u in unit),'exact':exact,'mc':mc,'historical':report['historical']['strategies']},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
