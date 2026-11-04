"""Frozen prospective paper ledger. No purchase, notification or scheduler integration."""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, time, timezone, timedelta
from decimal import Decimal
import hashlib
from itertools import combinations
import json
import math
import os
from pathlib import Path
import random
import re
import statistics
import sys
import urllib.request

ROOT=Path(__file__).resolve().parent
ZONE=timezone(timedelta(hours=8))
HISTORY_URL='https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?gameNo=85&provinceId=0&pageSize=100&isVerify=1&pageNo=1'
RULES={(5,2):101,(5,1):301,(5,0):501,(4,2):501,(4,1):601,(4,0):701,(3,2):701,(3,1):801,(2,2):801,(3,0):901,(2,1):901,(1,2):901,(0,2):901}

def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
def digest(value):return hashlib.sha256(value).hexdigest()
def load(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def utcnow():return datetime.now(timezone.utc)

def exclusive(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as f:
        f.write(canonical(value));f.flush();os.fsync(f.fileno())

def protocol(root=ROOT):return load(root/'protocol.json')

def lock_protocol(root=ROOT):
    p=protocol(root)
    record={'experiment_id':p['experiment_id'],'protocol_sha256':digest(canonical(p)), 'engine_sha256':digest((root/'paper_lab.py').read_bytes()),'locked_at_utc':utcnow().isoformat(),'python':sys.version,'timestamp_assurance':'local-clock-and-chat-hash-only'}
    exclusive(root/'protocol.lock.json',record)
    return record

def verify_lock(root=ROOT):
    p=protocol(root);lock=load(root/'protocol.lock.json')
    if lock['protocol_sha256']!=digest(canonical(p)):raise ValueError('PROTOCOL_CHANGED')
    if lock['engine_sha256']!=digest((root/'paper_lab.py').read_bytes()):raise ValueError('ENGINE_CHANGED')
    return lock

@contextmanager
def ledger_guard(root):
    path=root/'.ledger-write.lock'
    with path.open('x') as f:f.write(str(utcnow()))
    try:yield
    finally:path.unlink()

def ledger_events(root=ROOT):
    path=root/'ledger.jsonl';events=[];prev='0'*64
    if not path.exists():return events
    for line in path.read_text(encoding='utf-8').splitlines():
        event=json.loads(line);h=event.pop('event_hash')
        if event['previous_hash']!=prev or digest(canonical(event))!=h:raise ValueError('LEDGER_CHAIN_BROKEN')
        event['event_hash']=h;events.append(event);prev=h
    return events

def append_event(root, kind, issue, payload_hash):
    events=ledger_events(root)
    if any(e['kind']==kind and e['issue']==issue for e in events):raise ValueError('DUPLICATE_LEDGER_EVENT')
    e={'kind':kind,'issue':issue,'payload_sha256':payload_hash,'recorded_at_utc':utcnow().isoformat(),'previous_hash':events[-1]['event_hash'] if events else '0'*64}
    e['event_hash']=digest(canonical(e))
    with (root/'ledger.jsonl').open('ab') as f:f.write(canonical(e)+b'\n');f.flush();os.fsync(f.fileno())

def official_rows(payload):
    if payload.get('success') is not True or str(payload.get('errorCode'))!='0':raise ValueError('OFFICIAL_REQUEST_FAILED')
    rows=payload.get('value',{}).get('list',[])
    if not rows:raise ValueError('EMPTY_HISTORY')
    issues=[]
    for row in rows:
        issue=str(row.get('lotteryDrawNum',''));nums=row.get('lotteryDrawResult','').split()
        if not re.fullmatch(r'\d{5}',issue) or str(row.get('lotteryGameNum'))!='85':raise ValueError('WRONG_GAME_OR_ISSUE')
        if len(nums)!=7:raise ValueError('INCOMPLETE_DRAW')
        f,b=list(map(int,nums[:5])),list(map(int,nums[5:]))
        if len(set(f))!=5 or len(set(b))!=2 or not all(1<=n<=35 for n in f) or not all(1<=n<=12 for n in b):raise ValueError('INVALID_NUMBERS')
        datetime.fromisoformat(row['lotteryDrawTime']);issues.append(issue)
    if len(set(issues))!=len(issues):raise ValueError('DUPLICATE_DRAW')
    return sorted(rows,key=lambda r:r['lotteryDrawNum'])

def refresh(output):
    req=urllib.request.Request(HISTORY_URL,headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.sporttery.cn/'})
    raw=urllib.request.urlopen(req,timeout=25).read();data=json.loads(raw);rows=official_rows(data)
    path=Path(output);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as f:f.write(raw)
    exclusive(str(path)+'.receipt.json',{'retrieved_at_utc':utcnow().isoformat(),'source':HISTORY_URL,'sha256':digest(raw),'latest_issue':rows[-1]['lotteryDrawNum']})
    return {'file':str(path),'latest_issue':rows[-1]['lotteryDrawNum'],'rows':len(rows)}

def get_history(source, issue, now, p):
    source=Path(source);raw=source.read_bytes();receipt=load(str(source)+'.receipt.json')
    if receipt['sha256']!=digest(raw) or receipt['source']!=HISTORY_URL:raise ValueError('SOURCE_RECEIPT_MISMATCH')
    captured=datetime.fromisoformat(receipt['retrieved_at_utc'])
    if not -300<=(now-captured).total_seconds()<=86400:raise ValueError('STALE_OR_FUTURE_SOURCE')
    rows=official_rows(json.loads(raw))
    if any(r['lotteryDrawNum']>=issue for r in rows):raise ValueError('TARGET_OR_FUTURE_ALREADY_IN_HISTORY')
    if int(issue)!=int(rows[-1]['lotteryDrawNum'])+1:raise ValueError('TARGET_NOT_NEXT_ISSUE')
    history=rows[-p['history_window']:]
    if len(history)!=p['history_window']:raise ValueError('INSUFFICIENT_HISTORY')
    if any(datetime.fromisoformat(r['lotteryDrawTime']).date()>now.astimezone(ZONE).date() for r in history):raise ValueError('FUTURE_DATED_HISTORY')
    seq=[int(r['lotteryDrawNum']) for r in history]
    if any(b-a!=1 for a,b in zip(seq,seq[1:])):raise ValueError('NONCONTIGUOUS_WINDOW')
    return history,receipt

def weights(history):
    f=[1.0]*35;b=[1.0]*12
    for row in history:
        nums=list(map(int,row['lotteryDrawResult'].split()))
        for n in nums[:5]:f[n-1]+=1
        for n in nums[5:]:b[n-1]+=1
    return f,b

def choose(rng, w, count):
    keys=[math.log(1.0-rng.random())/v for v in w]
    return [i+1 for i in sorted(range(len(w)),key=lambda i:(keys[i],i),reverse=True)[:count]]

def ticket(f,b,additional=False,multiplier=1):
    return {'front':sorted(f),'back':sorted(b),'additional':additional,'multiplier':multiplier}

def generate(p, history, issue):
    wf,wb=weights(history);portfolios=[]
    for rep in range(p['replicates']):
        for strategy in p['strategies']:
            family='compound' if strategy.startswith('compound') else ('dantuo' if strategy.startswith('dantuo') else 'single')
            seed=digest(f"{p['seed_namespace']}|{issue}|{rep}|{family}".encode())
            rng=random.Random(int(seed,16))
            hot=strategy in ('hot60_18','compound_hot','dantuo_hot')
            cold=strategy=='cold60_18'
            fweights=wf if hot else ([1/v for v in wf] if cold else [1.0]*35)
            bweights=wb if hot else ([1/v for v in wb] if cold else [1.0]*12)
            if family=='compound':
                f=choose(rng,fweights,6);b=choose(rng,bweights,3)
                ts=[ticket(a,c) for a in combinations(f,5) for c in combinations(b,2)]
            elif family=='dantuo':
                f=choose(rng,fweights,10);b=choose(rng,bweights,3)
                ts=[ticket(f[:4]+[v],c) for v in f[4:] for c in combinations(b,2)]
            else:
                count=12 if strategy=='random12_additional' else (1 if strategy=='repeat_one_18x' else 18)
                ts=[];seen=set()
                while len(ts)<count:
                    f=choose(rng,fweights,5);b=choose(rng,bweights,2);key=(tuple(sorted(f)),tuple(sorted(b)))
                    if key in seen:continue
                    seen.add(key);ts.append(ticket(f,b,strategy=='random12_additional',18 if strategy=='repeat_one_18x' else 1))
            validate_portfolio(ts,p)
            portfolios.append({'replicate':rep,'strategy':strategy,'seed_sha256':seed,'tickets':ts,'cost_yuan':36})
    return portfolios

def validate_portfolio(ts,p):
    seen=set();cost=0
    for t in ts:
        f,b=t['front'],t['back'];m=t['multiplier']
        if not isinstance(t['additional'],bool) or not isinstance(m,int) or not 1<=m<=99:raise ValueError('INVALID_TICKET_MODE')
        if len(f)!=5 or len(set(f))!=5 or len(b)!=2 or len(set(b))!=2 or not all(isinstance(x,int) and 1<=x<=35 for x in f) or not all(isinstance(x,int) and 1<=x<=12 for x in b):raise ValueError('INVALID_TICKET')
        key=(tuple(sorted(f)),tuple(sorted(b)))
        if key in seen:raise ValueError('DUPLICATE_TICKET_USE_MULTIPLIER')
        seen.add(key);cost+=(3 if t['additional'] else 2)*m
    if cost!=p['cost_yuan_per_strategy_replicate_issue']:raise ValueError('UNEQUAL_COST')

def prepare(source,issue,planned_date,root=ROOT,now=None):
    now=now or utcnow();p=protocol(root);lock=verify_lock(root)
    if not re.fullmatch(r'\d{5}',issue):raise ValueError('INVALID_TARGET_ISSUE')
    date=datetime.fromisoformat(planned_date).date()
    if date.weekday() not in p['draw_weekdays_python']:raise ValueError('UNEXPECTED_DRAW_WEEKDAY')
    deadline=datetime.combine(date,time(p['internal_freeze_hour_shanghai']),ZONE)
    if now>=deadline:raise ValueError('FREEZE_DEADLINE_PASSED')
    if now<datetime.fromisoformat(lock['locked_at_utc']):raise ValueError('CLOCK_BEFORE_PROTOCOL_LOCK')
    history,receipt=get_history(source,issue,now,p)
    portfolios=generate(p,history,issue)
    packet={'experiment_id':p['experiment_id'],'issue':issue,'planned_draw_date':planned_date,'internal_freeze_deadline':deadline.isoformat(),'schedule_basis':p['schedule_basis'],'generated_at_utc':now.isoformat(),'training_first_issue':history[0]['lotteryDrawNum'],'training_cutoff_issue':history[-1]['lotteryDrawNum'],'protocol_sha256':lock['protocol_sha256'],'engine_sha256':lock['engine_sha256'],'source_receipt':receipt,'history_sha256':digest(canonical(history)),'portfolios':portfolios}
    path=root/'packets'/issue
    with ledger_guard(root):
        path.mkdir(parents=True,exist_ok=False)
        exclusive(path/'source.json',load(source));exclusive(path/'history.json',history);exclusive(path/'packet.json',packet)
        seal={'issue':issue,'packet_sha256':digest(canonical(packet)),'history_sha256':digest(canonical(history)),'timestamp_assurance':'local-clock; hash may be retained in chat, no trusted third-party timestamp'}
        exclusive(path/'seal.json',seal);append_event(root,'PREPARED',issue,seal['packet_sha256'])
    return seal

def verify_packet(issue,root=ROOT):
    if not re.fullmatch(r'\d{5}',issue):raise ValueError('INVALID_TARGET_ISSUE')
    p=protocol(root);lock=verify_lock(root);path=root/'packets'/issue
    packet=load(path/'packet.json');seal=load(path/'seal.json');history=load(path/'history.json')
    raw_source=load(path/'source.json')
    source_history=official_rows(raw_source)[-p['history_window']:]
    if source_history!=history:raise ValueError('SNAPSHOT_HISTORY_MISMATCH')
    if digest(canonical(packet))!=seal['packet_sha256'] or digest(canonical(history))!=packet['history_sha256']:raise ValueError('PACKET_OR_HISTORY_TAMPERED')
    if packet['protocol_sha256']!=lock['protocol_sha256'] or packet['engine_sha256']!=lock['engine_sha256']:raise ValueError('PACKET_VERSION_MISMATCH')
    if datetime.fromisoformat(packet['generated_at_utc'])>=datetime.fromisoformat(packet['internal_freeze_deadline']):raise ValueError('PACKET_GENERATED_LATE')
    if any(r['lotteryDrawNum']>=issue for r in history):raise ValueError('FUTURE_LEAK')
    if generate(p,history,issue)!=packet['portfolios']:raise ValueError('REPLAY_MISMATCH')
    events=ledger_events(root)
    if not any(e['kind']=='PREPARED' and e['issue']==issue and e['payload_sha256']==seal['packet_sha256'] for e in events):raise ValueError('MISSING_PREPARE_EVENT')
    return packet

def amount(value):
    try:
        n=Decimal(str(value).replace(',',''))
        if not n.is_finite() or n<0:return None
        return n
    except Exception:return None

def settle_portfolio(ts,row):
    nums=list(map(int,row['lotteryDrawResult'].split()));f,b=set(nums[:5]),set(nums[5:]);gross=Decimal(0);hits=[]
    prizes={int(x['sort']):amount(x['stakeAmount']) for x in row['prizeLevelList']}
    for t in ts:
        fh=len(f.intersection(t['front']));bh=len(b.intersection(t['back']));group=RULES.get((fh,bh));base=Decimal(0);extra=Decimal(0)
        if group:
            base=prizes.get(group)
            if base is None:raise ValueError(f'MISSING_MATCHED_PRIZE:{group}')
            if t['additional'] and group in (101,301):
                extra=prizes.get(group+100)
                if extra is None:raise ValueError(f'MISSING_MATCHED_ADDITIONAL_PRIZE:{group+100}')
        payout=(base+extra)*t['multiplier'];gross+=payout
        if payout:hits.append({'front_hits':fh,'back_hits':bh,'group':group,'multiplier':t['multiplier'],'amount':str(payout)})
    tax=gross*Decimal('.2') if gross>10000 else Decimal(0)
    return {'gross_yuan':str(gross),'tax_yuan':str(tax),'after_tax_yuan':str(gross-tax),'net_profit_yuan':str(gross-tax-36),'winning_tickets':hits}

def settle(source,issue,root=ROOT,now=None):
    now=now or utcnow();packet=verify_packet(issue,root)
    source=Path(source);receipt=load(str(source)+'.receipt.json');raw=source.read_bytes()
    if receipt['sha256']!=digest(raw) or receipt['source']!=HISTORY_URL:raise ValueError('SOURCE_RECEIPT_MISMATCH')
    rows=official_rows(json.loads(raw));matches=[r for r in rows if r['lotteryDrawNum']==issue]
    if not matches:raise ValueError('TARGET_NOT_DRAWN_YET')
    row=matches[0]
    if now.astimezone(ZONE).date()<datetime.fromisoformat(row['lotteryDrawTime']).date():raise ValueError('FUTURE_RESULT')
    if int(row.get('lotteryDrawStatus',0))!=20:raise ValueError('DRAW_NOT_FINAL')
    result={'issue':issue,'settled_at_utc':now.isoformat(),'source_receipt':receipt,'official_row_sha256':digest(canonical(row)),'actual_draw_date':row['lotteryDrawTime'],'planned_date_changed':row['lotteryDrawTime']!=packet['planned_draw_date'],'status':'EXCLUDED_PROMOTION' if row.get('lotteryPromotionFlag') else 'SETTLED','results':[]}
    if result['status']=='SETTLED':
        for port in packet['portfolios']:
            result['results'].append({'strategy':port['strategy'],'replicate':port['replicate'],**settle_portfolio(port['tickets'],row)})
    with ledger_guard(root):
        path=root/'packets'/issue;exclusive(path/'official_result.json',row);exclusive(path/'settlement.json',result);append_event(root,result['status'],issue,digest(canonical(result)))
    return {'issue':issue,'status':result['status'],'portfolios_settled':len(result['results'])}

def status(root=ROOT):
    p=protocol(root);verify_lock(root);events=ledger_events(root);pending=[];excluded=[];by_strategy=defaultdict(lambda:defaultdict(list));validated=0
    prepared={e['issue'] for e in events if e['kind']=='PREPARED'}
    settled={e['issue'] for e in events if e['kind'] in ('SETTLED','EXCLUDED_PROMOTION')}
    disk={d.name for d in (root/'packets').iterdir() if d.is_dir()} if (root/'packets').exists() else set()
    if disk!=prepared or not settled.issubset(prepared):raise ValueError('ORPHANED_PACKET_OR_EVENT')
    for event in events:
        issue=event['issue'];path=root/'packets'/issue
        if event['kind']=='PREPARED':
            verify_packet(issue,root)
            if (path/'settlement.json').exists()!=(issue in settled):raise ValueError('INCOMPLETE_SETTLEMENT_EVENT')
            if not (path/'settlement.json').exists():pending.append(issue)
        else:
            result=load(path/'settlement.json')
            if digest(canonical(result))!=event['payload_sha256']:raise ValueError('SETTLEMENT_TAMPERED')
            row=load(path/'official_result.json')
            if digest(canonical(row))!=result['official_row_sha256']:raise ValueError('RESULT_ROW_TAMPERED')
            if result['status']=='EXCLUDED_PROMOTION':excluded.append(issue);continue
            validated+=1
            for v in result['results']:by_strategy[v['strategy']][v['replicate']].append(float(v['net_profit_yuan']))
    summaries={}
    for name,runs in by_strategy.items():
        totals=[sum(values) for values in runs.values()]
        summaries[name]={'issues':validated,'median_cumulative_net_yuan':statistics.median(totals),'median_roi':statistics.median(totals)/(validated*36)}
    return {'experiment_id':p['experiment_id'],'valid_settled_issues':validated,'pending_issues':pending,'excluded_promotional_issues':excluded,'replicates':p['replicates'],'strategies':len(p['strategies']),'summaries':summaries,'efficacy_conclusion':'not_evaluated; no significance or profit certification','ledger_last_hash':events[-1]['event_hash'] if events else None}

def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='cmd',required=True)
    sub.add_parser('lock');sub.add_parser('status')
    r=sub.add_parser('refresh');r.add_argument('--output',required=True)
    p=sub.add_parser('prepare');p.add_argument('--source',required=True);p.add_argument('--issue',required=True);p.add_argument('--planned-date',required=True)
    v=sub.add_parser('verify');v.add_argument('--issue',required=True)
    s=sub.add_parser('settle');s.add_argument('--source',required=True);s.add_argument('--issue',required=True)
    args=parser.parse_args()
    if args.cmd=='lock':answer=lock_protocol()
    elif args.cmd=='refresh':answer=refresh(args.output)
    elif args.cmd=='prepare':answer=prepare(args.source,args.issue,args.planned_date)
    elif args.cmd=='verify':
        packet=verify_packet(args.issue);answer={'issue':args.issue,'status':'verified','portfolios':len(packet['portfolios'])}
    elif args.cmd=='settle':answer=settle(args.source,args.issue)
    else:answer=status()
    print(json.dumps(answer,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
