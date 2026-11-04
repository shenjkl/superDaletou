"""Structural and input checks for saved candidate/control pairs.

These checks verify contracts and immutable input references, not that a
pseudorandom draw is independently certified or that a model has predictive
value. Exact regeneration is provided separately by generation replay.
"""
from datetime import date
import hashlib
from itertools import combinations
import json
import math
from pathlib import Path
import platform

from dlt.candidates import CandidateError, _canonical, _numbers, generate_pair
from dlt import candidates as candidate_module


def _fail(message):
    raise CandidateError(message)


def _seed(base, label):
    return int.from_bytes(hashlib.sha256(f'dlt-pair-v1:{base}:{label}'.encode()).digest()[:8], 'big')


def validate_pair_structure(pair, expected_config=None):
    """Check explicit structure, seeds, numbers, costs and both alternatives."""
    try:
        if not isinstance(pair, dict) or pair.get('version') != 'python-candidate-v1':
            _fail('组合版本不受支持')
        content = dict(pair)
        digest = content.pop('pair_sha256', None)
        if not isinstance(digest, str) or hashlib.sha256(_canonical(content)).hexdigest() != digest:
            _fail('组合摘要不一致')
        cfg = pair['config']
        if not isinstance(cfg, dict) or (expected_config is not None and cfg != expected_config):
            _fail('组合配置与固定请求不一致')
        if (type(cfg['seed']) is not int or not 0 <= cfg['seed'] <= 4294967295
                or type(cfg['additional']) is not bool or type(cfg['budget_fen']) is not int
                or cfg['budget_fen'] <= 0 or type(cfg['max_tickets']) is not int or cfg['max_tickets'] <= 0):
            _fail('组合种子、预算或限制无效')
        structure = cfg['structure']
        if structure not in ('singles', 'compound', 'dantuo'):
            _fail('组合结构不受支持')
        price = cfg['additional_price_fen'] if cfg['additional'] else cfg['base_price_fen']
        if type(price) is not int or price <= 0:
            _fail('组合价格无效')
        if structure == 'singles':
            count = cfg['budget_fen'] // price
        elif structure == 'compound':
            front_count, back_count = cfg.get('front_count', 5), cfg.get('back_count', 2)
            if (type(front_count) is not int or not 5 <= front_count <= 35
                    or type(back_count) is not int or not 2 <= back_count <= 12):
                _fail('复式选号数量无效')
            count = math.comb(front_count, 5) * math.comb(back_count, 2)
        else:
            count = 18
        if count < 1 or count > cfg['max_tickets'] or count * price > cfg['budget_fen']:
            _fail('组合数量或成本超出固定配置')
        grouped_layouts = []
        for label in ('candidate', 'control'):
            side = pair[label]
            if (not isinstance(side, dict) or type(side.get('seed')) is not int
                    or side['seed'] != _seed(cfg['seed'], label)
                    or type(side.get('count')) is not int or side['count'] != count
                    or type(side.get('cost_fen')) is not int or side['cost_fen'] != count*price
                    or type(side.get('remaining_fen')) is not int
                    or side['remaining_fen'] != cfg['budget_fen']-count*price):
                _fail('候选或对照的种子、注数、成本或余额不一致')
            tickets = side['tickets']
            if not isinstance(tickets, list) or len(tickets) != count:
                _fail('候选或对照子注数量不一致')
            found = set()
            for ticket in tickets:
                if (not isinstance(ticket, dict) or set(ticket) != {'front', 'back', 'additional', 'multiplier'}
                        or ticket['additional'] is not cfg['additional'] or type(ticket['multiplier']) is not int
                        or ticket['multiplier'] != 1):
                    _fail('子注追加或倍数与固定配置不一致')
                front = _numbers(ticket['front'], 35, 5, 'ticket front')
                back = _numbers(ticket['back'], 12, 2, 'ticket back')
                if front != ticket['front'] or back != ticket['back']:
                    _fail('子注号码顺序不规范')
                key = (tuple(front), tuple(back))
                if key in found:
                    _fail('组合内存在重复子注')
                found.add(key)
            has_ticket_groups = 'ticket_groups' in side
            grouped_layouts.append(has_ticket_groups)
            if has_ticket_groups:
                groups = side['ticket_groups']
                if not isinstance(groups, list) or not groups:
                    _fail('实体票分组不能为空')
                seen_indexes = set()
                maximum_ticket_cost = 3000000 if cfg['additional'] else 2000000
                for group in groups:
                    if (not isinstance(group, list) or not group
                            or any(type(index) is not int or not 0 <= index < count for index in group)
                            or len(set(group)) != len(group)
                            or seen_indexes.intersection(group)):
                        _fail('实体票分组索引无效、重复或重叠')
                    seen_indexes.update(group)
                    if len(group) * price > maximum_ticket_cost:
                        _fail('实体票超过官方单张投注金额上限')
                if seen_indexes != set(range(count)):
                    _fail('实体票分组没有覆盖全部子注')
                if len(groups) != 1 or groups[0] != list(range(count)):
                    _fail('当前版本只支持每个方案整体作为一张模拟票')
            pool = side['pool']
            if structure == 'singles':
                if pool is not None:
                    _fail('单式不应包含复式号码池')
                continue
            if not isinstance(pool, dict):
                _fail('复式或胆拖缺少号码池')
            if structure == 'compound':
                if set(pool) != {'front', 'back'}:
                    _fail('复式号码池字段无效')
                front = _numbers(pool['front'], 35, front_count, 'compound front')
                back = _numbers(pool['back'], 12, back_count, 'compound back')
                fronts = combinations(front, 5)
            else:
                if set(pool) != {'dan', 'drag', 'back'}:
                    _fail('胆拖号码池字段无效')
                dan = _numbers(pool['dan'], 35, 4, 'dan')
                drag = _numbers(pool['drag'], 35, 6, 'drag')
                back = _numbers(pool['back'], 12, 3, 'dantuo back')
                if set(dan) & set(drag):
                    _fail('胆码与拖码重叠')
                fronts = (tuple(sorted(dan+[value])) for value in drag)
            expected = {(tuple(one), tuple(rear)) for one in fronts for rear in combinations(back, 2)}
            if found != expected:
                _fail('子注与复式或胆拖号码池展开结果不一致')
        if grouped_layouts[0] != grouped_layouts[1]:
            _fail('候选与对照必须使用相同的实体票分组结构')
        if pair['candidate']['seed'] == pair['control']['seed']:
            _fail('候选与对照种子冲突')
    except CandidateError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        raise CandidateError('组合结构或配置无法核验') from exc


def validate_pair_history(db, pair, request):
    """Bind the generated window to the actual stored snapshot and request."""
    try:
        cfg = request['config']
        if pair['config'] != cfg or pair['model_artifact_id'] != request['artifact_id']:
            _fail('组合配置或模型与固定请求不一致')
        rows = db.execute('SELECT issue,draw_date,front_json,back_json,numbers_usable FROM draw_versions '
                          'WHERE snapshot_id=? ORDER BY draw_date,issue', (cfg['snapshot_id'],)).fetchall()
        issues = [row[0] for row in rows]
        first, last = issues.index(request['history_first_issue']), issues.index(request['history_cutoff_issue'])
        window = cfg['window']
        if (type(window) is not int or window < 1 or first > last or last-first+1 < window):
            _fail('组合历史范围无效')
        selected = rows[last-window+1:last+1]
        if any(row[4] != 1 for row in selected):
            _fail('组合历史号码不合格')
        history = [{'issue': issue, 'date': day, 'front': json.loads(front), 'back': json.loads(back)}
                   for issue, day, front, back, _ in selected]
        days = [date.fromisoformat(row['date']) for row in history]
        if (any(a >= b for a, b in zip(days, days[1:])) or days[-1] >= date.fromisoformat(cfg['target_date'])
                or cfg['target_issue'] in issues[first:last+1]):
            _fail('组合历史与目标期时间边界不一致')
        if (pair['history_first'] != history[0]['issue'] or pair['history_last'] != history[-1]['issue']
                or pair['history_sha256'] != hashlib.sha256(_canonical(history)).hexdigest()):
            _fail('组合历史窗口摘要与固定输入不一致')
        return history
    except CandidateError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
        raise CandidateError('组合历史窗口无法核验') from exc


def replay_supported(pair):
    return (pair.get('code_sha256') == hashlib.sha256(Path(candidate_module.__file__).read_bytes()).hexdigest()
            and pair.get('runtime') == platform.python_version())


def validate_random_control(pair, history):
    """Reproduce control when its original generator and Python still match.

    Temporarily use the random method for model pairs: the control has its own
    domain-separated PRNG seed and is unaffected by candidate scores. The
    larger attempt bound prevents the unused random candidate from failing
    first; it cannot change the already-determined successful control sequence.
    A mismatch of code/runtime returns False rather than claiming a replay.
    """
    if not replay_supported(pair):
        return False
    cfg = dict(pair['config'])
    original_method = cfg['method']
    if original_method in ('tree', 'lstm'):
        cfg['method'] = 'random'
    if cfg['structure'] == 'singles':
        cfg['max_sampling_attempts'] = max(cfg['max_sampling_attempts'], pair['control']['count']*200+10000)
    replay = generate_pair(history=history, config=cfg)
    if replay['control'] != pair['control']:
        _fail('随机对照与固定种子及生成代码的重放结果不一致')
    if original_method in ('random', 'hot', 'cold') and replay['candidate'] != pair['candidate']:
        _fail('候选方案与固定种子及生成代码的重放结果不一致')
    return True
