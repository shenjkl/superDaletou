"""Whitelist-only replay of one settled paper experiment; not a backup.

The bundle contains tickets and settlement inputs, which remain personal
research data. It excludes free text, source files, URLs, paths and credentials.
Hashes bind bytes locally; they do not authenticate official sources or time.
"""
from contextlib import closing
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import argparse
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from dlt.candidates import _canonical
from dlt.prospective import _time
from dlt.prospective_settlement import PaperResults, _official_url
from dlt.settlement import SettlementBlocked, exact_money_context, settle_portfolio
from dlt.snapshots import parse_snapshot
from dlt import settlement as settlement_module


LEGACY_FORMAT = 'dlt-paper-settlement-replay-v1'
FORMAT = 'dlt-paper-settlement-replay-v2'
MAX_BYTES = 4 * 1024 * 1024
MAX_REPLICAS = 100
MAX_TICKETS = 10000
HASH = re.compile(r'[0-9a-f]{64}')
ASSUMPTIONS = {'alternatives': 'separate_no_other_same_issue_winnings',
               'payout': 'fixed_published_amounts_without_added_winner_adjustment',
               'provenance': 'local_evidence_not_independently_authenticated'}


class PaperReplayError(ValueError):
    pass


def _unique_pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise PaperReplayError('重放包存在重复字段')
        result[key] = value
    return result


def _keys(value, expected):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise PaperReplayError('重放包字段无效')


def _decimal(value, *, negative=False):
    if isinstance(value, (bool, float)) or not isinstance(value, (str, int)):
        raise PaperReplayError('金额或税率必须是精确数值')
    try:
        number = Decimal(str(value).replace(',', ''))
    except (InvalidOperation, ValueError) as exc:
        raise PaperReplayError('金额或税率无效') from exc
    if (not number.is_finite() or (number < 0 and not negative)
            or abs(number) > Decimal('1e24') or number.as_tuple().exponent < -12):
        raise PaperReplayError('金额或税率超出重放范围')
    return format(number, 'f')


def _ticket(ticket):
    if not isinstance(ticket, dict):
        raise PaperReplayError('子注无效')
    result = {key: ticket.get(key) for key in ('front', 'back', 'additional', 'multiplier')}
    if (not isinstance(result['front'], list) or not isinstance(result['back'], list)
            or type(result['additional']) is not bool or type(result['multiplier']) is not int
            or not 1 <= result['multiplier'] <= 1000000):
        raise PaperReplayError('子注字段无效')
    # Number validity is independently checked by settle_portfolio.
    return result


def _clean_rule(rule, payouts):
    groups, extras = rule['hit_groups'], rule['additional_groups']
    if not isinstance(groups, dict) or not isinstance(extras, dict):
        raise PaperReplayError('奖级映射无效')
    if set(groups) != {f'{f},{b}' for f in range(6) for b in range(3)}:
        raise PaperReplayError('奖级映射不完整')
    names = set(value for value in groups.values() if value is not None) | set(extras) | set(extras.values())
    if (len(names) > 64 or any(not isinstance(name, str) or not name or len(name) > 256 for name in names)
            or not isinstance(payouts, dict)):
        raise PaperReplayError('奖级标识无效')
    aliases = {name: f'g{index}' for index, name in enumerate(sorted(names))}
    mapped = {key: None if value is None else aliases[value] for key, value in groups.items()}
    additional = {aliases[key]: aliases[value] for key, value in extras.items()}
    clean_payouts = {aliases[name]: _decimal(payouts[name]) for name in names}
    tax = rule['tax']
    if tax['comparison'] not in ('gt', 'gte') or tax['rounding'] not in ('down', 'half_up', 'half_even'):
        raise PaperReplayError('税费口径无效')
    clean = {'hit_groups': mapped, 'additional_groups': additional,
             'prices': {key: _decimal(rule['prices'][key]) for key in ('base', 'additional')},
             'tax': {'unit': tax['unit'], 'threshold': _decimal(tax['threshold']), 'rate': _decimal(tax['rate']),
                     'quantum': _decimal(tax['quantum']), 'comparison': tax['comparison'], 'rounding': tax['rounding']}}
    return clean, clean_payouts


def _replay_rule(issue, rule, *, legacy=False):
    if legacy:
        # v1 applied tax to the entire alternative portfolio implicitly. This
        # adapter replays that old arithmetic without asserting source proof.
        rule = {**rule, 'tax': {**rule['tax'], 'unit': 'same_person_same_issue_same_game'}}
    return {'version': 'replay-v1', 'source': 'replay-input-only', 'verified': True,
            'applicable_issues': [issue], **rule}


def _expected(result):
    return {**{key: _decimal(result[key], negative=key == 'net') for key in ('gross', 'tax', 'cost', 'net')},
            'winning_tickets': result['winning_tickets']}


def _same_amounts(left, right):
    try:
        return (all(Decimal(str(left[key])) == Decimal(str(right[key])) for key in ('gross', 'tax', 'cost', 'net'))
                and type(left['winning_tickets']) is int and left['winning_tickets'] == right['winning_tickets'])
    except (KeyError, TypeError, InvalidOperation):
        return False


@exact_money_context
def build_paper_replay(path, experiment_key, *, clock=None):
    if not isinstance(experiment_key, str) or not experiment_key or len(experiment_key) > 128:
        raise PaperReplayError('实验标识无效')
    source_path = Path(path).resolve()
    if not source_path.is_file():
        raise PaperReplayError('实验数据库不存在')
    paper = PaperResults(source_path, clock=clock)
    try:
        with closing(sqlite3.connect(source_path.as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            state = paper.inspect_current_in_transaction(db, experiment_key)
            if state['status'] != 'locally_attested':
                raise PaperReplayError('当前纸面结果不可用或需要复核')
            group = state['result']
            fixed = db.execute('SELECT content_sha256,content_json FROM fixed_experiments WHERE request_key=?',
                               (experiment_key,)).fetchone()
            if not fixed or hashlib.sha256(fixed[1]).hexdigest() != fixed[0]:
                raise PaperReplayError('固定实验摘要无效')
            content = json.loads(fixed[1])
            issue = group['issue']
            rule_review = paper.experiments._get_review(db, group['rule_review_id'], 'rule')
            tax_review = paper.experiments._get_review(db, group['tax_review_id'], 'tax')
            rule = rule_review['extracted']
            if (rule.get('source') != rule_review['source_url'] or rule.get('verified') is not True
                    or tax_review['extracted'].get('tax') != rule.get('tax')
                    or issue not in tax_review['extracted'].get('applicable_issues', [])):
                raise PaperReplayError('规则或税费资料不一致')
            row = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                             (group['source_snapshot_id'], issue)).fetchone()
            if not row or hashlib.sha256(row[0]).hexdigest() != group['row_sha256']:
                raise PaperReplayError('开奖记录摘要无效')
            _, calendar, _ = paper.experiments._calendar_for_fixed(db, content)
            observed = _time(group['observed_at'])
            if (not _official_url(group['source_url']) or observed < _time(content['registered_at'])
                    or observed < _time(calendar['extracted']['cutoff_at']) or observed > paper.experiments._now()):
                raise PaperReplayError('开奖来源或观察时间不符合登记条件')
            observation = db.execute('SELECT raw_sha256,raw_payload FROM observations '
                                     'WHERE snapshot_id=? AND source=? AND observed_at=?',
                                     (group['source_snapshot_id'], group['source_url'], group['observed_at'])).fetchone()
            if (not observation or not isinstance(observation[1], bytes)
                    or hashlib.sha256(observation[1]).hexdigest() != observation[0]):
                raise PaperReplayError('原始开奖观察记录缺失或摘要不符')
            source_payload, source_rows = parse_snapshot(observation[1])
            matching = [item for item in source_rows if item['issue'] == issue]
            canonical_source = _canonical(source_payload)
            stored_snapshot = db.execute('SELECT canonical_json,row_count FROM snapshots WHERE id=?',
                                         (group['source_snapshot_id'],)).fetchone()
            if (not stored_snapshot or stored_snapshot[0] != canonical_source or stored_snapshot[1] != len(source_rows)
                    or hashlib.sha256(canonical_source).hexdigest() != group['source_snapshot_id']
                    or len(matching) != 1 or _canonical(matching[0]['raw_row']) != row[0]):
                raise PaperReplayError('原始开奖观察与当前结果不一致')
            raw = json.loads(row[0])
            payouts = paper._payouts(rule, raw)
            if (set(payouts) != set(group['payouts']) or
                    any(Decimal(_decimal(payouts[key])) != Decimal(_decimal(group['payouts'][key])) for key in payouts)):
                raise PaperReplayError('奖金与当前结果不一致')
            nums = [int(value) for value in raw['lotteryDrawResult'].split()]
            draw = {'issue': issue, 'front': nums[:5], 'back': nums[5:], 'promotion': False}
            clean_rule, clean_payouts = _clean_rule(rule, payouts)
            pairs, saved = content['pairs'], group['replicas']
            if not isinstance(pairs, list) or not 1 <= len(pairs) <= MAX_REPLICAS or len(pairs) != len(saved):
                raise PaperReplayError('副本数量无效或超过上限')
            replicas, count, diffs = [], 0, []
            for original, recorded in zip(pairs, saved):
                if (original['replica'] != recorded['replica']
                        or original['pair']['pair_sha256'] != recorded['pair_sha256']):
                    raise PaperReplayError('配对副本与结果不一致')
                entry = {}
                for label in ('candidate', 'control'):
                    tickets = [_ticket(t) for t in original['pair'][label]['tickets']]
                    count += len(tickets)
                    if count > MAX_TICKETS:
                        raise PaperReplayError('子注数量超过重放上限')
                    original_result = settle_portfolio(issue=issue, draw=draw, tickets=tickets, payouts=payouts,
                                                       rule=rule, duplicate_policy='reject')
                    computed = settle_portfolio(issue=issue, draw=draw, tickets=tickets, payouts=clean_payouts,
                                                rule=_replay_rule(issue, clean_rule), duplicate_policy='reject')
                    if (recorded[label].get('issue') != issue or recorded[label].get('rule_version') != rule['version']
                            or not _same_amounts(recorded[label], original_result)
                            or not _same_amounts(original_result, computed)):
                        raise PaperReplayError('保存结果与重算金额不一致')
                    entry[label] = {'tickets': tickets, 'expected': _expected(computed)}
                difference = Decimal(entry['candidate']['expected']['net']) - Decimal(entry['control']['expected']['net'])
                if difference != Decimal(_decimal(recorded['difference_yuan'], negative=True)):
                    raise PaperReplayError('配对差额与重算结果不一致')
                entry['difference_yuan'] = _decimal(str(difference), negative=True)
                diffs.append(Fraction(difference))
                replicas.append(entry)
            average = sum(diffs, Fraction(0)) / len(diffs)
            if group['mean_difference_fraction'] != [average.numerator, average.denominator]:
                raise PaperReplayError('副本平均差额与重算结果不一致')
            hashes = {'experiment': fixed[0], 'result': state['group_id'],
                      'snapshot': group['source_snapshot_id'], 'draw_row': group['row_sha256'],
                      'workflow_at_settlement': group['code_sha256'],
                      'calculator_at_export': hashlib.sha256(Path(settlement_module.__file__).read_bytes()).hexdigest()}
            if any(not isinstance(value, str) or not HASH.fullmatch(value) for value in hashes.values()):
                raise PaperReplayError('来源摘要无效')
            body = {'format': FORMAT, 'issue': issue, 'draw': draw, 'rule': clean_rule, 'payouts': clean_payouts,
                    'assumptions': dict(ASSUMPTIONS),
                    'replicas': replicas, 'mean_difference_fraction': [average.numerator, average.denominator],
                    'source_hashes': hashes}
            bundle = {**body, 'bundle_sha256': hashlib.sha256(_canonical(body)).hexdigest()}
            if len(_canonical(bundle)) > MAX_BYTES:
                raise PaperReplayError('重放包超过大小上限')
            verify_paper_replay(bundle)
            return bundle
    except PaperReplayError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError, RecursionError,
            sqlite3.Error, OSError, InvalidOperation, SettlementBlocked) as exc:
        raise PaperReplayError('无法导出：实验、来源或结果不完整') from exc


@exact_money_context
def verify_paper_replay(bundle):
    try:
        return _verify_paper_replay(bundle)
    except PaperReplayError:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, InvalidOperation, RecursionError, OSError) as exc:
        raise PaperReplayError('重放包结构或输入无效') from exc


def _verify_paper_replay(bundle):
    _keys(bundle, ('format', 'issue', 'draw', 'rule', 'payouts', 'assumptions', 'replicas', 'mean_difference_fraction',
                   'source_hashes', 'bundle_sha256'))
    if len(_canonical(bundle)) > MAX_BYTES or bundle['format'] not in (FORMAT, LEGACY_FORMAT):
        raise PaperReplayError('重放包格式或大小无效')
    body = {key: value for key, value in bundle.items() if key != 'bundle_sha256'}
    if not isinstance(bundle['bundle_sha256'], str) or hashlib.sha256(_canonical(body)).hexdigest() != bundle['bundle_sha256']:
        raise PaperReplayError('重放包摘要不符')
    if bundle['assumptions'] != ASSUMPTIONS:
        raise PaperReplayError('重放包假设无效')
    _keys(bundle['source_hashes'], ('experiment', 'result', 'snapshot', 'draw_row',
                                    'workflow_at_settlement', 'calculator_at_export'))
    if any(not isinstance(v, str) or not HASH.fullmatch(v) for v in bundle['source_hashes'].values()):
        raise PaperReplayError('来源摘要格式无效')
    issue, draw, rule, payouts = bundle['issue'], bundle['draw'], bundle['rule'], bundle['payouts']
    if not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue):
        raise PaperReplayError('奖期无效')
    _keys(draw, ('issue', 'front', 'back', 'promotion'))
    _keys(rule, ('hit_groups', 'additional_groups', 'prices', 'tax'))
    _keys(rule['prices'], ('base', 'additional'))
    legacy = bundle['format'] == LEGACY_FORMAT
    _keys(rule['tax'], ('threshold', 'rate', 'quantum', 'comparison', 'rounding') if legacy
          else ('unit', 'threshold', 'rate', 'quantum', 'comparison', 'rounding'))
    if draw['issue'] != issue or draw['promotion'] is not False:
        raise PaperReplayError('开奖或派奖状态无效')
    if (not isinstance(payouts, dict) or not isinstance(rule['additional_groups'], dict)
            or any(not isinstance(k, str) or not re.fullmatch(r'g\d{1,2}', k) for k in payouts)):
        raise PaperReplayError('奖金映射无效')
    names = set(payouts)
    if (len(names) > 64 or set(rule['additional_groups']) - names
            or any(value not in names for value in rule['additional_groups'].values())
            or not isinstance(rule['hit_groups'], dict)
            or any(value is not None and value not in names for value in rule['hit_groups'].values())):
        raise PaperReplayError('奖级映射无效')
    for value in payouts.values():
        _decimal(value)
    for value in rule['prices'].values():
        _decimal(value)
    for key in ('threshold', 'rate', 'quantum'):
        _decimal(rule['tax'][key])
    replicas = bundle['replicas']
    if not isinstance(replicas, list) or not 1 <= len(replicas) <= MAX_REPLICAS:
        raise PaperReplayError('副本数量无效')
    differences, count = [], 0
    try:
        for replica in replicas:
            _keys(replica, ('candidate', 'control', 'difference_yuan'))
            nets = []
            for label in ('candidate', 'control'):
                portfolio = replica[label]
                _keys(portfolio, ('tickets', 'expected'))
                _keys(portfolio['expected'], ('gross', 'tax', 'cost', 'net', 'winning_tickets'))
                if not isinstance(portfolio['tickets'], list):
                    raise PaperReplayError('子注无效')
                for ticket in portfolio['tickets']:
                    _keys(ticket, ('front', 'back', 'additional', 'multiplier'))
                    _ticket(ticket)
                count += len(portfolio['tickets'])
                if count > MAX_TICKETS:
                    raise PaperReplayError('子注数量超过重放上限')
                for key in ('gross', 'tax', 'cost', 'net'):
                    _decimal(portfolio['expected'][key], negative=key == 'net')
                computed = settle_portfolio(issue=issue, draw=draw, tickets=portfolio['tickets'], payouts=payouts,
                                            rule=_replay_rule(issue, rule, legacy=legacy), duplicate_policy='reject')
                if not _same_amounts(portfolio['expected'], computed):
                    raise PaperReplayError('重算结果与记录不一致')
                nets.append(Decimal(computed['net']))
            diff = nets[0] - nets[1]
            if diff != Decimal(_decimal(replica['difference_yuan'], negative=True)):
                raise PaperReplayError('配对差额不一致')
            differences.append(Fraction(diff))
        average = sum(differences, Fraction(0)) / len(differences)
        fraction = bundle['mean_difference_fraction']
        if (not isinstance(fraction, list) or len(fraction) != 2
                or any(type(value) is not int for value in fraction) or fraction[1] <= 0
                or fraction != [average.numerator, average.denominator]):
            raise PaperReplayError('平均差额不一致')
    except (SettlementBlocked, KeyError, ValueError, TypeError, InvalidOperation) as exc:
        if isinstance(exc, PaperReplayError):
            raise
        raise PaperReplayError('重放输入或结算规则无效') from exc
    local_calculator = hashlib.sha256(Path(settlement_module.__file__).read_bytes()).hexdigest()
    return {'status': 'matched', 'replicas': len(replicas), 'tickets': count,
            'calculator_matches_export': local_calculator == bundle['source_hashes']['calculator_at_export'],
            'tax_unit_evidence': 'legacy_implicit_calculation_only' if legacy else 'explicit_bundle_assumption',
            'assurance': 'calculation match only; source origin and timestamps are not independently authenticated'}


def main():
    parser = argparse.ArgumentParser(description='Verify a whitelisted paper settlement replay bundle.')
    parser.add_argument('bundle')
    args = parser.parse_args()
    try:
        path = Path(args.bundle)
        with path.open('rb') as handle:
            raw = handle.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise PaperReplayError('重放包超过大小上限')
        result = verify_paper_replay(json.loads(raw, object_pairs_hook=_unique_pairs))
    except (PaperReplayError, OSError, ValueError, TypeError, RecursionError) as exc:
        print(json.dumps({'status': 'failed', 'error': str(exc) if isinstance(exc, PaperReplayError) else '重放包无法读取'}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
