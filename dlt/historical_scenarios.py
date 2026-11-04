"""Bounded historical payout assumptions for descriptive reconstruction.

These local snapshots are not authenticated by this module. The fixed published
amounts are a counterfactual, not a claim about actual incremental ticket sales.
Unsupported, incomplete, and disputed issues remain in the manifest as blocked.
"""
from contextlib import closing
from decimal import Decimal, InvalidOperation
import hashlib
import re
import sqlite3

from dlt.candidates import _canonical
from dlt.scenarios import ScenarioError
from dlt.snapshots import InvalidSnapshot, parse_snapshot
from dlt.prize_rules import (DLT_2026_PROMOTION_FIRST_ISSUE, DLT_2026_PROMOTION_LAST_ISSUE,
                             DLT_2026_PROMOTION_SORTS, DLT_2026_RULE_START_ISSUE,
                             dlt_2026_hit_groups, dlt_2026_additional_groups)


def promotion_data(raw, issue):
    """Read the bounded 2026 activity flag and its separately published awards."""
    flag = raw.get('lotteryPromotionFlag') if isinstance(raw, dict) else None
    if type(flag) is not int or flag not in (0, 1):
        raise ScenarioError('派奖或派奖状态未知')
    active = flag == 1
    in_activity = DLT_2026_PROMOTION_FIRST_ISSUE <= issue <= DLT_2026_PROMOTION_LAST_ISSUE
    if active != in_activity:
        raise ScenarioError('派奖标记与已核对的 2026 活动期号范围不一致')
    levels = raw.get('prizeLevelList')
    if not isinstance(levels, list):
        raise ScenarioError('历史奖金缺失')
    awards = {}
    for level in levels:
        if not isinstance(level, dict):
            raise ScenarioError('历史奖级字段无效')
        award_type = level.get('awardType')
        if not active:
            if ((award_type is not None and (type(award_type) is not int or award_type != 0))
                    or str(level.get('sort', '')) in DLT_2026_PROMOTION_SORTS.values()):
                raise ScenarioError('非派奖期包含派奖奖级字段')
            continue
        if type(award_type) is not int or award_type not in (0, 1):
            raise ScenarioError('派奖奖级类型缺失或未知')
        if award_type == 1:
            group = str(level.get('group', ''))
            expected_sort = DLT_2026_PROMOTION_SORTS.get(group)
            if (expected_sort is None or str(level.get('sort')) != expected_sort
                    or group in awards or 'stakeAmount' not in level):
                raise ScenarioError('派奖奖级与基础奖级映射不一致')
            amount = str(level['stakeAmount'])
            if not re.fullmatch(r'(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d{1,2})?', amount):
                raise ScenarioError('派奖单注奖金缺失或格式无效')
            awards[group] = amount
    if active and set(awards) != set(DLT_2026_PROMOTION_SORTS):
        raise ScenarioError('派奖期缺少完整三至七等奖派奖金额字段')
    return active, awards


def published_payouts(raw, rule, additional):
    """Resolve required base/additional amounts from one retained draw row."""
    if type(additional) is not bool or not isinstance(rule, dict):
        raise ScenarioError('历史奖金映射配置无效')
    mapping = rule.get('payout_sort_map')
    levels = raw.get('prizeLevelList') if isinstance(raw, dict) else None
    if not isinstance(mapping, dict) or not isinstance(levels, list):
        raise ScenarioError('开奖记录奖级映射缺失')
    by_sort = {}
    for item in levels:
        if not isinstance(item, dict) or 'sort' not in item or 'stakeAmount' not in item:
            raise ScenarioError('历史奖级字段缺失')
        sort = str(item['sort'])
        if sort in by_sort:
            raise ScenarioError('历史奖级重复')
        by_sort[sort] = str(item['stakeAmount'])
    hits, extras = rule.get('hit_groups'), rule.get('additional_groups')
    if not isinstance(hits, dict) or not isinstance(extras, dict):
        raise ScenarioError('历史奖金奖级映射无效')
    groups = {group for group in hits.values() if group is not None}
    if additional:
        groups.update(extras.values())
    if not groups.issubset(mapping):
        raise ScenarioError('历史奖金奖级映射不完整')
    result = {group: by_sort.get(str(mapping[group])) for group in groups}
    if any(not isinstance(amount, str) or not re.fullmatch(
            r'(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d{1,2})?', amount) for amount in result.values()):
        raise ScenarioError('所需公布奖金缺失或格式无效')
    return result


def amounts_match(left, right):
    if not isinstance(left, dict) or not isinstance(right, dict) or set(left) != set(right):
        return False
    try:
        return all(Decimal(str(left[key]).replace(',', '')) == Decimal(str(right[key]).replace(',', ''))
                   for key in left)
    except (InvalidOperation, ValueError, TypeError):
        return False


def backtest_scenarios(path, snapshot_id, issues, *, additional=False):
    if (not isinstance(issues, list) or not issues or len(set(issues)) != len(issues)
            or any(not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue) for issue in issues)
            or type(additional) is not bool):
        raise ScenarioError('奖期清单或追加设置无效')
    with closing(sqlite3.connect(path)) as db:
        source = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (snapshot_id,)).fetchone()
        if not source or hashlib.sha256(source[0]).hexdigest() != snapshot_id:
            raise ScenarioError('历史快照缺失或摘要不符')
        try:
            _, parsed = parse_snapshot(source[0])
        except InvalidSnapshot as exc:
            raise ScenarioError('历史快照无法解析') from exc
        parsed = {row['issue']: row for row in parsed}
        result = {}
        for issue in issues:
            try:
                # Explicitly bounded to the locally researched 2026 regime.
                if not DLT_2026_RULE_START_ISSUE <= issue <= '26123':
                    raise ScenarioError('奖期超出本地历史研究口径适用范围')
                row = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                                 (snapshot_id, issue)).fetchone()
                if not row or issue not in parsed or _canonical(parsed[issue]['raw_row']) != row[0]:
                    raise ScenarioError('奖期记录与快照不一致')
                variants = db.execute('SELECT DISTINCT row_json FROM draw_versions WHERE issue=?', (issue,)).fetchall()
                if len(variants) != 1:
                    raise ScenarioError('奖期来源存在差异，需先复核')
                raw = parsed[issue]['raw_row']
                if type(raw.get('verify')) is not int or raw['verify'] != 1:
                    raise ScenarioError('开奖核验标记缺失')
                promotion, promotion_payouts = promotion_data(raw, issue)
                levels = raw.get('prizeLevelList')
                if not isinstance(levels, list):
                    raise ScenarioError('历史奖金缺失')
                payouts = {}
                for level in levels:
                    if not isinstance(level, dict) or 'sort' not in level or 'stakeAmount' not in level:
                        raise ScenarioError('历史奖级字段缺失')
                    key = str(level['sort'])
                    if key in payouts:
                        raise ScenarioError('历史奖级重复')
                    payouts[key] = level['stakeAmount']
                required = {'101', '301', '501', '601', '701', '801', '901'}
                if additional:
                    required.update({'201', '401'})
                if any(not re.fullmatch(r'(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d{1,2})?', str(payouts.get(k, '')))
                       for k in required):
                    raise ScenarioError('所需奖金缺失或格式无效')
                hits = dlt_2026_hit_groups()
                rule = {'version': 'local-2026-historical-assumptions-v1',
                        'source': 'local historical research; externally unverified assumption',
                        'verified': True, 'applicable_issues': [issue], 'hit_groups': hits,
                        'additional_groups': dlt_2026_additional_groups(),
                        'payout_sort_map': {code: code for code in ('101','201','301','401','501','601','701','801','901')},
                        'promotion_policy': 'official_draw_fields_and_ticket_group_threshold' if promotion else 'excluded',
                        'prices': {'base': '2', 'additional': '3'},
                        'tax': {'unit': 'same_person_same_issue_same_game', 'threshold': '10000', 'comparison': 'gt', 'rate': '0.20',
                                'rounding': 'down', 'quantum': '0.01'}}
                result[issue] = {'source_ref': f'本地历史快照 {snapshot_id} / {issue}',
                                 'source_dependency': {'snapshot_id': snapshot_id, 'issue': issue,
                                                       'row_sha256': hashlib.sha256(row[0]).hexdigest()},
                                 'promotion': promotion, 'promotion_payouts': promotion_payouts, 'rule': rule,
                                 'payouts': {key: payouts[key] for key in required},
                                 'assumptions': ('按留存开奖行基本奖金额与独立派奖奖金额；活动期逐票依据假设票面分组及18元门槛；'
                                                 '不重算新增中奖者分奖或限额；假设无其他同期开奖中奖收入；来源及税费口径未经独立核验'
                                                 if promotion else
                                                 '固定公布单注奖金，不含派奖；不重算新增中奖者分奖或限额；假设无其他同期开奖中奖收入；来源及税费口径未经独立核验')}
            except ScenarioError as exc:
                result[issue] = {'blocked_reason': str(exc)}
    return result
