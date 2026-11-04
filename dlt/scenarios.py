"""Construct an explicitly historical, fixed-payout research scenario."""
from contextlib import closing
from decimal import Decimal
import hashlib
import json
import re
from pathlib import Path
import sqlite3

from dlt.candidates import _canonical
from dlt.snapshots import InvalidSnapshot, parse_snapshot
from dlt.holdout_reservation import exploration_activity
from dlt.prize_rules import dlt_2026_hit_groups, dlt_2026_additional_groups


class ScenarioError(ValueError):
    pass


def historical_26123(path, snapshot_id, pair):
    """Use the existing 26123 historical fixture, not a forecast or tax ruling.

    Its prize mapping and published amounts are applied as a fixed counterfactual
    to the supplied pair. The local snapshot's origin is not authenticated here.
    """
    if not Path(path).is_file():
        raise ScenarioError('历史快照不存在')
    with exploration_activity(path, kind='historical_scenario', inputs=(), targets=('26123',)):
        return _historical_26123(path, snapshot_id, pair)


def _historical_26123(path, snapshot_id, pair):
    with closing(sqlite3.connect(path)) as db:
        source = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (snapshot_id,)).fetchone()
        row = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?', (snapshot_id, '26123')).fetchone()
        variants = db.execute('SELECT DISTINCT row_json FROM draw_versions WHERE issue=?', ('26123',)).fetchall()
    if not source or not row:
        raise ScenarioError('所选快照不包含第 26123 期')
    if hashlib.sha256(source[0]).hexdigest() != snapshot_id:
        raise ScenarioError('历史快照摘要不符')
    try:
        _, parsed = parse_snapshot(source[0])
    except InvalidSnapshot as exc:
        raise ScenarioError('历史快照无法解析') from exc
    found = [item for item in parsed if item['issue'] == '26123']
    if len(found) != 1 or _canonical(found[0]['raw_row']) != row[0]:
        raise ScenarioError('奖金记录与快照不一致')
    if len({item[0] for item in variants}) != 1:
        raise ScenarioError('第 26123 期来源存在差异，需先复核')
    raw = json.loads(row[0])
    if type(raw.get('verify')) is not int or raw['verify'] != 1:
        raise ScenarioError('历史开奖核验标记缺失')
    if type(raw.get('lotteryPromotionFlag')) is not int or raw['lotteryPromotionFlag'] != 0:
        raise ScenarioError('历史派奖状态未知或未受支持')
    levels = raw.get('prizeLevelList')
    if not isinstance(levels, list):
        raise ScenarioError('历史奖金缺失')
    payouts = {}
    for item in levels:
        if not isinstance(item, dict) or 'sort' not in item or 'stakeAmount' not in item:
            raise ScenarioError('历史奖级字段缺失')
        key = str(item['sort'])
        if key in payouts:
            raise ScenarioError('历史奖级重复')
        payouts[key] = item['stakeAmount']
    required = {'101', '201', '301', '401', '501', '601', '701', '801', '901'}
    if not required.issubset(payouts):
        raise ScenarioError('历史奖金不完整')
    hits = dlt_2026_hit_groups()
    config = pair['config']
    rule = {'version': 'historical-26123-fixed-assumptions-v1',
            'source': 'local research snapshot; externally unverified', 'verified': True,
            'applicable_issues': [config['target_issue']], 'hit_groups': hits,
            'additional_groups': dlt_2026_additional_groups(), 'promotion_policy': 'excluded',
            'prices': {'base': str(Decimal(config['base_price_fen'])/100),
                       'additional': str(Decimal(config['additional_price_fen'])/100)},
            'tax': {'unit': 'same_person_same_issue_same_game', 'threshold': '10000', 'comparison': 'gt', 'rate': '0.20', 'rounding': 'down', 'quantum': '0.01'}}
    return {'source_issue': '26123', 'reference': '本地第 26123 期历史奖金快照，来源未重新联网核验',
            'adjustment_assumption': '固定公布单注奖金；不重算新增中奖者分奖或限额；无其他同期开奖中奖收入',
            'promotion': False, 'rule': rule, 'payouts': {k: payouts[k] for k in required},
            'source_dependency': {'snapshot_id': snapshot_id, 'issue': '26123',
                                  'row_sha256': hashlib.sha256(row[0]).hexdigest()},
            'assurance': 'historical scenario and tax assumptions; not a verified current rule or prediction'}


def historical_scenario(path, snapshot_id, pair, issue):
    """Build a locally researched historical payout scenario for an actual pair.

    The supported issue range and source checks are the same as historical
    reconstruction. Neither the historical rule nor its tax assumptions are
    represented as the target issue's verified official rule.
    """
    if not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue):
        raise ScenarioError('历史奖金期号无效')
    if issue == '26123':
        # Keep the existing historical issue mapping; tax-unit metadata is
        # explicit in newly constructed scenarios and old requests remain frozen.
        return historical_26123(path, snapshot_id, pair)
    if not Path(path).is_file():
        raise ScenarioError('历史快照不存在')
    with exploration_activity(path, kind='historical_scenario', inputs=(), targets=(issue,)):
        return _historical_scenario(path, snapshot_id, pair, issue)


def _historical_scenario(path, snapshot_id, pair, issue):
    from dlt.historical_scenarios import backtest_scenarios
    manifest = backtest_scenarios(path, snapshot_id, [issue], additional=True)[issue]
    if 'blocked_reason' in manifest:
        raise ScenarioError(manifest['blocked_reason'])
    config = pair['config']
    rule = dict(manifest['rule'])
    rule['applicable_issues'] = [config['target_issue']]
    rule['prices'] = {'base': str(Decimal(config['base_price_fen']) / 100),
                      'additional': str(Decimal(config['additional_price_fen']) / 100)}
    return {'source_issue': issue, 'reference': f'本地第 {issue} 期历史奖金快照，来源未重新联网核验',
            'adjustment_assumption': '固定公布单注奖金；不重算新增中奖者分奖或限额；无其他同期开奖中奖收入',
            'promotion': False, 'rule': rule, 'payouts': manifest['payouts'],
            'source_dependency': manifest['source_dependency'],
            'assurance': 'historical scenario and tax assumptions; not a verified current rule or prediction'}
