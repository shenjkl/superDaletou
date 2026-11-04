"""User-specified fixed payout and tax assumptions for portfolio research only."""
from decimal import Decimal
import re

from dlt.scenarios import ScenarioError
from dlt.settlement import ROUNDING, SUPPORTED_TAX_UNIT
from dlt.prize_rules import dlt_2026_hit_groups


PAYOUT_KEYS = ('101', '201', '301', '401', '501', '601', '701', '801', '901')
BASE_PAYOUT_KEYS = ('101', '301', '501', '601', '701', '801', '901')
HIT_KEYS = tuple(f'{front},{back}' for front in range(6) for back in range(3))


def _default_hits():
    return dlt_2026_hit_groups()


def custom_scenario(pair, custom_input):
    allowed = ({'label', 'payouts'}, {'label', 'payouts', 'provenance'},
               {'label', 'payouts', 'tax'}, {'label', 'payouts', 'tax', 'provenance'},
               {'label', 'payouts', 'hit_groups'}, {'label', 'payouts', 'hit_groups', 'provenance'},
               {'label', 'payouts', 'tax', 'hit_groups'},
               {'label', 'payouts', 'tax', 'hit_groups', 'provenance'})
    if not isinstance(custom_input, dict) or set(custom_input) not in allowed:
        raise ScenarioError('自定义情景需要名称及完整奖金')
    label, payouts = custom_input['label'], custom_input['payouts']
    if (not isinstance(label, str) or not label.strip() or len(label.strip()) > 40
            or any(ord(char) < 32 for char in label)):
        raise ScenarioError('情景名称需为 1—40 个字符')
    if not isinstance(payouts, dict) or set(payouts) != set(PAYOUT_KEYS):
        raise ScenarioError('请填写全部基本及追加奖级奖金')
    normalized = {}
    for key in PAYOUT_KEYS:
        value = payouts[key]
        if not isinstance(value, str) or not re.fullmatch(r'(?:0|[1-9][0-9]{0,17})(?:\.[0-9]{1,2})?', value):
            raise ScenarioError('奖金须为非负金额，最多 18 位整数和 2 位小数')
        normalized[key] = format(Decimal(value).quantize(Decimal('0.01')), 'f')
    cfg = pair['config']
    hit_input = custom_input.get('hit_groups')
    hits = _default_hits() if hit_input is None else _normalize_hits(hit_input)
    tax_input = custom_input.get('tax')
    if tax_input is None:
        tax = {'unit': SUPPORTED_TAX_UNIT, 'threshold': '10000', 'comparison': 'gt',
               'rate': '0.20', 'rounding': 'down', 'quantum': '0.01'}
    else:
        tax = _normalize_tax(tax_input)
    rule = {'version': 'custom-fixed-payout-v2' if tax_input is not None or hit_input is not None else 'custom-fixed-payout-v1',
            'source': 'user-specified hypothetical payout and tax assumptions; not independently verified',
            'verified': True, 'applicable_issues': [cfg['target_issue']], 'hit_groups': hits,
            'additional_groups': {'101': '201', '301': '401'}, 'promotion_policy': 'excluded',
            'prices': {'base': str(Decimal(cfg['base_price_fen']) / 100),
                       'additional': str(Decimal(cfg['additional_price_fen']) / 100)}, 'tax': tax}
    inputs = {'label': label.strip(), 'payouts': normalized}
    if hit_input is not None:
        inputs['hit_groups'] = hits
    if tax_input is not None:
        inputs['tax'] = tax
    if 'provenance' in custom_input:
        provenance = custom_input['provenance']
        if (not isinstance(provenance, dict) or provenance.get('kind') not in ('synthetic', 'historical')
                or (provenance.get('kind') == 'synthetic' and set(provenance) != {'kind'})
                or (provenance.get('kind') == 'historical' and (set(provenance) != {'kind', 'issue'}
                    or not isinstance(provenance.get('issue'), str)
                    or not re.fullmatch(r'[0-9]{5}', provenance['issue'])))):
            raise ScenarioError('请声明自定义奖金为假设值，或填写历史来源期号')
        inputs['provenance'] = provenance.copy()
    return {'kind': 'custom_payout_v1', 'custom_input': inputs, 'source_issue': 'custom',
            'reference': '用户自定义固定奖金：' + inputs['label'],
            'adjustment_assumption': '固定假设奖金；不重算新增中奖者分奖或限额；不含派奖；假设没有其他同期开奖中奖收入',
            'promotion': False, 'rule': rule, 'payouts': normalized,
            'assurance': 'user-specified hypothetical scenario; not a verified current rule or prediction'}


def _normalize_tax(value):
    if (not isinstance(value, dict) or set(value) != {'unit', 'threshold', 'comparison', 'rate', 'rounding', 'quantum'}
            or value.get('unit') != SUPPORTED_TAX_UNIT or value.get('comparison') not in ('gt', 'gte')
            or value.get('rounding') not in ROUNDING or value.get('quantum') != '0.01'):
        raise ScenarioError('税费假设格式无效；仅支持同一人同一期同一游戏、精确到分的假设')
    threshold, rate = value.get('threshold'), value.get('rate')
    if (not isinstance(threshold, str) or not re.fullmatch(r'(?:0|[1-9][0-9]{0,17})(?:\.[0-9]{1,2})?', threshold)
            or not isinstance(rate, str) or not re.fullmatch(r'(?:0|1)(?:\.[0-9]{1,6})?', rate)):
        raise ScenarioError('税费门槛或税率格式无效')
    threshold_value, rate_value = Decimal(threshold), Decimal(rate)
    if rate_value > 1:
        raise ScenarioError('税率假设必须在 0 到 1 之间')
    return {'unit': SUPPORTED_TAX_UNIT, 'threshold': format(threshold_value.quantize(Decimal('0.01')), 'f'),
            'comparison': value['comparison'], 'rate': format(rate_value.normalize(), 'f'),
            'rounding': value['rounding'], 'quantum': '0.01'}


def _normalize_hits(value):
    if (not isinstance(value, dict) or set(value) != set(HIT_KEYS)
            or any(group is not None and group not in BASE_PAYOUT_KEYS for group in value.values())):
        raise ScenarioError('奖级映射必须为 18 种命中结果逐项指定有效奖级或不中奖')
    return {key: value[key] for key in HIT_KEYS}


def check_custom_scenario(pair, scenario):
    if scenario.get('kind') == 'custom_payout_v1':
        if custom_scenario(pair, scenario.get('custom_input')) != scenario:
            raise ScenarioError('自定义奖金情景与固定输入不一致')
