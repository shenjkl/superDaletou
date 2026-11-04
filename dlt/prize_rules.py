"""Prize mappings shared by the 2026-era DLT research paths.

This records the hit-to-prize structure described by the published rules
effective from issue 26014. It does not authenticate a source, verify that the
rules apply to a reviewed issue, or calculate published prize amounts.
"""
from types import MappingProxyType
from decimal import Decimal, InvalidOperation

DLT_2026_RULE_START_ISSUE = '26014'
DLT_2026_OFFICIAL_SORT_CODES = frozenset({'101', '201', '301', '401', '501', '601', '701', '801', '901'})
DLT_2026_PROMOTION_FIRST_ISSUE = '26050'
DLT_2026_PROMOTION_LAST_ISSUE = '26066'
DLT_2026_PROMOTION_SORTS = MappingProxyType({'501': '502', '601': '602', '701': '702', '801': '802', '901': '902'})

_DLT_2026_HIT_GROUPS = {
    f'{front},{back}': None
    for front in range(6)
    for back in range(3)
}
_DLT_2026_HIT_GROUPS.update({
    '5,2': '101',
    '5,1': '301',
    '5,0': '501',
    '4,2': '501',
    '4,1': '601',
    '4,0': '701',
    '3,2': '701',
    '3,1': '801',
    '2,2': '801',
    '3,0': '901',
    '2,1': '901',
    '1,2': '901',
    '0,2': '901',
})
DLT_2026_HIT_GROUPS = MappingProxyType(_DLT_2026_HIT_GROUPS)

DLT_2026_ADDITIONAL_GROUPS = MappingProxyType({'101': '201', '301': '401'})


def dlt_2026_hit_groups():
    """Return a fresh mapping so scenario callers cannot mutate the constant."""
    return dict(DLT_2026_HIT_GROUPS)


def dlt_2026_additional_groups():
    """Return a fresh additional-prize mapping."""
    return dict(DLT_2026_ADDITIONAL_GROUPS)


def validate_dlt_2026_rule_mapping(issue, rule):
    """Reject malformed current-rule mappings on prospective settlement paths.

    This verifies only the encoded rule structure. It does not authenticate a
    source, establish issue applicability, or verify the published payouts.
    """
    if not isinstance(issue, str) or issue < DLT_2026_RULE_START_ISSUE:
        return
    if not isinstance(rule, dict):
        raise ValueError('current DLT rule mapping is missing')
    hits, extras = rule.get('hit_groups'), rule.get('additional_groups')
    sort_map, prices = rule.get('payout_sort_map'), rule.get('prices')
    if not isinstance(hits, dict) or set(hits) != set(DLT_2026_HIT_GROUPS):
        raise ValueError('current DLT hit mapping must declare all 18 outcomes')
    if (not isinstance(sort_map, dict)
            or any(not isinstance(value, (str, int)) or isinstance(value, bool) for value in sort_map.values())):
        raise ValueError('current DLT published prize sort mapping is invalid')
    codes = [str(value) for value in sort_map.values()]
    if set(codes) != DLT_2026_OFFICIAL_SORT_CODES or len(codes) != len(set(codes)):
        raise ValueError('current DLT published prize sort mapping is incomplete')
    code_by_group = {group: str(code) for group, code in sort_map.items()}
    for outcome, expected_code in DLT_2026_HIT_GROUPS.items():
        actual_group = hits[outcome]
        if expected_code is None:
            if actual_group is not None:
                raise ValueError('current DLT hit mapping assigns a prize to a non-winning outcome')
        elif not isinstance(actual_group, str) or code_by_group.get(actual_group) != expected_code:
            raise ValueError('current DLT hit mapping does not match the published prize rank')

    expected_extras = {}
    for base_code, extra_code in DLT_2026_ADDITIONAL_GROUPS.items():
        base_group = next((group for group, code in code_by_group.items() if code == base_code), None)
        extra_group = next((group for group, code in code_by_group.items() if code == extra_code), None)
        if base_group is None or extra_group is None:
            raise ValueError('current DLT additional prize sort mapping is incomplete')
        expected_extras[base_group] = extra_group
    if extras != expected_extras:
        raise ValueError('current DLT additional prize mapping is invalid')
    if (not isinstance(prices, dict) or prices.get('base') != '2'
            or prices.get('additional') != '3'):
        raise ValueError('current DLT ticket prices must match the published rule')
    tax = rule.get('tax')
    if not isinstance(tax, dict):
        raise ValueError('current DLT tax rule is missing')
    try:
        threshold = Decimal(str(tax.get('threshold')))
        rate = Decimal(str(tax.get('rate')))
    except (InvalidOperation, ValueError):
        raise ValueError('current DLT tax parameters are invalid') from None
    if not threshold.is_finite() or not rate.is_finite():
        raise ValueError('current DLT tax parameters must be finite decimal values')
    if (tax.get('unit') != 'same_person_same_issue_same_game'
            or threshold != Decimal('10000') or tax.get('comparison') != 'gt'
            or rate != Decimal('0.20') or tax.get('quantum') != '0.01'):
        raise ValueError('current DLT tax rule must use the verified lottery tax unit, threshold, and rate')
