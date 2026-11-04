"""Pure portfolio settlement under an explicit, externally verified rule.

No statutory tax rate, payout table, calendar or guessed missing award is built
in. The caller must resolve applicable rules and source provenance first.
"""
from __future__ import annotations

from decimal import Context, Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP, ROUND_HALF_EVEN, localcontext
from functools import wraps
import re


class SettlementBlocked(ValueError):
    """A complete and applicable input is required before settlement."""


ROUNDING = {'down': ROUND_DOWN, 'half_up': ROUND_HALF_UP, 'half_even': ROUND_HALF_EVEN}

# The limits below bound the largest possible intermediate (including two
# decimal operands, ticket count and multiplier) to fewer than 512 digits.
MONEY_PRECISION = 512
MAX_MONEY_DIGITS = 128
MAX_MONEY_EXPONENT = 128
MAX_TICKETS = 10000
MAX_MULTIPLIER = 1000000
SUPPORTED_TAX_UNIT = 'same_person_same_issue_same_game'


def require_tax_unit(tax):
    """The implemented calculation assumes one person's same-issue game income.

    Whether a rule is applicable and whether other winnings exist remain the
    caller's responsibility; this field never authenticates a legal source.
    """
    if not isinstance(tax, dict) or tax.get('unit') != SUPPORTED_TAX_UNIT:
        raise SettlementBlocked('计税单位未声明或不受支持 (tax unit)：仅支持同一人、同一期、同一游戏')


def exact_money_context(function):
    """Isolate arithmetic from the caller's decimal precision and rounding."""
    @wraps(function)
    def wrapped(*args, **kwargs):
        with localcontext(Context(prec=MONEY_PRECISION)):
            return function(*args, **kwargs)
    return wrapped


def _amount(value, field):
    if isinstance(value, (float, bool)) or value is None:
        raise SettlementBlocked(f'{field}: exact decimal value required')
    raw = str(value)
    if len(raw) > 512:
        raise SettlementBlocked(f'{field}: amount exceeds resource limits')
    try:
        result = Decimal(raw.replace(',', ''))
    except InvalidOperation as exc:
        raise SettlementBlocked(f'{field}: invalid amount') from exc
    if not result.is_finite() or result < 0:
        raise SettlementBlocked(f'{field}: nonnegative finite amount required')
    parts = result.as_tuple()
    if (len(parts.digits) > MAX_MONEY_DIGITS or abs(parts.exponent) > MAX_MONEY_EXPONENT
            or result.adjusted() > MAX_MONEY_EXPONENT):
        raise SettlementBlocked(f'{field}: amount exceeds resource limits')
    return result


def _numbers(numbers, count, maximum, field):
    if (not isinstance(numbers, (list, tuple)) or len(numbers) != count
            or any(type(n) is not int or n < 1 or n > maximum for n in numbers)
            or len(set(numbers)) != count):
        raise SettlementBlocked(f'{field}: invalid numbers')
    return set(numbers)


from dlt.prize_rules import DLT_2026_PROMOTION_FIRST_ISSUE, DLT_2026_PROMOTION_LAST_ISSUE


@exact_money_context
def settle_portfolio(*, issue, draw, tickets, payouts, rule, duplicate_policy,
                     ticket_groups=None, promotion_payouts=None):
    """Calculate one alternative portfolio; rules and payout assumptions are explicit.

    Amounts are decimal strings or integers in yuan. Each ticket specifies
    front/back, boolean additional and positive integer multiplier. Tax is
    applied once to this portfolio's total gross under the explicit assumption
    that it comprises one person's same-issue, same-game winnings. The caller
    must separately justify the absence of other winnings and rule applicability.
    """
    if (not isinstance(issue, str) or not re.fullmatch(r'\d{5}', issue)
            or not isinstance(draw, dict) or draw.get('issue') != issue):
        raise SettlementBlocked('draw issue does not match')
    if not isinstance(rule, dict) or rule.get('verified') is not True or not rule.get('source') or not rule.get('version'):
        raise SettlementBlocked('verified rule version and source required')
    if issue not in rule.get('applicable_issues', []):
        raise SettlementBlocked('rule does not apply to issue')
    if draw.get('promotion') not in (True, False) or type(draw.get('promotion')) is not bool:
        raise SettlementBlocked('promotion status required')
    promotion = draw['promotion']
    if promotion and not (DLT_2026_PROMOTION_FIRST_ISSUE <= issue <= DLT_2026_PROMOTION_LAST_ISSUE):
        raise SettlementBlocked('promotion flag is outside the verified 2026 activity issue range')
    if duplicate_policy not in ('reject', 'allow'):
        raise SettlementBlocked('explicit duplicate policy required')
    if not isinstance(tickets, list) or not tickets:
        raise SettlementBlocked('non-empty portfolio required')
    if len(tickets) > MAX_TICKETS:
        raise SettlementBlocked('portfolio exceeds ticket resource limit')
    front = _numbers(draw.get('front'), 5, 35, 'draw front')
    back = _numbers(draw.get('back'), 2, 12, 'draw back')
    groups = rule.get('hit_groups')
    extras = rule.get('additional_groups')
    if not isinstance(groups, dict) or not groups or not isinstance(extras, dict):
        raise SettlementBlocked('complete hit mapping and additional mapping required')
    # Every possible hit count is explicit, including non-winning combinations
    # represented by null. This avoids silently assigning missing mappings zero.
    expected_hits = {f'{f},{b}' for f in range(6) for b in range(3)}
    if set(groups) != expected_hits:
        raise SettlementBlocked('all 18 hit mappings must be declared')
    if any(group is not None and (not isinstance(group, str) or not group) for group in groups.values()):
        raise SettlementBlocked('invalid prize group')
    if any(not isinstance(k, str) or not isinstance(v, str) or not v for k, v in extras.items()):
        raise SettlementBlocked('invalid additional prize mapping')
    if not isinstance(payouts, dict):
        raise SettlementBlocked('published payouts required')
    required = {group for group in groups.values() if group is not None}
    if any(not isinstance(ticket, dict) or type(ticket.get('additional')) is not bool for ticket in tickets):
        raise SettlementBlocked('each ticket requires an explicit additional flag')
    if any(type(ticket.get('multiplier')) is not int or not 1 <= ticket['multiplier'] <= MAX_MULTIPLIER
           for ticket in tickets):
        raise SettlementBlocked('positive integer multiplier within resource limit required')
    if any(ticket['additional'] for ticket in tickets):
        if not set(extras).issubset(required):
            raise SettlementBlocked('additional mapping references an unknown prize group')
        required.update(extras.values())
    amounts = {group: _amount(payouts.get(group), f'payout {group}') for group in required}
    price = rule.get('prices')
    if not isinstance(price, dict):
        raise SettlementBlocked('verified prices required')
    base_price, additional_price = _amount(price.get('base'), 'base price'), _amount(price.get('additional'), 'additional price')
    if base_price <= 0 or additional_price <= 0:
        raise SettlementBlocked('positive prices required')
    eligible_indexes = set()
    if promotion:
        if not isinstance(ticket_groups, list) or not ticket_groups:
            raise SettlementBlocked('promotion settlement requires explicit ticket-to-line groups')
        if not isinstance(promotion_payouts, dict):
            raise SettlementBlocked('published promotion prize amounts are required')
        seen_indexes = set()
        for group in ticket_groups:
            if (not isinstance(group, list) or not group
                    or any(type(index) is not int or not 0 <= index < len(tickets) for index in group)
                    or len(set(group)) != len(group) or seen_indexes.intersection(group)):
                raise SettlementBlocked('promotion ticket group is invalid or overlaps')
            seen_indexes.update(group)
            group_cost = sum(((additional_price if tickets[index]['additional'] else base_price)
                              * tickets[index]['multiplier'] for index in group), Decimal('0'))
            maximum_ticket_cost = Decimal('30000') if any(tickets[index]['additional'] for index in group) else Decimal('20000')
            if group_cost > maximum_ticket_cost:
                raise SettlementBlocked('promotion ticket exceeds the single-ticket purchase limit')
            if group_cost >= Decimal('18'):
                eligible_indexes.update(group)
        if seen_indexes != set(range(len(tickets))):
            raise SettlementBlocked('promotion ticket groups must cover every line')
        promotion_payouts = {key: _amount(value, f'promotion payout {key}')
                             for key, value in promotion_payouts.items()}
        if not promotion_payouts or not set(promotion_payouts).issubset(required):
            raise SettlementBlocked('promotion payout mapping references unknown prize groups')
    tax = rule.get('tax')
    require_tax_unit(tax)
    if not isinstance(tax, dict) or tax.get('comparison') not in ('gt', 'gte') or tax.get('rounding') not in ROUNDING:
        raise SettlementBlocked('explicit tax comparison and rounding required')
    threshold, rate, quantum = _amount(tax.get('threshold'), 'tax threshold'), _amount(tax.get('rate'), 'tax rate'), _amount(tax.get('quantum'), 'tax quantum')
    if rate > 1 or quantum != Decimal('0.01'):
        raise SettlementBlocked('tax rate or currency quantum invalid')
    gross = Decimal('0'); cost = Decimal('0'); winning = 0; promotion_gross = Decimal('0'); seen = set()
    for ticket_index, ticket in enumerate(tickets):
        f = _numbers(ticket.get('front'), 5, 35, 'ticket front')
        b = _numbers(ticket.get('back'), 2, 12, 'ticket back')
        multiple = ticket.get('multiplier')
        if type(multiple) is not int or not 1 <= multiple <= MAX_MULTIPLIER:
            raise SettlementBlocked('positive integer multiplier within resource limit required')
        key = (tuple(sorted(f)), tuple(sorted(b)), ticket['additional'])
        if key in seen and duplicate_policy == 'reject':
            raise SettlementBlocked('duplicate ticket')
        seen.add(key)
        group = groups[f'{len(f & front)},{len(b & back)}']
        prize = amounts[group] if group is not None else Decimal('0')
        if ticket['additional'] and group in extras:
            prize += amounts[extras[group]]
        if promotion and ticket_index in eligible_indexes and group in promotion_payouts:
            bonus = promotion_payouts[group]
            prize += bonus
            promotion_gross += bonus * multiple
        gross += prize * multiple
        cost += (additional_price if ticket['additional'] else base_price) * multiple
        if prize > 0:
            winning += multiple
    taxable = gross > threshold if tax['comparison'] == 'gt' else gross >= threshold
    withheld = (gross * rate).quantize(quantum, rounding=ROUNDING[tax['rounding']]) if taxable else Decimal('0')
    result = {'issue': issue, 'rule_version': rule['version'], 'gross': str(gross), 'tax': str(withheld),
              'cost': str(cost), 'net': str(gross-withheld-cost), 'winning_tickets': winning}
    if promotion:
        result.update({'promotion_bonus': str(promotion_gross),
                       'promotion_qualified_ticket_groups': sum(
                           1 for group in ticket_groups
                           if sum(((additional_price if tickets[index]['additional'] else base_price)
                                   * tickets[index]['multiplier'] for index in group), Decimal('0')) >= Decimal('18'))})
    return result
