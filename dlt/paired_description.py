"""Descriptive summaries of prespecified period-level paired differences.

These are observed-data diagnostics, not confidence intervals, tests or
evidence that the observed periods were independently selected.
"""
from decimal import Decimal, InvalidOperation
from fractions import Fraction


class PairedDescriptionError(ValueError):
    pass


def _fraction(value):
    return [value.numerator, value.denominator] if value is not None else None


def _amount(value, *, maximum=Decimal('1e30')):
    if not isinstance(value, str) or len(value) > 100:
        raise PairedDescriptionError('invalid settlement amount')
    try:
        d = Decimal(value)
    except InvalidOperation as exc:
        raise PairedDescriptionError('invalid settlement amount') from exc
    if not d.is_finite() or d.copy_abs() > maximum or abs(d.as_tuple().exponent) > 100:
        raise PairedDescriptionError('invalid settlement amount')
    return Fraction(d)


def _settlement(value):
    gross, tax, cost, net = (_amount(value[key]) for key in ('gross', 'tax', 'cost', 'net'))
    if gross < 0 or tax < 0 or cost <= 0 or net != gross-tax-cost:
        raise PairedDescriptionError('settlement amounts are inconsistent')
    return net, cost


def _stored_fraction(value):
    if (not isinstance(value, list) or len(value) != 2 or type(value[0]) is not int
            or type(value[1]) is not int or value[1] <= 0):
        raise PairedDescriptionError('invalid stored fraction')
    return Fraction(value[0], value[1])


def describe_pairs(observations, *, planned):
    """Summarize exact (issue, within-issue replica-mean difference) pairs.

    Quantiles use nearest rank. Leave-one-out range is descriptive sensitivity,
    not an interval for a population mean. Missing periods are never zero-filled.
    """
    if type(planned) is not int or planned < 0 or len(observations) > planned:
        raise PairedDescriptionError('invalid planned period count')
    issues = [issue for issue, _ in observations]
    if any(not isinstance(i, str) or not i for i in issues) or len(set(issues)) != len(issues):
        raise PairedDescriptionError('duplicate or invalid period')
    if any(not isinstance(x, Fraction) for _, x in observations):
        raise PairedDescriptionError('exact differences required')
    n = len(observations)
    base = {'version': 'observed-paired-periods-v1', 'planned': planned, 'included': n, 'missing': planned-n,
            'completeness': 'complete' if n == planned and n else 'partial' if n else 'no_results',
            'positive': sum(x > 0 for _, x in observations), 'zero': sum(x == 0 for _, x in observations),
            'negative': sum(x < 0 for _, x in observations), 'mean_fraction': None,
            'min_fraction': None, 'p25_fraction': None, 'median_fraction': None, 'p75_fraction': None,
            'max_fraction': None, 'leave_one_out_min_fraction': None, 'leave_one_out_max_fraction': None,
            'largest_absolute_issue': None, 'largest_absolute_share_fraction': None}
    if not n:
        return base
    values = sorted(x for _, x in observations); total = sum(values, Fraction(0))
    quantile = lambda num, den: values[(n*num+den-1)//den-1]
    base.update(mean_fraction=_fraction(total/n), min_fraction=_fraction(values[0]),
                p25_fraction=_fraction(quantile(1, 4)), median_fraction=_fraction(quantile(1, 2)),
                p75_fraction=_fraction(quantile(3, 4)), max_fraction=_fraction(values[-1]))
    if n > 1:
        # Dropping the largest value minimizes the remaining mean and vice versa.
        base['leave_one_out_min_fraction'] = _fraction((total-values[-1])/(n-1))
        base['leave_one_out_max_fraction'] = _fraction((total-values[0])/(n-1))
    absolute = sum((abs(x) for x in values), Fraction(0))
    if absolute:
        # Stable first-in-manifest tie break, not a selection of a winning period.
        issue, value = max(observations, key=lambda item: abs(item[1]))
        base['largest_absolute_issue'] = issue
        base['largest_absolute_share_fraction'] = _fraction(abs(value)/absolute)
    return base


def describe_backtest(report):
    """Recheck stored period aggregates before deriving historical statistics."""
    try:
        protocol = report['protocol']; targets = protocol['targets']; expected = protocol['replicas']
        periods = report['periods']
        if (not isinstance(targets, list) or not targets or len(set(targets)) != len(targets)
                or not isinstance(expected, list) or not expected or len(set(expected)) != len(expected)
                or not isinstance(periods, list) or len(periods) != len(targets)
                or report['planned_periods'] != len(targets)):
            raise PairedDescriptionError('period or replica manifest mismatch')
        valid, missing = [], []
        for issue, period in zip(targets, periods):
            replicas = period['replicas']
            if (period['target']['issue'] != issue or not isinstance(replicas, list)
                    or [r['replica'] for r in replicas] != expected):
                raise PairedDescriptionError('period or replica manifest mismatch')
            if any(r['status'] not in ('complete', 'failed') for r in replicas):
                raise PairedDescriptionError('invalid replica status')
            complete = all(r['status'] == 'complete' for r in replicas)
            if period['status'] != ('complete' if complete else 'incomplete'):
                raise PairedDescriptionError('incomplete period status mismatch')
            if not complete:
                if period['mean_difference_fraction'] is not None:
                    raise PairedDescriptionError('incomplete period must not publish a mean')
                missing.append(issue)
                continue
            differences = []
            for replica in replicas:
                candidate, control = replica['candidate_settlement'], replica['control_settlement']
                candidate_net, candidate_cost = _settlement(candidate)
                control_net, control_cost = _settlement(control)
                if (candidate_cost != control_cost
                        or _amount(replica['difference_yuan'], maximum=Decimal('2e30')) != candidate_net-control_net):
                    raise PairedDescriptionError('paired costs or difference mismatch')
                differences.append(candidate_net-control_net)
            mean = sum(differences, Fraction(0))/len(differences)
            if _stored_fraction(period['mean_difference_fraction']) != mean:
                raise PairedDescriptionError('within-period mean mismatch')
            valid.append((issue, mean))
        if (report['complete_periods'] != len(valid) or report['incomplete_issues'] != missing
                or report['completeness'] != ('complete' if not missing else 'partial')):
            raise PairedDescriptionError('period counts or completeness mismatch')
        summary = describe_pairs(valid, planned=len(targets))
        stored = report['descriptive_mean_fraction']
        if (stored is None) != (not valid) or (valid and _stored_fraction(stored) != _stored_fraction(summary['mean_fraction'])):
            raise PairedDescriptionError('cross-period mean mismatch')
        return summary
    except (KeyError, TypeError, InvalidOperation, ZeroDivisionError) as exc:
        raise PairedDescriptionError('historical paired report is incomplete or invalid') from exc
