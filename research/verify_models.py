"""Focused, offline checks of downloaded pure functions; no app startup or training."""
import ast
import json
import math
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent

def extract(path, names, namespace):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    nodes = [ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)]
    nodes += [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(path), 'exec'), namespace)
    return namespace

source = ROOT / 'sources/dlt-analysis/dlt_tool.py'
tree = ast.parse(source.read_text(encoding='utf-8'))
literal_names = {'PRIZE_RULES', 'DEFAULT_PRIZE_AMOUNTS'}
ns = {'math': math, 'FRONT_RANGE_SIZE': 35, 'FRONT_PICK': 5, 'BACK_RANGE_SIZE': 12, 'BACK_PICK': 2, 'OFFICIAL_RETURN_RATE': 0.51, 'BASE_BET_COST': 2.0}
for node in tree.body:
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id in literal_names:
        ns[node.target.id] = ast.literal_eval(node.value)
ns['PRIZE_ORDER'] = tuple(ns['PRIZE_RULES'])
ns['PRIZE_LABEL_BY_HITS'] = {pair: level for level, pairs in ns['PRIZE_RULES'].items() for pair in pairs}
extract(source, {'count_front_combinations', 'count_back_combinations', 'count_total_combinations', 'hypergeometric_probability', 'case_probability', 'case_count', 'prize_probabilities', 'any_prize_probability', 'self_consistency_check', 'expected_value'}, ns)
assert ns['count_total_combinations']() == 21425712
assert sum(ns['case_count'](a,b) for a in range(6) for b in range(3)) == 21425712
for a in range(6):
    for b in range(3):
        assert abs(ns['case_probability'](a,b) - ns['case_count'](a,b)/21425712) < 1e-12
assert ns['self_consistency_check']()['passed']

# Exact null expectation: maximum of five IID random front-hit counts versus one.
probs = [math.comb(5,a)*math.comb(30,5-a)/math.comb(35,5) for a in range(6)]
cdf = 0.0
expected_max = 0.0
for k in range(1,6):
    cdf += probs[k-1]
    expected_max += 1-cdf**5

core = extract(ROOT/'sources/LottoPilot/backend/app/services/backtest_core.py', {'train_slice_before_target', 'assert_no_future_leak'}, {})
history = [SimpleNamespace(issue='26001'), SimpleNamespace(issue='26002'), SimpleNamespace(issue='26003')]
assert [x.issue for x in core['train_slice_before_target'](history,2)] == ['26002','26001']
future_guard_rejected = False
try:
    core['assert_no_future_leak']([history[2]],history[1])
except ValueError:
    future_guard_rejected = True

pr = ROOT/'sources/LottoPilot/backend/app/services/recommendation/prize_rules.py'
rules = ast.parse(pr.read_text(encoding='utf-8'))
pns = {}
for node in rules.body:
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == 'DEFAULT_PRIZE_TABLES':
        pns[node.target.id] = ast.literal_eval(node.value)
extract(pr, {'map_prize_level'}, pns)
output = {
    'dlt_analysis': {'exact_probability_checks': 'passed', 'total': ns['count_total_combinations'](), 'any_prize_probability':ns['any_prize_probability'](), 'default_gross_return':ns['expected_value']()['gross_return_per_bet'], 'default_prize_levels':len(ns['PRIZE_RULES'])},
    'lottopilot': {'training_slice_check':'passed', 'future_guard_rejects_strictly_future_issue':future_guard_rejected, 'default_4_plus_2_level':pns['map_prize_level']('dlt',4,2), 'default_4_plus_1_level':pns['map_prize_level']('dlt',4,1)},
    'baseline_metric_null_expectation': {'single_ticket_front_hits':5*5/35, 'best_of_five_iid_tickets_front_hits':expected_max, 'apparent_lift_without_predictive_signal':expected_max-5*5/35, 'note':'IID illustration; actual diverse selection changes dependence. Not measured platform performance.'}
}
(ROOT/'verification_results.json').write_text(json.dumps(output,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(output,ensure_ascii=False,indent=2))
