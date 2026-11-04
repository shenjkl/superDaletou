import json
from fractions import Fraction
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from dlt.app import _evaluation_wire
from dlt.candidates import generate_pair
from dlt.evaluation import evaluate_pair
from test_candidates import config, input_rows
from test_evaluation import rehash, scenario


class MoneyWireTests(unittest.TestCase):
    def test_real_large_evaluation_survives_wire_and_exact_frontend_formatting(self):
        pair = generate_pair(history=input_rows(), config={**config(), 'budget_fen': 400})
        sc = scenario()
        sc['payouts']['first'] = sc['payouts']['second'] = '12345678901234567890123456.78'
        class FixedDraw:
            def __init__(self, seed): pass
            def sample(self, population, count):
                return [1, 2, 3, 4, 5] if count == 5 else [1, 2]
        from test_settlement import fixture
        tickets = fixture()['tickets']
        for label in ('candidate', 'control'):
            pair[label]['tickets'] = tickets
            pair[label]['cost_fen'] = 400
        with patch('dlt.evaluation.random.Random', FixedDraw):
            result = evaluate_pair(rehash(pair), scenario=sc, samples=1, seed=7,
                                   max_samples=1, max_ticket_draws=4)
        wire = _evaluation_wire({'result': result})['result']
        value = wire['portfolios']['candidate']
        self.assertTrue(all(isinstance(n, str) for n in value['mean_gross_tax_before']['fraction']))
        script = r"""
const fs = require('node:fs'), vm = require('node:vm');
const src = fs.readFileSync('prototype/app.js', 'utf8');
const a = src.indexOf('function decimalYuan('), b = src.indexOf('\nconst tag=', a);
const c = {}; vm.runInNewContext(src.slice(a,b), c);
const value = JSON.parse(fs.readFileSync(0, 'utf8'));
process.stdout.write(JSON.stringify({net: c.decimalYuan(value.net), mean: c.exactMeanYuan(value.mean)}));
"""
        run = subprocess.run(['node', '-e', script], cwd=Path(__file__).resolve().parents[1],
                             input=json.dumps({'net': value['net_quantiles_yuan']['p50'],
                                               'mean': value['mean_gross_tax_before']}).encode(),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=True)
        shown = json.loads(run.stdout)
        gross_cents = 1234567890123456789012345678 * 2
        net_cents = gross_cents - gross_cents // 5 - 400
        self.assertEqual(shown['net'], f'{net_cents // 100}.{net_cents % 100:02} 元')
        fraction = Fraction(*map(int, value['mean_gross_tax_before']['fraction']))
        cents = (fraction.numerator * 200 + fraction.denominator) // (2 * fraction.denominator)
        self.assertEqual(shown['mean'], f'{cents // 100}.{cents % 100:02} 元')


if __name__ == '__main__':
    unittest.main()
