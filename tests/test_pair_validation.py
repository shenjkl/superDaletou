from copy import deepcopy
import hashlib
import unittest

from dlt.candidates import CandidateError, _canonical, generate_pair
from dlt.pair_validation import validate_pair_structure, validate_random_control
from test_candidates import config, input_rows


def rehash(pair):
    content = {key: value for key, value in pair.items() if key != 'pair_sha256'}
    pair['pair_sha256'] = hashlib.sha256(_canonical(content)).hexdigest()
    return pair


class PairValidationTests(unittest.TestCase):
    def test_generated_structures_and_additional_are_accepted(self):
        for structure in ('singles', 'compound', 'dantuo'):
            for additional in (False, True):
                cfg = {**config('hot', structure), 'additional': additional, 'budget_fen': 18900}
                with self.subTest(structure=structure, additional=additional):
                    validate_pair_structure(generate_pair(history=input_rows(), config=cfg), cfg)

    def test_matching_checksum_does_not_make_unfair_pair_valid(self):
        baseline = generate_pair(history=input_rows(), config=config())
        for mutation in ('cost', 'count', 'seed', 'duplicate', 'addition', 'config'):
            pair = deepcopy(baseline)
            if mutation == 'cost': pair['control']['cost_fen'] += 1
            if mutation == 'count': pair['control']['count'] -= 1
            if mutation == 'seed': pair['control']['seed'] = pair['candidate']['seed']
            if mutation == 'duplicate': pair['control']['tickets'][1] = deepcopy(pair['control']['tickets'][0])
            if mutation == 'addition': pair['control']['tickets'][0]['additional'] = True
            if mutation == 'config': pair['config']['budget_fen'] += 100
            with self.subTest(mutation=mutation), self.assertRaises(CandidateError):
                validate_pair_structure(rehash(pair), baseline['config'])

    def test_compound_and_dantuo_tickets_must_match_the_declared_pool(self):
        for structure in ('compound', 'dantuo'):
            cfg = config('hot', structure)
            pair = generate_pair(history=input_rows(), config=cfg)
            # Preserve uniqueness, budget and checksum while changing the pool.
            if structure == 'compound':
                old = set(pair['control']['pool']['front'])
                pair['control']['pool']['front'][0] = next(v for v in range(1, 36) if v not in old)
            else:
                old = set(pair['control']['pool']['dan'] + pair['control']['pool']['drag'])
                pair['control']['pool']['drag'][0] = next(v for v in range(1, 36) if v not in old)
            with self.subTest(structure=structure), self.assertRaises(CandidateError):
                validate_pair_structure(rehash(pair), cfg)

    def test_model_control_replays_without_claiming_model_candidate_replay(self):
        scores = {'front_scores': [i/35 for i in range(35)], 'back_scores': [i/12 for i in range(12)],
                  'artifact_sha256': 'b'*64}
        cfg = config('tree', 'singles')
        pair = generate_pair(history=input_rows(), config=cfg, model_scores=scores)
        history = input_rows()[-cfg['window']:]
        self.assertTrue(validate_random_control(pair, history))
        altered = deepcopy(pair)
        original = set(altered['control']['tickets'][0]['front'])
        altered['control']['tickets'][0]['front'] = sorted(
            altered['control']['tickets'][0]['front'][1:] + [next(n for n in range(1,36) if n not in original)])
        with self.assertRaisesRegex(CandidateError, '随机对照'):
            validate_random_control(rehash(altered), history)
