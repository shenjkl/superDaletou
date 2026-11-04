"""Real local tree and LSTM training on chronological draw sequences.

Scores are research signals, not calibrated probabilities. Training never uses
validation labels for fitting or model selection. Training returns a bounded,
versioned artifact; task persistence, cancellation and recovery are handled by
the service layer, which publishes completed artifacts to the local repository.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import platform
import re
from pathlib import Path

import numpy as np
from dlt.training_limits import DEFAULT_TRAINING_LIMITS


ARTIFACT_SCHEMA = 'dlt-model-v1'
FEATURE_SCHEMA_VERSION = 'draw-window-onehot-47-v1'
ALGORITHM_VERSION = {
    'tree': 'numpy-tree-v1',
    'lstm': 'numpy-lstm-v1',
}
PREPROCESSING_VERSION = {
    'tree': 'window-mean-v1',
    'lstm': 'raw-sequence-v1',
}


class TrainingError(ValueError):
    pass


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _time(value):
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError('timezone missing')
        return result
    except (TypeError, ValueError) as exc:
        raise TrainingError('strict replay requires timezone-aware availability and prediction times') from exc


def _vector(row):
    result = np.zeros(47, dtype=np.float64)
    for field, size, count, offset in [('front', 35, 5, 0), ('back', 12, 2, 35)]:
        nums = row.get(field)
        if (not isinstance(nums, list) or len(nums) != count
                or any(type(n) is not int or n < 1 or n > size for n in nums) or len(set(nums)) != count):
            raise TrainingError(f'invalid {field} numbers')
        for n in nums:
            result[offset+n-1] = 1.0
    return result


def prepare_samples(rows, *, window, train_end, cutoff, availability_mode):
    """Build samples in source order; target and future outcomes never enter features.

    The input must end at cutoff. Later holdout or target rows must be passed to
    a separate evaluation, never to fitting. Strict replay requires evidence of
    availability for every input before each sample's target prediction time.
    """
    if type(window) is not int or window < 1 or not isinstance(rows, list):
        raise TrainingError('positive history window and row list required')
    if availability_mode not in ('historical_reconstruction', 'strict_replay'):
        raise TrainingError('explicit availability mode required')
    if len(rows) <= window + 1:
        raise TrainingError('not enough history for training and validation')
    issues, dates, vectors, available, predictions = [], [], [], [], []
    for row in rows:
        if not isinstance(row, dict) or not re.fullmatch(r'[0-9]{5}', str(row.get('issue', ''))):
            raise TrainingError('invalid issue')
        issue = str(row['issue'])
        try:
            day = date.fromisoformat(row['date'])
        except (KeyError, TypeError, ValueError) as exc:
            raise TrainingError('valid draw date required') from exc
        issues.append(issue); dates.append(day); vectors.append(_vector(row))
        if availability_mode == 'strict_replay':
            available.append(_time(row.get('available_at')))
            predictions.append(_time(row.get('prediction_at')))
    if len(set(issues)) != len(issues) or any(a >= b for a, b in zip(dates, dates[1:])):
        raise TrainingError('rows must have distinct issues and strictly increasing dates')
    if availability_mode == 'strict_replay' and any(pred >= avail for pred, avail in zip(predictions, available)):
        raise TrainingError('target outcome was already available at its prediction time')
    if issues[-1] != cutoff or train_end not in issues:
        raise TrainingError('source must end at the explicit cutoff and include train_end')
    train_index = issues.index(train_end)
    if train_index < window or train_index >= len(rows)-1:
        raise TrainingError('training and validation must both contain samples')
    vectors = np.asarray(vectors)
    train_x, train_y, validation_x, validation_y, train_targets, validation_targets = [], [], [], [], [], []
    for i in range(window, len(rows)):
        if availability_mode == 'strict_replay' and any(t >= predictions[i] for t in available[i-window:i]):
            raise TrainingError(f'input was not available before target {issues[i]}')
        x, y = vectors[i-window:i].copy(), vectors[i].copy()
        if i <= train_index:
            train_x.append(x); train_y.append(y); train_targets.append(issues[i])
        else:
            validation_x.append(x); validation_y.append(y); validation_targets.append(issues[i])
    return {'train_x': np.asarray(train_x), 'train_y': np.asarray(train_y),
            'validation_x': np.asarray(validation_x), 'validation_y': np.asarray(validation_y),
            'train_targets': train_targets, 'validation_targets': validation_targets}


def _tree_fit(x, y, depth, min_leaf):
    node = {'score': y.mean(axis=0).tolist(), 'count': int(len(y))}
    if depth == 0 or len(y) < min_leaf * 2:
        return node
    best = None
    for col in range(x.shape[1]):
        values = np.unique(x[:, col])
        for a, b in zip(values, values[1:]):
            threshold = float((a+b)/2)
            left = x[:, col] <= threshold
            if left.sum() < min_leaf or (~left).sum() < min_leaf:
                continue
            ly, ry = y[left], y[~left]
            loss = float(((ly-ly.mean(axis=0))**2).sum() + ((ry-ry.mean(axis=0))**2).sum())
            if best is None or loss < best[0]:
                best = (loss, col, threshold, left)
    if best is None:
        return node
    _, col, threshold, left = best
    return {'feature': col, 'threshold': threshold,
            'left': _tree_fit(x[left], y[left], depth-1, min_leaf),
            'right': _tree_fit(x[~left], y[~left], depth-1, min_leaf)}


def _tree_predict(tree, x):
    while 'feature' in tree:
        tree = tree['left'] if x[tree['feature']] <= tree['threshold'] else tree['right']
    return np.asarray(tree['score'], dtype=np.float64)


def _sigmoid(x):
    return 1.0 / (1.0 + np.exp(-np.clip(x, -40, 40)))


def _lstm_loss_grad(sequence, target, weights):
    """Full BPTT over one sequence; output is raw logits, loss is mean BCE."""
    w, b, wy, by = weights['w'], weights['b'], weights['wy'], weights['by']
    hsize = wy.shape[0]; h = np.zeros(hsize); c = np.zeros(hsize); cache = []
    for x in sequence:
        combined = np.concatenate([x, h]); z = combined @ w + b
        f = _sigmoid(z[:hsize]); i = _sigmoid(z[hsize:2*hsize]); o = _sigmoid(z[2*hsize:3*hsize]); g = np.tanh(z[3*hsize:])
        old_c = c; c = f*old_c + i*g; h = o*np.tanh(c)
        cache.append((combined, old_c, c, f, i, o, g))
    logits = h @ wy + by; pred = _sigmoid(logits)
    loss = float(np.mean(np.logaddexp(0, logits) - target*logits))
    dz = (pred-target)/len(target)
    grads = {'w': np.zeros_like(w), 'b': np.zeros_like(b), 'wy': np.outer(h, dz), 'by': dz.copy()}
    dh = wy @ dz; dc_next = np.zeros(hsize)
    for combined, old_c, c, f, i, o, g in reversed(cache):
        tc = np.tanh(c); do = dh*tc; dc = dc_next + dh*o*(1-tc*tc)
        df, di, dg = dc*old_c, dc*g, dc*i
        da = np.concatenate([df*f*(1-f), di*i*(1-i), do*o*(1-o), dg*(1-g*g)])
        grads['w'] += np.outer(combined, da); grads['b'] += da
        dh = (w @ da)[-hsize:]; dc_next = dc*f
    return loss, logits, grads


def _weights_init(hidden, seed):
    rng = np.random.default_rng(seed)
    w = rng.normal(0, 1/math.sqrt(47+hidden), (47+hidden, 4*hidden))
    b = np.zeros(4*hidden); b[:hidden] = 1.0
    return {'w': w, 'b': b, 'wy': rng.normal(0, 1/math.sqrt(hidden), (hidden, 47)), 'by': np.zeros(47)}


def train(rows, config):
    required = {'method', 'window', 'train_end', 'cutoff', 'availability_mode', 'snapshot_id',
                'seed', 'algorithm_version', 'feature_schema_version',
                'preprocessing_version', 'parameters'}
    allowed = required | {'holdout_start', 'holdout_end', 'rolling_issue'}
    if (not isinstance(config, dict) or not required.issubset(config)
            or set(config) - allowed):
        raise TrainingError('missing training configuration')
    limits = DEFAULT_TRAINING_LIMITS
    if (type(config.get('window')) is not int
            or not limits.window_periods[0] <= config['window'] <= limits.window_periods[1]
            or not limits.history_rows[0] <= len(rows) <= limits.history_rows[1]):
        raise TrainingError('training history or window exceeds configured limits')
    if not re.fullmatch(r'[0-9a-f]{64}', str(config['snapshot_id'])):
        raise TrainingError('snapshot content identifier required')
    if type(config['seed']) is not int or not 0 <= config['seed'] <= 4294967295:
        raise TrainingError('seed must be a 32-bit unsigned integer')
    sample = prepare_samples(rows, window=config['window'], train_end=config['train_end'],
                             cutoff=config['cutoff'], availability_mode=config['availability_mode'])
    method = config['method']; params = config.get('parameters', {})
    if (method not in ALGORITHM_VERSION
            or config.get('algorithm_version') != ALGORITHM_VERSION[method]
            or config.get('feature_schema_version') != FEATURE_SCHEMA_VERSION
            or config.get('preprocessing_version') != PREPROCESSING_VERSION[method]):
        raise TrainingError('unsupported model algorithm, feature or preprocessing version')
    if not isinstance(params, dict):
        raise TrainingError('parameters must be an object')
    x, y = sample['train_x'], sample['train_y']
    if method == 'tree':
        depth, leaf = params.get('max_depth'), params.get('min_leaf')
        if (set(params) != {'max_depth', 'min_leaf'} or type(depth) is not int
                or not limits.tree_max_depth[0] <= depth <= limits.tree_max_depth[1]
                or type(leaf) is not int
                or not limits.tree_min_leaf[0] <= leaf <= limits.tree_min_leaf[1]):
            raise TrainingError('tree max_depth and min_leaf must be explicit valid integers')
        model = {'tree': _tree_fit(x.mean(axis=1), y, depth, leaf)}
    elif method == 'lstm':
        hidden, epochs, rate = params.get('hidden_size'), params.get('epochs'), params.get('learning_rate')
        if (set(params) != {'hidden_size', 'epochs', 'learning_rate'}
                or type(hidden) is not int
                or not limits.lstm_hidden_size[0] <= hidden <= limits.lstm_hidden_size[1]
                or type(epochs) is not int
                or not limits.lstm_epochs[0] <= epochs <= limits.lstm_epochs[1]
                or type(rate) not in (int, float) or not math.isfinite(rate)
                or not limits.lstm_learning_rate[0] <= rate <= limits.lstm_learning_rate[1]):
            raise TrainingError('LSTM hidden_size, epochs and learning_rate must be explicit positive values')
        weights = _weights_init(hidden, config['seed'])
        for _ in range(epochs):
            total = {k: np.zeros_like(v) for k, v in weights.items()}
            for sequence, target in zip(x, y):
                _, _, grads = _lstm_loss_grad(sequence, target, weights)
                for k in total: total[k] += grads[k]/len(x)
            norm = math.sqrt(sum(float((g*g).sum()) for g in total.values()))
            scale = min(1.0, 5.0/norm) if norm else 1.0
            for k in weights: weights[k] -= rate*scale*total[k]
            if not all(np.isfinite(v).all() for v in weights.values()):
                raise TrainingError('training produced non-finite weights')
        model = {'weights': {k: v.tolist() for k, v in weights.items()}}
    else:
        raise TrainingError('supported training methods are tree and lstm')
    fields = ('issue', 'date', 'front', 'back', 'available_at', 'prediction_at') if config['availability_mode'] == 'strict_replay' else ('issue', 'date', 'front', 'back')
    normalized = [{k: r[k] for k in fields} for r in rows]
    artifact = {'schema': ARTIFACT_SCHEMA,
                'algorithm_version': ALGORITHM_VERSION[method],
                'feature_schema_version': FEATURE_SCHEMA_VERSION,
                'preprocessing_version': PREPROCESSING_VERSION[method],
                'config': json.loads(_canonical(config)), 'training_rows_sha256': hashlib.sha256(_canonical(normalized)).hexdigest(),
                'code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'runtime': {'python': platform.python_version(), 'numpy': np.__version__},
                'train_targets': sample['train_targets'], 'validation_targets': sample['validation_targets'],
                'history_first_issue': rows[0]['issue'], 'history_last_issue': rows[-1]['issue'],
                'cutoff_date': rows[-1]['date'], 'model': model,
                'score_semantics': 'uncalibrated research score; not a winning probability'}
    artifact['artifact_sha256'] = hashlib.sha256(_canonical(artifact)).hexdigest()
    return artifact


def predict(artifact, history, *, target_issue, target_date, prediction_at=None, target_deadline=None):
    """Score a window before an explicitly calendar-resolved target.

    The caller must verify the target calendar and snapshot provenance. Strict
    replay checks each input availability against the prediction instant.
    """
    if not isinstance(artifact, dict):
        raise TrainingError('artifact required')
    supplied = dict(artifact); digest = supplied.pop('artifact_sha256', None)
    if digest != hashlib.sha256(_canonical(supplied)).hexdigest():
        raise TrainingError('artifact integrity check failed')
    config = artifact.get('config') if isinstance(artifact.get('config'), dict) else None
    method = config.get('method') if config is not None else None
    if (artifact.get('schema') != ARTIFACT_SCHEMA
            or method not in ALGORITHM_VERSION
            or artifact.get('algorithm_version') != ALGORITHM_VERSION.get(method)
            or artifact.get('feature_schema_version') != FEATURE_SCHEMA_VERSION
            or method not in PREPROCESSING_VERSION
            or artifact.get('preprocessing_version') != PREPROCESSING_VERSION[method]
            or config.get('algorithm_version') != ALGORITHM_VERSION[method]
            or config.get('feature_schema_version') != FEATURE_SCHEMA_VERSION
            or config.get('preprocessing_version') != PREPROCESSING_VERSION[method]):
        raise TrainingError('unsupported model feature or preprocessing version')
    config = artifact['config']
    window = config.get('window')
    if (type(window) is not int
            or not DEFAULT_TRAINING_LIMITS.window_periods[0] <= window <= DEFAULT_TRAINING_LIMITS.window_periods[1]):
        raise TrainingError('model history window is invalid')
    if config.get('availability_mode') not in ('historical_reconstruction', 'strict_replay'):
        raise TrainingError('model availability mode is unsupported')
    if (not isinstance(history, list) or len(history) != window
            or any(not isinstance(row, dict) for row in history)):
        raise TrainingError('history must match model window')
    if not isinstance(target_issue, str) or not re.fullmatch(r'[0-9]{5}', target_issue):
        raise TrainingError('target issue required')
    try:
        day = date.fromisoformat(target_date)
        dates = [date.fromisoformat(row['date']) for row in history]
        cutoff_day = date.fromisoformat(artifact['cutoff_date'])
    except (KeyError, TypeError, ValueError) as exc:
        raise TrainingError('calendar-resolved dates required') from exc
    issues = [row.get('issue') for row in history]
    if any(not isinstance(issue, str) or not re.fullmatch(r'[0-9]{5}', issue) for issue in issues):
        raise TrainingError('history issue identifiers are invalid')
    if (len(set(issues)) != len(issues) or target_issue in issues
            or any(a >= b for a, b in zip(dates, dates[1:]))
            or dates[-1] >= day or cutoff_day >= day or dates[-1] < cutoff_day):
        raise TrainingError('target and history violate chronological boundaries')
    if config['availability_mode'] == 'strict_replay':
        instant = _time(prediction_at)
        deadline = _time(target_deadline)
        if deadline.astimezone(timezone(timedelta(hours=8))).date() != day:
            raise TrainingError('deadline does not match target date')
        if instant >= deadline:
            raise TrainingError('prediction must precede the calendar-resolved deadline')
        if any(_time(row.get('available_at')) >= instant for row in history):
            raise TrainingError('prediction inputs were not available before prediction time')
    sequence = np.asarray([_vector(row) for row in history])
    if config['method'] == 'tree':
        score = _tree_predict(artifact['model']['tree'], sequence.mean(axis=0))
    elif config['method'] == 'lstm':
        weights = {k: np.asarray(v, dtype=np.float64) for k, v in artifact['model']['weights'].items()}
        _, score, _ = _lstm_loss_grad(sequence, np.zeros(47), weights)
    else:
        raise TrainingError('unknown model type')
    if score.shape != (47,) or not np.isfinite(score).all():
        raise TrainingError('invalid model scores')
    return {'front_scores': score[:35].tolist(), 'back_scores': score[35:].tolist(),
            'semantics': artifact['score_semantics'], 'artifact_sha256': artifact['artifact_sha256']}
