"""Validate a fixed, locally timed rolling-model policy.

This module does not launch training, attest external publication times, or
turn historical observations into independent prospective evidence.
"""
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import platform
import numpy as np

from dlt.candidates import _canonical
from dlt.generation_replay import _read_history
from dlt.training_boundary import check_holdout_boundary
from dlt.training_jobs import TrainingJobs
from dlt.training_limits import DEFAULT_TRAINING_LIMITS


class RollingPolicyError(ValueError):
    pass


def _hash(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _instant(value):
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError('timezone required')
        return result
    except (TypeError, ValueError) as exc:
        raise RollingPolicyError('训练或数据的本地时间记录无效') from exc


def _artifact(db, ident):
    row = db.execute('SELECT artifact_json,first_recorded_at FROM model_artifacts WHERE artifact_id=?', (ident,)).fetchone()
    if not row:
        raise RollingPolicyError('滚动训练模型产物不存在')
    artifact = json.loads(row[0]); content = dict(artifact)
    if content.pop('artifact_sha256', None) != ident or _hash(content) != ident:
        raise RollingPolicyError('滚动训练模型摘要不符')
    return artifact, _instant(row[1])


def _profile(config):
    return {key: value for key, value in config.items() if key not in ('snapshot_id', 'train_end', 'cutoff', 'rolling_issue')}


def derive_rolling_spec(db, pair):
    ident = pair.get('model_artifact_id')
    if not ident or pair.get('config', {}).get('method') not in ('tree', 'lstm'):
        raise RollingPolicyError('滚动重训需要树模型或 LSTM 模板产物')
    artifact, _ = _artifact(db, ident)
    config = artifact['config']
    if config.get('method') != pair['config']['method'] or config.get('availability_mode') != 'historical_reconstruction':
        raise RollingPolicyError('模板模型方法或可用性模式不支持滚动重训')
    if config.get('rolling_issue') != pair['config'].get('target_issue'):
        raise RollingPolicyError('模板模型训练任务未绑定目标期')
    allowed_training = {'method', 'window', 'train_end', 'cutoff', 'availability_mode', 'snapshot_id',
                        'seed', 'algorithm_version', 'rolling_issue', 'parameters', 'holdout_start', 'holdout_end',
                        'feature_schema_version', 'preprocessing_version'}
    allowed_generation = {'method', 'structure', 'target_issue', 'target_date', 'snapshot_id', 'rule_version',
                          'calendar_version', 'budget_fen', 'base_price_fen', 'additional_price_fen', 'additional',
                          'window', 'seed', 'front_count', 'back_count', 'max_tickets', 'max_sampling_attempts',
                          'score_temperature'}
    expected_params = {'max_depth', 'min_leaf'} if config['method'] == 'tree' else {'hidden_size', 'epochs', 'learning_rate'}
    if (set(config) - allowed_training or set(pair['config']) - allowed_generation
            or not isinstance(config.get('parameters'), dict) or set(config['parameters']) != expected_params):
        raise RollingPolicyError('模板含未支持的模型或生成字段，无法固定滚动规则')
    try:
        TrainingJobs._validate('rolling', config, artifact['history_first_issue'],
                               DEFAULT_TRAINING_LIMITS.timeout_seconds[1])
    except ValueError as exc:
        raise RollingPolicyError('模板模型参数超出滚动训练上限') from exc
    rows = _read_history(db, config['snapshot_id'], artifact['history_first_issue'], artifact['history_last_issue'])
    if (not DEFAULT_TRAINING_LIMITS.history_rows[0] <= len(rows) <= DEFAULT_TRAINING_LIMITS.history_rows[1]
            or _hash(rows) != artifact['training_rows_sha256']):
        raise RollingPolicyError('模板模型训练输入缺失或摘要不符')
    issues = [r['issue'] for r in rows]
    if config.get('cutoff') != issues[-1] or config.get('train_end') not in issues:
        raise RollingPolicyError('模板模型训练与验证范围不一致')
    validation = len(issues)-1-issues.index(config['train_end'])
    if validation < 1 or issues.index(config['train_end']) < config['window']:
        raise RollingPolicyError('模板模型缺少独立的时间顺序验证范围')
    runtime = artifact.get('runtime')
    model_code = hashlib.sha256(Path(__file__).with_name('models.py').read_bytes()).hexdigest()
    generator_code = hashlib.sha256(Path(__file__).with_name('candidates.py').read_bytes()).hexdigest()
    if (runtime != {'python': platform.python_version(), 'numpy': np.__version__}
            or artifact.get('code_sha256') != model_code or pair.get('code_sha256') != generator_code
            or pair.get('runtime') != platform.python_version()):
        raise RollingPolicyError('模板模型或生成代码、运行环境与当前环境不一致')
    return {'version': 'rolling-model-v1', 'training_periods': len(rows), 'validation_periods': validation,
            'config_profile': _profile(config), 'algorithm_version': artifact['algorithm_version'],
            'code_sha256': artifact['code_sha256'], 'runtime': runtime}


def validate_rolling_pair(db, spec, pair, pair_key):
    """Check one saved pair against the same immutable rolling policy.

    Latest means the latest distinct draw dates in snapshots observed locally
    before both training starts and the pair is generated. It does not prove
    official completeness, source authenticity, or an independent timestamp.
    """
    if not isinstance(spec, dict) or spec.get('version') != 'rolling-model-v1':
        raise RollingPolicyError('滚动训练协议无效')
    row = db.execute('SELECT request_json,created_at FROM draft_pairs WHERE request_key=?', (pair_key,)).fetchone()
    if not row:
        raise RollingPolicyError('配对草稿不存在')
    request, created = json.loads(row[0]), _instant(row[1])
    if request['config'] != pair['config'] or request.get('artifact_id') != pair.get('model_artifact_id'):
        raise RollingPolicyError('配对请求与结果不一致')
    artifact, recorded = _artifact(db, pair['model_artifact_id'])
    cfg = artifact['config']
    if (recorded > created or _profile(cfg) != spec['config_profile']
            or artifact.get('algorithm_version') != spec['algorithm_version']
            or artifact.get('code_sha256') != spec['code_sha256'] or artifact.get('runtime') != spec['runtime']
            or cfg.get('snapshot_id') != pair['config'].get('snapshot_id')):
        raise RollingPolicyError('模型配置、时间或版本与固定滚动训练协议不一致')
    if cfg.get('rolling_issue') != pair['config'].get('target_issue'):
        raise RollingPolicyError('模型训练任务未绑定当前目标期')
    jobs = db.execute("SELECT request_json,created_at,finished_at FROM training_jobs "
                      "WHERE artifact_id=? AND status='complete'", (pair['model_artifact_id'],)).fetchall()
    starts = []
    for raw, started, finished in jobs:
        task = json.loads(raw)
        if (task.get('config') == cfg and task.get('first_issue') == artifact['history_first_issue']
                and _instant(started) <= _instant(finished) <= created and recorded <= _instant(finished)):
            starts.append(_instant(started))
    if not starts:
        raise RollingPolicyError('没有在配对生成前完成的目标期训练任务')
    rows = _read_history(db, cfg['snapshot_id'], artifact['history_first_issue'], artifact['history_last_issue'])
    issues = [row['issue'] for row in rows]
    n, validation = spec['training_periods'], spec['validation_periods']
    if (len(rows) != n or not 1 <= validation < n or cfg.get('cutoff') != issues[-1]
            or cfg.get('train_end') != issues[-1-validation] or _hash(rows) != artifact['training_rows_sha256']
            or request.get('history_cutoff_issue') != issues[-1] or pair.get('history_last') != issues[-1]):
        raise RollingPolicyError('训练、验证或生成历史窗口不符合固定滚动规则')
    source_issues = [r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue', (cfg['snapshot_id'],))]
    check_holdout_boundary(source_issues, artifact['history_first_issue'], cfg)
    try:
        target = date.fromisoformat(pair['config']['target_date'])
    except (TypeError, ValueError) as exc:
        raise RollingPolicyError('目标日期无效') from exc
    if date.fromisoformat(rows[-1]['date']) >= target:
        raise RollingPolicyError('目标期或未来数据不能进入训练')
    observations = db.execute('SELECT d.snapshot_id,d.issue,d.draw_date,o.observed_at FROM draw_versions d '
                              'JOIN observations o ON o.snapshot_id=d.snapshot_id').fetchall()
    observations = [(sid, issue, day, _instant(observed)) for sid, issue, day, observed in observations]
    wanted = [(row['issue'], row['date']) for row in rows]

    def check_as_of(instant):
        if not any(sid == cfg['snapshot_id'] and observed <= instant for sid, _, _, observed in observations):
            raise RollingPolicyError('所选快照在训练开始或配对生成时没有本机观察记录')
        if any(issue == cfg['rolling_issue'] and observed <= instant for _, issue, _, observed in observations):
            raise RollingPolicyError('目标期在训练或生成前已被观察')
        prior = {(issue, day) for _, issue, day, observed in observations
                 if observed <= instant and date.fromisoformat(day) < target}
        by_date = {}
        for issue, day in prior:
            by_date.setdefault(day, set()).add(issue)
        recent_dates = sorted(by_date)[-n:]
        if len(recent_dates) != n or any(len(by_date[day]) != 1 for day in recent_dates):
            raise RollingPolicyError('本机历史期数不足或同日奖期顺序不明确')
        expected = [(next(iter(by_date[day])), day) for day in recent_dates]
        if wanted != expected:
            raise RollingPolicyError('训练窗口不是当时本机已观察的最新连续历史期')

    check_as_of(created)
    for started in starts:
        try:
            check_as_of(started)
            break
        except RollingPolicyError:
            continue
    else:
        raise RollingPolicyError('没有训练开始时即可用且符合最新窗口的目标期训练任务')
