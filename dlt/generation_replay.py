"""Allowlisted, portable replay of one saved candidate/control generation.

The package reproduces ticket generation and can optionally retrain bounded
models. It is not a database backup, source authentication, or settlement.
"""
from contextlib import closing
import argparse
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import platform
import re
import sqlite3
import subprocess
import sys

import numpy as np

from dlt.candidates import _canonical, generate_pair
from dlt.model_repository import _assert_consistent_history
from dlt.models import predict, prepare_samples
from dlt.snapshots import parse_snapshot
from dlt.training_jobs import TrainingJobs
from dlt.training_boundary import check_holdout_boundary


class ReplayError(ValueError):
    pass


def _hash(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha_string(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def _read_history(db, ident, first, last, *, check_conflicts=True):
    if not _sha_string(ident):
        raise ReplayError('历史快照标识无效')
    row = db.execute('SELECT canonical_json,row_count FROM snapshots WHERE id=?', (ident,)).fetchone()
    if not row or not isinstance(row[0], bytes) or hashlib.sha256(row[0]).hexdigest() != ident:
        raise ReplayError('历史快照缺失或摘要不符')
    _, parsed = parse_snapshot(row[0])
    rows = sorted(parsed, key=lambda x: (x['draw_date'], x['issue']))
    projected = db.execute('SELECT issue,draw_date,front_json,back_json,numbers_usable FROM draw_versions '
                           'WHERE snapshot_id=? ORDER BY draw_date,issue', (ident,)).fetchall()
    expected = [(r['issue'], r['draw_date'], json.dumps(r['front']), json.dumps(r['back']), 1) for r in rows]
    if projected != expected or row[1] != len(rows):
        raise ReplayError('历史号码投影与快照不一致')
    issues = [r['issue'] for r in rows]
    if first not in issues or last not in issues or issues.index(first) > issues.index(last):
        raise ReplayError('历史范围不可用')
    chosen = rows[issues.index(first):issues.index(last)+1]
    if len(chosen) > 2000:
        raise ReplayError('历史范围超过导出上限')
    if check_conflicts:
        _assert_consistent_history(db, {r['issue'] for r in chosen})
    return [{'issue': r['issue'], 'date': r['draw_date'], 'front': r['front'], 'back': r['back']} for r in chosen]


def _clean_config(config):
    # Only enumerations, dates, numbers, and verified content identifiers may
    # leave the database. Free-form rule/calendar versions are replaced.
    keys = ('method', 'structure', 'target_issue', 'target_date', 'snapshot_id', 'budget_fen',
            'base_price_fen', 'additional_price_fen', 'additional', 'window', 'seed',
            'front_count', 'back_count', 'max_tickets', 'max_sampling_attempts', 'score_temperature')
    clean = {key: config[key] for key in keys if key in config}
    if clean.get('method') not in ('random', 'hot', 'cold', 'tree', 'lstm') or clean.get('structure') not in ('singles', 'compound', 'dantuo'):
        raise ReplayError('方法或结构不支持重放')
    if clean['structure'] != 'singles' and 'max_sampling_attempts' not in clean:
        clean['max_sampling_attempts'] = 0
    if not _sha_string(clean.get('snapshot_id')) or not re.fullmatch(r'[0-9]{5}', str(clean.get('target_issue', ''))):
        raise ReplayError('快照或目标奖期无效')
    if type(clean['target_issue']) is not str or type(clean.get('target_date')) is not str:
        raise ReplayError('目标奖期或日期无效')
    try:
        if date.fromisoformat(clean['target_date']).isoformat() != clean['target_date']:
            raise ValueError()
    except ValueError as exc:
        raise ReplayError('目标日期无效') from exc
    limits = {'budget_fen': 100_000_000, 'base_price_fen': 100_000_000,
              'additional_price_fen': 100_000_000, 'window': 2000, 'seed': 4_294_967_295,
              'max_tickets': 1000, 'max_sampling_attempts': 100_000}
    for key, maximum in limits.items():
        if type(clean.get(key)) is not int or not 0 <= clean[key] <= maximum:
            raise ReplayError('生成参数无效或超过导出上限')
    for key in ('front_count', 'back_count'):
        if key in clean and type(clean[key]) is not int:
            raise ReplayError('结构参数无效')
    if 'score_temperature' in clean and (type(clean['score_temperature']) not in (int, float)
            or not math.isfinite(clean['score_temperature'])):
        raise ReplayError('模型温度无效')
    if type(clean.get('additional')) is not bool:
        raise ReplayError('追加参数无效')
    clean['rule_version'] = 'redacted-rule-reference'
    clean['calendar_version'] = 'redacted-calendar-reference'
    return clean


def _clean_training_config(config, first):
    if not isinstance(config, dict):
        raise ReplayError('模型训练配置无效')
    keys = ('method', 'window', 'train_end', 'cutoff', 'availability_mode', 'snapshot_id', 'seed',
            'algorithm_version', 'holdout_start', 'holdout_end', 'rolling_issue', 'feature_schema_version',
            'preprocessing_version')
    clean = {key: config[key] for key in keys if key in config}
    params = config.get('parameters')
    if not isinstance(params, dict):
        raise ReplayError('模型训练参数缺失')
    fields = {'tree': ('max_depth', 'min_leaf'), 'lstm': ('hidden_size', 'epochs', 'learning_rate')}.get(clean.get('method'))
    if not fields:
        raise ReplayError('不支持的模型训练方法')
    clean['parameters'] = {key: params[key] for key in fields if key in params}
    for key in ('train_end', 'cutoff'):
        if not isinstance(clean.get(key), str) or not re.fullmatch(r'[0-9]{5}', clean[key]):
            raise ReplayError('模型训练奖期无效')
    if 'rolling_issue' in clean and (not isinstance(clean['rolling_issue'], str)
            or not re.fullmatch(r'[0-9]{5}', clean['rolling_issue'])):
        raise ReplayError('模型滚动训练目标期无效')
    if not isinstance(first, str) or not re.fullmatch(r'[0-9]{5}', first) or not _sha_string(clean.get('snapshot_id')):
        raise ReplayError('模型训练范围无效')
    try:
        TrainingJobs._validate('replay', clean, first, 120)
    except ValueError as exc:
        raise ReplayError('模型训练参数超出重放上限') from exc
    return clean


def _clean_model(model, config):
    def number(value):
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ReplayError('模型权重无效')
        return value

    def vector(values, size):
        if not isinstance(values, list) or len(values) != size:
            raise ReplayError('模型权重维度无效')
        return [number(x) for x in values]

    if not isinstance(model, dict):
        raise ReplayError('模型产物格式无效')
    if config['method'] == 'tree':
        if set(model) != {'tree'}:
            raise ReplayError('树模型产物字段无效')
        def node(value, depth):
            if not isinstance(value, dict) or depth > 5:
                raise ReplayError('树模型结构无效')
            if set(value) == {'score', 'count'}:
                if type(value['count']) is not int or not 1 <= value['count'] <= 500:
                    raise ReplayError('树模型样本计数无效')
                return {'score': vector(value['score'], 47), 'count': value['count']}
            if set(value) != {'feature', 'threshold', 'left', 'right'} or type(value['feature']) is not int or not 0 <= value['feature'] < 47:
                raise ReplayError('树模型分支无效')
            return {'feature': value['feature'], 'threshold': number(value['threshold']),
                    'left': node(value['left'], depth+1), 'right': node(value['right'], depth+1)}
        return {'tree': node(model['tree'], 0)}
    if set(model) != {'weights'} or not isinstance(model['weights'], dict) or set(model['weights']) != {'w', 'b', 'wy', 'by'}:
        raise ReplayError('LSTM 权重字段无效')
    weights = model['weights']; hidden = config['parameters']['hidden_size']
    def matrix(value, rows, columns):
        if not isinstance(value, list) or len(value) != rows:
            raise ReplayError('模型权重维度无效')
        return [vector(row, columns) for row in value]
    return {'weights': {'w': matrix(weights['w'], 47+hidden, 4*hidden),
                        'b': vector(weights['b'], 4*hidden),
                        'wy': matrix(weights['wy'], hidden, 47), 'by': vector(weights['by'], 47)}}


def _version(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9]{1,3}(?:\.[0-9]{1,3}){2}', value) is not None


def _make_training(artifact, history, artifact_id):
    if len(history) > 500 or not history:
        raise ReplayError('模型训练历史超过重放上限')
    config = _clean_training_config(artifact['config'], history[0]['issue'])
    if config['cutoff'] != history[-1]['issue']:
        raise ReplayError('模型训练截止期不一致')
    runtime = artifact.get('runtime')
    if not isinstance(runtime, dict) or not _version(runtime.get('python')) or not _version(runtime.get('numpy')):
        raise ReplayError('模型运行时版本无效')
    model = _clean_model(artifact['model'], config)
    return {'config': config, 'history': history, 'model': model, 'model_sha256': _hash(model),
            'model_code_sha256': artifact['code_sha256'], 'original_artifact_sha256': artifact_id,
            'runtime': {'python': runtime['python'], 'numpy': runtime['numpy']}}


def build_generation_replay(path, pair_key):
    if not isinstance(pair_key, str) or not 1 <= len(pair_key) <= 128:
        raise ReplayError('方案标识无效')
    db_path = Path(path).resolve()
    if not db_path.is_file():
        raise ReplayError('研究数据库不存在')
    try:
        with closing(sqlite3.connect(db_path.as_uri()+'?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA trusted_schema=OFF')
            db.execute('BEGIN')
            row = db.execute('SELECT request_json,pair_sha256,pair_json FROM draft_pairs WHERE request_key=?', (pair_key,)).fetchone()
            if not row:
                raise ReplayError('方案不存在')
            request, original = json.loads(row[0]), json.loads(row[2])
            content = dict(original); digest = content.pop('pair_sha256', None)
            if not _sha_string(digest) or digest != row[1] or _hash(content) != digest or original['config'] != request['config']:
                raise ReplayError('方案记录摘要或配置不一致')
            current_code = hashlib.sha256(Path(__file__).with_name('candidates.py').read_bytes()).hexdigest()
            if original.get('version') != 'python-candidate-v1' or original.get('code_sha256') != current_code:
                raise ReplayError('当前生成代码与方案版本不一致，无法重放')
            config = _clean_config(request['config'])
            history = _read_history(db, config['snapshot_id'], request['history_first_issue'], request['history_cutoff_issue'])
            model_scores = None
            training_package = None
            artifact_id = request.get('artifact_id')
            if config['method'] in ('tree', 'lstm'):
                if not _sha_string(artifact_id) or artifact_id != original.get('model_artifact_id'):
                    raise ReplayError('模型引用缺失或不一致')
                model_row = db.execute('SELECT artifact_json FROM model_artifacts WHERE artifact_id=?', (artifact_id,)).fetchone()
                if not model_row:
                    raise ReplayError('模型产物缺失')
                artifact = json.loads(model_row[0]); artifact_content = dict(artifact)
                if artifact_content.pop('artifact_sha256', None) != artifact_id or _hash(artifact_content) != artifact_id:
                    raise ReplayError('模型产物摘要不符')
                model_code = hashlib.sha256(Path(__file__).with_name('models.py').read_bytes()).hexdigest()
                if artifact.get('code_sha256') != model_code or artifact['config']['method'] != config['method']:
                    raise ReplayError('当前模型代码或方法与产物不一致')
                if artifact['config']['availability_mode'] != 'historical_reconstruction':
                    raise ReplayError('当前只支持历史重建模型')
                training = _read_history(db, artifact['config']['snapshot_id'], artifact['history_first_issue'], artifact['history_last_issue'])
                if _hash(training) != artifact['training_rows_sha256']:
                    raise ReplayError('模型训练输入摘要不符')
                training_issues = [r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue', (artifact['config']['snapshot_id'],))]
                check_holdout_boundary(training_issues, artifact['history_first_issue'], artifact['config'])
                training_package = _make_training(artifact, training, artifact_id)
                model_scores = predict(artifact, history[-artifact['config']['window']:],
                                       target_issue=config['target_issue'], target_date=config['target_date'])
                model_scores = {'front_scores': model_scores['front_scores'], 'back_scores': model_scores['back_scores'],
                                'artifact_sha256': artifact_id}
            elif artifact_id is not None or original.get('model_artifact_id') is not None:
                raise ReplayError('非模型方法包含模型引用')
            replayed = generate_pair(history=history, config=config, model_scores=model_scores)
            if (replayed['candidate'] != original['candidate'] or replayed['control'] != original['control']
                    or replayed['history_sha256'] != original['history_sha256']):
                raise ReplayError('当前输入重放与保存的方案不一致')
            version = original.get('runtime')
            if not isinstance(version, str) or not re.fullmatch(r'[0-9]{1,3}(?:\.[0-9]{1,3}){2}', version):
                raise ReplayError('原运行时版本无效')
            bundle = {'format': 'dlt-generation-replay-v2', 'original_pair_sha256': digest,
                      'generator_code_sha256': current_code, 'original_python': version,
                      'export_python': platform.python_version(), 'config': config, 'history': history,
                      'model_scores': model_scores, 'model_training': training_package,
                      'candidate': replayed['candidate'], 'control': replayed['control']}
            bundle['bundle_sha256'] = _hash(bundle)
            verify_generation_replay(bundle)
            return bundle
    except ReplayError:
        raise
    except (ValueError, TypeError, KeyError, IndexError, AttributeError, sqlite3.Error, OSError) as exc:
        raise ReplayError('重放包创建失败：记录无效、来源冲突或依赖缺失') from exc


def _verify_training(training, scores, generation_config, generation_history, retrain):
    keys = {'config', 'history', 'model', 'model_sha256', 'model_code_sha256', 'original_artifact_sha256', 'runtime'}
    if not isinstance(training, dict) or set(training) != keys:
        raise ReplayError('模型训练重放字段无效')
    history = training['history']
    if not isinstance(history, list) or not 1 <= len(history) <= 500 or any(
            not isinstance(row, dict) or set(row) != {'issue', 'date', 'front', 'back'} for row in history):
        raise ReplayError('模型训练历史无效或超过上限')
    cfg = _clean_training_config(training['config'], history[0]['issue'])
    if cfg != training['config'] or cfg['method'] != generation_config['method'] or history[-1]['issue'] != cfg['cutoff']:
        raise ReplayError('模型训练配置与生成方法不一致')
    if 'rolling_issue' in cfg and cfg['rolling_issue'] != generation_config['target_issue']:
        raise ReplayError('模型滚动训练目标期与生成目标期不一致')
    if not _sha_string(training['original_artifact_sha256']) or training['original_artifact_sha256'] != scores.get('artifact_sha256'):
        raise ReplayError('模型引用与评分不一致')
    model_code = hashlib.sha256(Path(__file__).with_name('models.py').read_bytes()).hexdigest()
    if training['model_code_sha256'] != model_code:
        raise ReplayError('当前模型代码与重放包不同')
    runtime = training['runtime']
    if not isinstance(runtime, dict) or set(runtime) != {'python', 'numpy'} or not all(_version(v) for v in runtime.values()):
        raise ReplayError('模型运行时版本无效')
    model = _clean_model(training['model'], cfg)
    if model != training['model'] or not _sha_string(training['model_sha256']) or _hash(model) != training['model_sha256']:
        raise ReplayError('模型权重摘要不符')
    prepare_samples(history, window=cfg['window'], train_end=cfg['train_end'], cutoff=cfg['cutoff'],
                    availability_mode=cfg['availability_mode'])
    from dlt.models import (ALGORITHM_VERSION, ARTIFACT_SCHEMA,
                            FEATURE_SCHEMA_VERSION, PREPROCESSING_VERSION)
    artifact = {'schema': ARTIFACT_SCHEMA,
                'algorithm_version': ALGORITHM_VERSION[cfg['method']],
                'feature_schema_version': FEATURE_SCHEMA_VERSION,
                'preprocessing_version': PREPROCESSING_VERSION[cfg['method']],
                'config': cfg, 'cutoff_date': history[-1]['date'], 'model': model,
                'score_semantics': 'uncalibrated research score; not a winning probability'}
    artifact['artifact_sha256'] = _hash(artifact)
    prediction = predict(artifact, generation_history[-cfg['window']:], target_issue=generation_config['target_issue'],
                         target_date=generation_config['target_date'])
    if prediction['front_scores'] != scores.get('front_scores') or prediction['back_scores'] != scores.get('back_scores'):
        raise ReplayError('模型权重无法重现导出评分')
    if not retrain:
        return 'not_run'
    if runtime != {'python': platform.python_version(), 'numpy': np.__version__}:
        raise ReplayError('当前 Python 或 NumPy 版本与训练时不同，未执行重训')
    try:
        result = subprocess.run([sys.executable, '-m', 'dlt.replay_train_worker'],
                                input=_canonical({'history': history, 'config': cfg}), capture_output=True,
                                timeout=120, check=False,
                                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
        if result.returncode != 0 or len(result.stdout) > 10*1024*1024:
            raise ReplayError('模型重训失败，未确认训练结果一致')
        trained = json.loads(result.stdout)
    except subprocess.TimeoutExpired as exc:
        raise ReplayError('模型重训超过 120 秒，未确认训练结果一致') from exc
    except (OSError, ValueError) as exc:
        raise ReplayError('无法完成模型重训') from exc
    if _hash(trained['model']) != training['model_sha256']:
        raise ReplayError('重训模型与保存权重不一致')
    return 'matched'


def verify_generation_replay(bundle, *, retrain=False):
    if not isinstance(bundle, dict):
        raise ReplayError('重放包格式无效')
    allowed = {'format', 'original_pair_sha256', 'generator_code_sha256', 'original_python', 'export_python',
               'config', 'history', 'model_scores', 'candidate', 'control', 'bundle_sha256'}
    version = bundle.get('format')
    if version == 'dlt-generation-replay-v2':
        allowed.add('model_training')
    if set(bundle) != allowed or not _sha_string(bundle.get('original_pair_sha256')):
        raise ReplayError('重放包字段无效')
    for key in ('original_python', 'export_python'):
        if not isinstance(bundle.get(key), str) or not re.fullmatch(r'[0-9]{1,3}(?:\.[0-9]{1,3}){2}', bundle[key]):
            raise ReplayError('运行时版本无效')
    content = dict(bundle); digest = content.pop('bundle_sha256', None)
    if not _sha_string(digest) or _hash(content) != digest or version not in ('dlt-generation-replay-v1', 'dlt-generation-replay-v2'):
        raise ReplayError('重放包摘要或格式不符')
    if hashlib.sha256(Path(__file__).with_name('candidates.py').read_bytes()).hexdigest() != bundle.get('generator_code_sha256'):
        raise ReplayError('当前生成代码与重放包不同')
    try:
        config = _clean_config(bundle['config'])
        if config != bundle['config']:
            raise ReplayError('重放配置包含不支持的字段')
        if not isinstance(bundle['history'], list) or len(bundle['history']) > 2000:
            raise ReplayError('历史记录超过重放上限')
        if any(not isinstance(row, dict) or set(row) != {'issue', 'date', 'front', 'back'} for row in bundle['history']):
            raise ReplayError('历史记录字段无效')
        scores = bundle['model_scores']
        if scores is not None and (not isinstance(scores, dict) or set(scores) != {'front_scores', 'back_scores', 'artifact_sha256'}):
            raise ReplayError('模型评分字段无效')
        training_status = 'not_available' if scores is not None else 'not_applicable'
        if version == 'dlt-generation-replay-v2':
            training = bundle['model_training']
            if (scores is None) != (training is None):
                raise ReplayError('模型训练与评分不匹配')
            if training is not None:
                training_status = _verify_training(training, scores, config, bundle['history'], retrain)
        elif retrain and scores is not None:
            raise ReplayError('旧版重放包不包含模型训练数据')
        replayed = generate_pair(history=bundle['history'], config=config, model_scores=bundle['model_scores'])
        if replayed['candidate'] != bundle['candidate'] or replayed['control'] != bundle['control']:
            raise ReplayError('重放的号码或成本不一致')
    except (ValueError, TypeError, KeyError, IndexError) as exc:
        if isinstance(exc, ReplayError):
            raise
        raise ReplayError('重放输入无效') from exc
    return {'status': 'matched', 'training': training_status, 'bundle_sha256': digest,
            'original_pair_sha256': bundle['original_pair_sha256']}


def main():
    parser = argparse.ArgumentParser(description='Verify a generation replay package without accessing a database.')
    parser.add_argument('package')
    parser.add_argument('--retrain', action='store_true', help='also retrain the exported model in a bounded subprocess')
    args = parser.parse_args()
    try:
        with open(args.package, 'rb') as handle:
            raw = handle.read(10 * 1024 * 1024 + 1)
        if len(raw) > 10 * 1024 * 1024:
            raise ReplayError('重放包超过大小上限')
        result = verify_generation_replay(json.loads(raw), retrain=args.retrain)
    except (ReplayError, ValueError, OSError, RecursionError) as exc:
        message = str(exc) if isinstance(exc, ReplayError) else '重放包无法读取或格式无效'
        print(json.dumps({'status': 'failed', 'error': message}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
