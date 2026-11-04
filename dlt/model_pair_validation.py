"""Replay a model-generated candidate from retained local inputs and weights.

This verifies prediction and ticket generation, not model training provenance,
source authenticity, independent timing or predictive value. Optional retrain
verification remains a separate bounded operation in generation_replay.
"""
import hashlib
from pathlib import Path
import platform

import numpy as np

from dlt.candidates import CandidateError, _canonical, generate_pair
from dlt.pair_validation import replay_supported


def validate_model_artifact(db, artifact, *, check_conflicts=True):
    """Verify bounded model structure, training inputs and matching runtime."""
    from dlt import models as model_module
    from dlt.generation_replay import _read_history, _make_training
    from dlt.training_boundary import check_holdout_boundary

    try:
        content = dict(artifact); digest = content.pop('artifact_sha256')
        if hashlib.sha256(_canonical(content)).hexdigest() != digest:
            raise CandidateError('模型产物摘要不一致')
        method = artifact.get('config', {}).get('method') if isinstance(artifact.get('config'), dict) else None
        if (artifact.get('schema') != model_module.ARTIFACT_SCHEMA
                or method not in model_module.ALGORITHM_VERSION
                or artifact.get('algorithm_version') != model_module.ALGORITHM_VERSION.get(method)
                or artifact.get('feature_schema_version') != model_module.FEATURE_SCHEMA_VERSION
                or method not in model_module.PREPROCESSING_VERSION
                or artifact.get('preprocessing_version') != model_module.PREPROCESSING_VERSION[method]):
            raise CandidateError('模型特征或预处理版本不受支持')
        runtime = artifact.get('runtime')
        if (artifact.get('code_sha256') != hashlib.sha256(Path(model_module.__file__).read_bytes()).hexdigest()
                or runtime != {'python': platform.python_version(), 'numpy': np.__version__}):
            return False
        if artifact['config']['availability_mode'] != 'historical_reconstruction':
            raise CandidateError('当前无法重放该模型的时间证明模式')
        training = _read_history(db, artifact['config']['snapshot_id'], artifact['history_first_issue'],
                                 artifact['history_last_issue'], check_conflicts=check_conflicts)
        if hashlib.sha256(_canonical(training)).hexdigest() != artifact['training_rows_sha256']:
            raise CandidateError('模型训练历史与产物摘要不一致')
        issues = [r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue',
                                          (artifact['config']['snapshot_id'],))]
        check_holdout_boundary(issues, artifact['history_first_issue'], artifact['config'])
        # Bound model dimensions before performing any numerical computation.
        _make_training(artifact, training, artifact['artifact_sha256'])
        samples = model_module.prepare_samples(training, window=artifact['config']['window'],
            train_end=artifact['config']['train_end'], cutoff=artifact['config']['cutoff'],
            availability_mode=artifact['config']['availability_mode'])
        if (samples['train_targets'] != artifact['train_targets']
                or samples['validation_targets'] != artifact['validation_targets']
                or artifact['cutoff_date'] != training[-1]['date']):
            raise CandidateError('模型训练目标、验证目标或截止日期与输入不一致')
        return True
    except CandidateError:
        raise
    except (ValueError, TypeError, KeyError, IndexError, AttributeError, OSError) as exc:
        raise CandidateError(f'模型产物核验失败：{str(exc)[:300]}') from exc


def validate_model_candidate(db, pair, request, artifact, *, check_conflicts=True):
    """Return False when the original executable environment is unavailable."""
    from dlt import models as model_module
    from dlt.generation_replay import _read_history

    try:
        config = pair['config']
        if config['method'] not in ('tree', 'lstm'):
            raise CandidateError('仅模型方案可以执行模型候选重放')
        if (artifact['artifact_sha256'] != pair['model_artifact_id']
                or artifact['artifact_sha256'] != request['artifact_id']
                or artifact['config']['method'] != config['method']):
            raise CandidateError('模型产物与方案引用不一致')
        if not replay_supported(pair) or not validate_model_artifact(db, artifact, check_conflicts=check_conflicts):
            return False
        history = _read_history(db, config['snapshot_id'], request['history_first_issue'], request['history_cutoff_issue'],
                                check_conflicts=check_conflicts)
        window = artifact['config']['window']
        if len(history) < window:
            raise CandidateError('模型推理历史不足')
        scores = model_module.predict(artifact, history[-window:], target_issue=config['target_issue'],
                                      target_date=config['target_date'], prediction_at=request.get('prediction_at'),
                                      target_deadline=request.get('target_deadline'))
        result = generate_pair(history=history, config=config, model_scores=scores)
        if (result['candidate'] != pair['candidate'] or result['control'] != pair['control']
                or result['history_sha256'] != pair['history_sha256']):
            raise CandidateError('模型候选或随机对照与产物和固定种子重放结果不一致')
        return True
    except CandidateError:
        raise
    except (ValueError, TypeError, KeyError, IndexError, AttributeError, OSError) as exc:
        raise CandidateError(f'模型候选重放失败：{str(exc)[:300]}') from exc
