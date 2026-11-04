"""Validate an optional excluded period for one historical training job.

This is a per-job boundary, not a global holdout reservation or a statement
that the researcher has not seen the selected data elsewhere.
"""
import re


class TrainingBoundaryError(ValueError):
    pass


def check_holdout_fields(config):
    has_start = 'holdout_start' in config
    has_end = 'holdout_end' in config
    if has_start != has_end:
        raise TrainingBoundaryError('训练排除范围必须同时填写起始期和结束期')
    if not has_start:
        return False
    for key in ('holdout_start', 'holdout_end'):
        if not isinstance(config[key], str) or not re.fullmatch(r'[0-9]{5}', config[key]):
            raise TrainingBoundaryError('训练排除范围的奖期格式无效')
    return True


def check_rolling_fields(config):
    if 'rolling_issue' in config and (not isinstance(config['rolling_issue'], str)
            or not re.fullmatch(r'[0-9]{5}', config['rolling_issue'])):
        raise TrainingBoundaryError('滚动训练目标期格式无效')


def check_rolling_target(db, config):
    check_rolling_fields(config)
    if 'rolling_issue' in config and db.execute('SELECT 1 FROM draw_versions WHERE issue=? LIMIT 1',
                                                (config['rolling_issue'],)).fetchone():
        raise TrainingBoundaryError('滚动训练目标期已开奖或已被记录，不能作为前瞻训练')


def check_holdout_boundary(issues, first_issue, config):
    if not check_holdout_fields(config):
        return
    if (not isinstance(issues, list) or len(issues) != len(set(issues))
            or any(issue not in issues for issue in (first_issue, config.get('cutoff'),
                                                      config['holdout_start'], config['holdout_end']))):
        raise TrainingBoundaryError('训练起止期和排除范围必须存在于所选快照')
    first = issues.index(first_issue)
    cutoff = issues.index(config['cutoff'])
    start = issues.index(config['holdout_start'])
    end = issues.index(config['holdout_end'])
    if first > cutoff or start > end:
        raise TrainingBoundaryError('训练或排除范围的起始期不得晚于结束期')
    if cutoff >= start:
        raise TrainingBoundaryError('数据截止期必须早于训练排除范围，不能使用排除期')
