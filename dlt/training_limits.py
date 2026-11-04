"""Initial bounded training limits shared by task and rolling-training paths.

These are conservative implementation defaults, not performance-calibrated
production limits. A future runtime settings surface must validate any override
against the same dimensions before accepting tasks.
"""
from dataclasses import dataclass
import math


@dataclass(frozen=True)
class TrainingLimits:
    timeout_seconds: tuple[int, int] = (1, 120)
    window_periods: tuple[int, int] = (1, 120)
    history_rows: tuple[int, int] = (3, 500)
    tree_max_depth: tuple[int, int] = (0, 5)
    tree_min_leaf: tuple[int, int] = (1, 100)
    lstm_hidden_size: tuple[int, int] = (1, 32)
    lstm_epochs: tuple[int, int] = (1, 10)
    lstm_learning_rate: tuple[float, float] = (0.0001, 1.0)

    def __post_init__(self):
        integer_ranges = (
            'timeout_seconds', 'window_periods', 'history_rows', 'tree_max_depth',
            'tree_min_leaf', 'lstm_hidden_size', 'lstm_epochs',
        )
        minimums = {
            'timeout_seconds': 1, 'window_periods': 1, 'history_rows': 3,
            'tree_max_depth': 0, 'tree_min_leaf': 1, 'lstm_hidden_size': 1,
            'lstm_epochs': 1,
        }
        for name in integer_ranges:
            bounds = getattr(self, name)
            if (type(bounds) is not tuple or len(bounds) != 2
                    or any(type(value) is not int for value in bounds)
                    or bounds[0] < minimums[name] or bounds[0] > bounds[1]):
                raise ValueError(f'invalid training limit range: {name}')
        bounds = self.lstm_learning_rate
        if (type(bounds) is not tuple or len(bounds) != 2
                or any(type(value) not in (int, float) or not math.isfinite(value) for value in bounds)
                or bounds[0] <= 0 or bounds[0] > bounds[1]):
            raise ValueError('invalid training limit range: lstm_learning_rate')


DEFAULT_TRAINING_LIMITS = TrainingLimits()
