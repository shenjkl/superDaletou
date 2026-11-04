"""Shared initial resource limits for historical backtests and fixed evaluation."""
from dataclasses import dataclass


@dataclass(frozen=True)
class BacktestLimits:
    max_targets: int = 50
    max_replicas: int = 10
    max_target_replica_pairs: int = 100
    min_history_periods: int = 12
    max_history_periods: int = 200
    timeout_seconds: tuple[int, int] = (1, 120)
    max_tickets: int = 100
    max_sampling_attempts: int = 20000
    budget_fen: tuple[int, int] = (200, 20000)
    max_model_validation_periods: int = 100

    def __post_init__(self):
        for name in ('max_targets', 'max_replicas', 'max_target_replica_pairs',
                     'max_history_periods', 'max_tickets', 'max_sampling_attempts',
                     'max_model_validation_periods'):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f'invalid backtest limit: {name}')
        for name in ('timeout_seconds', 'budget_fen'):
            bounds = getattr(self, name)
            if (type(bounds) is not tuple or len(bounds) != 2
                    or any(type(value) is not int for value in bounds)
                    or bounds[0] < 1 or bounds[0] > bounds[1]):
                raise ValueError(f'invalid backtest limit range: {name}')
        if (type(self.min_history_periods) is not int or self.min_history_periods < 1
                or self.min_history_periods > self.max_history_periods):
            raise ValueError('invalid backtest limit: min_history_periods')


DEFAULT_BACKTEST_LIMITS = BacktestLimits()
