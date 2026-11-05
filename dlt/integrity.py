"""Read-only structural checks for research databases.

This is a diagnostic, not a backup, restore authorization, authenticity check,
or a guarantee that every application invariant or secret has been inspected.
"""
from __future__ import annotations

import argparse
from contextlib import closing, nullcontext
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import parse_qs, urlencode, urlsplit

from dlt.snapshots import InvalidSnapshot, _canonical, parse_snapshot


KNOWN_TABLES = {
    'snapshots', 'observations', 'draw_versions', 'source_quarantine', 'bundled_snapshot_imports', 'bundled_snapshot_import_fences',
    'bundled_snapshot_import_retries', 'draft_pairs', 'draft_evaluations', 'model_selection_decisions',
    'pair_write_requests', 'pair_write_fences', 'pair_write_retries',
    'evaluation_write_requests', 'evaluation_write_fences', 'evaluation_write_retries',
    'selection_write_requests', 'selection_write_fences', 'selection_write_retries',
    'model_artifacts', 'training_jobs', 'backtest_jobs', 'backtest_reports',
    'reviewed_sources', 'source_write_requests', 'source_write_fences', 'source_write_retries',
    'fixed_experiments', 'fixed_experiment_write_intents', 'fixed_experiment_write_fences',
    'experiment_closures', 'source_corrections',
    'prospective_closure_write_requests', 'prospective_closure_write_fences',
    'prospective_closure_write_retries',
    'source_correction_followups', 'paper_resolutions', 'paper_groups', 'paper_current',
    'scoped_source_corrections', 'scoped_correction_followups',
    'paper_events', 'paper_publications', 'paper_write_requests', 'paper_write_fences', 'paper_write_retries',
    'update_plans', 'update_runs', 'update_attempts',
    'update_followups', 'update_plan_changes', 'update_run_configs', 'update_write_requests',
    'update_write_fences', 'update_write_retries',
    'experiment_series', 'experiment_series_write_requests', 'experiment_series_write_fences',
    'experiment_series_write_retries', 'series_periods', 'series_closures', 'sqlite_sequence',
    'rolling_training_plans', 'rolling_training_events', 'rolling_training_write_requests',
    'rolling_training_write_fences', 'rolling_training_write_retries',
    'rolling_registration_plans', 'rolling_registration_events', 'rolling_registration_write_requests',
    'rolling_registration_write_fences', 'rolling_registration_write_retries',
    'static_registration_plans', 'static_registration_events', 'static_registration_write_requests',
    'static_registration_write_fences', 'static_registration_write_retries',
    'holdout_reservations',
    'holdout_write_requests', 'holdout_write_fences', 'holdout_write_retries',
    'exploration_activities',
    'fixed_evaluations',
    'fixed_evaluation_attempts',
    'fixed_evaluation_corrections',
    'fixed_evaluation_write_requests', 'fixed_evaluation_write_fences', 'fixed_evaluation_write_retries',
    'research_decisions', 'research_hypothesis_drafts', 'research_write_intents', 'research_write_fences',
    'research_write_slots', 'research_write_acknowledgments', 'recovery_quarantine', 'recovery_write_receipts',
    'custom_origin_declarations',
    'custom_origin_write_fences', 'custom_origin_write_retries',
    'task_cancellations',
}
BASE_TABLES = {'snapshots', 'observations', 'draw_versions'}

# Minimum current columns required by each recognized table. Extra columns are
# allowed for forward-compatible/legacy metadata, but a missing required
# column means the table cannot satisfy the contracts checked below. Keeping
# this separate from TABLE_KEYS also catches damaged empty tables whose
# missing non-key columns would otherwise go unnoticed by row-level queries.
TABLE_COLUMNS = {
    'source_quarantine': frozenset(('id', 'source', 'observed_at', 'raw_sha256', 'reason', 'raw_payload', 'recorded_at')),
    'backtest_jobs': frozenset(('request_key', 'request_json', 'status', 'created_at', 'finished_at', 'error')),
    'backtest_reports': frozenset(('request_key', 'request_json', 'report_sha256', 'report_json')),
    'bundled_snapshot_import_fences': frozenset(('request_key', 'request_sha256', 'request_json', 'created_at')),
    'bundled_snapshot_import_retries': frozenset(('request_key', 'retry_of', 'request_sha256', 'request_json', 'created_at')),
    'bundled_snapshot_imports': frozenset(('request_key', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'custom_origin_declarations': frozenset(('event_id', 'evaluation_key', 'content_sha256', 'content_json', 'created_at')),
    'custom_origin_write_fences': frozenset(('event_id', 'request_sha256', 'request_json', 'created_at')),
    'custom_origin_write_retries': frozenset(('event_id', 'retry_of', 'request_sha256', 'request_json', 'created_at')),
    'draft_evaluations': frozenset(('request_key', 'pair_key', 'request_json', 'result_sha256', 'result_json')),
    'draft_pairs': frozenset(('request_key', 'request_json', 'pair_sha256', 'pair_json', 'created_at')),
    'model_selection_decisions': frozenset(('seq', 'pair_key', 'previous_seq', 'selection_sha256', 'selection_json', 'request_key', 'created_at')),
    'draw_versions': frozenset(('snapshot_id', 'issue', 'draw_date', 'front_json', 'back_json', 'numbers_usable', 'prize_fields_present', 'settlement_ready', 'settlement_reason', 'row_json')),
    'evaluation_write_fences': frozenset(('request_key', 'request_sha256', 'request_json', 'created_at')),
    'evaluation_write_requests': frozenset(('request_key', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'evaluation_write_retries': frozenset(('request_key', 'retry_of', 'request_sha256', 'request_json', 'created_at')),
    'selection_write_fences': frozenset(('request_key', 'request_sha256', 'request_json', 'created_at')),
    'selection_write_requests': frozenset(('request_key', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'selection_write_retries': frozenset(('request_key', 'retry_of', 'request_sha256', 'request_json', 'created_at')),
    'experiment_closures': frozenset(('experiment_key', 'closed_at', 'reason')),
    'experiment_series': frozenset(('series_key', 'content_sha256', 'content_json')),
    'experiment_series_write_fences': frozenset(('request_key', 'request_sha256', 'request_json', 'created_at')),
    'experiment_series_write_requests': frozenset(('request_key', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'experiment_series_write_retries': frozenset(('request_key', 'retry_of', 'request_sha256', 'request_json', 'created_at')),
    'exploration_activities': frozenset(('id', 'content_sha256', 'content_json', 'status', 'started_at', 'finished_at')),
    'fixed_evaluation_attempts': frozenset(('request_key', 'reservation_id', 'attempt_no', 'retry_of', 'retry_requested', 'status', 'started_at', 'finished_at', 'error', 'report_sha256', 'report_json')),
    'fixed_evaluation_corrections': frozenset(('id', 'reservation_id', 'base_report_sha256', 'report_sha256', 'record_json')),
    'fixed_evaluation_write_fences': frozenset(('request_key', 'operation', 'reservation_id', 'request_sha256', 'request_json', 'created_at')),
    'fixed_evaluation_write_requests': frozenset(('request_key', 'operation', 'reservation_id', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'fixed_evaluation_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'reservation_id', 'request_sha256', 'request_json', 'created_at')),
    'fixed_evaluations': frozenset(('reservation_id', 'protocol_sha256', 'protocol_json', 'created_at', 'status', 'started_at', 'finished_at', 'error', 'report_sha256', 'report_json')),
    'fixed_experiment_write_fences': frozenset(('request_key', 'request_sha256', 'fenced_at')),
    'fixed_experiment_write_intents': frozenset(('request_key', 'request_sha256', 'series_key', 'retry_of', 'created_at')),
    'fixed_experiments': frozenset(('request_key', 'request_json', 'content_sha256', 'content_json')),
    'holdout_reservations': frozenset(('id', 'content_json', 'created_at')),
    'holdout_write_fences': frozenset(('request_key', 'request_sha256', 'request_json', 'created_at')),
    'holdout_write_requests': frozenset(('request_key', 'reservation_id', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'holdout_write_retries': frozenset(('request_key', 'retry_of', 'request_sha256', 'request_json', 'created_at')),
    'model_artifacts': frozenset(('artifact_id', 'snapshot_id', 'method', 'cutoff', 'availability_mode', 'artifact_json', 'first_recorded_at')),
    'observations': frozenset(('snapshot_id', 'source', 'observed_at', 'raw_sha256', 'raw_payload')),
    'pair_write_fences': frozenset(('request_key', 'request_sha256', 'request_json', 'created_at')),
    'pair_write_requests': frozenset(('request_key', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'pair_write_retries': frozenset(('request_key', 'retry_of', 'request_sha256', 'request_json', 'created_at')),
    'paper_current': frozenset(('experiment_key', 'group_id')),
    'paper_events': frozenset(('id', 'experiment_key', 'snapshot_id', 'status', 'reason', 'recorded_at')),
    'paper_groups': frozenset(('id', 'experiment_key', 'issue', 'row_sha256', 'content_json')),
    'paper_publications': frozenset(('seq', 'experiment_key', 'previous_group', 'group_id', 'snapshot_id', 'published_at')),
    'paper_resolutions': frozenset(('seq', 'issue', 'selected_snapshot', 'selected_row_sha256', 'variants_sha256', 'note', 'reviewed_at')),
    'paper_write_fences': frozenset(('request_key', 'operation', 'object_key', 'request_sha256', 'request_json', 'created_at')),
    'paper_write_requests': frozenset(('request_key', 'operation', 'object_key', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'paper_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'object_key', 'request_sha256', 'request_json', 'created_at')),
    'prospective_closure_write_fences': frozenset(('request_key', 'operation', 'object_key', 'request_sha256', 'request_json', 'created_at')),
    'prospective_closure_write_requests': frozenset(('request_key', 'operation', 'object_key', 'request_sha256', 'request_json', 'result_json', 'created_at')),
    'prospective_closure_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'object_key', 'request_sha256', 'request_json', 'created_at')),
    'recovery_quarantine': frozenset(('id', 'created_at', 'reason')),
    'recovery_write_receipts': frozenset(('request_key', 'operation', 'request_sha256', 'created_at')),
    'research_decisions': frozenset(('request_key', 'request_json', 'record_sha256', 'record_json')),
    'research_hypothesis_drafts': frozenset(('request_key', 'decision_key', 'request_json', 'record_sha256', 'record_json')),
    'research_write_acknowledgments': frozenset(('operation', 'request_key', 'request_sha256', 'result_status', 'slot_version', 'acknowledged_at')),
    'research_write_fences': frozenset(('operation', 'request_key', 'request_sha256', 'created_at')),
    'research_write_intents': frozenset(('operation', 'request_key', 'request_sha256', 'object_kind', 'object_key', 'retry_of', 'created_at')),
    'research_write_slots': frozenset(('operation', 'object_kind', 'object_key', 'request_key', 'slot_version')),
    'reviewed_sources': frozenset(('id', 'record_json', 'document')),
    'rolling_registration_events': frozenset(('seq', 'plan_id', 'status', 'message', 'at')),
    'rolling_registration_plans': frozenset(('id', 'training_plan_id', 'request_sha256', 'request_json', 'created_at', 'status', 'message', 'updated_at')),
    'rolling_registration_write_fences': frozenset(('request_key', 'operation', 'request_json', 'created_at')),
    'rolling_registration_write_requests': frozenset(('request_key', 'operation', 'request_json', 'result_json', 'created_at')),
    'rolling_registration_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'request_json', 'created_at')),
    'rolling_training_events': frozenset(('seq', 'plan_id', 'status', 'attempt', 'job_key', 'message', 'at')),
    'rolling_training_plans': frozenset(('id', 'series_key', 'issue', 'request_sha256', 'request_json', 'created_at', 'status', 'attempt', 'resolved_json', 'job_key', 'message', 'updated_at')),
    'rolling_training_write_fences': frozenset(('request_key', 'operation', 'request_json', 'created_at')),
    'rolling_training_write_requests': frozenset(('request_key', 'operation', 'request_json', 'result_json', 'created_at')),
    'rolling_training_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'request_json', 'created_at')),
    'scoped_correction_followups': frozenset(('correction_id', 'status', 'token', 'lease_until', 'attempts', 'details_json', 'updated_at')),
    'scoped_source_corrections': frozenset(('seq', 'issue', 'old_review_id', 'new_review_id', 'kind', 'note', 'declared_at')),
    'series_closures': frozenset(('series_key', 'closed_at', 'reason')),
    'series_periods': frozenset(('series_key', 'issue', 'experiment_key')),
    'snapshots': frozenset(('id', 'raw_sha256', 'canonical_json', 'row_count')),
    'source_correction_followups': frozenset(('correction_id', 'status', 'token', 'lease_until', 'attempts', 'details_json', 'updated_at')),
    'source_corrections': frozenset(('seq', 'old_review_id', 'new_review_id', 'kind', 'note', 'declared_at')),
    'source_write_fences': frozenset(('request_key', 'operation', 'object_key', 'request_sha256', 'request_json', 'created_at')),
    'source_write_requests': frozenset(('request_key', 'operation', 'object_key', 'request_sha256', 'request_json', 'result_json', 'created_at', 'retry_of')),
    'source_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'object_key', 'request_sha256', 'request_json', 'created_at')),
    'static_registration_events': frozenset(('seq', 'plan_id', 'status', 'message', 'at')),
    'static_registration_plans': frozenset(('id', 'series_key', 'issue', 'request_sha256', 'request_json', 'created_at', 'status', 'resolved_json', 'message', 'updated_at')),
    'static_registration_write_fences': frozenset(('request_key', 'operation', 'request_json', 'created_at')),
    'static_registration_write_requests': frozenset(('request_key', 'operation', 'request_json', 'result_json', 'created_at')),
    'static_registration_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'request_json', 'created_at')),
    'task_cancellations': frozenset(('request_key', 'operation', 'request_json', 'result_json', 'created_at')),
    'training_jobs': frozenset(('request_key', 'request_json', 'name', 'status', 'created_at', 'finished_at', 'artifact_id', 'error')),
    'update_attempts': frozenset(('run_id', 'number', 'at', 'status', 'detail')),
    'update_followups': frozenset(('seq', 'run_id', 'status', 'details_json', 'recorded_at')),
    'update_plan_changes': frozenset(('seq', 'plan_id', 'old_sha256', 'old_config', 'new_sha256', 'new_config', 'old_due', 'new_due', 'changed_at')),
    'update_plans': frozenset(('id', 'config_json', 'enabled', 'next_due')),
    'update_run_configs': frozenset(('run_id', 'config_sha256', 'config_json')),
    'update_runs': frozenset(('id', 'plan_id', 'token', 'due_at', 'started_at', 'lease_until', 'status', 'ended_at', 'snapshot_id', 'error')),
    'update_write_fences': frozenset(('request_key', 'operation', 'request_json', 'request_sha256', 'created_at')),
    'update_write_requests': frozenset(('request_key', 'operation', 'request_json', 'result_json', 'created_at')),
    'update_write_retries': frozenset(('request_key', 'retry_of', 'operation', 'request_json', 'created_at')),
}

# Required-column declarations extracted from the literal CREATE TABLE
# contracts in dlt/*.py. Extra columns remain forward-compatible.
TABLE_COLUMN_TYPES = {
    'source_quarantine': {'id': 'TEXT', 'source': 'TEXT', 'observed_at': 'TEXT', 'raw_sha256': 'TEXT',
                          'reason': 'TEXT', 'raw_payload': 'BLOB', 'recorded_at': 'TEXT'},
    'backtest_jobs': {'created_at': 'TEXT', 'error': 'TEXT', 'finished_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'status': 'TEXT'},
    'backtest_reports': {'report_json': 'BLOB', 'report_sha256': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'bundled_snapshot_import_fences': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'bundled_snapshot_import_retries': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'bundled_snapshot_imports': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB'},
    'custom_origin_declarations': {'content_json': 'BLOB', 'content_sha256': 'TEXT', 'created_at': 'TEXT', 'evaluation_key': 'TEXT', 'event_id': 'TEXT'},
    'custom_origin_write_fences': {'created_at': 'TEXT', 'event_id': 'TEXT', 'request_json': 'BLOB', 'request_sha256': 'TEXT'},
    'custom_origin_write_retries': {'created_at': 'TEXT', 'event_id': 'TEXT', 'request_json': 'BLOB', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'draft_evaluations': {'pair_key': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'result_json': 'BLOB', 'result_sha256': 'TEXT'},
    'draft_pairs': {'created_at': 'TEXT', 'pair_json': 'BLOB', 'pair_sha256': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'model_selection_decisions': {'created_at': 'TEXT', 'pair_key': 'TEXT', 'previous_seq': 'INTEGER', 'request_key': 'TEXT', 'selection_json': 'BLOB', 'selection_sha256': 'TEXT', 'seq': 'INTEGER'},
    'draw_versions': {'back_json': 'TEXT', 'draw_date': 'TEXT', 'front_json': 'TEXT', 'issue': 'TEXT', 'numbers_usable': 'INTEGER', 'prize_fields_present': 'INTEGER', 'row_json': 'BLOB', 'settlement_ready': 'INTEGER', 'settlement_reason': 'TEXT', 'snapshot_id': 'TEXT'},
    'evaluation_write_fences': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'evaluation_write_requests': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB'},
    'evaluation_write_retries': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'selection_write_fences': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'selection_write_requests': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB'},
    'selection_write_retries': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'experiment_closures': {'closed_at': 'TEXT', 'experiment_key': 'TEXT', 'reason': 'TEXT'},
    'experiment_series': {'content_json': 'BLOB', 'content_sha256': 'TEXT', 'series_key': 'TEXT'},
    'experiment_series_write_fences': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'experiment_series_write_requests': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB'},
    'experiment_series_write_retries': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'exploration_activities': {'content_json': 'BLOB', 'content_sha256': 'TEXT', 'finished_at': 'TEXT', 'id': 'TEXT', 'started_at': 'TEXT', 'status': 'TEXT'},
    'fixed_evaluation_attempts': {'attempt_no': 'INTEGER', 'error': 'TEXT', 'finished_at': 'TEXT', 'report_json': 'BLOB', 'report_sha256': 'TEXT', 'request_key': 'TEXT', 'reservation_id': 'TEXT', 'retry_of': 'TEXT', 'retry_requested': 'INTEGER', 'started_at': 'TEXT', 'status': 'TEXT'},
    'fixed_evaluation_corrections': {'base_report_sha256': 'TEXT', 'id': 'TEXT', 'record_json': 'BLOB', 'report_sha256': 'TEXT', 'reservation_id': 'TEXT'},
    'fixed_evaluation_write_fences': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'reservation_id': 'TEXT'},
    'fixed_evaluation_write_requests': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'reservation_id': 'TEXT', 'result_json': 'BLOB'},
    'fixed_evaluation_write_retries': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'reservation_id': 'TEXT', 'retry_of': 'TEXT'},
    'fixed_evaluations': {'created_at': 'TEXT', 'error': 'TEXT', 'finished_at': 'TEXT', 'protocol_json': 'BLOB', 'protocol_sha256': 'TEXT', 'report_json': 'BLOB', 'report_sha256': 'TEXT', 'reservation_id': 'TEXT', 'started_at': 'TEXT', 'status': 'TEXT'},
    'fixed_experiment_write_fences': {'fenced_at': 'TEXT', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'fixed_experiment_write_intents': {'created_at': 'TEXT', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT', 'series_key': 'TEXT'},
    'fixed_experiments': {'content_json': 'BLOB', 'content_sha256': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'holdout_reservations': {'content_json': 'BLOB', 'created_at': 'TEXT', 'id': 'TEXT'},
    'holdout_write_fences': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'holdout_write_requests': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'reservation_id': 'TEXT', 'result_json': 'BLOB'},
    'holdout_write_retries': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'model_artifacts': {'artifact_id': 'TEXT', 'artifact_json': 'BLOB', 'availability_mode': 'TEXT', 'cutoff': 'TEXT', 'first_recorded_at': 'TEXT', 'method': 'TEXT', 'snapshot_id': 'TEXT'},
    'observations': {'observed_at': 'TEXT', 'raw_payload': 'BLOB', 'raw_sha256': 'TEXT', 'snapshot_id': 'TEXT', 'source': 'TEXT'},
    'pair_write_fences': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'pair_write_requests': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB'},
    'pair_write_retries': {'created_at': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'paper_current': {'experiment_key': 'TEXT', 'group_id': 'TEXT'},
    'paper_events': {'experiment_key': 'TEXT', 'id': 'TEXT', 'reason': 'TEXT', 'recorded_at': 'TEXT', 'snapshot_id': 'TEXT', 'status': 'TEXT'},
    'paper_groups': {'content_json': 'BLOB', 'experiment_key': 'TEXT', 'id': 'TEXT', 'issue': 'TEXT', 'row_sha256': 'TEXT'},
    'paper_publications': {'experiment_key': 'TEXT', 'group_id': 'TEXT', 'previous_group': 'TEXT', 'published_at': 'TEXT', 'seq': 'INTEGER', 'snapshot_id': 'TEXT'},
    'paper_resolutions': {'issue': 'TEXT', 'note': 'TEXT', 'reviewed_at': 'TEXT', 'selected_row_sha256': 'TEXT', 'selected_snapshot': 'TEXT', 'seq': 'INTEGER', 'variants_sha256': 'TEXT'},
    'paper_write_fences': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'paper_write_requests': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB'},
    'paper_write_retries': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'prospective_closure_write_fences': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'prospective_closure_write_requests': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB'},
    'prospective_closure_write_retries': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'recovery_quarantine': {'created_at': 'TEXT', 'id': 'INTEGER', 'reason': 'TEXT'},
    'recovery_write_receipts': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'research_decisions': {'record_json': 'BLOB', 'record_sha256': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'research_hypothesis_drafts': {'decision_key': 'TEXT', 'record_json': 'BLOB', 'record_sha256': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'research_write_acknowledgments': {'acknowledged_at': 'TEXT', 'operation': 'TEXT', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_status': 'TEXT', 'slot_version': 'INTEGER'},
    'research_write_fences': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'research_write_intents': {'created_at': 'TEXT', 'object_key': 'TEXT', 'object_kind': 'TEXT', 'operation': 'TEXT', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'research_write_slots': {'object_key': 'TEXT', 'object_kind': 'TEXT', 'operation': 'TEXT', 'request_key': 'TEXT', 'slot_version': 'INTEGER'},
    'reviewed_sources': {'document': 'BLOB', 'id': 'TEXT', 'record_json': 'BLOB'},
    'rolling_registration_events': {'at': 'TEXT', 'message': 'TEXT', 'plan_id': 'TEXT', 'seq': 'INTEGER', 'status': 'TEXT'},
    'rolling_registration_plans': {'created_at': 'TEXT', 'id': 'TEXT', 'message': 'TEXT', 'request_json': 'BLOB', 'request_sha256': 'TEXT', 'status': 'TEXT', 'training_plan_id': 'TEXT', 'updated_at': 'TEXT'},
    'rolling_registration_write_fences': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'rolling_registration_write_requests': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'result_json': 'BLOB'},
    'rolling_registration_write_retries': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'retry_of': 'TEXT'},
    'rolling_training_events': {'at': 'TEXT', 'attempt': 'INTEGER', 'job_key': 'TEXT', 'message': 'TEXT', 'plan_id': 'TEXT', 'seq': 'INTEGER', 'status': 'TEXT'},
    'rolling_training_plans': {'attempt': 'INTEGER', 'created_at': 'TEXT', 'id': 'TEXT', 'issue': 'TEXT', 'job_key': 'TEXT', 'message': 'TEXT', 'request_json': 'BLOB', 'request_sha256': 'TEXT', 'resolved_json': 'BLOB', 'series_key': 'TEXT', 'status': 'TEXT', 'updated_at': 'TEXT'},
    'rolling_training_write_fences': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'rolling_training_write_requests': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'result_json': 'BLOB'},
    'rolling_training_write_retries': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'retry_of': 'TEXT'},
    'scoped_correction_followups': {'attempts': 'INTEGER', 'correction_id': 'INTEGER', 'details_json': 'BLOB', 'lease_until': 'TEXT', 'status': 'TEXT', 'token': 'TEXT', 'updated_at': 'TEXT'},
    'scoped_source_corrections': {'declared_at': 'TEXT', 'issue': 'TEXT', 'kind': 'TEXT', 'new_review_id': 'TEXT', 'note': 'TEXT', 'old_review_id': 'TEXT', 'seq': 'INTEGER'},
    'series_closures': {'closed_at': 'TEXT', 'reason': 'TEXT', 'series_key': 'TEXT'},
    'series_periods': {'experiment_key': 'TEXT', 'issue': 'TEXT', 'series_key': 'TEXT'},
    'snapshots': {'canonical_json': 'BLOB', 'id': 'TEXT', 'raw_sha256': 'TEXT', 'row_count': 'INTEGER'},
    'source_correction_followups': {'attempts': 'INTEGER', 'correction_id': 'INTEGER', 'details_json': 'BLOB', 'lease_until': 'TEXT', 'status': 'TEXT', 'token': 'TEXT', 'updated_at': 'TEXT'},
    'source_corrections': {'declared_at': 'TEXT', 'kind': 'TEXT', 'new_review_id': 'TEXT', 'note': 'TEXT', 'old_review_id': 'TEXT', 'seq': 'INTEGER'},
    'source_write_fences': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'source_write_requests': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'result_json': 'BLOB', 'retry_of': 'TEXT'},
    'source_write_retries': {'created_at': 'TEXT', 'object_key': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT', 'retry_of': 'TEXT'},
    'static_registration_events': {'at': 'TEXT', 'message': 'TEXT', 'plan_id': 'TEXT', 'seq': 'INTEGER', 'status': 'TEXT'},
    'static_registration_plans': {'created_at': 'TEXT', 'id': 'TEXT', 'issue': 'TEXT', 'message': 'TEXT', 'request_json': 'BLOB', 'request_sha256': 'TEXT', 'resolved_json': 'BLOB', 'series_key': 'TEXT', 'status': 'TEXT', 'updated_at': 'TEXT'},
    'static_registration_write_fences': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT'},
    'static_registration_write_requests': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'result_json': 'BLOB'},
    'static_registration_write_retries': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'retry_of': 'TEXT'},
    'task_cancellations': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'result_json': 'BLOB'},
    'training_jobs': {'artifact_id': 'TEXT', 'created_at': 'TEXT', 'error': 'TEXT', 'finished_at': 'TEXT', 'name': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'status': 'TEXT'},
    'update_attempts': {'at': 'TEXT', 'detail': 'TEXT', 'number': 'INTEGER', 'run_id': 'TEXT', 'status': 'TEXT'},
    'update_followups': {'details_json': 'BLOB', 'recorded_at': 'TEXT', 'run_id': 'TEXT', 'seq': 'INTEGER', 'status': 'TEXT'},
    'update_plan_changes': {'changed_at': 'TEXT', 'new_config': 'BLOB', 'new_due': 'TEXT', 'new_sha256': 'TEXT', 'old_config': 'BLOB', 'old_due': 'TEXT', 'old_sha256': 'TEXT', 'plan_id': 'TEXT', 'seq': 'INTEGER'},
    'update_plans': {'config_json': 'BLOB', 'enabled': 'INTEGER', 'id': 'TEXT', 'next_due': 'TEXT'},
    'update_run_configs': {'config_json': 'BLOB', 'config_sha256': 'TEXT', 'run_id': 'TEXT'},
    'update_runs': {'due_at': 'TEXT', 'ended_at': 'TEXT', 'error': 'TEXT', 'id': 'TEXT', 'lease_until': 'TEXT', 'plan_id': 'TEXT', 'snapshot_id': 'TEXT', 'started_at': 'TEXT', 'status': 'TEXT', 'token': 'TEXT'},
    'update_write_fences': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'request_sha256': 'TEXT'},
    'update_write_requests': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'result_json': 'BLOB'},
    'update_write_retries': {'created_at': 'TEXT', 'operation': 'TEXT', 'request_json': 'BLOB', 'request_key': 'TEXT', 'retry_of': 'TEXT'},
}
# Extracted required-column nullability/default declarations from literal CREATE TABLE SQL.
TABLE_NOT_NULL_COLUMNS = {
    'source_quarantine': frozenset(('id', 'source', 'observed_at', 'raw_sha256', 'reason', 'raw_payload', 'recorded_at')),
    'backtest_jobs': frozenset(('created_at', 'request_json', 'request_key', 'status')),
    'backtest_reports': frozenset(('report_json', 'report_sha256', 'request_json', 'request_key')),
    'bundled_snapshot_import_fences': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256')),
    'bundled_snapshot_import_retries': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'bundled_snapshot_imports': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'result_json')),
    'custom_origin_declarations': frozenset(('content_json', 'content_sha256', 'created_at', 'evaluation_key', 'event_id')),
    'custom_origin_write_fences': frozenset(('created_at', 'event_id', 'request_json', 'request_sha256')),
    'custom_origin_write_retries': frozenset(('created_at', 'event_id', 'request_json', 'request_sha256', 'retry_of')),
    'draft_evaluations': frozenset(('pair_key', 'request_json', 'request_key', 'result_json', 'result_sha256')),
    'draft_pairs': frozenset(('created_at', 'pair_json', 'pair_sha256', 'request_json', 'request_key')),
    'model_selection_decisions': frozenset(('created_at', 'pair_key', 'selection_json', 'selection_sha256')),
    'draw_versions': frozenset(('back_json', 'draw_date', 'front_json', 'issue', 'numbers_usable', 'prize_fields_present', 'row_json', 'settlement_ready', 'settlement_reason', 'snapshot_id')),
    'evaluation_write_fences': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256')),
    'evaluation_write_requests': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'result_json')),
    'evaluation_write_retries': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'selection_write_fences': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256')),
    'selection_write_requests': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'result_json')),
    'selection_write_retries': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'experiment_closures': frozenset(('closed_at', 'experiment_key', 'reason')),
    'experiment_series': frozenset(('content_json', 'content_sha256', 'series_key')),
    'experiment_series_write_fences': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256')),
    'experiment_series_write_requests': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'result_json')),
    'experiment_series_write_retries': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'exploration_activities': frozenset(('content_json', 'content_sha256', 'id', 'started_at', 'status')),
    'fixed_evaluation_attempts': frozenset(('attempt_no', 'request_key', 'reservation_id', 'retry_requested', 'started_at', 'status')),
    'fixed_evaluation_corrections': frozenset(('base_report_sha256', 'id', 'record_json', 'report_sha256', 'reservation_id')),
    'fixed_evaluation_write_fences': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'request_sha256', 'reservation_id')),
    'fixed_evaluation_write_requests': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'request_sha256', 'reservation_id', 'result_json')),
    'fixed_evaluation_write_retries': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'request_sha256', 'reservation_id', 'retry_of')),
    'fixed_evaluations': frozenset(('created_at', 'protocol_json', 'protocol_sha256', 'reservation_id', 'status')),
    'fixed_experiment_write_fences': frozenset(('fenced_at', 'request_key', 'request_sha256')),
    'fixed_experiment_write_intents': frozenset(('created_at', 'request_key', 'request_sha256')),
    'fixed_experiments': frozenset(('content_json', 'content_sha256', 'request_json', 'request_key')),
    'holdout_reservations': frozenset(('content_json', 'created_at', 'id')),
    'holdout_write_fences': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256')),
    'holdout_write_requests': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'reservation_id', 'result_json')),
    'holdout_write_retries': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'model_artifacts': frozenset(('artifact_id', 'artifact_json', 'availability_mode', 'cutoff', 'first_recorded_at', 'method', 'snapshot_id')),
    'observations': frozenset(('observed_at', 'raw_payload', 'raw_sha256', 'snapshot_id', 'source')),
    'pair_write_fences': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256')),
    'pair_write_requests': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'result_json')),
    'pair_write_retries': frozenset(('created_at', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'paper_current': frozenset(('experiment_key', 'group_id')),
    'paper_events': frozenset(('experiment_key', 'id', 'reason', 'recorded_at', 'snapshot_id', 'status')),
    'paper_groups': frozenset(('content_json', 'experiment_key', 'id', 'issue', 'row_sha256')),
    'paper_publications': frozenset(('experiment_key', 'group_id', 'published_at', 'seq', 'snapshot_id')),
    'paper_resolutions': frozenset(('issue', 'note', 'reviewed_at', 'selected_row_sha256', 'selected_snapshot', 'seq', 'variants_sha256')),
    'paper_write_fences': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256')),
    'paper_write_requests': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256')),
    'paper_write_retries': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'prospective_closure_write_fences': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256')),
    'prospective_closure_write_requests': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256', 'result_json')),
    'prospective_closure_write_retries': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'recovery_quarantine': frozenset(('created_at', 'id', 'reason')),
    'recovery_write_receipts': frozenset(('created_at', 'operation', 'request_key', 'request_sha256')),
    'research_decisions': frozenset(('record_json', 'record_sha256', 'request_json', 'request_key')),
    'research_hypothesis_drafts': frozenset(('decision_key', 'record_json', 'record_sha256', 'request_json', 'request_key')),
    'research_write_acknowledgments': frozenset(('acknowledged_at', 'operation', 'request_key', 'request_sha256', 'result_status', 'slot_version')),
    'research_write_fences': frozenset(('created_at', 'operation', 'request_key', 'request_sha256')),
    'research_write_intents': frozenset(('created_at', 'object_key', 'object_kind', 'operation', 'request_key', 'request_sha256')),
    'research_write_slots': frozenset(('object_key', 'object_kind', 'operation', 'request_key', 'slot_version')),
    'reviewed_sources': frozenset(('document', 'id', 'record_json')),
    'rolling_registration_events': frozenset(('at', 'message', 'plan_id', 'seq', 'status')),
    'rolling_registration_plans': frozenset(('created_at', 'id', 'message', 'request_json', 'request_sha256', 'status', 'training_plan_id', 'updated_at')),
    'rolling_registration_write_fences': frozenset(('created_at', 'operation', 'request_json', 'request_key')),
    'rolling_registration_write_requests': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'result_json')),
    'rolling_registration_write_retries': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'retry_of')),
    'rolling_training_events': frozenset(('at', 'attempt', 'message', 'plan_id', 'seq', 'status')),
    'rolling_training_plans': frozenset(('attempt', 'created_at', 'id', 'issue', 'message', 'request_json', 'request_sha256', 'series_key', 'status', 'updated_at')),
    'rolling_training_write_fences': frozenset(('created_at', 'operation', 'request_json', 'request_key')),
    'rolling_training_write_requests': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'result_json')),
    'rolling_training_write_retries': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'retry_of')),
    'scoped_correction_followups': frozenset(('attempts', 'correction_id', 'status')),
    'scoped_source_corrections': frozenset(('declared_at', 'issue', 'kind', 'new_review_id', 'note', 'old_review_id', 'seq')),
    'series_closures': frozenset(('closed_at', 'reason', 'series_key')),
    'series_periods': frozenset(('experiment_key', 'issue', 'series_key')),
    'snapshots': frozenset(('canonical_json', 'id', 'raw_sha256', 'row_count')),
    'source_correction_followups': frozenset(('attempts', 'correction_id', 'status')),
    'source_corrections': frozenset(('declared_at', 'kind', 'new_review_id', 'note', 'old_review_id', 'seq')),
    'source_write_fences': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256')),
    'source_write_requests': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256', 'result_json')),
    'source_write_retries': frozenset(('created_at', 'object_key', 'operation', 'request_json', 'request_key', 'request_sha256', 'retry_of')),
    'static_registration_events': frozenset(('at', 'message', 'plan_id', 'seq', 'status')),
    'static_registration_plans': frozenset(('created_at', 'id', 'issue', 'message', 'request_json', 'request_sha256', 'series_key', 'status', 'updated_at')),
    'static_registration_write_fences': frozenset(('created_at', 'operation', 'request_json', 'request_key')),
    'static_registration_write_requests': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'result_json')),
    'static_registration_write_retries': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'retry_of')),
    'task_cancellations': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'result_json')),
    'training_jobs': frozenset(('created_at', 'name', 'request_json', 'request_key', 'status')),
    'update_attempts': frozenset(('at', 'number', 'run_id', 'status')),
    'update_followups': frozenset(('details_json', 'recorded_at', 'run_id', 'seq', 'status')),
    'update_plan_changes': frozenset(('changed_at', 'new_config', 'new_due', 'new_sha256', 'old_config', 'old_due', 'old_sha256', 'plan_id', 'seq')),
    'update_plans': frozenset(('config_json', 'enabled', 'id', 'next_due')),
    'update_run_configs': frozenset(('config_json', 'config_sha256', 'run_id')),
    'update_runs': frozenset(('due_at', 'id', 'lease_until', 'plan_id', 'started_at', 'status', 'token')),
    'update_write_fences': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'request_sha256')),
    'update_write_requests': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'result_json')),
    'update_write_retries': frozenset(('created_at', 'operation', 'request_json', 'request_key', 'retry_of')),
}

TABLE_COLUMN_DEFAULTS = {
    'backtest_jobs': {'created_at': None, 'error': None, 'finished_at': None, 'request_json': None, 'request_key': None, 'status': None},
    'backtest_reports': {'report_json': None, 'report_sha256': None, 'request_json': None, 'request_key': None},
    'bundled_snapshot_import_fences': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'bundled_snapshot_import_retries': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'bundled_snapshot_imports': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None},
    'custom_origin_declarations': {'content_json': None, 'content_sha256': None, 'created_at': None, 'evaluation_key': None, 'event_id': None},
    'custom_origin_write_fences': {'created_at': None, 'event_id': None, 'request_json': None, 'request_sha256': None},
    'custom_origin_write_retries': {'created_at': None, 'event_id': None, 'request_json': None, 'request_sha256': None, 'retry_of': None},
    'draft_evaluations': {'pair_key': None, 'request_json': None, 'request_key': None, 'result_json': None, 'result_sha256': None},
    'draft_pairs': {'created_at': None, 'pair_json': None, 'pair_sha256': None, 'request_json': None, 'request_key': None},
    'model_selection_decisions': {'created_at': None, 'pair_key': None, 'previous_seq': None, 'request_key': None, 'selection_json': None, 'selection_sha256': None, 'seq': None},
    'draw_versions': {'back_json': None, 'draw_date': None, 'front_json': None, 'issue': None, 'numbers_usable': None, 'prize_fields_present': None, 'row_json': None, 'settlement_ready': None, 'settlement_reason': None, 'snapshot_id': None},
    'evaluation_write_fences': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'evaluation_write_requests': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None},
    'evaluation_write_retries': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'selection_write_fences': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'selection_write_requests': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None},
    'selection_write_retries': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'experiment_closures': {'closed_at': None, 'experiment_key': None, 'reason': None},
    'experiment_series': {'content_json': None, 'content_sha256': None, 'series_key': None},
    'experiment_series_write_fences': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'experiment_series_write_requests': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None},
    'experiment_series_write_retries': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'exploration_activities': {'content_json': None, 'content_sha256': None, 'finished_at': None, 'id': None, 'started_at': None, 'status': None},
    'fixed_evaluation_attempts': {'attempt_no': None, 'error': None, 'finished_at': None, 'report_json': None, 'report_sha256': None, 'request_key': None, 'reservation_id': None, 'retry_of': None, 'retry_requested': None, 'started_at': None, 'status': None},
    'fixed_evaluation_corrections': {'base_report_sha256': None, 'id': None, 'record_json': None, 'report_sha256': None, 'reservation_id': None},
    'fixed_evaluation_write_fences': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'reservation_id': None},
    'fixed_evaluation_write_requests': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'reservation_id': None, 'result_json': None},
    'fixed_evaluation_write_retries': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'reservation_id': None, 'retry_of': None},
    'fixed_evaluations': {'created_at': None, 'error': None, 'finished_at': None, 'protocol_json': None, 'protocol_sha256': None, 'report_json': None, 'report_sha256': None, 'reservation_id': None, 'started_at': None, 'status': None},
    'fixed_experiment_write_fences': {'fenced_at': None, 'request_key': None, 'request_sha256': None},
    'fixed_experiment_write_intents': {'created_at': None, 'request_key': None, 'request_sha256': None, 'retry_of': None, 'series_key': None},
    'fixed_experiments': {'content_json': None, 'content_sha256': None, 'request_json': None, 'request_key': None},
    'holdout_reservations': {'content_json': None, 'created_at': None, 'id': None},
    'holdout_write_fences': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'holdout_write_requests': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'reservation_id': None, 'result_json': None},
    'holdout_write_retries': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'model_artifacts': {'artifact_id': None, 'artifact_json': None, 'availability_mode': None, 'cutoff': None, 'first_recorded_at': None, 'method': None, 'snapshot_id': None},
    'observations': {'observed_at': None, 'raw_payload': None, 'raw_sha256': None, 'snapshot_id': None, 'source': None},
    'pair_write_fences': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'pair_write_requests': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None},
    'pair_write_retries': {'created_at': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'paper_current': {'experiment_key': None, 'group_id': None},
    'paper_events': {'experiment_key': None, 'id': None, 'reason': None, 'recorded_at': None, 'snapshot_id': None, 'status': None},
    'paper_groups': {'content_json': None, 'experiment_key': None, 'id': None, 'issue': None, 'row_sha256': None},
    'paper_publications': {'experiment_key': None, 'group_id': None, 'previous_group': None, 'published_at': None, 'seq': None, 'snapshot_id': None},
    'paper_resolutions': {'issue': None, 'note': None, 'reviewed_at': None, 'selected_row_sha256': None, 'selected_snapshot': None, 'seq': None, 'variants_sha256': None},
    'paper_write_fences': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'paper_write_requests': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None},
    'paper_write_retries': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'prospective_closure_write_fences': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'prospective_closure_write_requests': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None},
    'prospective_closure_write_retries': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'recovery_quarantine': {'created_at': None, 'id': None, 'reason': None},
    'recovery_write_receipts': {'created_at': None, 'operation': None, 'request_key': None, 'request_sha256': None},
    'research_decisions': {'record_json': None, 'record_sha256': None, 'request_json': None, 'request_key': None},
    'research_hypothesis_drafts': {'decision_key': None, 'record_json': None, 'record_sha256': None, 'request_json': None, 'request_key': None},
    'research_write_acknowledgments': {'acknowledged_at': None, 'operation': None, 'request_key': None, 'request_sha256': None, 'result_status': None, 'slot_version': None},
    'research_write_fences': {'created_at': None, 'operation': None, 'request_key': None, 'request_sha256': None},
    'research_write_intents': {'created_at': None, 'object_key': None, 'object_kind': None, 'operation': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'research_write_slots': {'object_key': None, 'object_kind': None, 'operation': None, 'request_key': None, 'slot_version': None},
    'reviewed_sources': {'document': None, 'id': None, 'record_json': None},
    'rolling_registration_events': {'at': None, 'message': None, 'plan_id': None, 'seq': None, 'status': None},
    'rolling_registration_plans': {'created_at': None, 'id': None, 'message': None, 'request_json': None, 'request_sha256': None, 'status': None, 'training_plan_id': None, 'updated_at': None},
    'rolling_registration_write_fences': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None},
    'rolling_registration_write_requests': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'result_json': None},
    'rolling_registration_write_retries': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'retry_of': None},
    'rolling_training_events': {'at': None, 'attempt': None, 'job_key': None, 'message': None, 'plan_id': None, 'seq': None, 'status': None},
    'rolling_training_plans': {'attempt': None, 'created_at': None, 'id': None, 'issue': None, 'job_key': None, 'message': None, 'request_json': None, 'request_sha256': None, 'resolved_json': None, 'series_key': None, 'status': None, 'updated_at': None},
    'rolling_training_write_fences': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None},
    'rolling_training_write_requests': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'result_json': None},
    'rolling_training_write_retries': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'retry_of': None},
    'scoped_correction_followups': {'attempts': '0', 'correction_id': None, 'details_json': None, 'lease_until': None, 'status': None, 'token': None, 'updated_at': None},
    'scoped_source_corrections': {'declared_at': None, 'issue': None, 'kind': None, 'new_review_id': None, 'note': None, 'old_review_id': None, 'seq': None},
    'series_closures': {'closed_at': None, 'reason': None, 'series_key': None},
    'series_periods': {'experiment_key': None, 'issue': None, 'series_key': None},
    'snapshots': {'canonical_json': None, 'id': None, 'raw_sha256': None, 'row_count': None},
    'source_correction_followups': {'attempts': '0', 'correction_id': None, 'details_json': None, 'lease_until': None, 'status': None, 'token': None, 'updated_at': None},
    'source_corrections': {'declared_at': None, 'kind': None, 'new_review_id': None, 'note': None, 'old_review_id': None, 'seq': None},
    'source_write_fences': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'source_write_requests': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'result_json': None, 'retry_of': None},
    'source_write_retries': {'created_at': None, 'object_key': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None, 'retry_of': None},
    'static_registration_events': {'at': None, 'message': None, 'plan_id': None, 'seq': None, 'status': None},
    'static_registration_plans': {'created_at': None, 'id': None, 'issue': None, 'message': None, 'request_json': None, 'request_sha256': None, 'resolved_json': None, 'series_key': None, 'status': None, 'updated_at': None},
    'static_registration_write_fences': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None},
    'static_registration_write_requests': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'result_json': None},
    'static_registration_write_retries': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'retry_of': None},
    'task_cancellations': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'result_json': None},
    'training_jobs': {'artifact_id': None, 'created_at': None, 'error': None, 'finished_at': None, 'name': None, 'request_json': None, 'request_key': None, 'status': None},
    'update_attempts': {'at': None, 'detail': None, 'number': None, 'run_id': None, 'status': None},
    'update_followups': {'details_json': None, 'recorded_at': None, 'run_id': None, 'seq': None, 'status': None},
    'update_plan_changes': {'changed_at': None, 'new_config': None, 'new_due': None, 'new_sha256': None, 'old_config': None, 'old_due': None, 'old_sha256': None, 'plan_id': None, 'seq': None},
    'update_plans': {'config_json': None, 'enabled': None, 'id': None, 'next_due': None},
    'update_run_configs': {'config_json': None, 'config_sha256': None, 'run_id': None},
    'update_runs': {'due_at': None, 'ended_at': None, 'error': None, 'id': None, 'lease_until': None, 'plan_id': None, 'snapshot_id': None, 'started_at': None, 'status': None, 'token': None},
    'update_write_fences': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'request_sha256': None},
    'update_write_requests': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'result_json': None},
    'update_write_retries': {'created_at': None, 'operation': None, 'request_json': None, 'request_key': None, 'retry_of': None},
}

TABLE_CHECKS = {
    'recovery_quarantine': frozenset(('id=1', "reason='isolated recovery candidate'")),
    'recovery_write_receipts': frozenset(("operation in('recovery_prepare','recovery_activate')", 'length(request_sha256)=64')),
    'research_write_acknowledgments': frozenset(("result_status in('committed','fenced')", 'slot_version > 0')),
    'research_write_slots': frozenset(('slot_version > 0',)),
    'snapshots': frozenset(('row_count > 0',)),
    'task_cancellations': frozenset(("operation in('training_cancel','backtest_cancel')",)),
    'update_plans': frozenset(('enabled in(0,1)',)),
    'update_write_fences': frozenset(("operation in('update_plan_create','update_plan_edit','update_plan_set_enabled','update_plan_run_now','update_run_cancel','autostart_set_enabled')",)),
    'update_write_requests': frozenset(("operation in('update_plan_create','update_plan_edit','update_plan_set_enabled','update_plan_run_now','update_run_cancel','autostart_set_enabled')",)),
}

# (ordered primary key, foreign-key column / referenced table / column).
# These contracts prevent a structurally intact SQLite file from silently
# losing uniqueness or foreign-key enforcement after table reconstruction.
TABLE_KEYS = {
    'source_quarantine': (('id',), ()),
    'snapshots': (('id',), ()),
    'observations': (('snapshot_id', 'source', 'observed_at'), (('snapshot_id', 'snapshots', 'id'),)),
    'draw_versions': (('snapshot_id', 'issue'), (('snapshot_id', 'snapshots', 'id'),)),
    'bundled_snapshot_imports': (('request_key',), ()),
    'bundled_snapshot_import_fences': (('request_key',), ()),
    'bundled_snapshot_import_retries': (('request_key',), (('retry_of', 'bundled_snapshot_import_fences', 'request_key'),)),
    'draft_pairs': (('request_key',), ()),
    'model_selection_decisions': (('seq',), (('pair_key', 'draft_pairs', 'request_key'),)),
    'draft_evaluations': (('request_key',), (('pair_key', 'draft_pairs', 'request_key'),)),
    'pair_write_requests': (('request_key',), ()),
    'pair_write_fences': (('request_key',), ()),
    'pair_write_retries': (('request_key',), (('retry_of', 'pair_write_fences', 'request_key'),)),
    'evaluation_write_requests': (('request_key',), ()),
    'evaluation_write_fences': (('request_key',), ()),
    'evaluation_write_retries': (('request_key',), (('retry_of', 'evaluation_write_fences', 'request_key'),)),
    'selection_write_requests': (('request_key',), ()),
    'selection_write_fences': (('request_key',), ()),
    'selection_write_retries': (('request_key',), (('retry_of', 'selection_write_fences', 'request_key'),)),
    'model_artifacts': (('artifact_id',), (('snapshot_id', 'snapshots', 'id'),)),
    'training_jobs': (('request_key',), (('artifact_id', 'model_artifacts', 'artifact_id'),)),
    'backtest_jobs': (('request_key',), ()),
    'backtest_reports': (('request_key',), ()),
    'reviewed_sources': (('id',), ()),
    'fixed_experiments': (('request_key',), ()),
    'fixed_experiment_write_intents': (('request_key',), (('retry_of', 'fixed_experiment_write_intents', 'request_key'),)),
    'fixed_experiment_write_fences': (('request_key',), (('request_key', 'fixed_experiment_write_intents', 'request_key'),)),
    'experiment_closures': (('experiment_key',), (('experiment_key', 'fixed_experiments', 'request_key'),)),
    'experiment_series': (('series_key',), ()),
    'experiment_series_write_requests': (('request_key',), (('request_key', 'experiment_series', 'series_key'),)),
    'experiment_series_write_fences': (('request_key',), ()),
    'experiment_series_write_retries': (('request_key',), (('retry_of', 'experiment_series_write_fences', 'request_key'),)),
    'series_periods': (('series_key', 'issue'), (('series_key', 'experiment_series', 'series_key'),
                                                  ('experiment_key', 'fixed_experiments', 'request_key'))),
    'series_closures': (('series_key',), (('series_key', 'experiment_series', 'series_key'),)),
    'prospective_closure_write_requests': (('request_key',), ()),
    'prospective_closure_write_fences': (('request_key',), ()),
    'prospective_closure_write_retries': (('request_key',), (('retry_of', 'prospective_closure_write_fences', 'request_key'),)),
    'source_corrections': (('seq',), (('old_review_id', 'reviewed_sources', 'id'),
                                       ('new_review_id', 'reviewed_sources', 'id'))),
    'source_correction_followups': (('correction_id',), (('correction_id', 'source_corrections', 'seq'),)),
    'scoped_source_corrections': (('seq',), (('old_review_id', 'reviewed_sources', 'id'),
                                               ('new_review_id', 'reviewed_sources', 'id'))),
    'scoped_correction_followups': (('correction_id',), (('correction_id', 'scoped_source_corrections', 'seq'),)),
    'paper_resolutions': (('seq',), ()),
    'paper_groups': (('id',), (('experiment_key', 'fixed_experiments', 'request_key'),)),
    'paper_current': (('experiment_key',), (('experiment_key', 'fixed_experiments', 'request_key'),
                                             ('group_id', 'paper_groups', 'id'))),
    'paper_events': (('id',), ()),
    'paper_publications': (('seq',), ()),
    'paper_write_requests': (('request_key',), ()),
    'paper_write_fences': (('request_key',), ()),
    'paper_write_retries': (('request_key',), (('retry_of', 'paper_write_fences', 'request_key'),)),
    'update_plans': (('id',), ()),
    'update_runs': (('id',), (('plan_id', 'update_plans', 'id'),)),
    'update_attempts': (('run_id', 'number'), (('run_id', 'update_runs', 'id'),)),
    'update_followups': (('seq',), (('run_id', 'update_runs', 'id'),)),
    'update_plan_changes': (('seq',), (('plan_id', 'update_plans', 'id'),)),
    'update_run_configs': (('run_id',), (('run_id', 'update_runs', 'id'),)),
    'update_write_requests': (('request_key',), ()),
    'update_write_fences': (('request_key',), ()),
    'update_write_retries': (('request_key',), ()),
    'rolling_training_plans': (('id',), (('series_key', 'experiment_series', 'series_key'),)),
    'rolling_training_events': (('seq',), (('plan_id', 'rolling_training_plans', 'id'),)),
    'rolling_registration_plans': (('id',), (('training_plan_id', 'rolling_training_plans', 'id'),)),
    'rolling_registration_events': (('seq',), (('plan_id', 'rolling_registration_plans', 'id'),)),
    'rolling_registration_write_requests': (('request_key',), ()),
    'rolling_registration_write_fences': (('request_key',), ()),
    'rolling_registration_write_retries': (('request_key',), (('retry_of', 'rolling_registration_write_fences', 'request_key'),)),
    'static_registration_plans': (('id',), (('series_key', 'experiment_series', 'series_key'),)),
    'static_registration_events': (('seq',), (('plan_id', 'static_registration_plans', 'id'),)),
    'static_registration_write_requests': (('request_key',), ()),
    'static_registration_write_fences': (('request_key',), ()),
    'static_registration_write_retries': (('request_key',), (('retry_of', 'static_registration_write_fences', 'request_key'),)),
    'holdout_reservations': (('id',), ()),
    'holdout_write_requests': (('request_key',), (('reservation_id', 'holdout_reservations', 'id'),)),
    'holdout_write_fences': (('request_key',), ()),
    'holdout_write_retries': (('request_key',), (('retry_of', 'holdout_write_fences', 'request_key'),)),
    'exploration_activities': (('id',), ()),
    'fixed_evaluations': (('reservation_id',), (('reservation_id', 'holdout_reservations', 'id'),)),
    'fixed_evaluation_attempts': (('request_key',), (('reservation_id', 'fixed_evaluations', 'reservation_id'),
                                                         ('retry_of', 'fixed_evaluation_attempts', 'request_key'))),
    'fixed_evaluation_corrections': (('id',), (('reservation_id', 'fixed_evaluations', 'reservation_id'),)),
    'fixed_evaluation_write_requests': (('request_key',), (('reservation_id', 'holdout_reservations', 'id'),)),
    'fixed_evaluation_write_fences': (('request_key',), (('reservation_id', 'holdout_reservations', 'id'),)),
    'fixed_evaluation_write_retries': (('request_key',), (('retry_of', 'fixed_evaluation_write_fences', 'request_key'),)),
    'research_decisions': (('request_key',), ()),
    'research_hypothesis_drafts': (('request_key',), (('decision_key', 'research_decisions', 'request_key'),)),
    'research_write_intents': (('operation', 'request_key'), ()),
    'research_write_fences': (('operation', 'request_key'), ()),
    'research_write_slots': (('operation', 'object_kind', 'object_key'),
                             (('operation', 'research_write_intents', 'operation'),
                              ('request_key', 'research_write_intents', 'request_key'))),
    'research_write_acknowledgments': (('operation', 'request_key'),
                                       (('operation', 'research_write_intents', 'operation'),
                                        ('request_key', 'research_write_intents', 'request_key'))),
    'task_cancellations': (('request_key',), ()),
    'rolling_training_write_requests': (('request_key',), ()),
    'rolling_training_write_fences': (('request_key',), ()),
    'rolling_training_write_retries': (('request_key',), (('retry_of', 'rolling_training_write_fences', 'request_key'),)),
    'custom_origin_declarations': (('event_id',), (('evaluation_key', 'draft_evaluations', 'request_key'),)),
    'custom_origin_write_fences': (('event_id',), ()),
    'custom_origin_write_retries': (('event_id',), (('retry_of', 'custom_origin_write_fences', 'event_id'),)),
    'source_write_requests': (('request_key',), ()),
    'source_write_fences': (('request_key',), ()),
    'source_write_retries': (('request_key',), (('retry_of', 'source_write_fences', 'request_key'),)),
    'recovery_quarantine': (('id',), ()),
    'recovery_write_receipts': (('request_key',), ()),
}
FOREIGN_KEY_GROUPS = {
    # These two references intentionally remain separate foreign keys. A
    # reconstructed schema that combines them into one composite key has the
    # same flattened column/target triples, but enforces different semantics.
    'source_corrections': {
        ('reviewed_sources', (('old_review_id', 'id'),)),
        ('reviewed_sources', (('new_review_id', 'id'),)),
    },
    'scoped_source_corrections': {
        ('reviewed_sources', (('old_review_id', 'id'),)),
        ('reviewed_sources', (('new_review_id', 'id'),)),
    },
    'research_write_slots': {('research_write_intents', (('operation', 'operation'),
                                                          ('request_key', 'request_key')))},
    'research_write_acknowledgments': {('research_write_intents', (('operation', 'operation'),
                                                                    ('request_key', 'request_key')))},
}
UNIQUE_COLUMNS = {
    'bundled_snapshot_import_retries': set((('retry_of',),)),
    'custom_origin_write_retries': set((('retry_of',),)),
    'evaluation_write_retries': set((('retry_of',),)),
    'experiment_series_write_retries': set((('retry_of',),)),
    'fixed_evaluation_attempts': set((('reservation_id', 'attempt_no'),)),
    'fixed_evaluation_corrections': set((('reservation_id', 'base_report_sha256', 'report_sha256'),)),
    'fixed_evaluation_write_retries': set((('retry_of',),)),
    'fixed_experiment_write_intents': set((('retry_of',),)),
    'holdout_write_retries': set((('retry_of',),)),
    'pair_write_retries': set((('retry_of',),)),
    'selection_write_retries': set((('retry_of',),)),
    'model_selection_decisions': set((('pair_key', 'seq'), ('request_key',))),
    'paper_write_retries': set((('retry_of',),)),
    'prospective_closure_write_retries': set((('retry_of',),)),
    'research_write_intents': set((('operation', 'retry_of'),)),
    'research_write_slots': set((('operation', 'request_key'),)),
    'rolling_registration_write_retries': set((('retry_of',),)),
    'rolling_training_plans': set((('series_key', 'issue'),)),
    'rolling_training_write_retries': set((('retry_of',),)),
    'scoped_source_corrections': set((('issue', 'new_review_id'), ('issue', 'old_review_id'))),
    'series_periods': set((('experiment_key',),)),
    'source_corrections': set((('new_review_id',), ('old_review_id',))),
    'source_write_retries': set((('retry_of',),)),
    'static_registration_write_retries': set((('retry_of',),)),
    'update_write_retries': set((('retry_of',),)),
}

# Exact contracts for immutable UPDATE/DELETE trigger pairs.
IMMUTABLE_TRIGGERS = {
    'source_quarantine': ('source_quarantine', 'immutable source quarantine'),
    'task_cancellations': ('task_cancellations', 'immutable task cancellation'),
    'backtest_reports': ('backtest_reports', 'immutable backtest report'),
    'bundled_snapshot_import_fences': ('bundled_snapshot_import_fences', 'immutable bundled snapshot import fence'),
    'bundled_snapshot_import_retries': ('bundled_snapshot_import_retries', 'immutable bundled snapshot import retry'),
    'bundled_snapshot_imports': ('bundled_snapshot_imports', 'immutable bundled snapshot import'),
    'custom_origin_declarations': ('custom_origin_declarations', 'immutable custom origin declaration'),
    'custom_origin_write_fences': ('custom_origin_write_fences', 'immutable custom origin write fence'),
    'custom_origin_write_retries': ('custom_origin_write_retries', 'immutable custom origin write retry'),
    'draft_evaluations': ('draft_evaluations', 'immutable evaluation'),
    'draft_pairs': ('draft_pairs', 'immutable draft pair'),
    'draw_versions': ('draws', 'immutable draw'),
    'evaluation_write_fences': ('evaluation_write_fences', 'immutable evaluation fence'),
    'evaluation_write_requests': ('evaluation_write_requests', 'immutable evaluation write'),
    'evaluation_write_retries': ('evaluation_write_retries', 'immutable evaluation retry'),
    'experiment_closures': ('experiment_closures', 'immutable experiment closure'),
    'experiment_series': ('experiment_series', 'immutable experiment series'),
    'experiment_series_write_fences': ('experiment_series_write_fences', 'immutable experiment series fence'),
    'experiment_series_write_requests': ('experiment_series_write_requests', 'immutable experiment series write'),
    'experiment_series_write_retries': ('experiment_series_write_retries', 'immutable experiment series retry'),
    'fixed_evaluation_corrections': ('fixed_evaluation_corrections', 'immutable evaluation correction'),
    'fixed_evaluation_write_fences': ('fixed_evaluation_write_fences', 'immutable fixed evaluation write fence'),
    'fixed_evaluation_write_requests': ('fixed_evaluation_write_requests', 'immutable fixed evaluation write request'),
    'fixed_evaluation_write_retries': ('fixed_evaluation_write_retries', 'immutable fixed evaluation write retry'),
    'fixed_experiment_write_fences': ('fixed_experiment_write_fences', 'immutable fixed experiment write fence'),
    'fixed_experiment_write_intents': ('fixed_experiment_write_intents', 'immutable fixed experiment write intent'),
    'fixed_experiments': ('fixed_experiments', 'immutable fixed experiment'),
    'holdout_reservations': ('holdout_reservations', 'immutable holdout reservation'),
    'holdout_write_fences': ('holdout_write_fences', 'immutable holdout write fence'),
    'holdout_write_requests': ('holdout_write_requests', 'immutable holdout write request'),
    'holdout_write_retries': ('holdout_write_retries', 'immutable holdout write retry'),
    'model_artifacts': ('model_artifacts', 'immutable model artifact'),
    'model_selection_decisions': ('model_selection_decisions', 'immutable model selection decision'),
    'observations': ('observations', 'immutable observation'),
    'pair_write_fences': ('pair_write_fences', 'immutable pair fence'),
    'pair_write_requests': ('pair_write_requests', 'immutable pair write'),
    'pair_write_retries': ('pair_write_retries', 'immutable pair retry'),
    'paper_events': ('paper_events', 'immutable paper event'),
    'paper_groups': ('paper_groups', 'immutable paper group'),
    'paper_publications': ('paper_publications', 'immutable publication'),
    'paper_resolutions': ('paper_resolutions', 'immutable resolution'),
    'paper_write_fences': ('paper_write_fences', 'immutable paper write fence'),
    'paper_write_retries': ('paper_write_retries', 'immutable paper write retry'),
    'prospective_closure_write_fences': ('prospective_closure_write_fences', 'immutable prospective closure write fence'),
    'prospective_closure_write_requests': ('prospective_closure_write_requests', 'immutable prospective closure write request'),
    'prospective_closure_write_retries': ('prospective_closure_write_retries', 'immutable prospective closure write retry'),
    'recovery_quarantine': ('recovery_quarantine', 'immutable recovery quarantine'),
    'recovery_write_receipts': ('recovery_write_receipts', 'immutable recovery write receipt'),
    'research_decisions': ('research_decisions', 'immutable research decision'),
    'research_hypothesis_drafts': ('research_hypothesis_drafts', 'immutable hypothesis draft'),
    'research_write_acknowledgments': ('research_write_acknowledgments', 'immutable research write acknowledgment'),
    'research_write_fences': ('research_write_fences', 'immutable research write fence'),
    'research_write_intents': ('research_write_intents', 'immutable research write intent'),
    'reviewed_sources': ('reviewed_sources', 'immutable source review'),
    'rolling_registration_events': ('rolling_registration_events', 'immutable registration event'),
    'rolling_registration_write_fences': ('rolling_registration_fence', 'immutable rolling registration fence'),
    'rolling_registration_write_requests': ('rolling_registration_write', 'immutable rolling registration write'),
    'rolling_registration_write_retries': ('rolling_registration_retry', 'immutable rolling registration retry'),
    'rolling_training_events': ('rolling_events', 'immutable rolling event'),
    'rolling_training_write_fences': ('rolling_training_write_fence', 'immutable rolling training write fence'),
    'rolling_training_write_requests': ('rolling_training_write', 'immutable rolling training write'),
    'rolling_training_write_retries': ('rolling_training_write_retry', 'immutable rolling training write retry'),
    'scoped_source_corrections': ('scoped_source_corrections', 'immutable scoped source correction'),
    'selection_write_fences': ('selection_write_fences', 'immutable selection fence'),
    'selection_write_requests': ('selection_write_requests', 'immutable selection write'),
    'selection_write_retries': ('selection_write_retries', 'immutable selection retry'),
    'series_closures': ('series_closures', 'immutable series closure'),
    'series_periods': ('series_periods', 'immutable series period'),
    'snapshots': ('snapshots', 'immutable snapshot'),
    'source_corrections': ('source_corrections', 'immutable source correction'),
    'source_write_fences': ('source_write_fences', 'immutable source write fence'),
    'source_write_requests': ('source_write_requests', 'immutable source write request'),
    'source_write_retries': ('source_write_retries', 'immutable source write retry'),
    'static_registration_events': ('static_registration_events', 'immutable static registration event'),
    'static_registration_write_fences': ('static_registration_fence', 'immutable static registration fence'),
    'static_registration_write_requests': ('static_registration_write', 'immutable static registration write'),
    'static_registration_write_retries': ('static_registration_retry', 'immutable static registration retry'),
    'update_attempts': ('update_attempts', 'immutable update attempt'),
    'update_followups': ('update_followups', 'immutable update followup'),
    'update_plan_changes': ('update_plan_changes', 'immutable update plan change'),
    'update_run_configs': ('update_run_configs', 'immutable update run config'),
    'update_write_fences': ('update_write_fences', 'immutable update write fence'),
    'update_write_requests': ('update_write_requests', 'immutable update write request'),
    'update_write_retries': ('update_write_retries', 'immutable update write retry'),
}

def _expected_triggers(tables):
    expected = {}
    for table, (prefix, message) in IMMUTABLE_TRIGGERS.items():
        if table in tables:
            for operation in ('UPDATE', 'DELETE'):
                name = f'{prefix}_no_{operation.lower()}'
                expected[name] = (table, f"CREATE TRIGGER {name} BEFORE {operation} ON {table} "
                                  f"BEGIN SELECT RAISE(ABORT, '{message}'); END")
    if 'bundled_snapshot_import_fences' in tables:
        expected['bundled_snapshot_imports_no_fenced_insert'] = ('bundled_snapshot_imports',
            'CREATE TRIGGER bundled_snapshot_imports_no_fenced_insert BEFORE INSERT ON bundled_snapshot_imports '
            'WHEN EXISTS (SELECT 1 FROM bundled_snapshot_import_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced bundled snapshot import'); END")
        expected['bundled_snapshot_import_fences_no_accepted_insert'] = ('bundled_snapshot_import_fences',
            'CREATE TRIGGER bundled_snapshot_import_fences_no_accepted_insert BEFORE INSERT ON bundled_snapshot_import_fences '
            'WHEN EXISTS (SELECT 1 FROM bundled_snapshot_imports WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'accepted bundled snapshot import'); END")
    if 'experiment_series_write_fences' in tables:
        expected['experiment_series_write_requests_no_fenced_insert'] = ('experiment_series_write_requests',
            'CREATE TRIGGER experiment_series_write_requests_no_fenced_insert BEFORE INSERT ON experiment_series_write_requests '
            'WHEN EXISTS (SELECT 1 FROM experiment_series_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced experiment series write'); END")
        expected['experiment_series_write_fences_no_accepted_insert'] = ('experiment_series_write_fences',
            'CREATE TRIGGER experiment_series_write_fences_no_accepted_insert BEFORE INSERT ON experiment_series_write_fences '
            'WHEN EXISTS (SELECT 1 FROM experiment_series_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'accepted experiment series write'); END")
        expected['experiment_series_no_fenced_insert'] = ('experiment_series',
            'CREATE TRIGGER experiment_series_no_fenced_insert BEFORE INSERT ON experiment_series '
            'WHEN EXISTS (SELECT 1 FROM experiment_series_write_fences WHERE request_key=NEW.series_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced experiment series write'); END")
    if 'pair_write_fences' in tables:
        expected['pair_write_requests_no_fenced_insert'] = ('pair_write_requests',
            'CREATE TRIGGER pair_write_requests_no_fenced_insert BEFORE INSERT ON pair_write_requests '
            'WHEN EXISTS (SELECT 1 FROM pair_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced pair write'); END")
        expected['pair_write_fences_no_accepted_insert'] = ('pair_write_fences',
            'CREATE TRIGGER pair_write_fences_no_accepted_insert BEFORE INSERT ON pair_write_fences '
            'WHEN EXISTS (SELECT 1 FROM pair_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'accepted pair write'); END")
        if 'draft_pairs' in tables:
            expected['draft_pairs_no_fenced_insert'] = ('draft_pairs',
                'CREATE TRIGGER draft_pairs_no_fenced_insert BEFORE INSERT ON draft_pairs '
                'WHEN EXISTS (SELECT 1 FROM pair_write_fences WHERE request_key=NEW.request_key) '
                "BEGIN SELECT RAISE(ABORT, 'fenced pair write'); END")
    if 'evaluation_write_fences' in tables:
        expected['evaluation_write_requests_no_fenced_insert'] = ('evaluation_write_requests',
            'CREATE TRIGGER evaluation_write_requests_no_fenced_insert BEFORE INSERT ON evaluation_write_requests '
            'WHEN EXISTS (SELECT 1 FROM evaluation_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced evaluation write'); END")
        expected['evaluation_write_fences_no_accepted_insert'] = ('evaluation_write_fences',
            'CREATE TRIGGER evaluation_write_fences_no_accepted_insert BEFORE INSERT ON evaluation_write_fences '
            'WHEN EXISTS (SELECT 1 FROM evaluation_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'accepted evaluation write'); END")
        if 'draft_evaluations' in tables:
            expected['draft_evaluations_no_fenced_insert'] = ('draft_evaluations',
                'CREATE TRIGGER draft_evaluations_no_fenced_insert BEFORE INSERT ON draft_evaluations '
                'WHEN EXISTS (SELECT 1 FROM evaluation_write_fences WHERE request_key=NEW.request_key) '
                "BEGIN SELECT RAISE(ABORT, 'fenced evaluation write'); END")
    if 'selection_write_fences' in tables:
        expected['selection_write_requests_no_fenced_insert'] = ('selection_write_requests',
            'CREATE TRIGGER selection_write_requests_no_fenced_insert BEFORE INSERT ON selection_write_requests '
            'WHEN EXISTS (SELECT 1 FROM selection_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced selection write'); END")
        expected['selection_write_fences_no_accepted_insert'] = ('selection_write_fences',
            'CREATE TRIGGER selection_write_fences_no_accepted_insert BEFORE INSERT ON selection_write_fences '
            'WHEN EXISTS (SELECT 1 FROM selection_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'accepted selection write'); END")
    for table, prefix, message in (
        ('model_selection_decisions', 'model_selection_decisions', 'immutable model selection decision'),
        ('selection_write_requests', 'selection_write_requests', 'immutable selection write'),
        ('selection_write_fences', 'selection_write_fences', 'immutable selection fence'),
        ('selection_write_retries', 'selection_write_retries', 'immutable selection retry'),
    ):
        if table in tables:
            for operation in ('UPDATE', 'DELETE'):
                name = f'{prefix}_no_{operation.lower()}'
                expected[name] = (table, f"CREATE TRIGGER {name} BEFORE {operation} ON {table} "
                                  f"BEGIN SELECT RAISE(ABORT, '{message}'); END")
    if 'model_selection_decisions' in tables:
        expected['model_selection_decisions_sequence'] = ('model_selection_decisions',
            'CREATE TRIGGER model_selection_decisions_sequence BEFORE INSERT ON model_selection_decisions '
            'WHEN NEW.previous_seq IS NOT (SELECT MAX(seq) FROM model_selection_decisions WHERE pair_key=NEW.pair_key) '
            "BEGIN SELECT RAISE(ABORT, 'model selection decision sequence conflict'); END")
    if 'paper_write_requests' in tables:
        expected['paper_write_requests_no_delete'] = ('paper_write_requests',
            "CREATE TRIGGER paper_write_requests_no_delete BEFORE DELETE ON paper_write_requests "
            "BEGIN SELECT RAISE(ABORT, 'immutable paper write receipt'); END")
        expected['paper_write_requests_guard_update'] = ('paper_write_requests',
            "CREATE TRIGGER paper_write_requests_guard_update BEFORE UPDATE ON paper_write_requests "
            "WHEN OLD.request_key!=NEW.request_key OR OLD.operation!=NEW.operation OR OLD.object_key!=NEW.object_key "
            "OR OLD.request_sha256!=NEW.request_sha256 OR OLD.request_json!=NEW.request_json "
            "OR OLD.created_at!=NEW.created_at OR NEW.result_json IS NULL "
            "OR (OLD.result_json IS NOT NULL AND NOT (OLD.operation='draw_resolve' "
            "AND json_type(OLD.result_json, '$.resolution_id')='integer' "
            "AND json_type(NEW.result_json, '$.resolution')='object' "
            "AND json_type(NEW.result_json, '$.recalculations')='object' "
            "AND (SELECT COUNT(*) FROM json_each(NEW.result_json))=2 "
            "AND (SELECT COUNT(*) FROM json_each(NEW.result_json, '$.resolution'))=3 "
            "AND json_type(NEW.result_json, '$.resolution.resolution_id')='integer' "
            "AND json_type(NEW.result_json, '$.resolution.variants_sha256')='text' "
            "AND json_type(NEW.result_json, '$.resolution.selected_row_sha256')='text' "
            "AND json_extract(NEW.result_json, '$.resolution.resolution_id')=json_extract(OLD.result_json, '$.resolution_id') "
            "AND json_extract(NEW.result_json, '$.resolution.variants_sha256')=json_extract(OLD.result_json, '$.variants_sha256') "
            "AND json_extract(NEW.result_json, '$.resolution.selected_row_sha256')=json_extract(OLD.result_json, '$.selected_row_sha256'))) "
            "BEGIN SELECT RAISE(ABORT, 'immutable paper write receipt'); END")
    if 'paper_write_fences' in tables:
        expected['paper_write_requests_no_fenced_insert'] = ('paper_write_requests',
            'CREATE TRIGGER paper_write_requests_no_fenced_insert BEFORE INSERT ON paper_write_requests '
            'WHEN EXISTS (SELECT 1 FROM paper_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced paper write'); END")
        expected['paper_write_fences_no_committed_insert'] = ('paper_write_fences',
            'CREATE TRIGGER paper_write_fences_no_committed_insert BEFORE INSERT ON paper_write_fences '
            'WHEN EXISTS (SELECT 1 FROM paper_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'committed paper write'); END")
    if 'research_write_fences' in tables:
        for table, kind, message in (('research_decisions', 'decision', 'fenced research decision'),
                                     ('research_hypothesis_drafts', 'draft', 'fenced research draft')):
            name = f'{table}_no_fenced_insert'
            expected[name] = (table, f"CREATE TRIGGER {name} BEFORE INSERT ON {table} "
                              f"WHEN EXISTS (SELECT 1 FROM research_write_fences WHERE operation='{kind}' AND request_key=NEW.request_key) "
                              f"BEGIN SELECT RAISE(ABORT, '{message}'); END")
    if 'custom_origin_write_fences' in tables:
        expected['custom_origin_declarations_no_fenced_insert'] = ('custom_origin_declarations',
            'CREATE TRIGGER custom_origin_declarations_no_fenced_insert BEFORE INSERT ON custom_origin_declarations '
            'WHEN EXISTS (SELECT 1 FROM custom_origin_write_fences WHERE event_id=NEW.event_id) '
            "BEGIN SELECT RAISE(ABORT, 'fenced custom origin declaration'); END")
    if 'source_write_fences' in tables:
        expected['source_write_requests_no_fenced_insert'] = ('source_write_requests',
            'CREATE TRIGGER source_write_requests_no_fenced_insert BEFORE INSERT ON source_write_requests '
            'WHEN EXISTS (SELECT 1 FROM source_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced source write'); END")
        expected['source_write_fences_no_committed_insert'] = ('source_write_fences',
            'CREATE TRIGGER source_write_fences_no_committed_insert BEFORE INSERT ON source_write_fences '
            'WHEN EXISTS (SELECT 1 FROM source_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'committed source write'); END")
    if 'holdout_write_fences' in tables:
        expected['holdout_write_requests_no_fenced_insert'] = ('holdout_write_requests',
            'CREATE TRIGGER holdout_write_requests_no_fenced_insert BEFORE INSERT ON holdout_write_requests '
            'WHEN EXISTS (SELECT 1 FROM holdout_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced holdout reservation request'); END")
        expected['holdout_write_fences_no_committed_insert'] = ('holdout_write_fences',
            'CREATE TRIGGER holdout_write_fences_no_committed_insert BEFORE INSERT ON holdout_write_fences '
            'WHEN EXISTS (SELECT 1 FROM holdout_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'committed holdout reservation request'); END")
    if 'fixed_evaluation_write_fences' in tables:
        expected['fixed_evaluation_write_requests_no_fenced_insert'] = ('fixed_evaluation_write_requests',
            'CREATE TRIGGER fixed_evaluation_write_requests_no_fenced_insert BEFORE INSERT ON fixed_evaluation_write_requests '
            'WHEN EXISTS (SELECT 1 FROM fixed_evaluation_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced fixed evaluation write'); END")
        expected['fixed_evaluation_write_fences_no_committed_insert'] = ('fixed_evaluation_write_fences',
            'CREATE TRIGGER fixed_evaluation_write_fences_no_committed_insert BEFORE INSERT ON fixed_evaluation_write_fences '
            'WHEN EXISTS (SELECT 1 FROM fixed_evaluation_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'committed fixed evaluation write'); END")
        expected['fixed_evaluation_attempts_no_fenced_insert'] = ('fixed_evaluation_attempts',
            'CREATE TRIGGER fixed_evaluation_attempts_no_fenced_insert BEFORE INSERT ON fixed_evaluation_attempts '
            "WHEN EXISTS (SELECT 1 FROM fixed_evaluation_write_fences WHERE request_key=NEW.request_key AND operation='start') "
            "BEGIN SELECT RAISE(ABORT, 'fenced fixed evaluation start'); END")
    if 'fixed_experiment_write_fences' in tables:
        expected['fixed_experiments_no_fenced_insert'] = ('fixed_experiments',
            'CREATE TRIGGER fixed_experiments_no_fenced_insert BEFORE INSERT ON fixed_experiments '
            'WHEN EXISTS (SELECT 1 FROM fixed_experiment_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced fixed experiment request'); END")
    if 'prospective_closure_write_fences' in tables:
        expected['prospective_closure_write_requests_no_fenced_insert'] = ('prospective_closure_write_requests',
            'CREATE TRIGGER prospective_closure_write_requests_no_fenced_insert BEFORE INSERT ON prospective_closure_write_requests '
            'WHEN EXISTS (SELECT 1 FROM prospective_closure_write_fences WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'fenced prospective closure write'); END")
        expected['prospective_closure_write_fences_no_committed_insert'] = ('prospective_closure_write_fences',
            'CREATE TRIGGER prospective_closure_write_fences_no_committed_insert BEFORE INSERT ON prospective_closure_write_fences '
            'WHEN EXISTS (SELECT 1 FROM prospective_closure_write_requests WHERE request_key=NEW.request_key) '
            "BEGIN SELECT RAISE(ABORT, 'committed prospective closure write'); END")
        expected['experiment_closures_no_unresolved_fence'] = ('experiment_closures',
            'CREATE TRIGGER experiment_closures_no_unresolved_fence BEFORE INSERT ON experiment_closures '
            "WHEN EXISTS (SELECT 1 FROM prospective_closure_write_fences f WHERE f.operation='experiment_close' "
            'AND f.object_key=NEW.experiment_key AND NOT EXISTS (SELECT 1 FROM prospective_closure_write_retries r '
            'WHERE r.retry_of=f.request_key AND (EXISTS (SELECT 1 FROM prospective_closure_write_requests q '
            'WHERE q.request_key=r.request_key) OR EXISTS (SELECT 1 FROM prospective_closure_write_fences f2 '
            "WHERE f2.request_key=r.request_key)))) BEGIN SELECT RAISE(ABORT, 'unresolved fenced experiment closure'); END")
        expected['series_closures_no_unresolved_fence'] = ('series_closures',
            'CREATE TRIGGER series_closures_no_unresolved_fence BEFORE INSERT ON series_closures '
            "WHEN EXISTS (SELECT 1 FROM prospective_closure_write_fences f WHERE f.operation='series_close' "
            'AND f.object_key=NEW.series_key AND NOT EXISTS (SELECT 1 FROM prospective_closure_write_retries r '
            'WHERE r.retry_of=f.request_key AND (EXISTS (SELECT 1 FROM prospective_closure_write_requests q '
            'WHERE q.request_key=r.request_key) OR EXISTS (SELECT 1 FROM prospective_closure_write_fences f2 '
            "WHERE f2.request_key=r.request_key)))) BEGIN SELECT RAISE(ABORT, 'unresolved fenced series closure'); END")
    if 'source_corrections' in tables:
        expected['source_corrections_queue'] = ('source_corrections',
            'CREATE TRIGGER source_corrections_queue AFTER INSERT ON source_corrections '
            "BEGIN INSERT INTO source_correction_followups(correction_id,status) VALUES (new.seq,'pending'); END")
    if 'scoped_source_corrections' in tables:
        expected['scoped_source_corrections_queue'] = ('scoped_source_corrections',
            'CREATE TRIGGER scoped_source_corrections_queue AFTER INSERT ON scoped_source_corrections '
            "BEGIN INSERT INTO scoped_correction_followups(correction_id,status) VALUES (new.seq,'pending'); END")
    if 'rolling_training_plans' in tables:
        expected['rolling_plan_fixed'] = ('rolling_training_plans',
            'CREATE TRIGGER rolling_plan_fixed BEFORE UPDATE OF id,series_key,issue,request_sha256,request_json,created_at ON rolling_training_plans '
            "BEGIN SELECT RAISE(ABORT, 'immutable rolling plan'); END")
        expected['rolling_plan_no_delete'] = ('rolling_training_plans',
            "CREATE TRIGGER rolling_plan_no_delete BEFORE DELETE ON rolling_training_plans BEGIN SELECT RAISE(ABORT, 'immutable rolling plan'); END")
    if 'rolling_training_events' in tables:
        for operation in ('UPDATE', 'DELETE'):
            name = f'rolling_events_no_{operation.lower()}'
            expected[name] = ('rolling_training_events', f'CREATE TRIGGER {name} BEFORE {operation} ON rolling_training_events '
                              "BEGIN SELECT RAISE(ABORT, 'immutable rolling event'); END")
    if 'rolling_registration_plans' in tables:
        expected['rolling_registration_fixed'] = ('rolling_registration_plans',
            'CREATE TRIGGER rolling_registration_fixed BEFORE UPDATE OF id,training_plan_id,request_sha256,request_json,created_at ON rolling_registration_plans '
            "BEGIN SELECT RAISE(ABORT, 'immutable registration plan'); END")
        expected['rolling_registration_no_delete'] = ('rolling_registration_plans',
            "CREATE TRIGGER rolling_registration_no_delete BEFORE DELETE ON rolling_registration_plans BEGIN SELECT RAISE(ABORT, 'immutable registration plan'); END")
    if 'rolling_registration_events' in tables:
        for operation in ('UPDATE', 'DELETE'):
            name = f'rolling_registration_events_no_{operation.lower()}'
            expected[name] = ('rolling_registration_events', f'CREATE TRIGGER {name} BEFORE {operation} ON rolling_registration_events '
                              "BEGIN SELECT RAISE(ABORT, 'immutable registration event'); END")
    if 'static_registration_plans' in tables:
        expected['static_registration_fixed'] = ('static_registration_plans',
            'CREATE TRIGGER static_registration_fixed BEFORE UPDATE OF id,series_key,issue,request_sha256,request_json,created_at ON static_registration_plans '
            "BEGIN SELECT RAISE(ABORT, 'immutable static registration plan'); END")
        expected['static_registration_no_delete'] = ('static_registration_plans',
            "CREATE TRIGGER static_registration_no_delete BEFORE DELETE ON static_registration_plans BEGIN SELECT RAISE(ABORT, 'immutable static registration plan'); END")
    if 'static_registration_events' in tables:
        for operation in ('UPDATE', 'DELETE'):
            name = f'static_registration_events_no_{operation.lower()}'
            expected[name] = ('static_registration_events', f'CREATE TRIGGER {name} BEFORE {operation} ON static_registration_events '
                              "BEGIN SELECT RAISE(ABORT, 'immutable static registration event'); END")
    if 'exploration_activities' in tables:
        expected['exploration_activities_fixed'] = ('exploration_activities',
            'CREATE TRIGGER exploration_activities_fixed BEFORE UPDATE OF id,content_sha256,content_json,started_at ON exploration_activities '
            "BEGIN SELECT RAISE(ABORT, 'immutable exploration activity'); END")
        expected['exploration_activities_no_delete'] = ('exploration_activities',
            "CREATE TRIGGER exploration_activities_no_delete BEFORE DELETE ON exploration_activities BEGIN SELECT RAISE(ABORT, 'immutable exploration activity'); END")
    if 'fixed_evaluations' in tables:
        expected['fixed_evaluations_fixed'] = ('fixed_evaluations',
            'CREATE TRIGGER fixed_evaluations_fixed BEFORE UPDATE OF reservation_id,protocol_sha256,protocol_json,created_at ON fixed_evaluations '
            "BEGIN SELECT RAISE(ABORT, 'immutable evaluation protocol'); END")
        expected['fixed_evaluations_no_delete'] = ('fixed_evaluations',
            "CREATE TRIGGER fixed_evaluations_no_delete BEFORE DELETE ON fixed_evaluations BEGIN SELECT RAISE(ABORT, 'immutable evaluation protocol'); END")
        expected['fixed_evaluations_terminal'] = ('fixed_evaluations',
            "CREATE TRIGGER fixed_evaluations_terminal BEFORE UPDATE ON fixed_evaluations WHEN OLD.status='complete' BEGIN SELECT RAISE(ABORT, 'immutable evaluation result'); END")
    if 'fixed_evaluation_attempts' in tables:
        expected['fixed_evaluation_attempts_fixed'] = ('fixed_evaluation_attempts',
            'CREATE TRIGGER fixed_evaluation_attempts_fixed BEFORE UPDATE OF request_key,reservation_id,attempt_no,retry_of,retry_requested,started_at ON fixed_evaluation_attempts '
            "BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt'); END")
        expected['fixed_evaluation_attempts_no_delete'] = ('fixed_evaluation_attempts',
            "CREATE TRIGGER fixed_evaluation_attempts_no_delete BEFORE DELETE ON fixed_evaluation_attempts BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt'); END")
        expected['fixed_evaluation_attempts_terminal'] = ('fixed_evaluation_attempts',
            "CREATE TRIGGER fixed_evaluation_attempts_terminal BEFORE UPDATE ON fixed_evaluation_attempts WHEN OLD.status IN ('complete','failed') BEGIN SELECT RAISE(ABORT, 'immutable evaluation attempt result'); END")
    return expected


def _expected_indexes(tables):
    expected = {}
    if 'rolling_registration_plans' in tables:
        expected['rolling_registration_one_active'] = (
            'rolling_registration_plans',
            'CREATE UNIQUE INDEX rolling_registration_one_active ON rolling_registration_plans(training_plan_id) '
            "WHERE status IN ('waiting','complete')")
    if 'static_registration_plans' in tables:
        expected['static_registration_one_active'] = (
            'static_registration_plans',
            'CREATE UNIQUE INDEX static_registration_one_active ON static_registration_plans(series_key,issue) '
            "WHERE status IN ('waiting','complete')")
    return expected


def _normalized_sql(value):
    return ' '.join(value.split()) if isinstance(value, str) else ''


def _check_expressions(sql):
    """Return normalized CHECK bodies while preserving quoted literals."""
    result, cursor, upper = [], 0, sql.upper() if isinstance(sql, str) else ''
    while True:
        match = re.search(r'\bCHECK\s*\(', upper[cursor:])
        if not match:
            return frozenset(result)
        opening = cursor + match.start()
        opening = sql.find('(', opening)
        depth, quote, index = 0, None, opening
        while index < len(sql):
            char = sql[index]
            if quote:
                if char == quote:
                    if index + 1 < len(sql) and sql[index + 1] == quote:
                        index += 1
                    else:
                        quote = None
            elif char in "'\"`":
                quote = char
            elif char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    body = ' '.join(sql[opening + 1:index].split()).casefold()
                    body = re.sub(r'\s*([(),])\s*', r'\1', body)
                    result.append(body)
                    cursor = index + 1
                    break
            index += 1
        else:
            return frozenset(result)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _require(condition):
    if not condition:
        raise ValueError('integrity check failed')


def check_database(path):
    """Check one consistent, read-only SQLite snapshot without migrations.

    Accepts a database path or a caller-owned SQLite connection. Error details
    name only a table and row ordinal, never user text or paths. A supplied
    connection is left open but put into query-only mode for the check.
    A missing historical source byte sequence is incomplete, not fabricated.
    """
    result = {'status': 'passed', 'counts': {}, 'errors': [], 'limitations': [],
              'scope': 'selected structural checks only; not restore approval or source authentication'}
    errors, limitations = result['errors'], result['limitations']
    if isinstance(path, sqlite3.Connection):
        db_context = nullcontext(path)
    else:
        db_path = Path(path).resolve()
        if not db_path.is_file():
            result['status'] = 'failed'
            errors.append({'area': 'database', 'reason': 'database file not found'})
            return result
        db_context = closing(sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True))
    try:
        with db_context as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA trusted_schema=OFF')
            db.execute('BEGIN')
            checks = db.execute('PRAGMA integrity_check').fetchall()
            if checks != [('ok',)]:
                errors.append({'area': 'sqlite', 'reason': 'integrity_check failed'})
            if db.execute('PRAGMA foreign_key_check').fetchone():
                errors.append({'area': 'references', 'reason': 'foreign key check failed'})
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            missing = BASE_TABLES - tables
            if missing:
                errors.append({'area': 'schema', 'reason': 'required research tables missing'})
            if tables - KNOWN_TABLES:
                limitations.append('unknown tables are present and were not checked')
            if missing:
                result['status'] = 'failed'
                return result

            for table in sorted(tables & TABLE_KEYS.keys()):
                # Table names come from the fixed contract, never user input.
                expected_pk, expected_fk = TABLE_KEYS[table]
                columns = db.execute(f'PRAGMA table_info("{table}")').fetchall()
                actual_columns = {row[1] for row in columns}
                missing_columns = TABLE_COLUMNS.get(table, frozenset()) - actual_columns
                if missing_columns:
                    errors.append({'area': 'schema', 'reason': 'required table columns are missing'})
                expected_types = TABLE_COLUMN_TYPES.get(table, {})
                if any(row[2].strip().upper() != expected_types[row[1]]
                       for row in columns if row[1] in expected_types):
                    errors.append({'area': 'schema', 'reason': 'recovery metadata column type differs'})
                actual_not_null = {row[1] for row in columns if row[3] or row[5]}
                missing_not_null = TABLE_NOT_NULL_COLUMNS.get(table, frozenset()) - actual_not_null
                if table == 'observations' and missing_not_null == frozenset(('raw_payload',)):
                    # Older databases may have acquired raw_payload through a
                    # nullable ALTER TABLE. Keep them explicitly incomplete;
                    # the missing original bytes cannot support full replay.
                    limitations.append('legacy observation schema permits missing original bytes')
                elif missing_not_null:
                    errors.append({'area': 'schema', 'reason': 'required column nullability differs'})
                expected_defaults = TABLE_COLUMN_DEFAULTS.get(table, {})
                actual_defaults = {row[1]: row[4] for row in columns if row[1] in expected_defaults}
                if actual_defaults != expected_defaults:
                    errors.append({'area': 'schema', 'reason': 'required column defaults differ'})
                expected_checks = TABLE_CHECKS.get(table, frozenset())
                if expected_checks:
                    ddl = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                    if not ddl or not expected_checks <= _check_expressions(ddl[0]):
                        errors.append({'area': 'schema', 'reason': 'required CHECK constraint missing or altered'})
                actual_pk = tuple(name for _, name in sorted((row[5], row[1]) for row in columns if row[5]))
                foreign = db.execute(f'PRAGMA foreign_key_list("{table}")').fetchall()
                actual_fk = sorted((row[3], row[2], row[4]) for row in foreign)
                foreign_groups = {}
                for row in foreign:
                    foreign_groups.setdefault(row[0], []).append(row)
                expected_targets = {}
                for _, target, _ in expected_fk:
                    expected_targets[target] = expected_targets.get(target, 0) + 1
                ambiguous_fk_group_contract_missing = (
                    any(count > 1 for count in expected_targets.values())
                    and table not in FOREIGN_KEY_GROUPS)
                foreign_sequences_valid = all(
                    sorted(item[1] for item in rows) == list(range(len(rows)))
                    and all(item[5:8] == ('NO ACTION', 'NO ACTION', 'NONE') for item in rows)
                    for rows in foreign_groups.values())
                grouped_fk = {(
                    rows[0][2], tuple((item[3], item[4]) for item in sorted(rows, key=lambda item: item[1])))
                    for rows in foreign_groups.values()}
                if (actual_pk != expected_pk or actual_fk != sorted(expected_fk)
                        or not foreign_sequences_valid
                        or ambiguous_fk_group_contract_missing
                        or (table in FOREIGN_KEY_GROUPS and grouped_fk != FOREIGN_KEY_GROUPS[table])
                        or any(target not in tables for _, target, _ in expected_fk)):
                    errors.append({'area': 'schema', 'reason': 'primary or foreign key contract differs'})
                indexes = db.execute(f'PRAGMA index_list("{table}")').fetchall()
                unique = set()
                for index in indexes:
                    if index[2] and not index[4]:
                        name = index[1].replace('"', '""')
                        unique.add(tuple(row[2] for row in db.execute(f'PRAGMA index_info("{name}")')))
                if not UNIQUE_COLUMNS.get(table, set()) <= unique:
                    errors.append({'area': 'schema', 'reason': 'required uniqueness constraint missing'})
            if 'source_corrections' in tables and 'source_correction_followups' not in tables:
                errors.append({'area': 'schema', 'reason': 'correction queue table missing'})
            expected_indexes = _expected_indexes(tables)
            actual_indexes = {name: (table, sql) for name, table, sql in db.execute(
                "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")}
            for name, (table, sql) in expected_indexes.items():
                actual = actual_indexes.get(name)
                if not actual or actual[0] != table or _normalized_sql(actual[1]) != _normalized_sql(sql):
                    errors.append({'area': 'schema', 'reason': 'required explicit index missing or altered'})
            if set(actual_indexes) - set(expected_indexes):
                limitations.append('unknown explicit indexes are present and were not checked')
            if 'scoped_source_corrections' in tables and 'scoped_correction_followups' not in tables:
                errors.append({'area': 'schema', 'reason': 'scoped correction queue table missing'})
            for correction_table, queue_table in (('source_corrections', 'source_correction_followups'),
                                                  ('scoped_source_corrections', 'scoped_correction_followups')):
                if correction_table in tables and queue_table in tables:
                    if db.execute(f'SELECT 1 FROM {correction_table} c LEFT JOIN {queue_table} f ON f.correction_id=c.seq '
                                  'WHERE f.correction_id IS NULL LIMIT 1').fetchone():
                        errors.append({'area': queue_table, 'reason': 'correction lacks durable followup'})

            def check_rows(table, query, callback):
                if table not in tables:
                    return
                count = 0
                for count, row in enumerate(db.execute(query), 1):
                    try:
                        callback(row)
                    except (ValueError, TypeError, KeyError, IndexError, AttributeError, AssertionError,
                            ArithmeticError, sqlite3.Error):
                        errors.append({'area': table, 'row': count, 'reason': 'content or reference check failed'})
                result['counts'][table] = count

            def check_correction_followup(row):
                from dlt.scheduler import _parse
                correction_id, status, token, lease_until, attempts, details_raw, updated_at = row
                _require(type(correction_id) is int and correction_id > 0)
                _require(status in ('pending', 'running', 'complete', 'failed'))
                _require(type(attempts) is int and attempts >= 0)
                if status == 'running':
                    _require(isinstance(token, str) and bool(token) and isinstance(lease_until, str))
                    _parse(lease_until)
                if details_raw is not None:
                    details = json.loads(details_raw)
                    _require(isinstance(details, dict) and _canonical(details) == details_raw)
                if status in ('complete', 'failed'):
                    _require(details_raw is not None and attempts > 0)
                if updated_at is not None:
                    _parse(updated_at)

            for followup_table in ('source_correction_followups', 'scoped_correction_followups'):
                check_rows(followup_table,
                           f'SELECT correction_id,status,token,lease_until,attempts,details_json,updated_at FROM {followup_table}',
                           check_correction_followup)

            expected = _expected_triggers(tables)
            triggers = {name: (table, sql) for name, table, sql in db.execute(
                "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger'")}
            for name, (table, sql) in expected.items():
                actual = triggers.get(name)
                if not actual or actual[0] != table or _normalized_sql(actual[1]) != _normalized_sql(sql):
                    errors.append({'area': 'schema', 'reason': 'required trigger missing or altered'})
            if set(triggers) - set(expected):
                limitations.append('unknown triggers are present and were not checked')

            if 'model_selection_decisions' in tables:
                def check_model_selection_decision(row):
                    seq, pair_key, previous_seq, digest, selection_raw, request_key, created_at = row
                    previous = db.execute('SELECT MAX(seq) FROM model_selection_decisions WHERE pair_key=? AND seq<?',
                                          (pair_key, seq)).fetchone()[0]
                    selection = json.loads(selection_raw)
                    pair_request_row = db.execute('SELECT request_json FROM draft_pairs WHERE request_key=?',
                                                  (pair_key,)).fetchone()
                    _require(pair_request_row is not None)
                    pair_request = json.loads(pair_request_row[0])
                    from dlt.pair_repository import _validate_model_selection
                    _require(previous_seq == previous and isinstance(selection, dict)
                             and _canonical(selection) == selection_raw
                             and _sha(selection_raw) == digest
                             and pair_request.get('artifact_id') is not None)
                    _validate_model_selection(db, selection, pair_request['artifact_id'],
                                              pair_request['config']['method'])
                    _require(isinstance(created_at, str) and bool(datetime.fromisoformat(created_at).tzinfo))
                    if request_key is not None:
                        from dlt.pair_evaluation_writes import status_in_transaction
                        receipt = status_in_transaction(db, 'selection_decision', request_key)
                        _require(receipt['status'] == 'accepted')

                check_rows('model_selection_decisions',
                           'SELECT seq,pair_key,previous_seq,selection_sha256,selection_json,request_key,created_at '
                           'FROM model_selection_decisions ORDER BY pair_key,seq',
                           check_model_selection_decision)

            if 'recovery_quarantine' in tables:
                markers = db.execute('SELECT id,created_at,reason FROM recovery_quarantine').fetchall()
                valid_marker = len(markers) == 1 and markers[0][0] == 1 and markers[0][2] == 'isolated recovery candidate'
                if valid_marker:
                    try:
                        marker_time = datetime.fromisoformat(markers[0][1])
                        valid_marker = marker_time.tzinfo is not None and marker_time.utcoffset() is not None
                    except (TypeError, ValueError):
                        valid_marker = False
                if not valid_marker:
                    errors.append({'area': 'recovery_quarantine', 'reason': 'invalid quarantine marker'})
                result['counts']['recovery_quarantine'] = len(markers)

            def check_paper_resolution(row):
                issue, snapshot_id, selected_sha, variants_sha, note, reviewed_at = row
                _require(isinstance(issue, str) and re.fullmatch(r'\d{5}', issue))
                _require(isinstance(snapshot_id, str) and re.fullmatch(r'[0-9a-f]{64}', snapshot_id))
                _require(isinstance(selected_sha, str) and re.fullmatch(r'[0-9a-f]{64}', selected_sha))
                _require(isinstance(variants_sha, str) and re.fullmatch(r'[0-9a-f]{64}', variants_sha))
                _require(isinstance(note, str) and note.strip() and len(note) <= 1000)
                stamp = datetime.fromisoformat(reviewed_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                selected = db.execute('SELECT row_sha256 FROM draw_versions WHERE snapshot_id=? AND issue=?',
                                      (snapshot_id, issue)).fetchone()
                _require(selected and selected[0] == selected_sha)
            check_rows('paper_resolutions',
                       'SELECT issue,selected_snapshot,selected_row_sha256,variants_sha256,note,reviewed_at FROM paper_resolutions',
                       check_paper_resolution)

            def check_recovery_write_receipt(row):
                from dlt.source_write_protocol import _request_key
                request_key, operation, request_sha256, created_at = row
                _request_key(request_key)
                _require(operation in ('recovery_prepare', 'recovery_activate')
                         and isinstance(request_sha256, str)
                         and re.fullmatch(r'[0-9a-f]{64}', request_sha256))
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)
            check_rows('recovery_write_receipts',
                       'SELECT request_key,operation,request_sha256,created_at FROM recovery_write_receipts',
                       check_recovery_write_receipt)

            def check_decision(row):
                from dlt.research_decisions import _record
                record, _ = _record(db, 'research_decisions', row[0])
                evidence = record['evidence']
                if evidence['kind'] == 'series':
                    target = db.execute('SELECT content_sha256 FROM experiment_series WHERE series_key=?', (evidence['key'],)).fetchone()
                elif evidence['kind'] == 'fixed_evaluation':
                    target = db.execute('SELECT protocol_sha256 FROM fixed_evaluations WHERE reservation_id=?', (evidence['key'],)).fetchone()
                else:
                    raise ValueError('unknown decision target')
                _require(target and target[0] == evidence['protocol_sha256'])
            check_rows('research_decisions', 'SELECT request_key FROM research_decisions', check_decision)

            def check_hypothesis(row):
                from dlt.research_decisions import load_draft
                load_draft(db, row[0])
            check_rows('research_hypothesis_drafts', 'SELECT request_key FROM research_hypothesis_drafts', check_hypothesis)

            def check_research_write_intent(row):
                from dlt.research_decisions import ResearchDecisions
                operation, key = row
                _require('research_write_fences' in tables and 'research_decisions' in tables
                         and 'research_hypothesis_drafts' in tables)
                intent = ResearchDecisions._intent(db, operation, key)
                state = ResearchDecisions._write_state(db, operation, key, intent['request_sha256'])
                if state['status'] == 'committed':
                    request = state['record']['request']
                    target = ((request['kind'], request['key']) if operation == 'decision'
                              else ('decision', request['decision_key']))
                    _require(target == (intent['object_kind'], intent['object_key']))
                if intent['retry_of']:
                    old = ResearchDecisions._intent(db, operation, intent['retry_of'])
                    _require((old['object_kind'], old['object_key']) == (intent['object_kind'], intent['object_key']))
                    _require(ResearchDecisions._write_state(db, operation, old['request_key'], old['request_sha256'])['status'] == 'fenced')
            check_rows('research_write_intents', 'SELECT operation,request_key FROM research_write_intents', check_research_write_intent)

            def check_research_write_fence(row):
                from dlt.research_decisions import ResearchDecisions
                operation, key, digest, created_at = row
                ResearchDecisions._write_input(operation, key, digest)
                recorded = datetime.fromisoformat(created_at)
                _require(recorded.tzinfo is not None)
                table = 'research_decisions' if operation == 'decision' else 'research_hypothesis_drafts'
                _require(table in tables and not db.execute(f'SELECT 1 FROM {table} WHERE request_key=?', (key,)).fetchone())
            check_rows('research_write_fences', 'SELECT operation,request_key,request_sha256,created_at FROM research_write_fences', check_research_write_fence)

            def check_research_write_slot(row):
                from dlt.research_decisions import ResearchDecisions
                operation, kind, key, request_key, version = row
                _require(type(version) is int and version > 0)
                intent = ResearchDecisions._intent(db, operation, request_key)
                _require((intent['object_kind'], intent['object_key']) == (kind, key))
                _require(not db.execute('SELECT 1 FROM research_write_acknowledgments '
                                        'WHERE operation=? AND request_key=?',
                                        (operation, request_key)).fetchone())
                _require(ResearchDecisions._write_state(db, operation, request_key,
                                                        intent['request_sha256'])['status']
                         in ('unknown', 'committed', 'fenced'))
            check_rows('research_write_slots', 'SELECT operation,object_kind,object_key,request_key,slot_version '
                       'FROM research_write_slots', check_research_write_slot)

            def check_research_write_acknowledgment(row):
                from dlt.research_decisions import ResearchDecisions
                operation, key, digest, result_status, version, acknowledged_at = row
                ResearchDecisions._write_input(operation, key, digest)
                _require(type(version) is int and version > 0 and result_status in ('committed', 'fenced'))
                intent = ResearchDecisions._intent(db, operation, key)
                _require(intent['request_sha256'] == digest)
                _require(ResearchDecisions._write_state(db, operation, key, digest)['status'] == result_status)
                saved_at = datetime.fromisoformat(acknowledged_at)
                _require(saved_at.tzinfo is not None)
                _require(not db.execute('SELECT 1 FROM research_write_slots '
                                        'WHERE operation=? AND request_key=?', (operation, key)).fetchone())
            check_rows('research_write_acknowledgments',
                       'SELECT operation,request_key,request_sha256,result_status,slot_version,acknowledged_at '
                       'FROM research_write_acknowledgments', check_research_write_acknowledgment)

            def check_series_hypothesis(row):
                from dlt.research_decisions import load_draft
                content = json.loads(row[0])
                manifest = content['manifest']
                if 'hypothesis_draft_key' in manifest:
                    draft = load_draft(db, manifest['hypothesis_draft_key'])
                    _require(draft['hypothesis'] == manifest['hypothesis'])
            check_rows('experiment_series', 'SELECT content_json FROM experiment_series', check_series_hypothesis)

            def check_snapshot(row):
                ident, raw_digest, raw, count = row
                _require(_sha(raw) == ident)
                _require(db.execute('SELECT 1 FROM observations WHERE snapshot_id=? AND raw_sha256=?',
                                    (ident, raw_digest)).fetchone())
                payload, parsed = parse_snapshot(raw)
                _require(_canonical(payload) == raw and len(parsed) == count)
                stored = db.execute('SELECT issue,draw_date,front_json,back_json,numbers_usable,'
                                    'prize_fields_present,settlement_ready,settlement_reason,row_json '
                                    'FROM draw_versions WHERE snapshot_id=?', (ident,)).fetchall()
                expected = {(x['issue'], x['draw_date'], json.dumps(x['front']), json.dumps(x['back']),
                             1, int(x['prize_fields_present']), 0, x['settlement_reason'], _canonical(x['raw_row']))
                            for x in parsed}
                _require(len(stored) == len(expected) and set(stored) == expected)
            check_rows('snapshots', 'SELECT id,raw_sha256,canonical_json,row_count FROM snapshots', check_snapshot)

            def check_observation(row):
                ident, digest, raw = row
                if raw is None:
                    if 'one or more historical observations lack original bytes' not in limitations:
                        limitations.append('one or more historical observations lack original bytes')
                    return
                _require(_sha(raw) == digest)
                payload, _ = parse_snapshot(raw)
                _require(_sha(_canonical(payload)) == ident)
            check_rows('observations', 'SELECT snapshot_id,raw_sha256,raw_payload FROM observations', check_observation)

            def check_source_quarantine(row):
                ident, source, observed_at, digest, reason, raw, recorded_at = row
                source_parts = urlsplit(source)
                query = parse_qs(source_parts.query, keep_blank_values=True, strict_parsing=True)
                _require(source_parts.scheme == 'https' and source_parts.hostname == 'webapi.sporttery.cn'
                         and source_parts.port in (None, 443)
                         and source_parts.path == '/gateway/lottery/getHistoryPageListV1.qry'
                         and set(query) == {'gameNo', 'provinceId', 'pageSize', 'isVerify', 'pageNo'}
                         and all(len(values) == 1 for values in query.values())
                         and query['gameNo'][0] == '85' and query['provinceId'][0] == '0'
                         and query['isVerify'][0] == '1'
                         and query['pageSize'][0].isdigit() and 1 <= int(query['pageSize'][0]) <= 100
                         and query['pageNo'][0].isdigit() and 1 <= int(query['pageNo'][0]) <= 1000)
                expected_source = ('https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?'
                                   + urlencode({'gameNo': 85, 'provinceId': 0,
                                                'pageSize': int(query['pageSize'][0]), 'isVerify': 1,
                                                'pageNo': int(query['pageNo'][0])}))
                _require(isinstance(raw, bytes) and len(raw) <= 5 * 1024 * 1024
                         and _sha(raw) == digest and isinstance(reason, str) and reason.strip()
                         and len(reason) <= 1000
                         and source == expected_source)
                observed = datetime.fromisoformat(observed_at)
                recorded = datetime.fromisoformat(recorded_at)
                _require(observed.tzinfo is not None and observed.utcoffset() is not None
                         and recorded.tzinfo is not None and recorded.utcoffset() is not None)
                expected_id = _sha(_canonical({'source': source, 'observed_at': observed_at,
                                               'raw_sha256': digest}))
                _require(ident == expected_id)
                try:
                    _, parsed = parse_snapshot(raw)
                    page_sizes = query.get('pageSize', [])
                    invalid = (len(page_sizes) != 1 or not page_sizes[0].isdigit()
                               or len(parsed) > int(page_sizes[0]))
                except (InvalidSnapshot, TypeError, ValueError):
                    invalid = True
                _require(invalid)
            check_rows('source_quarantine',
                       'SELECT id,source,observed_at,raw_sha256,reason,raw_payload,recorded_at FROM source_quarantine',
                       check_source_quarantine)

            def check_bundled_snapshot_import(row):
                key, digest, request_raw, result_raw, created_at = row
                _require(isinstance(request_raw, bytes) and hashlib.sha256(request_raw).hexdigest() == digest)
                request = json.loads(request_raw)
                _require(_canonical(request) == request_raw and set(request) == {'source', 'snapshot_id', 'raw_sha256'}
                         and isinstance(request['source'], str) and re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id'])
                         and re.fullmatch(r'[0-9a-f]{64}', request['raw_sha256']))
                result = json.loads(result_raw)
                _require(isinstance(result, dict) and result.get('snapshot_id') == request['snapshot_id']
                         and result.get('source') == request['source'] and type(result.get('row_count')) is int
                         and result['row_count'] > 0)
                snapshot = db.execute('SELECT raw_sha256,row_count FROM snapshots WHERE id=?',
                                      (request['snapshot_id'],)).fetchone()
                _require(snapshot == (request['raw_sha256'], result['row_count']))
                observed = datetime.fromisoformat(result['observed_at'])
                created = datetime.fromisoformat(created_at)
                _require(observed.tzinfo is not None and observed.utcoffset() is not None
                         and created.tzinfo is not None and created.utcoffset() is not None)
                _require(db.execute('SELECT 1 FROM observations WHERE snapshot_id=? AND source=? AND raw_sha256=?',
                                    (request['snapshot_id'], request['source'], request['raw_sha256'])).fetchone())
            check_rows('bundled_snapshot_imports', 'SELECT request_key,request_sha256,request_json,result_json,created_at '
                       'FROM bundled_snapshot_imports', check_bundled_snapshot_import)

            def check_bundled_snapshot_import_fence(row):
                key, digest, request_raw, created_at = row
                _require(re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', key, re.I))
                _require(isinstance(request_raw, bytes) and hashlib.sha256(request_raw).hexdigest() == digest)
                request = json.loads(request_raw)
                _require(_canonical(request) == request_raw and set(request) == {'source', 'snapshot_id', 'raw_sha256'}
                         and isinstance(request['source'], str) and bool(request['source'].strip())
                         and re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id'])
                         and re.fullmatch(r'[0-9a-f]{64}', request['raw_sha256']))
                created = datetime.fromisoformat(created_at)
                _require(created.tzinfo is not None and created.utcoffset() is not None)
                _require(not db.execute('SELECT 1 FROM bundled_snapshot_imports WHERE request_key=?', (key,)).fetchone())

            check_rows('bundled_snapshot_import_fences',
                       'SELECT request_key,request_sha256,request_json,created_at FROM bundled_snapshot_import_fences',
                       check_bundled_snapshot_import_fence)

            def check_bundled_snapshot_import_retry(row):
                key, retry_of, digest, request_raw, created_at = row
                original = db.execute('SELECT request_sha256,request_json,created_at FROM bundled_snapshot_import_fences '
                                      'WHERE request_key=?', (retry_of,)).fetchone()
                _require(original is not None and original[:2] == (digest, request_raw))
                _require(not db.execute('SELECT 1 FROM bundled_snapshot_import_retries WHERE retry_of=?',
                                        (retry_of,)).fetchone())
                _require(not db.execute('SELECT 1 FROM bundled_snapshot_import_retries WHERE request_key=?',
                                        (retry_of,)).fetchone())
                _require(isinstance(request_raw, bytes) and hashlib.sha256(request_raw).hexdigest() == digest
                         and _canonical(json.loads(request_raw)) == request_raw)
                retry_time, fence_time = datetime.fromisoformat(created_at), datetime.fromisoformat(original[2])
                _require(retry_time.tzinfo is not None and retry_time.utcoffset() is not None
                         and fence_time.tzinfo is not None and fence_time.utcoffset() is not None
                         and retry_time >= fence_time)
                accepted = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_imports WHERE request_key=?',
                                     (key,)).fetchone()
                fence = db.execute('SELECT request_sha256,request_json FROM bundled_snapshot_import_fences WHERE request_key=?',
                                   (key,)).fetchone()
                _require(accepted == (digest, request_raw) or fence == (digest, request_raw))

            check_rows('bundled_snapshot_import_retries',
                       'SELECT request_key,retry_of,request_sha256,request_json,created_at '
                       'FROM bundled_snapshot_import_retries', check_bundled_snapshot_import_retry)

            def check_reservation(row):
                ident, raw = row
                _require(_sha(raw) == ident)
                record = json.loads(raw)
                from dlt.holdout_reservation import reservation_lineage
                records, _ = reservation_lineage(db)
                _require(records.get(ident) == record)
                _require(record['version'] in ('local-exclusion-v1', 'local-exclusion-successor-v1')
                         and record['audit_status'] == 'no_recorded_use'
                         and record['independence'] == 'not_verified')
                issues = record['issues']
                _require(isinstance(issues, list) and 1 <= len(issues) <= 200 and len(issues) == len(set(issues)))
                snapshot = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (record['snapshot_id'],)).fetchone()
                _require(snapshot)
                _, parsed = parse_snapshot(snapshot[0])
                source = [item['issue'] for item in sorted(parsed, key=lambda item: (item['draw_date'], item['issue']))]
                start, end = source.index(record['start_issue']), source.index(record['end_issue'])
                _require(start <= end and issues == source[start:end+1])
            check_rows('holdout_reservations', 'SELECT id,content_json FROM holdout_reservations', check_reservation)

            def check_holdout_write_request(row):
                from dlt.holdout_reservation import _request
                request_key, reservation_id, digest, request_raw, result_raw, created_at = row
                request = json.loads(request_raw)
                normalized = _request(request.get('snapshot_id'), request.get('start_issue'),
                                      request.get('end_issue'), request.get('confirmed'))
                _require(normalized == request and _canonical(request) == request_raw
                         and _sha(request_raw) == digest)
                _require(not db.execute('SELECT 1 FROM holdout_write_fences WHERE request_key=?',
                                        (request_key,)).fetchone())
                saved = db.execute('SELECT content_json,created_at FROM holdout_reservations WHERE id=?',
                                   (reservation_id,)).fetchone()
                _require(saved)
                result = json.loads(result_raw)
                content = json.loads(saved[0])
                _require(all(content.get(field) == request[field]
                             for field in ('snapshot_id', 'start_issue', 'end_issue')))
                _require(_canonical(result) == result_raw and result == {
                    'id': reservation_id, 'content': content, 'created_at': saved[1]})
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)
            check_rows('holdout_write_requests',
                       'SELECT request_key,reservation_id,request_sha256,request_json,result_json,created_at '
                       'FROM holdout_write_requests', check_holdout_write_request)

            def check_holdout_write_fence(row):
                from dlt.holdout_reservation import _request
                request_key, digest, raw, created_at = row
                request = json.loads(raw)
                normalized = _request(request.get('snapshot_id'), request.get('start_issue'),
                                      request.get('end_issue'), request.get('confirmed'))
                _require(normalized == request and _canonical(request) == raw and _sha(raw) == digest)
                _require(not db.execute('SELECT 1 FROM holdout_write_requests WHERE request_key=?',
                                        (request_key,)).fetchone())
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)
            check_rows('holdout_write_fences',
                       'SELECT request_key,request_sha256,request_json,created_at FROM holdout_write_fences',
                       check_holdout_write_fence)

            def check_holdout_write_retry(row):
                request_key, retry_of, digest, raw, created_at = row
                original = db.execute('SELECT request_sha256,request_json FROM holdout_write_fences WHERE request_key=?',
                                      (retry_of,)).fetchone()
                _require(request_key != retry_of and original == (digest, raw))
                accepted = db.execute('SELECT request_sha256,request_json FROM holdout_write_requests WHERE request_key=?',
                                      (request_key,)).fetchone()
                fenced = db.execute('SELECT request_sha256,request_json FROM holdout_write_fences WHERE request_key=?',
                                    (request_key,)).fetchone()
                _require(accepted == (digest, raw) or fenced == (digest, raw))
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)
            check_rows('holdout_write_retries',
                       'SELECT request_key,retry_of,request_sha256,request_json,created_at FROM holdout_write_retries',
                       check_holdout_write_retry)

            def check_activity(row):
                digest, raw, status, finished = row
                _require(_sha(raw) == digest)
                activity = json.loads(raw)
                _require(activity['version'] == 'exploration-activity-v1' and activity['kind'] in
                         ('training', 'generation', 'backtest', 'historical_scenario', 'portfolio_scenario_evaluation'))
                _require(all(isinstance(activity[k], list) and len(activity[k]) == len(set(activity[k])) for k in ('inputs', 'targets')))
                _require((status == 'running' and finished is None) or (status == 'finished' and finished is not None))
            check_rows('exploration_activities', 'SELECT content_sha256,content_json,status,finished_at FROM exploration_activities', check_activity)

            def check_fixed_evaluation(row):
                reservation_id, digest, raw, status, started, finished, error, report_sha, report_raw = row
                _require(_sha(raw) == digest)
                spec = json.loads(raw)
                _require(spec['reservation_id'] == reservation_id and spec['version'] == 'fixed-descriptive-evaluation-v1'
                         and spec['independence'] == 'not_verified' and spec['aggregation'] == 'arithmetic_mean_v1'
                         and spec['primary_metric'] == 'after_tax_net_paired_difference'
                         and spec['missing_policy'] == 'incomplete_period_unpublished')
                reservation = db.execute('SELECT content_json FROM holdout_reservations WHERE id=?', (reservation_id,)).fetchone()
                _require(reservation)
                reserved = json.loads(reservation[0])
                methods = spec['methods']
                _require(isinstance(methods, list) and 1 <= len(methods) <= 5
                         and len({item['id'] for item in methods}) == len(methods))
                from dlt.backtest import _validate
                first = _validate(methods[0]['protocol']); baseline = first['candidate']
                for item in methods:
                    p = _validate(item['protocol']); c = p['candidate']
                    _require(p['snapshot_id'] == reserved['snapshot_id'] and p['targets'] == reserved['issues']
                             and p['replicas'] == first['replicas']
                             and all(c.get(k) == baseline.get(k) for k in ('budget_fen', 'base_price_fen', 'additional_price_fen', 'additional', 'structure', 'front_count', 'back_count', 'max_tickets')))
                _require((status == 'pending' and started is None and finished is None and error is None and report_raw is None and report_sha is None)
                         or (status == 'running' and started is not None and finished is None and error is None and report_raw is None and report_sha is None)
                         or (status == 'failed' and started is not None and finished is not None and error is not None and report_raw is None and report_sha is None)
                         or (status == 'complete' and started is not None and finished is not None and error is None and report_raw is not None and report_sha is not None))
                if report_raw is not None:
                    _require(_sha(report_raw) == report_sha)
                    report = json.loads(report_raw)
                    _require(report['reservation_id'] == reservation_id and report['protocol_sha256'] == digest
                             and [item['id'] for item in report['methods']] == [item['id'] for item in methods])
                    for actual, expected in zip(report['methods'], methods):
                        result = actual['result']; p = expected['protocol']
                        _require(result['protocol'] == p and result['protocol_sha256'] == _sha(_canonical(p))
                                 and result['scenarios'] == spec['scenarios']
                                 and result['scenarios_sha256'] == _sha(_canonical(spec['scenarios'])))
                attempts = db.execute('SELECT request_key,attempt_no,retry_of,retry_requested,status,started_at,finished_at,error,report_sha256,report_json '
                                      'FROM fixed_evaluation_attempts WHERE reservation_id=? ORDER BY attempt_no', (reservation_id,)).fetchall()
                _require(bool(attempts) == (status != 'pending'))
                previous = None
                for index, attempt in enumerate(attempts):
                    key, number, retry_of, is_retry, attempt_status, a_started, a_finished, a_error, a_sha, a_raw = attempt
                    _require(number == index and retry_of == (previous[0] if previous else None)
                             and is_retry == int(index > 0) and (not previous or previous[4] == 'failed'))
                    _require((attempt_status == 'running' and a_started is not None and a_finished is None and a_error is None and a_sha is None and a_raw is None)
                             or (attempt_status == 'failed' and a_started is not None and a_finished is not None and a_error is not None and a_sha is None and a_raw is None)
                             or (attempt_status == 'complete' and a_started is not None and a_finished is not None and a_error is None and a_sha is not None and a_raw is not None and _sha(a_raw) == a_sha))
                    previous = attempt
                if previous:
                    _require((previous[4],previous[5],previous[6],previous[7],previous[8],previous[9])
                             == (status,started,finished,error,report_sha,report_raw))
            check_rows('fixed_evaluations', 'SELECT reservation_id,protocol_sha256,protocol_json,status,started_at,finished_at,error,report_sha256,report_json FROM fixed_evaluations', check_fixed_evaluation)

            def check_fixed_correction(row):
                ident, reservation_id, base_sha, report_sha, raw = row
                _require(_sha(raw) == ident)
                obj = json.loads(raw)
                _require(set(obj) == {'version','reservation_id','base_report_sha256','report_sha256','report','created_at',
                                     'workflow_code_sha256','calculation_code_sha256','settlement_code_sha256'}
                         and obj['version'] == 'fixed-amount-correction-v1'
                         and obj['reservation_id'] == reservation_id and obj['base_report_sha256'] == base_sha
                         and obj['report_sha256'] == report_sha and _sha(_canonical(obj['report'])) == report_sha
                         and all(isinstance(obj[k], str) and re.fullmatch(r'[0-9a-f]{64}', obj[k])
                                 for k in ('workflow_code_sha256','calculation_code_sha256','settlement_code_sha256')))
                stamp = datetime.fromisoformat(obj['created_at'])
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                original = db.execute('SELECT status,report_sha256,report_json FROM fixed_evaluations WHERE reservation_id=?',
                                      (reservation_id,)).fetchone()
                _require(original and original[0] == 'complete' and original[1] == base_sha and _sha(original[2]) == base_sha)
                from dlt.fixed_evaluation import _without_amounts
                from dlt.paired_description import describe_backtest
                _require(_without_amounts(json.loads(original[2])) == _without_amounts(obj['report']))
                for item in obj['report']['methods']:
                    describe_backtest(item['result'])
            check_rows('fixed_evaluation_corrections', 'SELECT id,reservation_id,base_report_sha256,report_sha256,record_json FROM fixed_evaluation_corrections', check_fixed_correction)

            fixed_write_tables = {'fixed_evaluation_write_requests', 'fixed_evaluation_write_fences',
                                  'fixed_evaluation_write_retries'}
            if tables & fixed_write_tables and not fixed_write_tables <= tables:
                errors.append({'area': 'fixed_evaluation_writes', 'reason': 'write journal schema is incomplete'})
            if fixed_write_tables <= tables:
                from dlt.fixed_evaluation import _write_request

                def check_fixed_write_request(row):
                    request_key, operation, reservation_id, digest, request_raw, result_raw, created_at = row
                    _require(operation in ('freeze', 'start', 'supersede', 'recalculate_amounts')
                             and _sha(request_raw) == digest)
                    request = json.loads(request_raw)
                    normalized = _write_request(operation, request)
                    _require(normalized == request and _canonical(normalized) == request_raw
                             and normalized['reservation_id'] == reservation_id)
                    result = json.loads(result_raw)
                    _require(isinstance(result, dict) and _canonical(result) == result_raw)
                    stamp = datetime.fromisoformat(created_at)
                    _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                    if operation in ('freeze', 'start', 'recalculate_amounts'):
                        _require(result.get('reservation_id') == reservation_id)
                        evaluation = db.execute('SELECT protocol_json FROM fixed_evaluations WHERE reservation_id=?',
                                                (reservation_id,)).fetchone()
                        _require(evaluation)
                        spec = json.loads(evaluation[0])
                        if operation == 'freeze':
                            _require(spec.get('methods') == request['methods']
                                     and spec.get('hypothesis') == request['hypothesis']
                                     and spec.get('timeout') == request['timeout'])
                        elif operation == 'start':
                            attempt = db.execute('SELECT reservation_id,retry_requested FROM fixed_evaluation_attempts '
                                                 'WHERE request_key=?', (request_key,)).fetchone()
                            _require(attempt == (reservation_id, int(request['retry']))
                                     and result.get('requested_attempt', {}).get('request_key') == request_key)
                        else:
                            correction = result.get('correction')
                            _require(isinstance(correction, dict) and db.execute(
                                'SELECT 1 FROM fixed_evaluation_corrections WHERE id=? AND reservation_id=?',
                                (correction.get('id'), reservation_id)).fetchone())
                    else:
                        ident, content, result_created = result.get('id'), result.get('content'), result.get('created_at')
                        _require(isinstance(ident, str) and isinstance(content, dict)
                                 and content.get('supersedes') == reservation_id and result_created == content.get('created_at', result_created))
                        reservation = db.execute('SELECT content_json,created_at FROM holdout_reservations WHERE id=?',
                                                 (ident,)).fetchone()
                        _require(reservation and json.loads(reservation[0]) == content
                                 and reservation[1] == result_created)
                check_rows('fixed_evaluation_write_requests',
                           'SELECT request_key,operation,reservation_id,request_sha256,request_json,result_json,created_at '
                           'FROM fixed_evaluation_write_requests', check_fixed_write_request)

                def check_fixed_write_fence(row):
                    request_key, operation, reservation_id, digest, request_raw, created_at = row
                    _require(operation in ('freeze', 'start', 'supersede', 'recalculate_amounts')
                             and _sha(request_raw) == digest
                             and not db.execute('SELECT 1 FROM fixed_evaluation_write_requests WHERE request_key=?',
                                                (request_key,)).fetchone())
                    request = json.loads(request_raw)
                    normalized = _write_request(operation, request)
                    _require(normalized == request and _canonical(normalized) == request_raw
                             and normalized['reservation_id'] == reservation_id)
                    stamp = datetime.fromisoformat(created_at)
                    _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                check_rows('fixed_evaluation_write_fences',
                           'SELECT request_key,operation,reservation_id,request_sha256,request_json,created_at '
                           'FROM fixed_evaluation_write_fences', check_fixed_write_fence)

                def check_fixed_write_retry(row):
                    request_key, retry_of, operation, reservation_id, digest, request_raw, created_at = row
                    original = db.execute('SELECT operation,reservation_id,request_sha256,request_json,created_at '
                                          'FROM fixed_evaluation_write_fences WHERE request_key=?', (retry_of,)).fetchone()
                    _require(request_key != retry_of and original
                             and original[:4] == (operation, reservation_id, digest, request_raw)
                             and _sha(request_raw) == digest)
                    request = json.loads(request_raw)
                    normalized = _write_request(operation, request)
                    _require(normalized == request and _canonical(normalized) == request_raw)
                    saved = db.execute('SELECT operation,reservation_id,request_sha256,request_json '
                                       'FROM fixed_evaluation_write_requests WHERE request_key=?', (request_key,)).fetchone()
                    fenced = db.execute('SELECT operation,reservation_id,request_sha256,request_json '
                                        'FROM fixed_evaluation_write_fences WHERE request_key=?', (request_key,)).fetchone()
                    _require(saved == (operation, reservation_id, digest, request_raw)
                             or fenced == (operation, reservation_id, digest, request_raw))
                    when, original_when = datetime.fromisoformat(created_at), datetime.fromisoformat(original[4])
                    _require(when.tzinfo is not None and when.utcoffset() is not None
                             and original_when.tzinfo is not None and original_when.utcoffset() is not None
                             and original_when <= when)
                check_rows('fixed_evaluation_write_retries',
                           'SELECT request_key,retry_of,operation,reservation_id,request_sha256,request_json,created_at '
                           'FROM fixed_evaluation_write_retries', check_fixed_write_retry)

            closure_write_tables = {'prospective_closure_write_requests', 'prospective_closure_write_fences',
                                    'prospective_closure_write_retries'}
            if tables & closure_write_tables and not closure_write_tables <= tables:
                errors.append({'area': 'prospective_closure_writes', 'reason': 'write journal schema is incomplete'})
            if closure_write_tables <= tables:
                from dlt.prospective_closure_writes import normalize as normalize_closure_write

                def checked_closure_request(operation, object_key, digest, request_raw):
                    _require(operation in ('experiment_close', 'series_close') and _sha(request_raw) == digest)
                    request = json.loads(request_raw)
                    normalized = normalize_closure_write(operation, request)
                    _require(normalized == request and _canonical(normalized) == request_raw
                             and normalized['object_key'] == object_key)
                    return normalized

                def check_prospective_closure_request(row):
                    request_key, operation, object_key, digest, request_raw, result_raw, created_at = row
                    request = checked_closure_request(operation, object_key, digest, request_raw)
                    _require(not db.execute('SELECT 1 FROM prospective_closure_write_fences WHERE request_key=?',
                                            (request_key,)).fetchone())
                    result = json.loads(result_raw)
                    _require(isinstance(result, dict) and set(result) == {'closed_at', 'reason'}
                             and result['reason'] == request['reason'] and _canonical(result) == result_raw)
                    stamp = datetime.fromisoformat(created_at)
                    closed_at = datetime.fromisoformat(result['closed_at'])
                    _require(stamp.tzinfo is not None and stamp.utcoffset() is not None
                             and closed_at.tzinfo is not None and closed_at.utcoffset() is not None)
                    table, key_column = (('experiment_closures', 'experiment_key') if operation == 'experiment_close'
                                         else ('series_closures', 'series_key'))
                    closure = db.execute(f'SELECT closed_at,reason FROM {table} WHERE {key_column}=?',
                                         (object_key,)).fetchone()
                    _require(closure == (result['closed_at'], result['reason']))
                check_rows('prospective_closure_write_requests',
                           'SELECT request_key,operation,object_key,request_sha256,request_json,result_json,created_at '
                           'FROM prospective_closure_write_requests', check_prospective_closure_request)

                def check_prospective_closure_fence(row):
                    request_key, operation, object_key, digest, request_raw, created_at = row
                    request = checked_closure_request(operation, object_key, digest, request_raw)
                    _require(not db.execute('SELECT 1 FROM prospective_closure_write_requests WHERE request_key=?',
                                            (request_key,)).fetchone())
                    table, key_column = (('fixed_experiments', 'request_key') if operation == 'experiment_close'
                                         else ('experiment_series', 'series_key'))
                    _require(db.execute(f'SELECT 1 FROM {table} WHERE {key_column}=?',
                                        (request['object_key'],)).fetchone())
                    stamp = datetime.fromisoformat(created_at)
                    _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                check_rows('prospective_closure_write_fences',
                           'SELECT request_key,operation,object_key,request_sha256,request_json,created_at '
                           'FROM prospective_closure_write_fences', check_prospective_closure_fence)

                def check_prospective_closure_retry(row):
                    request_key, retry_of, operation, object_key, digest, request_raw, created_at = row
                    original = db.execute('SELECT operation,object_key,request_sha256,request_json,created_at '
                                          'FROM prospective_closure_write_fences WHERE request_key=?',
                                          (retry_of,)).fetchone()
                    request = checked_closure_request(operation, object_key, digest, request_raw)
                    accepted = db.execute('SELECT operation,object_key,request_sha256,request_json '
                                          'FROM prospective_closure_write_requests WHERE request_key=?',
                                          (request_key,)).fetchone()
                    fenced = db.execute('SELECT operation,object_key,request_sha256,request_json '
                                        'FROM prospective_closure_write_fences WHERE request_key=?',
                                        (request_key,)).fetchone()
                    _require(request_key != retry_of and original is not None
                             and original[:4] == (operation, object_key, digest, request_raw)
                             and (accepted == (operation, object_key, digest, request_raw)
                                  or fenced == (operation, object_key, digest, request_raw)))
                    original_when, when = datetime.fromisoformat(original[4]), datetime.fromisoformat(created_at)
                    _require(original_when.tzinfo is not None and original_when.utcoffset() is not None
                             and when.tzinfo is not None and when.utcoffset() is not None
                             and original_when <= when)
                check_rows('prospective_closure_write_retries',
                           'SELECT request_key,retry_of,operation,object_key,request_sha256,request_json,created_at '
                           'FROM prospective_closure_write_retries', check_prospective_closure_retry)

            def check_embedded(row, name):
                digest, raw = row
                obj = json.loads(raw)
                _require(isinstance(obj, dict) and obj.pop(name) == digest)
                _require(_sha(_canonical(obj)) == digest)

            def check_update_plan_change(row):
                plan_id, old_sha, old_raw, new_sha, new_raw, old_due, new_due, changed_at = row
                from dlt.scheduler import _parse, _validate
                _require(_sha(old_raw) == old_sha and _sha(new_raw) == new_sha)
                old, new = _validate(json.loads(old_raw)), _validate(json.loads(new_raw))
                _require(old.get('run_once') is not True and new.get('run_once') is not True)
                _parse(old_due); _parse(changed_at)
                _require(_parse(new_due) == _parse(new['start_at']))
                _require(db.execute('SELECT 1 FROM update_plans WHERE id=?', (plan_id,)).fetchone())
            check_rows('update_plan_changes', 'SELECT plan_id,old_sha256,old_config,new_sha256,new_config,old_due,new_due,changed_at FROM update_plan_changes', check_update_plan_change)

            def check_update_plan(row):
                ident, raw, enabled, next_due = row
                from dlt.scheduler import _parse, _validate
                _require(isinstance(ident, str) and bool(ident))
                _require(type(enabled) is int and enabled in (0, 1))
                config = _validate(json.loads(raw))
                due = _parse(next_due)
                if config.get('run_once') is not True:
                    _require(due >= _parse(config['start_at']))
            check_rows('update_plans', 'SELECT id,config_json,enabled,next_due FROM update_plans', check_update_plan)

            def check_update_run(row):
                ident, plan_id, token, due_at, started_at, lease_until, status, ended_at, snapshot_id, error = row
                from dlt.scheduler import _parse
                _require(isinstance(ident, str) and bool(ident)
                         and db.execute('SELECT 1 FROM update_plans WHERE id=?', (plan_id,)).fetchone())
                _require(isinstance(token, str) and bool(token))
                due, started, lease = _parse(due_at), _parse(started_at), _parse(lease_until)
                _require(lease >= started and due <= started)
                _require(status in ('running', 'cancelling', 'cancelled', 'interrupted', 'success', 'failed'))
                if status in ('running', 'cancelling'):
                    _require(ended_at is None)
                else:
                    _require(ended_at is not None and _parse(ended_at) >= started)
                if status == 'success':
                    _require(isinstance(snapshot_id, str) and bool(db.execute(
                        'SELECT 1 FROM snapshots WHERE id=?', (snapshot_id,)).fetchone()))
                else:
                    _require(snapshot_id is None)
                if status == 'failed':
                    _require(isinstance(error, str) and bool(error))
            check_rows('update_runs', 'SELECT id,plan_id,token,due_at,started_at,lease_until,status,ended_at,snapshot_id,error FROM update_runs', check_update_run)

            if 'update_attempts' in tables:
                from dlt.scheduler import _parse, _validate
                for run_id, plan_id, started_at, ended_at, run_status in db.execute(
                        'SELECT id,plan_id,started_at,ended_at,status FROM update_runs'):
                    plan = db.execute('SELECT config_json FROM update_plans WHERE id=?', (plan_id,)).fetchone()
                    _require(plan is not None)
                    retry_limit = _validate(json.loads(plan[0]))['retry_count'] + 1
                    attempts = db.execute('SELECT number,at,status,detail FROM update_attempts WHERE run_id=? ORDER BY number',
                                          (run_id,)).fetchall()
                    _require(len(attempts) <= retry_limit)
                    _require(run_status not in ('success', 'failed') or bool(attempts))
                    for index, (number, at, attempt_status, detail) in enumerate(attempts, 1):
                        _require(number == index and attempt_status in ('success', 'failed'))
                        attempt_time = _parse(at)
                        _require(attempt_time >= _parse(started_at))
                        _require(isinstance(detail, str) and bool(detail) and len(detail) <= 2000)
                        _require(attempt_status != 'success' or index == len(attempts))
                        if ended_at is not None:
                            _require(attempt_time <= _parse(ended_at))
                    if attempts and run_status == 'success':
                        _require(attempts[-1][2] == 'success')
                    if attempts and run_status == 'failed':
                        _require(attempts[-1][2] == 'failed')
                    if attempts and run_status in ('running', 'cancelling'):
                        _require(attempts[-1][2] == 'failed')

            def check_update_followup(row):
                run_id, status, details_raw, recorded_at = row
                _require(status in ('complete', 'incomplete', 'failed', 'skipped'))
                details = json.loads(details_raw)
                _require(isinstance(details, dict) and _canonical(details) == details_raw)
                run = db.execute('SELECT status,snapshot_id FROM update_runs WHERE id=?', (run_id,)).fetchone()
                _require(run and run[0] == 'success' and isinstance(run[1], str))
                _parse(recorded_at)
            check_rows('update_followups',
                       'SELECT run_id,status,details_json,recorded_at FROM update_followups',
                       check_update_followup)

            if 'update_plan_changes' in tables:
                for plan_id, current in db.execute('SELECT id,config_json FROM update_plans'):
                    changes = db.execute('SELECT old_sha256,new_sha256 FROM update_plan_changes WHERE plan_id=? ORDER BY seq', (plan_id,)).fetchall()
                    if changes and (not isinstance(current, bytes)
                                    or any(changes[i][1] != changes[i+1][0] for i in range(len(changes)-1))
                                    or changes[-1][1] != _sha(current)):
                        errors.append({'area': 'update_plan_changes', 'reason': 'configuration change chain does not match current plan'})

            def check_update_run_config(row):
                digest, raw = row
                from dlt.scheduler import _validate
                _require(_sha(raw) == digest)
                _validate(json.loads(raw))
            check_rows('update_run_configs', 'SELECT config_sha256,config_json FROM update_run_configs', check_update_run_config)

            def check_update_write(row):
                request_key, operation, request_raw, result_raw, created_at = row
                request, result = json.loads(request_raw), json.loads(result_raw)
                from dlt.scheduler import UPDATE_WRITE_OPERATIONS
                _require(operation in UPDATE_WRITE_OPERATIONS and isinstance(request, dict) and isinstance(result, dict))
                _require(request_raw == _canonical(request) and result_raw == _canonical(result))
                if operation == 'update_plan_create':
                    from dlt.scheduler import _validate
                    _require(set(request) == {'config'}); _validate(request['config'])
                    _require(isinstance(result.get('id'), str) and result.get('enabled') is False)
                    _require(db.execute('SELECT 1 FROM update_plans WHERE id=?', (result['id'],)).fetchone())
                elif operation == 'update_plan_edit':
                    from dlt.scheduler import _validate
                    _require(set(request) == {'plan_id', 'expected_sha256', 'config'})
                    _validate(request['config'])
                    _require(isinstance(request['expected_sha256'], str) and len(request['expected_sha256']) == 64)
                    _require(type(result.get('changed')) is bool and isinstance(result.get('config_sha256'), str)
                             and len(result['config_sha256']) == 64)
                    _require(db.execute('SELECT 1 FROM update_plans WHERE id=?', (request['plan_id'],)).fetchone())
                elif operation == 'update_plan_set_enabled':
                    _require(set(request) == {'plan_id', 'enabled'} and type(request['enabled']) is bool)
                    _require(result == {'plan_id': request['plan_id'], 'enabled': request['enabled']})
                    _require(db.execute('SELECT 1 FROM update_plans WHERE id=?', (request['plan_id'],)).fetchone())
                elif operation == 'update_plan_run_now':
                    _require(set(request) == {'plan_id'} and result.get('status') == 'queued'
                             and result.get('plan_id') == request['plan_id'] and isinstance(result.get('id'), str))
                    child = db.execute('SELECT config_json FROM update_plans WHERE id=?', (result['id'],)).fetchone()
                    _require(child is not None)
                    from dlt.scheduler import _validate
                    child_config = _validate(json.loads(child[0]))
                    _require(child_config.get('run_once') is True and child_config.get('parent_plan_id') == request['plan_id'])
                elif operation == 'autostart_set_enabled':
                    _require(set(request) == {'enabled', 'port'} and type(request['enabled']) is bool
                             and type(request['port']) is int and 1 <= request['port'] <= 65535)
                    state = result.get('autostart')
                    _require(isinstance(state, dict) and state.get('state') == ('enabled' if request['enabled'] else 'disabled'))
                else:
                    _require(set(request) == {'plan_id', 'run_id'})
                    _require(result.get('plan_id') == request['plan_id'] and result.get('run_id') == request['run_id']
                             and result.get('status') in ('cancelling', 'cancelled', 'success', 'failed', 'interrupted'))
                    _require(db.execute('SELECT 1 FROM update_runs WHERE id=? AND plan_id=?',
                                        (request['run_id'], request['plan_id'])).fetchone())
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                _require(not db.execute('SELECT 1 FROM rolling_training_write_fences WHERE request_key=?',
                                        (request_key,)).fetchone())
                _require(isinstance(request_key, str) and request_key.strip() and len(request_key) <= 128)
            check_rows('update_write_requests',
                       'SELECT request_key,operation,request_json,result_json,created_at FROM update_write_requests',
                       check_update_write)

            def check_update_write_fence(row):
                request_key, operation, request_raw, digest, created_at = row
                request = json.loads(request_raw)
                from dlt.scheduler import UPDATE_WRITE_OPERATIONS, _validate_update_write
                _require(operation in UPDATE_WRITE_OPERATIONS and isinstance(request, dict))
                _require(request_raw == _canonical(request))
                _validate_update_write(operation, request)
                _require(digest == _sha(_canonical({'operation': operation, 'request': request})))
                _require(not db.execute('SELECT 1 FROM update_write_requests WHERE request_key=?',
                                        (request_key,)).fetchone())
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                _require(isinstance(request_key, str) and request_key.strip() and len(request_key) <= 128)
            check_rows('update_write_fences',
                       'SELECT request_key,operation,request_json,request_sha256,created_at FROM update_write_fences',
                       check_update_write_fence)

            def check_update_write_retry(row):
                request_key, retry_of, operation, request_raw, created_at = row
                request = json.loads(request_raw)
                from dlt.scheduler import UPDATE_WRITE_OPERATIONS, _update_write_object, _validate_update_write
                _require(operation in UPDATE_WRITE_OPERATIONS and isinstance(request, dict))
                _require(request_raw == _canonical(request))
                _validate_update_write(operation, request)
                original = db.execute('SELECT operation,request_json FROM update_write_fences WHERE request_key=?',
                                      (retry_of,)).fetchone()
                current = db.execute('SELECT operation,request_json FROM update_write_requests WHERE request_key=?',
                                     (request_key,)).fetchone()
                if not current:
                    current = db.execute('SELECT operation,request_json FROM update_write_fences WHERE request_key=?',
                                         (request_key,)).fetchone()
                _require(original is not None and current is not None)
                _require(original[0] == operation and current == (operation, request_raw))
                _require(_update_write_object(operation, json.loads(original[1])) == _update_write_object(operation, request))
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                _require(isinstance(request_key, str) and request_key.strip() and len(request_key) <= 128)
                _require(isinstance(retry_of, str) and retry_of.strip() and len(retry_of) <= 128)
            check_rows('update_write_retries',
                       'SELECT request_key,retry_of,operation,request_json,created_at FROM update_write_retries',
                       check_update_write_retry)

            def check_model_artifact(row):
                artifact_id, snapshot_id, method, cutoff, availability_mode, raw, first_recorded_at = row
                check_embedded((artifact_id, raw), 'artifact_sha256')
                artifact = json.loads(raw)
                config = artifact.get('config')
                _require(isinstance(config, dict)
                         and (config.get('snapshot_id'), config.get('method'), config.get('cutoff'),
                              config.get('availability_mode'))
                         == (snapshot_id, method, cutoff, availability_mode))
                from dlt.scheduler import _parse
                _parse(first_recorded_at)
                from dlt.model_pair_validation import validate_model_artifact
                validate_model_artifact(db, artifact, check_conflicts=False)
            check_rows('model_artifacts',
                       'SELECT artifact_id,snapshot_id,method,cutoff,availability_mode,artifact_json,first_recorded_at '
                       'FROM model_artifacts', check_model_artifact)

            def check_training_job(row):
                request_key, request_raw, name, status, created_at, finished_at, artifact_id, error = row
                request = json.loads(request_raw)
                _require(request_raw == _canonical(request) and isinstance(request, dict)
                         and request.get('name') == name
                         and status in ('running', 'complete', 'failed', 'cancelled'))
                from dlt.training_jobs import TrainingJobs
                normalized = TrainingJobs._validate(name, request.get('config'), request.get('first_issue'),
                                                    request.get('timeout'),
                                                    trial_group_id=request.get('trial_group_id'),
                                                    retry_of=request.get('retry_of'))
                _require(normalized == request)
                config = request['config']
                issues = [item[0] for item in db.execute(
                    'SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue',
                    (config['snapshot_id'],))]
                _require(request['first_issue'] in issues and config['train_end'] in issues
                         and config['cutoff'] in issues)
                first, split, cutoff = (issues.index(request['first_issue']),
                                        issues.index(config['train_end']), issues.index(config['cutoff']))
                from dlt.training_limits import DEFAULT_TRAINING_LIMITS
                _require(first <= split < cutoff
                         and DEFAULT_TRAINING_LIMITS.history_rows[0] <= cutoff-first+1
                         <= DEFAULT_TRAINING_LIMITS.history_rows[1]
                         and split-first >= config['window'])
                from dlt.scheduler import _parse
                created = _parse(created_at)
                finished = _parse(finished_at) if finished_at is not None else None
                _require(finished is None or finished >= created)
                if status == 'running':
                    _require(finished is None and artifact_id is None and error is None)
                elif status == 'complete':
                    _require(finished is not None and isinstance(artifact_id, str) and error is None)
                    artifact_row = db.execute('SELECT snapshot_id,method,cutoff,availability_mode,artifact_json,first_recorded_at '
                                              'FROM model_artifacts WHERE artifact_id=?', (artifact_id,)).fetchone()
                    _require(artifact_row is not None)
                    snapshot_id, method, model_cutoff, availability_mode, artifact_raw, recorded_at = artifact_row
                    artifact = json.loads(artifact_raw)
                    recorded = _parse(recorded_at)
                    _require(recorded <= finished
                             and (snapshot_id, method, model_cutoff, availability_mode)
                             == (config['snapshot_id'], config['method'], config['cutoff'],
                                 config['availability_mode'])
                             and artifact.get('artifact_sha256') == artifact_id
                             and artifact.get('config') == config
                             and artifact.get('history_first_issue') == request['first_issue']
                             and artifact.get('history_last_issue') == config['cutoff'])
                elif status == 'failed':
                    _require(finished is not None and artifact_id is None
                             and isinstance(error, str) and 0 < len(error) <= 2000)
                else:
                    _require(finished is not None and artifact_id is None and error is None)
                retry_of = request.get('retry_of')
                if retry_of is not None:
                    _require(isinstance(retry_of, str) and retry_of and retry_of != request_key)
                    original = db.execute('SELECT request_json,status FROM training_jobs WHERE request_key=?',
                                          (retry_of,)).fetchone()
                    _require(original is not None and original[1] in ('failed', 'cancelled'))
                    original_request = json.loads(original[0])
                    retry_request = dict(request)
                    retry_request.pop('retry_of', None)
                    original_request.pop('retry_of', None)
                    _require(retry_request == original_request)
            check_rows('training_jobs',
                       'SELECT request_key,request_json,name,status,created_at,finished_at,artifact_id,error '
                       'FROM training_jobs', check_training_job)

            def check_backtest_job(row):
                request_key, request_raw, status, created_at, finished_at, error = row
                request = json.loads(request_raw)
                _require(request_raw == _canonical(request) and isinstance(request, dict)
                         and status in ('running', 'complete', 'failed', 'cancelled')
                         and set(request) in ({'protocol', 'timeout'}, {'protocol', 'timeout', 'retry_of'}))
                from dlt.backtest import _validate
                from dlt.backtest_limits import DEFAULT_BACKTEST_LIMITS
                protocol = _validate(request['protocol'])
                _require(protocol == request['protocol']
                         and type(request['timeout']) is int
                         and DEFAULT_BACKTEST_LIMITS.timeout_seconds[0] <= request['timeout']
                         <= DEFAULT_BACKTEST_LIMITS.timeout_seconds[1])
                from dlt.scheduler import _parse
                created = _parse(created_at)
                finished = _parse(finished_at) if finished_at is not None else None
                _require(finished is None or finished >= created)
                issues = [item[0] for item in db.execute(
                    'SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue',
                    (protocol['snapshot_id'],))]
                target_positions = [issues.index(issue) for issue in protocol['targets'] if issue in issues]
                _require(len(target_positions) == len(protocol['targets'])
                         and target_positions == sorted(target_positions))
                report_row = db.execute('SELECT request_json,report_sha256,report_json FROM backtest_reports '
                                        'WHERE request_key=?', (request_key,)).fetchone()
                if status == 'running':
                    _require(finished is None and error is None and report_row is None)
                elif status == 'complete':
                    _require(finished is not None and error is None and report_row is not None)
                elif status == 'failed':
                    _require(finished is not None and isinstance(error, str) and 0 < len(error) <= 2000)
                    _require(report_row is None)
                else:
                    _require(finished is not None and error is None and report_row is None)
                if status == 'complete':
                    report_request_raw, report_digest, report_raw = report_row
                    _require(isinstance(report_request_raw, bytes) and isinstance(report_raw, bytes)
                             and _sha(report_raw) == report_digest)
                    report_request = json.loads(report_request_raw)
                    report = json.loads(report_raw)
                    _require(_canonical(report_request) == report_request_raw
                             and set(report_request) == {'protocol', 'scenarios'}
                             and _canonical(report) == report_raw
                             and report.get('protocol') == protocol
                             and report_request == {'protocol': protocol, 'scenarios': report.get('scenarios')})
                retry_of = request.get('retry_of')
                if retry_of is not None:
                    _require(isinstance(retry_of, str) and retry_of and retry_of != request_key)
                    original = db.execute('SELECT request_json,status FROM backtest_jobs WHERE request_key=?',
                                          (retry_of,)).fetchone()
                    _require(original is not None and original[1] in ('failed', 'cancelled'))
                    original_request = json.loads(original[0])
                    retry_request = dict(request)
                    retry_request.pop('retry_of', None)
                    original_request.pop('retry_of', None)
                    _require(retry_request == original_request)
            check_rows('backtest_jobs',
                       'SELECT request_key,request_json,status,created_at,finished_at,error FROM backtest_jobs',
                       check_backtest_job)

            def check_task_cancellation(row):
                request_key, operation, request_raw, result_raw, created_at = row
                request, result = json.loads(request_raw), json.loads(result_raw)
                from dlt.task_cancellations import OPERATIONS, canonical
                _require(operation in OPERATIONS and isinstance(request, dict))
                _require(request_raw == canonical(request) and result_raw == canonical(result))
                _require(isinstance(request.get('target_request_key'), str)
                         and 0 < len(request['target_request_key']) <= 128)
                _require(result.get('request_key') == request_key and result.get('operation') == operation
                         and result.get('request') == request and result.get('status') in ('accepted', 'rejected'))
                _require((result.get('status') == 'accepted' and result.get('target_status') in
                          ('complete', 'failed', 'cancelled') and result.get('reason') is None)
                         or (result.get('status') == 'rejected' and result.get('target_status') is None
                             and result.get('reason') == 'target_not_found'))
                if result['status'] == 'accepted':
                    job_table = 'training_jobs' if operation == 'training_cancel' else 'backtest_jobs'
                    target = db.execute(f'SELECT status FROM {job_table} WHERE request_key=?',
                                        (request['target_request_key'],)).fetchone()
                    _require(target and target[0] == result['target_status'])
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
            check_rows('task_cancellations',
                       'SELECT request_key,operation,request_json,result_json,created_at FROM task_cancellations',
                       check_task_cancellation)

            def check_pair(row):
                digest, raw, request_raw = row
                check_embedded((digest, raw), 'pair_sha256')
                obj, request = json.loads(raw), json.loads(request_raw)
                _require(obj['model_artifact_id'] == request['artifact_id'])
                _require(obj['config'] == request['config'])
                from dlt.pair_validation import validate_pair_structure, validate_pair_history, validate_random_control
                validate_pair_structure(obj, request['config'])
                history = validate_pair_history(db, obj, request)
                validate_random_control(obj, history)
                if obj['model_artifact_id'] and 'model_selection' in request:
                    from dlt.pair_repository import _validate_model_selection
                    _validate_model_selection(db, request['model_selection'], obj['model_artifact_id'],
                                              request['config']['method'])
                _require(db.execute('SELECT 1 FROM snapshots WHERE id=?', (obj['config']['snapshot_id'],)).fetchone())
                if obj['model_artifact_id']:
                    from dlt.model_repository import ModelRepository
                    from dlt.model_pair_validation import validate_model_candidate
                    artifact = ModelRepository.get_in_transaction(db, obj['model_artifact_id'])
                    validate_model_candidate(db, obj, request, artifact, check_conflicts=False)
            check_rows('draft_pairs', 'SELECT pair_sha256,pair_json,request_json FROM draft_pairs', check_pair)

            def check_pair_write_request(row):
                key, digest, request_raw, result_raw, created_at = row
                request, result = json.loads(request_raw), json.loads(result_raw)
                _require(isinstance(request_raw, bytes) and _sha(request_raw) == digest
                         and _canonical(request) == request_raw and _canonical(result) == result_raw)
                _require(result.get('request_key') == key and result.get('request') == request)
                pair_row = db.execute('SELECT request_json,pair_sha256,pair_json FROM draft_pairs WHERE request_key=?',
                                      (key,)).fetchone()
                _require(pair_row and pair_row[0] == request_raw and result.get('pair') == json.loads(pair_row[2]))
                pair = json.loads(pair_row[2])
                pair_digest = pair.pop('pair_sha256', None)
                _require(pair_digest == pair_row[1] and _sha(_canonical(pair)) == pair_digest)
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)

            check_rows('pair_write_requests',
                       'SELECT request_key,request_sha256,request_json,result_json,created_at FROM pair_write_requests',
                       check_pair_write_request)

            def check_pair_write_fence(row):
                key, digest, request_raw, created_at = row
                _require(isinstance(request_raw, bytes) and _sha(request_raw) == digest
                         and _canonical(json.loads(request_raw)) == request_raw)
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                _require(not db.execute('SELECT 1 FROM pair_write_requests WHERE request_key=?', (key,)).fetchone())
                _require(not db.execute('SELECT 1 FROM draft_pairs WHERE request_key=?', (key,)).fetchone())

            check_rows('pair_write_fences',
                       'SELECT request_key,request_sha256,request_json,created_at FROM pair_write_fences',
                       check_pair_write_fence)

            def check_pair_write_retry(row):
                key, retry_of, digest, request_raw, created_at = row
                original = db.execute('SELECT request_sha256,request_json,created_at FROM pair_write_fences WHERE request_key=?',
                                      (retry_of,)).fetchone()
                _require(original is not None and original[:2] == (digest, request_raw))
                _require(not db.execute('SELECT 1 FROM pair_write_retries WHERE retry_of=?', (retry_of,)).fetchone()
                         and not db.execute('SELECT 1 FROM pair_write_retries WHERE request_key=?', (retry_of,)).fetchone())
                stamp, fence_stamp = datetime.fromisoformat(created_at), datetime.fromisoformat(original[2])
                _require(stamp.tzinfo is not None and fence_stamp.tzinfo is not None and stamp >= fence_stamp)
                accepted = db.execute('SELECT request_sha256,request_json FROM pair_write_requests WHERE request_key=?',
                                      (key,)).fetchone()
                fence = db.execute('SELECT request_sha256,request_json FROM pair_write_fences WHERE request_key=?',
                                   (key,)).fetchone()
                _require(accepted == (digest, request_raw) or fence == (digest, request_raw))

            check_rows('pair_write_retries',
                       'SELECT request_key,retry_of,request_sha256,request_json,created_at FROM pair_write_retries',
                       check_pair_write_retry)

            def check_bytes(row):
                digest, raw = row
                _require(_sha(raw) == digest)
                json.loads(raw)

            def check_series_write_request(row):
                key, digest, request_raw, result_raw, created_at = row
                _require(re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', key, re.I))
                _require(isinstance(request_raw, bytes) and _sha(request_raw) == digest
                         and _canonical(json.loads(request_raw)) == request_raw)
                request, result = json.loads(request_raw), json.loads(result_raw)
                _require(_canonical(result) == result_raw and result.get('manifest') == request)
                content = db.execute('SELECT content_sha256,content_json FROM experiment_series WHERE series_key=?',
                                     (key,)).fetchone()
                _require(content == (_sha(result_raw), result_raw))
                created = datetime.fromisoformat(created_at)
                _require(created.tzinfo is not None and created.utcoffset() is not None)

            check_rows('experiment_series_write_requests',
                       'SELECT request_key,request_sha256,request_json,result_json,created_at '
                       'FROM experiment_series_write_requests', check_series_write_request)

            def check_series_write_fence(row):
                key, digest, request_raw, created_at = row
                _require(re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', key, re.I))
                _require(isinstance(request_raw, bytes) and _sha(request_raw) == digest
                         and _canonical(json.loads(request_raw)) == request_raw)
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                _require(not db.execute('SELECT 1 FROM experiment_series_write_requests WHERE request_key=?',
                                        (key,)).fetchone())
                _require(not db.execute('SELECT 1 FROM experiment_series WHERE series_key=?', (key,)).fetchone())

            check_rows('experiment_series_write_fences',
                       'SELECT request_key,request_sha256,request_json,created_at '
                       'FROM experiment_series_write_fences', check_series_write_fence)

            def check_series_write_retry(row):
                key, retry_of, digest, request_raw, created_at = row
                original = db.execute('SELECT request_sha256,request_json,created_at FROM experiment_series_write_fences '
                                      'WHERE request_key=?', (retry_of,)).fetchone()
                _require(original is not None and original[:2] == (digest, request_raw))
                _require(not db.execute('SELECT 1 FROM experiment_series_write_retries WHERE retry_of=?',
                                        (retry_of,)).fetchone())
                _require(not db.execute('SELECT 1 FROM experiment_series_write_retries WHERE request_key=?',
                                        (retry_of,)).fetchone())
                _require(isinstance(request_raw, bytes) and _sha(request_raw) == digest
                         and _canonical(json.loads(request_raw)) == request_raw)
                retry_time, fence_time = datetime.fromisoformat(created_at), datetime.fromisoformat(original[2])
                _require(retry_time.tzinfo is not None and retry_time.utcoffset() is not None
                         and fence_time.tzinfo is not None and fence_time.utcoffset() is not None
                         and retry_time >= fence_time)
                accepted = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_requests '
                                     'WHERE request_key=?', (key,)).fetchone()
                fence = db.execute('SELECT request_sha256,request_json FROM experiment_series_write_fences '
                                   'WHERE request_key=?', (key,)).fetchone()
                _require(accepted == (digest, request_raw) or fence == (digest, request_raw))

            check_rows('experiment_series_write_retries',
                       'SELECT request_key,retry_of,request_sha256,request_json,created_at '
                       'FROM experiment_series_write_retries', check_series_write_retry)

            def check_draft_evaluation(row):
                from decimal import Decimal
                from fractions import Fraction
                from math import comb
                from dlt.candidates import _canonical as canonical_request
                from dlt.custom_scenarios import check_custom_scenario
                from dlt.evaluation_repository import EvaluationRepository
                from dlt.pair_repository import PairRepository
                pair_key, request_raw, digest, raw = row
                _require(_sha(raw) == digest)
                request, evaluation = json.loads(request_raw), json.loads(raw)
                _require(request['pair_key'] == pair_key)
                scenario = request['scenario']
                _require(evaluation['scenario_sha256'] == _sha(canonical_request(scenario)))
                pair_row = db.execute('SELECT pair_sha256,pair_json FROM draft_pairs WHERE request_key=?', (pair_key,)).fetchone()
                _require(pair_row is not None)
                pair = PairRepository._decode(pair_row)
                check_custom_scenario(pair, scenario)
                if scenario.get('source_issue') == 'custom':
                    _require(scenario.get('kind') == 'custom_payout_v1')
                dependency = scenario.get('source_dependency')
                if dependency is not None:
                    _require(isinstance(dependency, dict) and dependency['issue'] == scenario['source_issue'])
                    source = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (dependency['snapshot_id'],)).fetchone()
                    draw = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                                      (dependency['snapshot_id'], dependency['issue'])).fetchone()
                    _require(source and _sha(source[0]) == dependency['snapshot_id']
                             and draw and _sha(draw[0]) == dependency['row_sha256'])
                expected = {'source_issue': scenario['source_issue'], 'reference': scenario['reference'],
                            'adjustment_assumption': scenario['adjustment_assumption'],
                            'rule_version': scenario['rule']['version'], 'target_issue': pair['config']['target_issue'],
                            'promotion': scenario['promotion']}
                _require(evaluation['scenario'] == expected and evaluation['pair_sha256'] == pair['pair_sha256'])
                _require(evaluation['sampling']['samples'] == request['samples']
                         and evaluation['sampling']['seed'] == request['seed'])
                EvaluationRepository._check_result_shape(evaluation)
                universe = comb(35, 5) * comb(12, 2)
                for label in ('candidate', 'control'):
                    saved = evaluation['portfolios'][label]
                    tickets = pair[label]['tickets']
                    _require(Decimal(saved['cost_yuan']) == Decimal(pair[label]['cost_fen']).scaleb(-2))
                    unique = len({(tuple(sorted(t['front'])), tuple(sorted(t['back']))) for t in tickets})
                    coverage = saved['jackpot_coverage']
                    _require(saved['ticket_rows'] == len(tickets) and coverage['unique_combinations'] == unique
                             and coverage['outcomes'] == universe
                             and abs(float(coverage['probability']) - unique / universe) < 1e-12)
                    multiples = {extra: sum(t['multiplier'] for t in tickets if t['additional'] is extra)
                                 for extra in (False, True)}
                    expected_mean = Fraction(0)
                    for front_hits in range(6):
                        for back_hits in range(3):
                            group = scenario['rule']['hit_groups'][f'{front_hits},{back_hits}']
                            if group is None:
                                continue
                            payout = Fraction(Decimal(str(scenario['payouts'][group]).replace(',', '')))
                            extra_group = scenario['rule']['additional_groups'].get(group)
                            extra = (Fraction(Decimal(str(scenario['payouts'][extra_group]).replace(',', '')))
                                     if multiples[True] and extra_group is not None else Fraction(0))
                            count = comb(5, front_hits) * comb(30, 5-front_hits) * comb(2, back_hits) * comb(10, 2-back_hits)
                            expected_mean += (payout * (multiples[False] + multiples[True]) + extra * multiples[True]) * count / universe
                    fraction = saved['mean_gross_tax_before']['fraction']
                    _require(Fraction(fraction[0], fraction[1]) == expected_mean)
            check_rows('draft_evaluations', 'SELECT pair_key,request_json,result_sha256,result_json FROM draft_evaluations', check_draft_evaluation)

            def check_evaluation_write_request(row):
                key, digest, write_raw, result_raw, created_at = row
                write_request, result = json.loads(write_raw), json.loads(result_raw)
                _require(isinstance(write_raw, bytes) and _sha(write_raw) == digest
                         and _canonical(write_request) == write_raw and _canonical(result) == result_raw)
                evaluation = db.execute('SELECT request_json,result_sha256,result_json FROM draft_evaluations WHERE request_key=?',
                                        (key,)).fetchone()
                _require(evaluation and result.get('request') == json.loads(evaluation[0])
                         and result.get('result') == json.loads(evaluation[2])
                         and _sha(evaluation[2]) == evaluation[1])
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)

            check_rows('evaluation_write_requests',
                       'SELECT request_key,request_sha256,request_json,result_json,created_at FROM evaluation_write_requests',
                       check_evaluation_write_request)

            def check_evaluation_write_fence(row):
                key, digest, request_raw, created_at = row
                _require(isinstance(request_raw, bytes) and _sha(request_raw) == digest
                         and _canonical(json.loads(request_raw)) == request_raw)
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                _require(not db.execute('SELECT 1 FROM evaluation_write_requests WHERE request_key=?', (key,)).fetchone())
                _require(not db.execute('SELECT 1 FROM draft_evaluations WHERE request_key=?', (key,)).fetchone())

            check_rows('evaluation_write_fences',
                       'SELECT request_key,request_sha256,request_json,created_at FROM evaluation_write_fences',
                       check_evaluation_write_fence)

            def check_evaluation_write_retry(row):
                key, retry_of, digest, request_raw, created_at = row
                original = db.execute('SELECT request_sha256,request_json,created_at FROM evaluation_write_fences '
                                      'WHERE request_key=?', (retry_of,)).fetchone()
                _require(original is not None and original[:2] == (digest, request_raw))
                _require(not db.execute('SELECT 1 FROM evaluation_write_retries WHERE retry_of=?', (retry_of,)).fetchone()
                         and not db.execute('SELECT 1 FROM evaluation_write_retries WHERE request_key=?', (retry_of,)).fetchone())
                stamp, fence_stamp = datetime.fromisoformat(created_at), datetime.fromisoformat(original[2])
                _require(stamp.tzinfo is not None and fence_stamp.tzinfo is not None and stamp >= fence_stamp)
                accepted = db.execute('SELECT request_sha256,request_json FROM evaluation_write_requests WHERE request_key=?',
                                      (key,)).fetchone()
                fence = db.execute('SELECT request_sha256,request_json FROM evaluation_write_fences WHERE request_key=?',
                                   (key,)).fetchone()
                _require(accepted == (digest, request_raw) or fence == (digest, request_raw))

            check_rows('evaluation_write_retries',
                       'SELECT request_key,retry_of,request_sha256,request_json,created_at FROM evaluation_write_retries',
                       check_evaluation_write_retry)
            def check_custom_origin(row):
                from dlt.custom_origin import _scenario, declarations
                event_id, evaluation_key = row
                records = declarations(db, evaluation_key, _scenario(db, evaluation_key))
                _require(any(record['event_id'] == event_id for record in records))
            check_rows('custom_origin_declarations', 'SELECT event_id,evaluation_key FROM custom_origin_declarations', check_custom_origin)
            def check_custom_origin_fence(row):
                from dlt.custom_origin import _canonical as origin_canonical, _request, _digest
                event_id, digest, raw, created_at = row
                request = json.loads(raw)
                _require(origin_canonical(request) == raw and hashlib.sha256(raw).hexdigest() == digest)
                normalized = _request(request.get('evaluation_key'), request.get('kind'), request.get('issue'),
                                      request.get('reason'), request.get('confirmed'))
                _require(normalized == request and _digest(request) == digest)
                _require(not db.execute('SELECT 1 FROM custom_origin_declarations WHERE event_id=?', (event_id,)).fetchone())
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)
            check_rows('custom_origin_write_fences',
                       'SELECT event_id,request_sha256,request_json,created_at FROM custom_origin_write_fences',
                       check_custom_origin_fence)
            def check_custom_origin_retry(row):
                from dlt.custom_origin import _canonical as origin_canonical, _request, _digest, _stored_declaration
                event_id, retry_of, digest, raw, created_at = row
                request = json.loads(raw)
                original = db.execute('SELECT request_sha256,request_json FROM custom_origin_write_fences WHERE event_id=?',
                                      (retry_of,)).fetchone()
                _require(original == (digest, raw) and origin_canonical(request) == raw)
                normalized = _request(request.get('evaluation_key'), request.get('kind'), request.get('issue'),
                                      request.get('reason'), request.get('confirmed'))
                _require(normalized == request and _digest(request) == digest)
                saved = _stored_declaration(db, event_id)
                fence = db.execute('SELECT request_sha256,request_json FROM custom_origin_write_fences WHERE event_id=?',
                                   (event_id,)).fetchone()
                _require((saved is not None and saved[0] == request) or fence == (digest, raw))
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)
            check_rows('custom_origin_write_retries',
                       'SELECT event_id,retry_of,request_sha256,request_json,created_at FROM custom_origin_write_retries',
                       check_custom_origin_retry)
            def check_source_write_retry_link(request_key, operation, object_key, digest, raw, retry_of):
                from dlt.source_write_protocol import _request_key
                _request_key(request_key)
                retry = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                                   'FROM source_write_retries WHERE request_key=?', (request_key,)).fetchone()
                if retry_of is None:
                    _require(retry is None)
                else:
                    _require(retry == (retry_of, operation, object_key, digest, raw))

            def check_source_write_request(row):
                from dlt.candidates import _canonical as source_canonical
                from dlt.source_write_protocol import object_key as source_object_key
                key, operation, object_id, digest, raw, result_raw, created_at, retry_of = row
                request = json.loads(raw); result_value = json.loads(result_raw)
                _require(operation in ('source_review', 'source_correction', 'recovery_prepare', 'recovery_activate'))
                _require(source_canonical(request) == raw and hashlib.sha256(raw).hexdigest() == digest)
                _require(source_object_key(operation, request) == object_id and isinstance(result_value, dict))
                check_source_write_retry_link(key, operation, object_id, digest, raw, retry_of)
                if operation == 'recovery_prepare':
                    _require(set(request) == {'source', 'destination', 'acknowledge'}
                             and request.get('acknowledge') is True
                             and result_value.get('status') == 'prepared'
                             and result_value.get('check', {}).get('status') == 'passed')
                elif operation == 'recovery_activate':
                    _require(set(request) == {'candidate', 'destination', 'acknowledge', 'enable_restored_plans'}
                             and request.get('acknowledge') is True
                             and request.get('enable_restored_plans') is True
                             and result_value.get('status') == 'restored_copy_ready'
                             and result_value.get('check', {}).get('status') == 'passed')
                elif operation == 'source_review':
                    reviewed_id = result_value.get('review_id')
                    saved = db.execute('SELECT record_json,document FROM reviewed_sources WHERE id=?',
                                       (reviewed_id,)).fetchone()
                    _require(saved is not None)
                    record = json.loads(saved[0])
                    _require(record.get('kind') == request.get('kind')
                             and record.get('source_url') == request.get('source_url')
                             and record.get('document_sha256') == request.get('document_sha256')
                             and len(saved[1]) == request.get('document_size'))
                else:
                    correction_id = result_value.get('correction_id')
                    scope_issue = request.get('issue')
                    table = 'source_corrections' if scope_issue is None else 'scoped_source_corrections'
                    fields = ('old_review_id,new_review_id,kind,note' if scope_issue is None
                              else 'issue,old_review_id,new_review_id,kind,note')
                    saved = db.execute(f'SELECT {fields} FROM {table} WHERE seq=?', (correction_id,)).fetchone()
                    if scope_issue is None:
                        wanted = (request['old_review_id'], request['new_review_id'],
                                  json.loads(db.execute('SELECT record_json FROM reviewed_sources WHERE id=?',
                                                        (request['old_review_id'],)).fetchone()[0])['kind'],
                                  request['note'])
                    else:
                        kind = json.loads(db.execute('SELECT record_json FROM reviewed_sources WHERE id=?',
                                                      (request['old_review_id'],)).fetchone()[0])['kind']
                        wanted = (scope_issue, request['old_review_id'], request['new_review_id'], kind,
                                  request['note'])
                    _require(saved == wanted)
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)

            check_rows('source_write_requests',
                       'SELECT request_key,operation,object_key,request_sha256,request_json,result_json,created_at,retry_of '
                       'FROM source_write_requests', check_source_write_request)

            def check_source_write_fence(row):
                from dlt.candidates import _canonical as source_canonical
                from dlt.source_write_protocol import _request_key, object_key as source_object_key
                key, operation, object_id, digest, raw, created_at = row
                _request_key(key); request = json.loads(raw)
                _require(operation in ('source_review', 'source_correction', 'recovery_prepare', 'recovery_activate'))
                _require(source_canonical(request) == raw and hashlib.sha256(raw).hexdigest() == digest)
                _require(source_object_key(operation, request) == object_id)
                _require(not db.execute('SELECT 1 FROM source_write_requests WHERE request_key=?', (key,)).fetchone())
                retry = db.execute('SELECT retry_of,operation,object_key,request_sha256,request_json '
                                   'FROM source_write_retries WHERE request_key=?', (key,)).fetchone()
                if retry:
                    original = db.execute('SELECT operation,object_key,request_sha256,request_json '
                                          'FROM source_write_fences WHERE request_key=?', (retry[0],)).fetchone()
                    _require(retry[1:] == (operation, object_id, digest, raw))
                    if operation in ('recovery_prepare', 'recovery_activate'):
                        old_request, new_request = json.loads(original[3]), request
                        old_request.pop('destination', None); new_request.pop('destination', None)
                        _require(original[0] == operation
                                 and old_request == new_request and original[1] != object_id)
                    else:
                        _require(original == (operation, object_id, digest, raw))
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)

            check_rows('source_write_fences',
                       'SELECT request_key,operation,object_key,request_sha256,request_json,created_at '
                       'FROM source_write_fences', check_source_write_fence)

            def check_source_write_retry(row):
                from dlt.candidates import _canonical as source_canonical
                from dlt.source_write_protocol import _request_key, object_key as source_object_key
                key, retry_of, operation, object_id, digest, raw, created_at = row
                _request_key(key); request = json.loads(raw)
                original = db.execute('SELECT operation,object_key,request_sha256,request_json '
                                      'FROM source_write_fences WHERE request_key=?', (retry_of,)).fetchone()
                if operation in ('recovery_prepare', 'recovery_activate'):
                    original_request = json.loads(original[3]) if original else {}
                    new_request = json.loads(raw)
                    original_request.pop('destination', None); new_request.pop('destination', None)
                    same_original = bool(original and original[0] == operation
                                         and original_request == new_request and original[1] != object_id)
                else:
                    same_original = original == (operation, object_id, digest, raw)
                _require(same_original
                         and source_canonical(request) == raw and source_object_key(operation, request) == object_id
                         and hashlib.sha256(raw).hexdigest() == digest)
                accepted = db.execute('SELECT 1 FROM source_write_requests WHERE request_key=?', (key,)).fetchone()
                fenced = db.execute('SELECT 1 FROM source_write_fences WHERE request_key=?', (key,)).fetchone()
                _require(bool(accepted) ^ bool(fenced))
                when = datetime.fromisoformat(created_at)
                _require(when.tzinfo is not None and when.utcoffset() is not None)

            check_rows('source_write_retries',
                       'SELECT request_key,retry_of,operation,object_key,request_sha256,request_json,created_at '
                       'FROM source_write_retries', check_source_write_retry)
            def check_backtest_pair_contract(row):
                request_key, request_raw, digest, report_raw = row
                check_bytes((digest, report_raw))
                _require(isinstance(request_raw, bytes))
                request = json.loads(request_raw)
                report = json.loads(report_raw)
                _require(request_raw == _canonical(request) and isinstance(request, dict)
                         and set(request) == {'protocol', 'scenarios'}
                         and report.get('protocol') == request.get('protocol')
                         and report.get('scenarios') == request.get('scenarios'))
                job = (db.execute('SELECT request_json,status FROM backtest_jobs WHERE request_key=?',
                                  (request_key,)).fetchone()
                       if 'backtest_jobs' in tables else None)
                if job:
                    job_request = json.loads(job[0])
                    _require(job[1] == 'complete' and job_request.get('protocol') == report['protocol'])
                from dlt.pair_validation import validate_pair_structure, validate_pair_history, validate_random_control
                from dlt.backtest import _seed
                protocol = report['protocol']
                issues = [r[0] for r in db.execute('SELECT issue FROM draw_versions WHERE snapshot_id=? ORDER BY draw_date,issue',
                                                  (protocol['snapshot_id'],))]
                for period in report['periods']:
                    target = period['target']
                    for replica in period['replicas']:
                        if replica['status'] != 'complete':
                            continue
                        expected = {**protocol['candidate'], 'snapshot_id': protocol['snapshot_id'],
                                    'seed': _seed(protocol['seed'], target['issue'], replica['replica']),
                                    'target_issue': target['issue'], 'target_date': target['date']}
                        validate_pair_structure(replica['pair'], expected)
                        index = issues.index(target['issue'])
                        _require(index >= protocol['history_periods'])
                        history = validate_pair_history(db, replica['pair'], {'config': expected,
                            'artifact_id': replica['pair']['model_artifact_id'],
                            'history_first_issue': issues[index-protocol['history_periods']],
                            'history_cutoff_issue': issues[index-1]})
                        validate_random_control(replica['pair'], history)
                        if expected['method'] in ('tree', 'lstm'):
                            from dlt.model_pair_validation import validate_model_candidate
                            request = {'config': expected, 'artifact_id': replica['pair']['model_artifact_id'],
                                       'history_first_issue': issues[index-protocol['history_periods']],
                                       'history_cutoff_issue': issues[index-1]}
                            validate_model_candidate(db, replica['pair'], request, replica['artifact'], check_conflicts=False)
            check_rows('backtest_reports',
                       'SELECT request_key,request_json,report_sha256,report_json FROM backtest_reports',
                       check_backtest_pair_contract)

            def check_review(row):
                ident, raw, document = row
                _require(_sha(raw) == ident)
                obj = json.loads(raw)
                _require(_sha(document) == obj['document_sha256'])
            check_rows('reviewed_sources', 'SELECT id,record_json,document FROM reviewed_sources', check_review)

            def check_experiment(row):
                from dlt.fixed_contract import validate_fixed_record
                validate_fixed_record(db, *row)
            check_rows('fixed_experiments', 'SELECT request_json,content_sha256,content_json FROM fixed_experiments', check_experiment)

            def check_fixed_write_intent(row):
                request_key, digest, series_key, retry_of, created_at = row
                _require(isinstance(request_key, str) and request_key.strip() and len(request_key) <= 128)
                _require(isinstance(digest, str) and re.fullmatch(r'[0-9a-f]{64}', digest))
                _require(series_key is None or isinstance(series_key, str) and series_key.strip() and len(series_key) <= 128)
                _require(retry_of is None or isinstance(retry_of, str) and retry_of.strip()
                         and retry_of != request_key and len(retry_of) <= 128)
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                if retry_of is not None:
                    original = db.execute('SELECT series_key,request_sha256 FROM fixed_experiment_write_intents WHERE request_key=?',
                                          (retry_of,)).fetchone()
                    fenced = db.execute('SELECT request_sha256 FROM fixed_experiment_write_fences WHERE request_key=?',
                                        (retry_of,)).fetchone()
                    _require(original and fenced and original[0] == series_key and original[1] == fenced[0])
            check_rows('fixed_experiment_write_intents',
                       'SELECT request_key,request_sha256,series_key,retry_of,created_at FROM fixed_experiment_write_intents',
                       check_fixed_write_intent)

            def check_fixed_write_fence(row):
                request_key, digest, fenced_at = row
                intent = db.execute('SELECT request_sha256 FROM fixed_experiment_write_intents WHERE request_key=?',
                                    (request_key,)).fetchone()
                _require(intent and intent[0] == digest and re.fullmatch(r'[0-9a-f]{64}', digest))
                _require(not db.execute('SELECT 1 FROM fixed_experiments WHERE request_key=?', (request_key,)).fetchone())
                stamp = datetime.fromisoformat(fenced_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
            check_rows('fixed_experiment_write_fences',
                       'SELECT request_key,request_sha256,fenced_at FROM fixed_experiment_write_fences',
                       check_fixed_write_fence)
            check_rows('experiment_series', 'SELECT content_sha256,content_json FROM experiment_series', check_bytes)

            def check_rolling_plan(row):
                ident, series_key, issue, digest, raw, status, attempt, resolved_raw, job_key = row
                request = json.loads(raw)
                _require(_sha(_canonical(request)) == digest)
                _require(ident == _sha(_canonical({'version': 'rolling-training-plan-v1', 'series_key': series_key, 'issue': issue})))
                _require(request['series_key'] == series_key and request['issue'] == issue)
                series = db.execute('SELECT content_json FROM experiment_series WHERE series_key=?', (series_key,)).fetchone()
                _require(series)
                content = json.loads(series[0]); spec = content['rolling_spec']
                _require(content['manifest']['retraining_policy'] == 'rolling_retrain_v1'
                         and issue in content['manifest']['observation_issues'])
                from dlt.prospective import ProspectiveRepository, _time
                calendar = ProspectiveRepository._get_review(db, request['calendar_review_id'], 'calendar')['extracted']
                _require(calendar['issue'] == issue and 1 <= request['timeout'] <= 120)
                _time(request['start_at']); _time(calendar['cutoff_at'])
                _require(type(attempt) is int and 0 <= attempt <= 9)
                allowed = {'waiting': {'waiting', 'dispatching', 'blocked', 'failed', 'cancelled'},
                           'dispatching': {'running', 'blocked', 'failed', 'cancelled'},
                           'running': {'complete', 'blocked', 'failed', 'cancelled'},
                           'blocked': {'waiting', 'cancelled'}, 'failed': {'waiting', 'cancelled'},
                           'complete': set(), 'cancelled': set()}
                events = db.execute('SELECT status,attempt,job_key FROM rolling_training_events '
                                    'WHERE plan_id=? ORDER BY seq', (ident,)).fetchall()
                _require(events and events[0] == ('waiting', 0, None))
                previous = events[0]
                for event in events[1:]:
                    state, number, event_job = event
                    _require(state in allowed.get(previous[0], set()))
                    retried = previous[0] in ('blocked', 'failed') and state == 'waiting'
                    _require(number == previous[1] + int(retried))
                    _require(event_job is None if retried else not previous[2] or event_job == previous[2])
                    _require(state != 'waiting' or event_job is None)
                    _require(state not in ('dispatching', 'running', 'complete') or event_job is not None)
                    _require(previous[2] is not None or event_job is None or state == 'dispatching')
                    if event_job is not None:
                        _require(event_job == f'rolling-{ident}-{number}')
                    if state in ('running', 'complete'):
                        historical = db.execute('SELECT request_json FROM training_jobs WHERE request_key=?', (event_job,)).fetchone()
                        _require(historical)
                        historical_request = json.loads(historical[0])
                        historical_config = historical_request['config']
                        _require(historical_config['rolling_issue'] == issue
                                 and historical_config['cutoff'] == request['expected_cutoff_issue']
                                 and historical_request['timeout'] == request['timeout'])
                        _require({k: v for k, v in historical_config.items()
                                  if k not in ('snapshot_id', 'train_end', 'cutoff', 'rolling_issue')}
                                 == spec['config_profile'])
                        _require(db.execute('SELECT 1 FROM snapshots WHERE id=?', (historical_config['snapshot_id'],)).fetchone())
                    previous = event
                _require(previous == (status, attempt, job_key))
                _require((resolved_raw is None) == (job_key is None))
                _require(status not in ('dispatching', 'running', 'complete') or job_key is not None)
                if job_key is None:
                    return
                resolved = json.loads(resolved_raw); config = resolved['config']
                _require(config['rolling_issue'] == issue and config['cutoff'] == request['expected_cutoff_issue'])
                _require({k: v for k, v in config.items() if k not in ('snapshot_id', 'train_end', 'cutoff', 'rolling_issue')}
                         == spec['config_profile'])
                _require(_time(resolved['deadline']) == _time(calendar['cutoff_at']))
                snapshot = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (config['snapshot_id'],)).fetchone()
                _require(snapshot)
                _, parsed = parse_snapshot(snapshot[0])
                parsed.sort(key=lambda item: (item['draw_date'], item['issue']))
                issues = [item['issue'] for item in parsed]
                first, last = issues.index(resolved['first_issue']), issues.index(config['cutoff'])
                _require(first <= last and last-first+1 == spec['training_periods'])
                chosen = parsed[first:last+1]
                rows = [{'issue': item['issue'], 'date': item['draw_date'], 'front': item['front'], 'back': item['back']} for item in chosen]
                _require(_sha(_canonical(rows)) == resolved['history_sha256'])
                _require(config['train_end'] == issues[last-spec['validation_periods']])
                job = db.execute('SELECT request_json,status,artifact_id FROM training_jobs WHERE request_key=?', (job_key,)).fetchone()
                _require(job is not None or status in ('dispatching', 'failed', 'blocked', 'cancelled'))
                if job:
                    task = json.loads(job[0])
                    _require(task['config'] == config and task['first_issue'] == resolved['first_issue']
                             and task['timeout'] == request['timeout'])
                    if job[1] == 'complete':
                        artifact = db.execute('SELECT artifact_json FROM model_artifacts WHERE artifact_id=?', (job[2],)).fetchone()
                        _require(artifact)
                        model = json.loads(artifact[0])
                        _require(model['config'] == config and model['history_first_issue'] == resolved['first_issue']
                                 and model['training_rows_sha256'] == resolved['history_sha256'])
                    if status == 'complete':
                        _require(job[1] == 'complete' and job[2] is not None)
            check_rows('rolling_training_plans', 'SELECT id,series_key,issue,request_sha256,request_json,status,attempt,resolved_json,job_key FROM rolling_training_plans', check_rolling_plan)

            def check_plan_event(row):
                from dlt.scheduler import _parse
                table, seq, plan_id, status, message, at = row
                _require(type(seq) is int and seq > 0 and isinstance(plan_id, str) and bool(plan_id))
                plan_table = {'rolling_training_events': 'rolling_training_plans',
                              'rolling_registration_events': 'rolling_registration_plans',
                              'static_registration_events': 'static_registration_plans'}[table]
                _require(db.execute(f'SELECT 1 FROM {plan_table} WHERE id=?', (plan_id,)).fetchone())
                _require(status in ('waiting', 'dispatching', 'running', 'complete', 'blocked', 'failed', 'cancelled'))
                _require(isinstance(message, str) and len(message) <= 2000)
                _parse(at)
                previous = db.execute(f'SELECT seq FROM {table} WHERE plan_id=? AND seq<? ORDER BY seq DESC LIMIT 1',
                                      (plan_id, seq)).fetchone()
                following = db.execute(f'SELECT seq FROM {table} WHERE plan_id=? AND seq>? ORDER BY seq LIMIT 1',
                                        (plan_id, seq)).fetchone()
                _require((previous is None or seq == previous[0] + 1)
                         and (following is None or following[0] == seq + 1))

            for event_table in ('rolling_registration_events', 'static_registration_events'):
                check_rows(event_table, f"SELECT '{event_table}',seq,plan_id,status,message,at FROM {event_table}",
                           check_plan_event)

            def check_rolling_training_event(row):
                table, seq, plan_id, status, attempt, job_key, message, at = row
                check_plan_event((table, seq, plan_id, status, message, at))
                _require(type(attempt) is int and attempt >= 0)
                _require(job_key is None or (isinstance(job_key, str) and bool(job_key)))
                _require(status not in ('running', 'complete') or job_key is not None)
            check_rows('rolling_training_events',
                       "SELECT 'rolling_training_events',seq,plan_id,status,attempt,job_key,message,at FROM rolling_training_events",
                       check_rolling_training_event)

            def check_rolling_training_write(row):
                request_key, operation, request_raw, result_raw, created_at = row
                request = json.loads(request_raw); result = json.loads(result_raw)
                _require(isinstance(request_key, str) and re.fullmatch(
                    r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I))
                _require(operation in ('create', 'cancel', 'retry') and isinstance(request, dict) and isinstance(result, dict))
                plan = result.get('plan')
                _require(isinstance(plan, dict))
                if operation == 'create':
                    _require(plan.get('request') == request and plan.get('id') == hashlib.sha256(_canonical(
                        {'version': 'rolling-training-plan-v1', 'series_key': request.get('series_key'),
                         'issue': request.get('issue')})).hexdigest())
                    _require(_canonical(request) == _canonical(plan.get('request')))
                else:
                    _require(set(request) == {'id'} and request['id'] == plan.get('id'))
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)

            check_rows('rolling_training_write_requests',
                       'SELECT request_key,operation,request_json,result_json,created_at FROM rolling_training_write_requests',
                       check_rolling_training_write)

            def check_rolling_training_fence(row):
                request_key, operation, request_raw, created_at = row
                request = json.loads(request_raw)
                _require(isinstance(request_key, str) and re.fullmatch(
                    r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I))
                _require(operation in ('create', 'cancel', 'retry') and isinstance(request, dict))
                _require(not db.execute('SELECT 1 FROM rolling_training_write_requests WHERE request_key=?',
                                        (request_key,)).fetchone())
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)

            check_rows('rolling_training_write_fences',
                       'SELECT request_key,operation,request_json,created_at FROM rolling_training_write_fences',
                       check_rolling_training_fence)

            def check_rolling_training_retry(row):
                request_key, retry_of, operation, request_raw, created_at = row
                request = json.loads(request_raw)
                original = db.execute('SELECT operation,request_json FROM rolling_training_write_fences WHERE request_key=?',
                                      (retry_of,)).fetchone()
                _require(original and original[0] == operation and original[1] == _canonical(request))
                _require(not db.execute('SELECT 1 FROM rolling_training_write_fences WHERE request_key=?',
                                        (request_key,)).fetchone())
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)

            check_rows('rolling_training_write_retries',
                       'SELECT request_key,retry_of,operation,request_json,created_at FROM rolling_training_write_retries',
                       check_rolling_training_retry)

            def check_registration_plan(row):
                ident, training_id, digest, raw, status = row
                request = json.loads(raw)
                _require(_sha(_canonical(request)) == digest and request['training_plan_id'] == training_id)
                _require(ident == _sha(_canonical({'version': 'rolling-registration-v1', 'request': request})))
                from dlt.prospective import ProspectiveRepository, _time
                for kind in ('rule', 'tax'):
                    ProspectiveRepository._get_review(db, request[kind+'_review_id'], kind)
                _time(request['internal_deadline'])
                training = db.execute('SELECT series_key,issue FROM rolling_training_plans WHERE id=?', (training_id,)).fetchone()
                _require(training)
                events = [r[0] for r in db.execute('SELECT status FROM rolling_registration_events WHERE plan_id=? ORDER BY seq', (ident,))]
                _require(events and events[0] == 'waiting' and events[-1] == status)
                allowed = {'waiting': {'complete', 'blocked', 'failed', 'cancelled'},
                           'blocked': {'waiting', 'cancelled'}, 'failed': {'waiting', 'cancelled'}, 'complete': set(), 'cancelled': set()}
                _require(all(b in allowed.get(a, set()) for a, b in zip(events, events[1:])))
                fixed = db.execute('SELECT content_json FROM fixed_experiments WHERE request_key=?', ('auto-exp-'+ident,)).fetchone()
                _require((status == 'complete') == bool(fixed))
                if fixed:
                    content = json.loads(fixed[0]); protocol = content['protocol']
                    _require(protocol['target_issue'] == training[1]
                             and protocol['rule_review_id'] == request['rule_review_id']
                             and protocol['tax_review_id'] == request['tax_review_id']
                             and protocol['internal_deadline'] == request['internal_deadline'])
                    _require(db.execute('SELECT 1 FROM series_periods WHERE series_key=? AND issue=? AND experiment_key=?',
                                        (*training, 'auto-exp-'+ident)).fetchone())
            check_rows('rolling_registration_plans', 'SELECT id,training_plan_id,request_sha256,request_json,status FROM rolling_registration_plans', check_registration_plan)

            def check_rolling_registration_write(row):
                request_key, operation, request_raw, result_raw, created_at = row
                request, result = json.loads(request_raw), json.loads(result_raw)
                _require(isinstance(request_key, str) and re.fullmatch(
                    r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', request_key, re.I))
                _require(operation in ('registration_create', 'registration_cancel', 'registration_retry') and isinstance(request, dict))
                plan = result.get('plan')
                _require(isinstance(plan, dict))
                if operation == 'registration_create':
                    _require(plan.get('request') == request)
                    _require(plan.get('id') == _sha(_canonical({'version': 'rolling-registration-v1', 'request': request})))
                else:
                    _require(set(request) == {'id'} and request['id'] == plan.get('id'))
                stamp = datetime.fromisoformat(created_at)
                _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)

            check_rows('rolling_registration_write_requests',
                       'SELECT request_key,operation,request_json,result_json,created_at FROM rolling_registration_write_requests',
                       check_rolling_registration_write)

            def check_static_registration(row):
                ident, series_key, issue, digest, raw, status, resolved_raw = row
                request = json.loads(raw)
                _require(_sha(_canonical(request)) == digest and request['series_key'] == series_key and request['issue'] == issue)
                _require(ident == _sha(_canonical({'version': 'static-registration-v1', 'request': request})))
                from dlt.prospective import ProspectiveRepository, _time
                for kind in ('calendar', 'rule', 'tax'):
                    ProspectiveRepository._get_review(db, request[kind+'_review_id'], kind)
                _time(request['internal_deadline']); _time(request['start_at'])
                _require(type(request['history_periods']) is int and 1 <= request['history_periods'] <= 2000)
                series_row = db.execute('SELECT content_json FROM experiment_series WHERE series_key=?', (series_key,)).fetchone()
                _require(series_row)
                series = json.loads(series_row[0])
                _require(series['manifest']['retraining_policy'] == 'fixed_artifact_or_untrained_v1'
                         and issue in series['manifest']['observation_issues'])
                events = [r[0] for r in db.execute('SELECT status FROM static_registration_events WHERE plan_id=? ORDER BY seq', (ident,))]
                allowed = {'waiting': {'complete', 'blocked', 'failed', 'cancelled'},
                           'blocked': {'waiting', 'cancelled'}, 'failed': {'waiting', 'cancelled'}, 'complete': set(), 'cancelled': set()}
                _require(events and events[0] == 'waiting' and events[-1] == status)
                _require(all(b in allowed.get(a, set()) for a, b in zip(events, events[1:])))
                resolved = json.loads(resolved_raw) if resolved_raw else None
                if resolved:
                    snapshot = db.execute('SELECT canonical_json FROM snapshots WHERE id=?', (resolved['snapshot_id'],)).fetchone()
                    _require(snapshot and _sha(snapshot[0]) == resolved['snapshot_id'])
                    _, parsed = parse_snapshot(snapshot[0]); parsed.sort(key=lambda x: (x['draw_date'], x['issue']))
                    issues = [item['issue'] for item in parsed]
                    first, last = issues.index(resolved['first_issue']), issues.index(resolved['cutoff_issue'])
                    _require(first <= last)
                    rows = [{'issue': item['issue'], 'date': item['draw_date'], 'front': item['front'], 'back': item['back']}
                            for item in parsed[first:last+1]]
                    _require(len(rows) == request['history_periods'] and resolved['cutoff_issue'] == request['expected_cutoff_issue']
                             and _sha(_canonical(rows)) == resolved['history_sha256'])
                fixed = db.execute('SELECT content_json FROM fixed_experiments WHERE request_key=?', ('static-exp-'+ident,)).fetchone()
                _require((status == 'complete') == bool(fixed))
                if fixed:
                    content = json.loads(fixed[0]); protocol = content['protocol']
                    _require(resolved and protocol['target_issue'] == issue and protocol['internal_deadline'] == request['internal_deadline'])
                    _require(all(protocol[k+'_review_id'] == request[k+'_review_id'] for k in ('calendar', 'rule', 'tax')))
                    _require(all(item['pair']['config']['snapshot_id'] == resolved['snapshot_id'] for item in content['pairs']))
                    _require(db.execute('SELECT 1 FROM series_periods WHERE series_key=? AND issue=? AND experiment_key=?',
                                        (series_key, issue, 'static-exp-'+ident)).fetchone())
            check_rows('static_registration_plans', 'SELECT id,series_key,issue,request_sha256,request_json,status,resolved_json FROM static_registration_plans', check_static_registration)

            def check_static_write(row):
                key, operation, raw_request, raw_result, created_at = row
                request, result = json.loads(raw_request), json.loads(raw_result)
                _require(isinstance(key, str) and re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',key,re.I))
                _require(operation in ('static_create','static_cancel','static_retry') and isinstance(request,dict))
                plan=result.get('plan');_require(isinstance(plan,dict))
                if operation=='static_create':
                    _require(plan.get('request')==request and plan.get('id')==_sha(_canonical({'version':'static-registration-v1','request':request})))
                else:_require(set(request)=={'id'} and request['id']==plan.get('id'))
                current=db.execute('SELECT request_json FROM static_registration_plans WHERE id=?',(plan.get('id'),)).fetchone()
                _require(current and json.loads(current[0])==plan.get('request'))
                _require(not db.execute('SELECT 1 FROM static_registration_write_fences WHERE request_key=?',(key,)).fetchone())
                stamp=datetime.fromisoformat(created_at);_require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
            check_rows('static_registration_write_requests','SELECT request_key,operation,request_json,result_json,created_at FROM static_registration_write_requests',check_static_write)

            def check_static_fence(row):
                key,operation,raw,created_at=row;request=json.loads(raw)
                _require(re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',key,re.I))
                _require(operation in ('static_create','static_cancel','static_retry') and isinstance(request,dict))
                _require(not db.execute('SELECT 1 FROM static_registration_write_requests WHERE request_key=?',(key,)).fetchone())
                stamp=datetime.fromisoformat(created_at);_require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
            check_rows('static_registration_write_fences','SELECT request_key,operation,request_json,created_at FROM static_registration_write_fences',check_static_fence)

            def check_static_retry(row):
                key,original_key,operation,raw,created_at=row;request=json.loads(raw)
                original=db.execute('SELECT operation,request_json FROM static_registration_write_fences WHERE request_key=?',(original_key,)).fetchone()
                _require(original and original==(operation,_canonical(request)))
                _require(not db.execute('SELECT 1 FROM static_registration_write_fences WHERE request_key=?',(key,)).fetchone())
                stamp=datetime.fromisoformat(created_at);_require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
            check_rows('static_registration_write_retries','SELECT request_key,retry_of,operation,request_json,created_at FROM static_registration_write_retries',check_static_retry)

            def check_closure(row, table):
                object_key, closed_at, reason = row
                target_table, target_column = (('fixed_experiments', 'request_key') if table == 'experiment_closures'
                                               else ('experiment_series', 'series_key'))
                _require(db.execute(f'SELECT 1 FROM {target_table} WHERE {target_column}=?',
                                    (object_key,)).fetchone())
                _require(isinstance(reason, str) and reason.strip() == reason and 0 < len(reason) <= 1000)
                from dlt.scheduler import _parse
                _parse(closed_at)

            check_rows('experiment_closures', 'SELECT experiment_key,closed_at,reason FROM experiment_closures',
                       lambda row: check_closure(row, 'experiment_closures'))
            check_rows('series_closures', 'SELECT series_key,closed_at,reason FROM series_closures',
                       lambda row: check_closure(row, 'series_closures'))

            if 'series_periods' in tables:
                def check_period(row):
                    series_key, issue, experiment_key = row
                    series = db.execute('SELECT content_json FROM experiment_series WHERE series_key=?', (series_key,)).fetchone()
                    experiment = db.execute('SELECT content_json FROM fixed_experiments WHERE request_key=?', (experiment_key,)).fetchone()
                    _require(series and experiment)
                    content, fixed = json.loads(series[0]), json.loads(experiment[0])
                    manifest, protocol = content['manifest'], fixed['protocol']
                    from dlt.prospective_series import _validate_contract
                    _require(issue == protocol['target_issue'] and issue in manifest['observation_issues'])
                    _validate_contract(content, protocol, fixed['pairs'])
                    if manifest['retraining_policy'] == 'rolling_retrain_v1':
                        from dlt.rolling_policy import validate_rolling_pair
                        for item in fixed['pairs']:
                            validate_rolling_pair(db, content['rolling_spec'], item['pair'], item['pair_key'])
                check_rows('series_periods', 'SELECT series_key,issue,experiment_key FROM series_periods', check_period)

            def check_group(row):
                ident, key, issue, row_hash, raw = row
                _require(_sha(raw) == ident)
                obj = json.loads(raw)
                _require((obj['experiment_key'], obj['issue'], obj['row_sha256']) == (key, issue, row_hash))
                fixed = db.execute('SELECT request_json,content_sha256,content_json FROM fixed_experiments WHERE request_key=?', (key,)).fetchone()
                draw = db.execute('SELECT row_json FROM draw_versions WHERE snapshot_id=? AND issue=?',
                                  (obj['source_snapshot_id'], issue)).fetchone()
                _require(fixed and fixed[1] == obj['experiment_sha256'] and draw and _sha(draw[0]) == row_hash)
                from dlt.prospective import ProspectiveRepository
                from dlt.prospective import _time
                from dlt.fixed_contract import validate_fixed_record
                fixed_content = validate_fixed_record(db, *fixed)
                _require(fixed_content['protocol']['target_issue'] == issue)
                calendar_id = obj.get('calendar_review_id') or fixed_content['protocol']['calendar_review_id']
                calendar_review = ProspectiveRepository._get_review(db, calendar_id, 'calendar')
                calendar = calendar_review['extracted']
                registered_at = _time(fixed_content['registered_at'])
                cutoff_at = _time(calendar.get('cutoff_at'))
                observed_at = _time(obj['observed_at'])
                _require(calendar.get('issue') == issue and observed_at >= registered_at
                         and observed_at >= cutoff_at)
                source = obj['source_url']
                from dlt.prospective_settlement import _calculate_results, _matches_calculation, _official_url
                _require(_official_url(source))
                observation = db.execute('SELECT raw_sha256,raw_payload FROM observations '
                                         'WHERE snapshot_id=? AND source=? AND observed_at=?',
                                         (obj['source_snapshot_id'], source, obj['observed_at'])).fetchone()
                _require(observation)
                if observation[1] is None:
                    if 'one or more historical observations lack original bytes' not in limitations:
                        limitations.append('one or more historical observations lack original bytes')
                    return
                _require(isinstance(observation[1], bytes) and _sha(observation[1]) == observation[0])
                observed_payload, observed_rows = parse_snapshot(observation[1])
                _require(_sha(_canonical(observed_payload)) == obj['source_snapshot_id'])
                matching_rows = [item for item in observed_rows if item['issue'] == issue]
                _require(len(matching_rows) == 1 and _canonical(matching_rows[0]['raw_row']) == draw[0]
                         and matching_rows[0]['draw_date'] == calendar.get('draw_date'))
                publication = db.execute('SELECT published_at FROM paper_publications WHERE group_id=? ORDER BY seq LIMIT 1',
                                         (ident,)).fetchone()
                _require(publication and publication[0] and observed_at <= _time(publication[0]))
                rule_review = ProspectiveRepository._get_review(db, obj['rule_review_id'], 'rule')
                tax_review = ProspectiveRepository._get_review(db, obj['tax_review_id'], 'tax')
                _require(issue in rule_review['extracted'].get('applicable_issues', [])
                         and issue in tax_review['extracted'].get('applicable_issues', [])
                         and tax_review['extracted'].get('tax') == rule_review['extracted'].get('tax'))
                from dlt.historical_scenarios import promotion_data
                draw_record = json.loads(draw[0])
                payouts, replicas, mean = _calculate_results(fixed_content, issue, draw_record,
                                                              rule_review['extracted'])
                promotion, promotion_payouts = promotion_data(draw_record, issue)
                _require(_matches_calculation(obj, payouts, replicas, mean,
                                              promotion_payouts if promotion else None))
                for field in ('rule_review_id', 'tax_review_id'):
                    _require(db.execute('SELECT 1 FROM reviewed_sources WHERE id=?', (obj[field],)).fetchone())
                for kind in ('rule', 'tax'):
                    chain = obj.get(kind+'_correction_chain')
                    if chain is not None:
                        fixed_content = db.execute('SELECT content_json FROM fixed_experiments WHERE request_key=?', (key,)).fetchone()
                        _require(fixed_content)
                        original = json.loads(fixed_content[0])['protocol'][kind+'_review_id']
                        _require(isinstance(chain, list) and chain and chain[0] == original
                                 and chain[-1] == obj[kind+'_review_id']
                                 and obj.get('original_'+kind+'_review_id') == original)
                        for before, after in zip(chain, chain[1:]):
                            global_link = db.execute('SELECT 1 FROM source_corrections WHERE old_review_id=? AND new_review_id=? AND kind=?',
                                                     (before, after, kind)).fetchone()
                            issue_link = (db.execute('SELECT 1 FROM scoped_source_corrections WHERE issue=? AND old_review_id=? AND new_review_id=? AND kind=?',
                                                     (issue, before, after, kind)).fetchone() if 'scoped_source_corrections' in tables else None)
                            _require(bool(global_link) != bool(issue_link))
                calendar_id = obj.get('calendar_review_id')
                if calendar_id is not None:
                    fixed_content = db.execute('SELECT content_json FROM fixed_experiments WHERE request_key=?', (key,)).fetchone()
                    _require(fixed_content)
                    original = json.loads(fixed_content[0])['protocol']['calendar_review_id']
                    chain = obj['calendar_correction_chain']
                    _require(obj['original_calendar_review_id'] == original and isinstance(chain, list)
                             and chain and chain[0] == original and chain[-1] == calendar_id)
                    for before, after in zip(chain, chain[1:]):
                        _require(db.execute("SELECT 1 FROM source_corrections WHERE old_review_id=? AND new_review_id=? AND kind='calendar'",
                                            (before, after)).fetchone())
                    _require(db.execute('SELECT 1 FROM reviewed_sources WHERE id=?', (calendar_id,)).fetchone())
            check_rows('paper_groups', 'SELECT id,experiment_key,issue,row_sha256,content_json FROM paper_groups', check_group)

            def check_current(row):
                key, group = row
                _require(db.execute('SELECT 1 FROM paper_groups WHERE id=? AND experiment_key=?', (group, key)).fetchone())
            check_rows('paper_current', 'SELECT experiment_key,group_id FROM paper_current', check_current)

            def check_paper_event(row):
                ident, experiment_key, snapshot_id, status, reason, recorded_at = row
                _require(status in ('complete', 'blocked', 'error'))
                _require(isinstance(reason, str) and len(reason) <= 2000)
                _require(ident == _sha(_canonical({'experiment': experiment_key, 'snapshot': snapshot_id,
                                                   'status': status, 'reason': reason})))
                experiment = db.execute('SELECT content_json FROM fixed_experiments WHERE request_key=?',
                                        (experiment_key,)).fetchone()
                _require(experiment is not None and db.execute('SELECT 1 FROM snapshots WHERE id=?',
                                                                (snapshot_id,)).fetchone())
                protocol = json.loads(experiment[0])['protocol']
                _require(isinstance(protocol.get('target_issue'), str))
                if status == 'complete':
                    group = db.execute('SELECT content_json FROM paper_groups WHERE id=? AND experiment_key=?',
                                       (reason, experiment_key)).fetchone()
                    _require(group is not None and _sha(group[0]) == reason
                             and json.loads(group[0]).get('source_snapshot_id') == snapshot_id)
                from dlt.scheduler import _parse
                _parse(recorded_at)
            check_rows('paper_events',
                       'SELECT id,experiment_key,snapshot_id,status,reason,recorded_at FROM paper_events',
                       check_paper_event)

            if 'paper_publications' in tables:
                latest = {}
                previous_seq = 0
                def check_publication(row):
                    nonlocal previous_seq
                    seq, key, previous, group, snapshot, published_at = row
                    _require(type(seq) is int and seq > previous_seq)
                    previous_seq = seq
                    from dlt.scheduler import _parse
                    _parse(published_at)
                    _require(previous == latest.get(key))
                    found = db.execute('SELECT content_json FROM paper_groups WHERE id=? AND experiment_key=?',
                                       (group, key)).fetchone()
                    _require(found and json.loads(found[0])['source_snapshot_id'] == snapshot)
                    latest[key] = group
                check_rows('paper_publications', 'SELECT seq,experiment_key,previous_group,group_id,snapshot_id,published_at '
                           'FROM paper_publications ORDER BY seq', check_publication)
                for key, group in latest.items():
                    row = db.execute('SELECT group_id FROM paper_current WHERE experiment_key=?', (key,)).fetchone()
                    if not row or row[0] != group:
                        errors.append({'area': 'paper_current', 'reason': 'current result differs from publication history'})
                for key, group in db.execute('SELECT experiment_key,group_id FROM paper_current'):
                    if latest.get(key) != group:
                        errors.append({'area': 'paper_publications', 'reason': 'current result lacks matching publication'})

            paper_write_tables = {'paper_write_requests', 'paper_write_fences', 'paper_write_retries'}
            if tables & paper_write_tables and not paper_write_tables <= tables:
                errors.append({'area': 'paper_writes', 'reason': 'write journal schema is incomplete'})
            if paper_write_tables <= tables:
                def check_paper_write(row, fenced=False):
                    if fenced:
                        request_key, operation, object_key, digest, request_raw, created_at = row
                        result_raw = None
                        _require(not db.execute('SELECT 1 FROM paper_write_requests WHERE request_key=?', (request_key,)).fetchone())
                    else:
                        request_key, operation, object_key, digest, request_raw, result_raw, created_at = row
                    _require(operation in ('draw_resolve', 'issue_recalculate', 'source_correction_retry')
                             and isinstance(request_raw, bytes) and hashlib.sha256(request_raw).hexdigest() == digest)
                    request = json.loads(request_raw)
                    _require(isinstance(request, dict) and _canonical(request) == request_raw)
                    if operation == 'source_correction_retry':
                        _require(set(request) == {'correction_id', 'scope'} and type(request.get('correction_id')) is int
                                 and request['correction_id'] > 0 and request.get('scope') in ('global', 'issue')
                                 and object_key == f"{request['scope']}:{request['correction_id']}")
                        table = 'source_correction_followups' if request['scope'] == 'global' else 'scoped_correction_followups'
                        _require(db.execute(f'SELECT 1 FROM {table} WHERE correction_id=?',
                                            (request['correction_id'],)).fetchone())
                        if not fenced:
                            result = json.loads(result_raw)
                            _require(result == {'status': 'accepted', 'correction_id': request['correction_id'],
                                                'scope': request['scope']})
                    else:
                        _require(request.get('issue') == object_key and isinstance(object_key, str)
                                 and re.fullmatch(r'\d{5}', object_key))
                    if operation == 'draw_resolve':
                        _require(set(request) == {'issue', 'snapshot_id', 'variants_sha256', 'row_sha256', 'note', 'confirmed'}
                                 and isinstance(request['snapshot_id'], str) and re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id'])
                                 and isinstance(request['variants_sha256'], str) and re.fullmatch(r'[0-9a-f]{64}', request['variants_sha256'])
                                 and isinstance(request['row_sha256'], str) and re.fullmatch(r'[0-9a-f]{64}', request['row_sha256'])
                                 and isinstance(request['note'], str) and request['note'].strip() and request['confirmed'] is True)
                        if not fenced:
                            result = json.loads(result_raw)
                            resolution = result.get('resolution') if isinstance(result, dict) and 'resolution' in result else result
                            _require(isinstance(resolution, dict) and type(resolution.get('resolution_id')) is int
                                     and resolution.get('variants_sha256') == request['variants_sha256']
                                     and resolution.get('selected_row_sha256') == request['row_sha256'])
                            if 'resolution' in result:
                                _require(set(result) == {'resolution', 'recalculations'} and isinstance(result['recalculations'], dict))
                    else:
                        _require(set(request) == {'issue', 'snapshot_id'} and isinstance(request['snapshot_id'], str)
                                 and re.fullmatch(r'[0-9a-f]{64}', request['snapshot_id']))
                        if not fenced and result_raw is not None:
                            _require(isinstance(json.loads(result_raw), dict))
                    stamp = datetime.fromisoformat(created_at)
                    _require(stamp.tzinfo is not None and stamp.utcoffset() is not None)
                check_rows('paper_write_requests', 'SELECT request_key,operation,object_key,request_sha256,request_json,result_json,created_at FROM paper_write_requests', check_paper_write)
                check_rows('paper_write_fences', 'SELECT request_key,operation,object_key,request_sha256,request_json,created_at FROM paper_write_fences',
                           lambda row: check_paper_write(row, fenced=True))
                def check_paper_retry(row):
                    request_key, retry_of, operation, object_key, digest, request_raw, created_at = row
                    original = db.execute('SELECT operation,object_key,request_sha256,request_json,created_at FROM paper_write_fences WHERE request_key=?',
                                          (retry_of,)).fetchone()
                    _require(request_key != retry_of and original and original[:4] == (operation, object_key, digest, request_raw)
                             and hashlib.sha256(request_raw).hexdigest() == digest)
                    request = json.loads(request_raw)
                    if operation == 'source_correction_retry':
                        _require(isinstance(request, dict) and set(request) == {'correction_id', 'scope'}
                                 and type(request.get('correction_id')) is int and request['correction_id'] > 0
                                 and request.get('scope') in ('global', 'issue')
                                 and object_key == f"{request['scope']}:{request['correction_id']}")
                    else:
                        _require(isinstance(request, dict) and request.get('issue') == object_key)
                    _require(_canonical(request) == request_raw)
                    accepted = db.execute('SELECT operation,object_key,request_sha256,request_json FROM paper_write_requests WHERE request_key=?',
                                          (request_key,)).fetchone()
                    fenced = db.execute('SELECT operation,object_key,request_sha256,request_json FROM paper_write_fences WHERE request_key=?',
                                        (request_key,)).fetchone()
                    _require(accepted == (operation, object_key, digest, request_raw)
                             or fenced == (operation, object_key, digest, request_raw))
                    when, original_when = datetime.fromisoformat(created_at), datetime.fromisoformat(original[4])
                    _require(when.tzinfo is not None and original_when.tzinfo is not None and original_when <= when)
                check_rows('paper_write_retries', 'SELECT request_key,retry_of,operation,object_key,request_sha256,request_json,created_at FROM paper_write_retries', check_paper_retry)

            if 'source_corrections' in tables and 'reviewed_sources' in tables:
                from dlt.prospective import ProspectiveRepository
                def check_correction(row):
                    old, new, kind = row
                    _, _, chain = ProspectiveRepository._effective_review(db, old, kind)
                    _require(new in chain)
                check_rows('source_corrections', 'SELECT old_review_id,new_review_id,kind FROM source_corrections', check_correction)
                if 'scoped_source_corrections' in tables:
                    def check_scoped_correction(row):
                        issue, old, new, kind = row
                        _require(kind in ('rule', 'tax') and isinstance(issue, str) and len(issue) == 5 and issue.isascii() and issue.isdigit())
                        _, _, chain = ProspectiveRepository._effective_review(db, old, kind, issue=issue)
                        _require(new in chain)
                        for ident in (old, new):
                            review = ProspectiveRepository._get_review(db, ident, kind)
                            _require(issue in review['extracted'].get('applicable_issues', []))
                    check_rows('scoped_source_corrections', 'SELECT issue,old_review_id,new_review_id,kind FROM scoped_source_corrections', check_scoped_correction)
    except (sqlite3.Error, OSError, ValueError) as exc:
        errors.append({'area': 'database', 'reason': 'unable to complete read-only check', 'error_type': type(exc).__name__})
    result['status'] = 'failed' if errors else 'incomplete' if limitations else 'passed'
    return result


def main():
    parser = argparse.ArgumentParser(description='Check selected research database integrity without modifying it.')
    parser.add_argument('database')
    args = parser.parse_args()
    result = check_database(args.database)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result['status'] == 'passed' else 2


if __name__ == '__main__':
    raise SystemExit(main())
