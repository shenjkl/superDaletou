"""Run explicitly enabled data-update plans until the local process stops."""
import argparse
import json
from pathlib import Path
import time

from dlt.scheduler import Scheduler
from dlt.recovery import prepare_runtime_database_path, verify_runtime_database_permissions


def main():
    parser = argparse.ArgumentParser(description='Manage and run local update schedules; plans start paused.')
    parser.add_argument('--db', required=True, help='Path to the local research SQLite database')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('run', help='Run enabled plans until the process stops')
    create = commands.add_parser('create', help='Create a paused plan from UTF-8 JSON')
    create.add_argument('--config', required=True)
    for name in ('enable', 'pause'):
        sub = commands.add_parser(name)
        sub.add_argument('plan_id')
    commands.add_parser('status')
    args = parser.parse_args()
    db_path, existed_before_start = prepare_runtime_database_path(args.db)
    scheduler = Scheduler(db_path)
    scheduler.initialize()
    verify_runtime_database_permissions(db_path, existed_before_start=existed_before_start)
    if args.command == 'create':
        print(scheduler.create_plan(json.loads(Path(args.config).read_text(encoding='utf-8'))))
        return
    if args.command in ('enable', 'pause'):
        scheduler.set_enabled(args.plan_id, args.command == 'enable')
        return
    if args.command == 'status':
        print(json.dumps([{'plan': plan, 'runs': scheduler.list_runs(plan['id'])}
                          for plan in scheduler.list_plans()], ensure_ascii=False, indent=2))
        return
    # Use the same single-owner lock as the browser service's background loop.
    # Other CLI commands only edit or inspect plans and do not take this lock.
    from dlt.app import SingleInstance
    try:
        with SingleInstance(str(db_path)+'.update'):
            while True:
                scheduler.run_due()
                time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
