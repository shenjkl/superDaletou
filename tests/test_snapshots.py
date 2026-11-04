import json
import hashlib
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from dlt.snapshots import InvalidSnapshot, SnapshotStore


def source(rows):
    return json.dumps({'success': True, 'errorCode': '0', 'value': {'list': rows}}, ensure_ascii=False).encode()


def draw(issue='26123', numbers='01 02 03 04 05 01 02', prizes=None):
    return {'lotteryGameNum': '85', 'lotteryDrawNum': issue, 'lotteryDrawTime': '2026-10-28',
            'lotteryDrawResult': numbers, 'prizeLevelList': prizes}


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SnapshotStore(Path(self.tmp.name) / 'data' / 'store.sqlite3')
        self.time = '2026-10-29T08:00:00+08:00'

    def tearDown(self):
        self.tmp.cleanup()

    def test_missing_prizes_allow_numbers_but_never_settle_without_rule(self):
        data = source([draw(), draw('26122', prizes=[{'sort': '101', 'stakeAmount': '5,000'}])])
        result = self.store.import_official(data, 'official', self.time)
        rows = self.store.get_draws(result['snapshot_id'])
        self.assertEqual(len(rows), 2)
        by_issue = {r['issue']: r for r in rows}
        self.assertEqual(by_issue['26123']['numbers_usable'], 1)
        self.assertEqual(by_issue['26123']['settlement_reason'], 'PRIZES_MISSING')
        self.assertEqual(by_issue['26122']['prize_fields_present'], 1)
        self.assertEqual(by_issue['26122']['settlement_ready'], 0)
        self.assertEqual(by_issue['26122']['settlement_reason'], 'RULE_NOT_VERIFIED')

    def test_prize_fields_accept_exact_fen_but_reject_bad_amount_formats(self):
        for amount, usable in [('5,000.12', 1), ('0.01', 1), ('5,00.12', 0), ('1.001', 0)]:
            with self.subTest(amount=amount):
                result = self.store.import_official(source([draw(prizes=[{'sort': 101, 'stakeAmount': amount}])]),
                                                    'fixture', self.time)
                row = self.store.get_draws(result['snapshot_id'])[0]
                self.assertEqual(row['prize_fields_present'], usable)
                self.assertEqual(row['settlement_ready'], 0)

    def test_same_observation_is_idempotent_and_immutable(self):
        data = source([draw()])
        a = self.store.import_official(data, 'official', self.time)
        b = self.store.import_official(data, 'official', self.time)
        self.assertEqual(a, b)
        with closing(sqlite3.connect(self.store.path)) as db, db:
            self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0], 1)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM draw_versions')

    def test_corrected_draw_keeps_both_versions_without_choosing_one(self):
        a = self.store.import_official(source([draw()]), 'official', self.time)
        b = self.store.import_official(source([draw(numbers='01 02 03 04 06 01 02')]), 'official', self.time)
        self.assertNotEqual(a['snapshot_id'], b['snapshot_id'])
        self.assertEqual(self.store.get_draws(a['snapshot_id'])[0]['front'], [1, 2, 3, 4, 5])
        self.assertEqual(self.store.get_draws(b['snapshot_id'])[0]['front'], [1, 2, 3, 4, 6])
        self.assertEqual(len(self.store.conflicts('26123')), 2)

    def test_invalid_rows_do_not_partially_write(self):
        valid = self.store.import_official(source([draw()]), 'official', self.time)
        for rows in ([draw('26124'), draw('26124')], [draw('26124'), draw('26125', '01 01 03 04 05 01 02')]):
            with self.subTest(rows=rows), self.assertRaises(InvalidSnapshot):
                self.store.import_official(source(rows), 'official', self.time)
        with closing(sqlite3.connect(self.store.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM draw_versions').fetchone()[0], 1)
        self.assertEqual(len(self.store.get_draws(valid['snapshot_id'])), 1)

    def test_no_timestamp_fabrication_and_reads_do_not_create_database(self):
        with self.assertRaises(FileNotFoundError):
            self.store.get_draws('missing')
        with self.assertRaises(InvalidSnapshot):
            self.store.import_official(source([draw()]), 'official', '2026-10-29T08:00:00')
        self.assertFalse(self.store.path.exists())

    def test_duplicate_json_key_and_conflicting_raw_observation_rejected(self):
        with self.assertRaises(InvalidSnapshot):
            self.store.import_official(b'{"success":true,"success":true}', 'official', self.time)
        data = source([draw()])
        self.store.import_official(data, 'official', self.time)
        with self.assertRaises(InvalidSnapshot):
            self.store.import_official(data + b' ', 'official', self.time)
        with closing(sqlite3.connect(self.store.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0], 1)

    def test_each_observation_preserves_exact_source_bytes(self):
        data = source([draw()])
        first = self.store.import_official(data, 'official', self.time)
        second = self.store.import_official(data + b' ', 'official', '2026-10-29T08:01:00+08:00')
        self.assertEqual(first['snapshot_id'], second['snapshot_id'])
        with closing(sqlite3.connect(self.store.path)) as db:
            raw = [r[0] for r in db.execute('SELECT raw_payload FROM observations ORDER BY observed_at')]
        self.assertEqual(raw, [data, data + b' '])

    def test_legacy_observation_schema_migrates_without_fabricating_bytes(self):
        data = source([draw()])
        canonical = json.dumps(json.loads(data), ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
        snapshot_id, raw_hash = hashlib.sha256(canonical).hexdigest(), hashlib.sha256(data).hexdigest()
        self.store.path.parent.mkdir(parents=True)
        with closing(sqlite3.connect(self.store.path)) as db, db:
            db.executescript('''
                CREATE TABLE snapshots(id TEXT PRIMARY KEY, raw_sha256 TEXT NOT NULL, canonical_json BLOB NOT NULL, row_count INTEGER NOT NULL);
                CREATE TABLE observations(snapshot_id TEXT NOT NULL, source TEXT NOT NULL, observed_at TEXT NOT NULL,
                    raw_sha256 TEXT NOT NULL, PRIMARY KEY(snapshot_id, source, observed_at));
            ''')
            db.execute('INSERT INTO snapshots VALUES (?, ?, ?, ?)', (snapshot_id, raw_hash, canonical, 1))
            db.execute('INSERT INTO observations VALUES (?, ?, ?, ?)', (snapshot_id, 'official', self.time, raw_hash))
        with self.assertRaisesRegex(InvalidSnapshot, 'legacy observation'):
            self.store.import_official(data, 'official', self.time)
        self.store.import_official(data, 'official', '2026-10-29T08:01:00+08:00')
        with closing(sqlite3.connect(self.store.path)) as db:
            records = db.execute('SELECT observed_at, raw_payload FROM observations ORDER BY observed_at').fetchall()
        self.assertIsNone(records[0][1])
        self.assertEqual(records[1][1], data)

    def test_reviewed_official_fixture_loads_without_writing_research_files(self):
        path = Path(__file__).resolve().parents[1] / 'research/data/official_page1.json'
        result = self.store.import_official(path.read_bytes(), 'reviewed official snapshot', self.time)
        self.assertGreater(result['row_count'], 0)
        self.assertTrue(all(not row['settlement_ready'] for row in self.store.get_draws(result['snapshot_id'])))


if __name__ == '__main__':
    unittest.main()
