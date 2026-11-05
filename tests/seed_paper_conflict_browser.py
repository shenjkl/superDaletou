"""Seed an isolated past-draw conflict for the paper write browser test."""
import sqlite3
import sys

from test_prospective_settlement import PaperResultsTests


target = sys.argv[1]
fixture = PaperResultsTests('test_complete_idempotent_pair_groups_and_close')
fixture.setUp()
try:
    original_id = fixture.import_draw(fixture.draw)
    original = fixture.paper.process_one('exp', original_id)
    updated = {**fixture.draw, 'prizeLevelList': [dict(row) for row in fixture.draw['prizeLevelList']]}
    updated['prizeLevelList'][0]['stakeAmount'] = '7000'
    selected_id = fixture.import_draw(updated)
    source = sqlite3.connect(fixture.path)
    destination = sqlite3.connect(target)
    try:
        source.backup(destination)
    finally:
        destination.close()
        source.close()
    print(original_id, selected_id, original['group_id'])
finally:
    fixture.tmp.cleanup()
