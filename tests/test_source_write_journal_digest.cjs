const assert = require('node:assert/strict');
const { createHash, webcrypto } = require('node:crypto');
const test = require('node:test');

const { canonical, digest } = require('../prototype/source-write-journal.js');

test('source write journal exports the canonical SHA-256 digest used by paper writes', async () => {
  const request = { issue: '26017', snapshot_id: 'snapshot', confirmed: true };
  const expected = createHash('sha256').update(canonical(request), 'utf8').digest('hex');
  assert.equal(await digest(request, webcrypto), expected);
  assert.equal(await digest({ confirmed: true, snapshot_id: 'snapshot', issue: '26017' }, webcrypto), expected);
});

test('source write journal refuses to send when Web Crypto digest support is missing', async () => {
  await assert.rejects(digest({ issue: '26017' }, null), /摘要能力不可用/);
});
