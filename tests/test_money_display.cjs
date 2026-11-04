// Exact display tests; no browser or service is started.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('function decimalYuan(');
const end = source.indexOf('\nconst tag=', start);
assert.ok(start >= 0 && end > start);
const c = {};
vm.runInNewContext(source.slice(start, end), c);

test('large and signed amounts do not pass through binary floating point', () => {
  assert.equal(c.decimalYuan('12345678901234567890123456.78'), '12345678901234567890123456.78 元');
  assert.equal(c.decimalYuan('-12345678901234567890123456.785'), '−12345678901234567890123456.79 元');
  assert.equal(c.decimalYuan('1.005'), '1.01 元');
  assert.equal(c.decimalYuan('-0.005'), '−0.01 元');
  assert.equal(c.decimalYuan('-0.004'), '0.00 元');
  assert.equal(c.decimalYuan('5E-3'), '0.01 元');
  assert.equal(c.decimalYuan('1E+24'), '1000000000000000000000000.00 元');
});

test('exact rational mean is rounded directly from integer numerator and denominator', () => {
  assert.equal(c.exactMeanYuan({fraction: ['1005', '1000']}), '1.01 元');
  assert.equal(c.exactMeanYuan({fraction: ['10049999999999999999999999999', '10000000000000000000000000000']}), '1.00 元');
  assert.equal(c.exactMeanYuan({fraction: ['1234567890123456789012345678', '100']}), '12345678901234567890123456.78 元');
  assert.equal(c.exactMeanYuan({fraction: [1, 0]}), '金额待复核');
  assert.equal(c.exactMeanYuan({fraction: [Number.MAX_SAFE_INTEGER + 1, 1]}), '金额待复核');
});

test('invalid and already imprecise inputs are not shown as money', () => {
  for (const value of [null, true, 'Infinity', '<img>', '1e9999', Number.MAX_SAFE_INTEGER + 1]) {
    assert.equal(c.decimalYuan(value), '金额待复核');
  }
  assert.equal(c.exactMeanYuan({yuan: '1.25'}), '金额待复核');
});
