const assert = require('assert')
const gonna = require('gonna')

const pgtypes = require('../lib/pg_types')
const types = pgtypes.types
const deparse = pgtypes.deparse

const BP = require('bufferput')
const samples = require('./samples')

const test_samples = function () {
  samples.forEach(function (s) {
    const buf = deparse(new BP(), s.t, s.v).buffer()
    const eq = buf.equals(s.r)
    assert(
      eq,
      'Unparse ' +
        s.t +
        ' not matching: ' +
        (s.v !== null ? s.v.toString() : 'null') +
        ' => ' +
        buf.toString('hex') +
        ' / ' +
        s.r.toString('hex')
    )
  })
}

test_samples()
