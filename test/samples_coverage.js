const assert = require('assert')

const pgtypes = require('../lib/pg_types')
const types = pgtypes.types

const samples = require('./samples')

const test_that_all_types_are_tested = function () {
  for (const k in types) {
    let has_null = 0
    let has_not_null = 0
    samples.forEach(function (s) {
      if (k === s.t && s.v === null) has_null++
      if (k === s.t && s.v !== null) has_not_null++
    })
    assert(has_null >= 1, 'Unparse ' + k + ' should have a sample testing the NULL value')
    assert(has_not_null >= 1, 'Unparse ' + k + ' should have at least one sample testing a NOT NULL value')
  }
}
test_that_all_types_are_tested()
