var assert = require('assert')
var gonna = require('gonna')

var pgtypes = require('../lib/pg_types')
var types = pgtypes.types
var parse = pgtypes.parse

var BP = require('bufferput')
var samples = require('./samples')

function size(ar) {
  var row_count = ar.length
  var row_sizes = []
  for (var i = 0; i < row_count; i++) {
    row_sizes.push(ar[i].length)
  }
  return [row_count, Math.max.apply(null, row_sizes)]
}

function flatten(arr) {
  return arr.reduce((acc, val) => (Array.isArray(val) ? acc.concat(flatten(val)) : acc.concat(val)), [])
}

var test_samples = function () {
  samples.forEach(function (s) {
    var buf = s.r
    var fieldLen = buf.readUInt32BE(0)
    var isNull = buf.readInt32BE(0)
    var UInt32Len = 4
    var type = s.t
    if (isNull === -1) {
      assert.equal(buf.length, UInt32Len, 'A "null" binary buffer should be 0xffffffff')
    } else {
      var got = parse(buf.slice(UInt32Len), s.t)
      var expected = s.v

      var gots = [got]
      var expecteds = [expected]

      if (s.t[0] === '_') {
        assert.equal(size(got).join(','), size(expected).join(','), 'array dimensions should match')
        gots = flatten(got)
        expecteds = flatten(expecteds)
        type = s.t.substr(1)
      }

      assert.equal(gots.length, expecteds.length, s.t + ': arrays should have the same global number of members')

      for (var i = 0; i < gots.length; i++) {
        got = gots[i]
        expected = expecteds[i]
        switch (type) {
          case 'bytea':
            got = got.toString()
            expected = got.toString()
            break
          case 'json':
            got = JSON.stringify(got)
            expected = JSON.stringify(expected)
            break
          case 'timestamptz':
            got = got.getTime()
            expected = expected.getTime()
            break
        }
        assert.equal(
          got,
          expected,
          s.t + ': parsed value is incorrect for ' + s.t + ' expected ' + expected + ', got ' + got
        )
      }
    }
  })
}

test_samples()
