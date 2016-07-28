var assert = require('assert');
var gonna = require('gonna');

var pgtypes = require('../lib/pg_types');
var types = pgtypes.types;
var deparse = pgtypes.deparse;

var BP = require('bufferput');
var samples = require('./samples');


var test_samples = function() {
  samples.forEach(function(s) {
    var buf = deparse(new BP(), s.t, s.v).buffer();
    var eq = buf.equals(s.r);
    assert(eq,  'Unparse ' + s.t + ' not matching: ' + ((s.v !== null) ? s.v.toString() : 'null') + ' => ' + buf.toString('hex') + ' / ' + s.r.toString('hex'));
  })
}

test_samples();
