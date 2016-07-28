var assert = require('assert');
var gonna = require('gonna');
var pg = require('pg');
var parser = require('../').parser;
var copy = require('pg-copy-streams').to;
var pgtypes = require('../lib/pg_types');
var types = pgtypes.types;
var through2 = require('through2')
var deepEqual = require('deeper');

var client = function() {
  var client = new pg.Client()
  client.connect()
  return client
}

var samples = {
  'bool': [null, true, false ],
  'bytea': [new Buffer([0x61]), null, new Buffer([0x62])],
  'int2': [23, -59, null],
  'int4': [2938, null, -99283],
  'text': ['aaa', 'ééé', null],
  'json': [JSON.stringify({}), JSON.stringify([1,2]), null],
  'float4': [0.26350000500679016, null, -3.2929999872755022e-12],
  'float8': [9000.12, 9.23e+29, null],
  'timestamptz': [new Date('2000-01-01T00:00:00Z'), null, new Date('1972-04-25T18:22:00Z')],
}


var testParser = function() {
  var fromClient = client()
  var idx = 1;
  var fields = [];
  var placeholders = [];
  var mapping = [];
  var rows = [];
  for (var t in samples) {
    fields.push('c'+idx+' '+t);
    placeholders.push('$'+idx);
    mapping.push({ key: 'c'+idx, type: t})
    for (var c=0; c<samples[t].length;c++) {
      rows[c] = rows[c] || [];
      rows[c].push(samples[t][c])
    }
    idx++
  }
  fromClient.query('CREATE TEMP TABLE plug ('+fields.join(',')+')')
  for (var i=0; i<rows.length; i++) {
    fromClient.query('INSERT INTO plug VALUES ('+placeholders.join(',')+')', rows[i])  
  }

  var txt = 'COPY plug TO STDOUT BINARY'
  var copyOut = fromClient.query(copy(txt))
  var p = parser({objectMode: true, mapping: mapping });
  
  var countDone = gonna('have correct count')
  var idx = 0;
  copyOut.pipe(p).pipe(through2.obj(function(obj, _, cb) {
    for (var i = 0; i < mapping.length; i++) {
      var expected = samples[mapping[i].type][idx];
      var got = obj[mapping[i].key];
      if (expected !== null && got !== null) {
      switch(mapping[i].type) {
        case 'bytea':
          expected = expected.toString();
          got = got.toString();
          break;
        case 'json':
          got = JSON.stringify(got);
          break;
        case 'timestamptz':
          expected = expected.getTime();
          got = got.getTime();
          break;
      }
      }
      assert.equal(expected, got, 'Mismatch for ' + mapping[i][1] + ' expected ' + expected + ' got ' + got)
    }
    idx++;
    cb();
  }, function(cb) {
    countDone();
    fromClient.end();
  }));
}

testParser()

