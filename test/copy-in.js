var assert = require('assert');
var gonna = require('gonna');
var pg = require('pg');
var deparser = require('../').deparser;
var copy = require('pg-copy-streams').from;

var client = function() {
  var client = new pg.Client()
  client.connect()
  return client
}

var testEmpty = function() {
  var fromClient = client();
  fromClient.query('CREATE TEMP TABLE plug (col1 text)')
  var txt = 'COPY plug FROM STDIN BINARY'
  var copyIn = fromClient.query(copy(txt))
  var copyUn = deparser({objectMode: true});
  copyUn.pipe(copyIn);
  copyUn.end() 
  var done = gonna('empty rows should not trigger error');
  copyIn.on('end', function() {
    done();
    fromClient.end();
  })
}
testEmpty();

var testType = function(type, ndim, value, expectedText) {
  var fromClient = client()
  
  var atype = type;
  if (ndim > 0) {
    atype = '_' + atype;
  }
  var coltype = type;
  while(ndim>0) {
    coltype += '[]';
    ndim--;
  }
  
  fromClient.query('CREATE TEMP TABLE plug (col1 '+coltype+')')

  var txt = 'COPY plug FROM STDIN BINARY'
  var copyIn = fromClient.query(copy(txt))
  var copyUn = deparser({objectMode: true});
  copyUn.pipe(copyIn);
  copyUn.end([
    { type: atype, value: value},
  ]);
  var countDone = gonna('have correct count')
  copyIn.on('end', function() {
    var sql = 'SELECT col1::text FROM plug';
    fromClient.query(sql, function(err, res) {
      assert.ifError(err)
      assert.equal(res.rows[0].col1, expectedText, 'expected ' + expectedText + ' for ' + coltype + ' row but got ' + (res.rows.length ? res.rows[0].col1 : '0 rows'))
      countDone()
      fromClient.end()
    })
  })
}

testType('bool',0,null,null)
testType('bool',0,true,'true')
testType('bool',0,false,'false')
testType('bool',1,[true, false],'{t,f}')
testType('int2', 0, 0, '0');
testType('int2', 0, 7, '7');
testType('int2', 1, [2,9], '{2,9}');
testType('int2', 2, [[1,2],[3,4]], '{{1,2},{3,4}}');
testType('int4', 0, null, null);
testType('int4', 0, 7, '7');
testType('int4', 1, [2,9], '{2,9}');
testType('int4', 2, [[1,2],[3,4]], '{{1,2},{3,4}}');
testType('float4', 0, 0.2736, '0.2736')
testType('float4', 0, 2.928e+27, '2.928e+27')
testType('float8', 0, 7.23e+50, '7.23e+50')
testType('json', 0, { a:1, b:2 }, '{"a":1,"b":2}')
testType('json', 1, [{a:1},{}], '{"{\\"a\\":1}","{}"}')
testType('timestamptz', 0, new Date('2017-04-25T18:22:00Z'), '2017-04-25 18:22:00+00')

