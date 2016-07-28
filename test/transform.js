var assert = require('assert')
var async = require('async');

var pg = require('pg');
var copyOut = require('pg-copy-streams').to;
var copyIn = require('pg-copy-streams').from;
var parser = require('../').parser;
var deparser = require('../').deparser;

var client = function(dsn) {
  var client = new pg.Client(dsn);
  client.connect();
  return client;
}

var dsnA = null; // configure database A connection parameters
var dsnB = null; // configure database B connection parameters

var clientA = client(dsnA);
var clientB = client(dsnB);

var queriesA = [
  "DROP TABLE IF EXISTS item",
  "CREATE TABLE item (id serial PRIMARY KEY, ref text, description text)",
  "INSERT INTO item (ref, description) VALUES ('1:CTX', 'A little item')",
  "INSERT INTO item (ref, description) VALUES ('2:CTX', 'A BIG item')"
]

var queriesB = [
  "DROP TABLE IF EXISTS product",
  "CREATE TABLE product (code int4 PRIMARY KEY, label text, description text, ts_creation timestamptz, matrix int2[][])" 
]

// we simplify by observing here that A=B when tests are executed
async.eachSeries(queriesA.concat(queriesB), clientA.query.bind(clientA), function(err) {
    assert.ifError(err)
    var AStream = clientA.query(copyOut('COPY item TO STDOUT BINARY'))
    var Parser  = new parser({
      objectMode: true,
      mapping: [
        { key: 'id', type: 'int4' },
        { key: 'ref', type: 'text' },
        { key: 'description', type: 'text'},
      ]
    })
    var Deparser = new deparser({
      objectMode: true,
      mapping: [
        function(row) { return { type: 'int4', value: parseInt(row.ref.split(':')[0])} },
        function(row) { return { type: 'text', value: row.ref.split(':')[1] } },
        function(row) { return { type: 'text', value: row.description.toLowerCase() } },
        function(row) { 
          var d = new Date('1999-01-01T00:00:00Z');
          var numberOfDaysToAdd = parseInt(row.ref.split(':')[0]);
          d.setDate(d.getDate() + numberOfDaysToAdd); 
          return { type: 'timestamptz', value: d } },
        function(row) {
          var id = parseInt(row.ref.split(':')[0]);
          return { type: '_int2', value: [[id, id+1], [id+2, id+3]]}
        }, 
      ]
    })
    var BStream = clientB.query(copyIn ('COPY product FROM STDIN BINARY'))

    var runStream = function(callback) {
      BStream.on('finish', callback);
      AStream.pipe(Parser).pipe(Deparser).pipe(BStream);
    }

    runStream(function(err) {
      assert.ifError(err)
      clientA.query('SELECT * FROM item', function(err, res) {
        assert.equal(res.rowCount, 2, 'expected 2 tuples on A, but got ' + res.rowCount);
        clientA.end();
      })
      clientB.query('SELECT * FROM product ORDER BY code ASC', function(err, res) {
        var d = new Date('1999-01-01T00:00:00Z');
        assert.equal(res.rowCount, 2, 'expected 2 tuples on B, but got ' + res.rowCount);
        
        // first row
        assert.equal(res.rows[0].code, 1)
        assert.equal(res.rows[0].label, 'CTX')
        assert.equal(res.rows[0].description, 'a little item')
        assert.equal(res.rows[0].ts_creation.getTime(), d.getTime() + 1*24*60*60*1000)
        assert.equal(JSON.stringify(res.rows[0].matrix), "[[1,2],[3,4]]")

        // second row
        assert.equal(res.rows[1].code, 2)
        assert.equal(res.rows[1].label, 'CTX')
        assert.equal(res.rows[1].description, 'a big item')
        assert.equal(JSON.stringify(res.rows[1].matrix), "[[2,3],[4,5]]")

        clientB.end();
      })
    })
})
