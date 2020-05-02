const assert = require('assert')
const gonna = require('gonna')
const pg = require('pg')
const { deparser } = require('../')
const { from: copyFrom } = require('pg-copy-streams')

const client = function () {
  const client = new pg.Client()
  client.connect()
  return client
}

const testEmpty = function () {
  const fromClient = client()
  fromClient.query('CREATE TEMP TABLE plug (col1 text)')
  const txt = 'COPY plug FROM STDIN BINARY'
  const copyIn = fromClient.query(copyFrom(txt))
  const copyUn = deparser({ objectMode: true })
  copyUn.pipe(copyIn)
  copyUn.end()
  const done = gonna('empty rows should not trigger error')
  copyIn.on('end', function () {
    done()
    fromClient.end()
  })
}
testEmpty()

const testType = function (type, ndim, value, expectedText) {
  const fromClient = client()

  let atype = type
  if (ndim > 0) {
    atype = '_' + atype
  }
  let coltype = type
  while (ndim > 0) {
    coltype += '[]'
    ndim--
  }

  fromClient.query('CREATE TEMP TABLE plug (col1 ' + coltype + ')')

  const txt = 'COPY plug FROM STDIN BINARY'
  const copyIn = fromClient.query(copyFrom(txt))
  const copyUn = deparser({ objectMode: true })
  copyUn.pipe(copyIn)
  copyUn.end([{ type: atype, value: value }])
  const countDone = gonna('have correct count')
  copyIn.on('end', function () {
    const sql = 'SELECT col1::text FROM plug'
    fromClient.query(sql, function (err, res) {
      assert.ifError(err)
      assert.equal(
        res.rows[0].col1,
        expectedText,
        'expected ' +
          expectedText +
          ' for ' +
          coltype +
          ' row but got ' +
          (res.rows.length ? res.rows[0].col1 : '0 rows')
      )
      countDone()
      fromClient.end()
    })
  })
}

testType('bool', 0, null, null)
testType('bool', 0, true, 'true')
testType('bool', 0, false, 'false')
testType('bool', 1, [true, false], '{t,f}')
testType('int2', 0, 0, '0')
testType('int2', 0, 7, '7')
testType('int2', 1, [2, 9], '{2,9}')
testType(
  'int2',
  2,
  [
    [1, 2],
    [3, 4],
  ],
  '{{1,2},{3,4}}'
)
testType('int4', 0, null, null)
testType('int4', 0, 7, '7')
testType('int4', 1, [2, 9], '{2,9}')
testType(
  'int4',
  2,
  [
    [1, 2],
    [3, 4],
  ],
  '{{1,2},{3,4}}'
)
testType('float4', 0, 0.2736, '0.2736')
testType('float4', 0, 2.928e27, '2.928e+27')
testType('float8', 0, 7.23e50, '7.23e+50')
testType('json', 0, { a: 1, b: 2 }, '{"a":1,"b":2}')
testType('json', 1, [{ a: 1 }, {}], '{"{\\"a\\":1}","{}"}')
testType('timestamptz', 0, new Date('2017-04-25T18:22:00Z'), '2017-04-25 18:22:00+00')
