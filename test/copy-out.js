const assert = require('assert')
const gonna = require('gonna')
const pg = require('pg')
const { parser } = require('../')
const { to: copyTo } = require('pg-copy-streams')
const through2 = require('through2')

const client = function () {
  const client = new pg.Client()
  client.connect()
  return client
}

const samples = {
  bool: [null, true, false],
  bytea: [Buffer.from([0x61]), null, Buffer.from([0x62])],
  int2: [23, -59, null],
  int4: [2938, null, -99283],
  text: ['aaa', 'ééé', null],
  json: [JSON.stringify({}), JSON.stringify([1, 2]), null],
  float4: [0.26350000500679016, null, -3.2929999872755022e-12],
  float8: [9000.12, 9.23e29, null],
  timestamptz: [new Date('2000-01-01T00:00:00Z'), null, new Date('1972-04-25T18:22:00Z')],
}

const testParser = function () {
  const fromClient = client()
  let idx = 1
  const fields = []
  const placeholders = []
  const mapping = []
  const rows = []
  for (const t in samples) {
    fields.push('c' + idx + ' ' + t)
    placeholders.push('$' + idx)
    mapping.push({ key: 'c' + idx, type: t })
    for (let c = 0; c < samples[t].length; c++) {
      rows[c] = rows[c] || []
      rows[c].push(samples[t][c])
    }
    idx++
  }
  fromClient.query('CREATE TEMP TABLE plug (' + fields.join(',') + ')')
  for (let i = 0; i < rows.length; i++) {
    fromClient.query('INSERT INTO plug VALUES (' + placeholders.join(',') + ')', rows[i])
  }

  const txt = 'COPY plug TO STDOUT BINARY'
  const copyOut = fromClient.query(copyTo(txt))
  const p = parser({ objectMode: true, mapping: mapping })

  const countDone = gonna('have correct count')
  idx = 0
  copyOut.pipe(p).pipe(
    through2.obj(
      function (obj, _, cb) {
        for (let i = 0; i < mapping.length; i++) {
          let expected = samples[mapping[i].type][idx]
          let got = obj[mapping[i].key]
          if (expected !== null && got !== null) {
            switch (mapping[i].type) {
              case 'bytea':
                expected = expected.toString()
                got = got.toString()
                break
              case 'json':
                got = JSON.stringify(got)
                break
              case 'timestamptz':
                expected = expected.getTime()
                got = got.getTime()
                break
            }
          }
          assert.equal(expected, got, 'Mismatch for ' + mapping[i][1] + ' expected ' + expected + ' got ' + got)
        }
        idx++
        cb()
      },
      function (cb) {
        countDone()
        fromClient.end()
      }
    )
  )
}

testParser()
