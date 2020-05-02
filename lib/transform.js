const through2 = require('through2')
const MultiFork = require('multi-fork')

const parser = require('./parser')
const deparser = require('./deparser')

const shift = function () {
  return through2.obj(function (row, _, cb) {
    row.shift()
    this.push(row)
    cb()
  })
}

module.exports = function (opt) {
  const mapping = opt.mapping
  const transform = opt.transform
  const copyIns = opt.targets

  const first = parser({ mapping: mapping })
  const n = copyIns.length
  let f = n
  const finish = function () {
    f--
    if (f === 0) {
      first.emit('close')
    }
  }
  const classifier = function (row, cb) {
    cb(null, row[0])
  }
  const M = new MultiFork(n, { classifier: classifier })
  for (let i = 0; i < n; i++) {
    copyIns[i].on('finish', finish)
    M.streams[i].pipe(shift()).pipe(deparser()).pipe(copyIns[i])
  }
  first.pipe(transform).pipe(M)
  return first
}
