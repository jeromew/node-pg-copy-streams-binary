var through2 = require('through2')
var MultiFork = require('multi-fork')

var parser = require('./parser')
var deparser = require('./deparser')

var shift = function() {
  return through2.obj(function(row,_,cb) {
    row.shift()
    this.push(row)
    cb()
  })
}


module.exports = function(opt) {

  var mapping = opt.mapping
  var transform = opt.transform
  var copyIns = opt.targets

  var first = parser({ mapping: mapping }); 
  var n = copyIns.length;
  var f = n;
  var finish = function() {
    f--;
    if (f===0) {
      first.emit('close');
    }
  }
  var classifier = function(row, cb) { cb(null, row[0]) }
  var M = new MultiFork(n, { classifier: classifier })
  for(var i=0; i<n; i++) {
    copyIns[i].on('finish', finish);
    M.streams[i].pipe(shift()).pipe(deparser()).pipe(copyIns[i])
  }
  first.pipe(transform).pipe(M);
  return first;
}
