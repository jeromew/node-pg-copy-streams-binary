/**
 * Documentation is extracted from
 * [1] https://www.postgresql.org/docs/current/static/sql-copy.html for the COPY binary format
 * [2] https://github.com/postgres/postgres/tree/master/src/backend/utils/adt for the send/recv binary formats of types
 */

module.exports = function(txt, options) {
  return new CopyStream(txt, options)
}

var Transform = require('stream').Transform
var util = require('util')
var BP = require('bufferput');
var parse = require('./pg_types').parse;

var CopyStream = function(options) {
  options.objectMode = true;
  Transform.call(this, options)

  // PGCOPY\n\377\r\n\0 (signature + flags field + Header extension area length)
  this.COPYHeaderFull = (new BP())
                        .put(Buffer.from([0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00]))
                        .word32be(0)
                        .word32be(0)
                        .buffer();

  this.COPYTrailer = Buffer.from([0xff, 0xff]);

  this._headerReceived = false;
  this._trailerReceived = false;
  this._remainder = false;

  this.mapping = options.mapping || false

}

util.inherits(CopyStream, Transform)
 
CopyStream.prototype._transform = function(chunk, enc, cb) { 
  if(this._remainder && chunk) {
    chunk = Buffer.concat([this._remainder, chunk])
  }

  var offset = 0; 
  if (!this._headerReceived && chunk.length >= this.COPYHeaderFull.length) {
    if (this.COPYHeaderFull.equals(chunk.slice(0, this.COPYHeaderFull.length))) {
      this._headerReceived = true;
      offset += this.COPYHeaderFull.length;
    }   
  }

  // Copy-out mode (data transfer from the server) is initiated when the backend executes a COPY TO STDOUT SQL statement.
  // The backend sends a CopyOutResponse message to the frontend, followed by zero or more CopyData messages (always one per row)
  var UInt16Len = 2;
  while (this._headerReceived && (chunk.length - offset) >= UInt16Len) {
    var fieldCount = chunk.readUInt16BE(offset);
    offset += 2;
    var UInt32Len = 4;
    var UInt16_0xff = 65535;
    var UInt32_0xffffffff = 4294967295;
    if (fieldCount === UInt16_0xff) {
      this._trailerReceived = true;
      this.push(null);
      return cb();
    }
    var fields = this.mapping ? {} : [];
    for (var i=0; i<fieldCount; i++) {
      var v;
      var fieldLen = chunk.readUInt32BE(offset);
      offset += UInt32Len;
      if (fieldLen === UInt32_0xffffffff) {
        v = null;
      } else {
        var v = chunk.slice(offset, offset + fieldLen);
        if (this.mapping) {
          v = parse(v, this.mapping[i].type)
        }
        offset += fieldLen;
      }
      if (this.mapping) {
        fields[this.mapping[i].key] = v;
      } else {
        fields.push(v);
      }
    }
    this.push(fields);
  }

  if(chunk.length - offset) {
    var slice = chunk.slice(offset)
    this._remainder = slice
  } else {
    this._remainder = false
  } 
  cb();
}

CopyStream.prototype._flush = function(cb) {
  cb();
}
