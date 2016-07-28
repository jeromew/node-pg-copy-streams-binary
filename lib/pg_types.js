var ieee754 = require('ieee754');
var int64 = require('int64-buffer');
var flatten = require('flatten');
var BufferPut = require('bufferput');

// bool
var boolsend = function(buf, value) {
  buf.word8be(value ? 1 : 0);
}
var boolrecv = function(buf) {
  return buf.readUInt8(0) ? true : false;
}

// bytea
var byteasend = function(buf, value) {
  buf.put(value)
}
var bytearecv = function(buf) {
  return buf;
}

// int2
var int2send = function(buf, value) {
  buf.word16be(value);
}
var int2recv = function(buf) {
  return buf.readInt16BE(0);
}

// int4
var int4send = function(buf, value) {
  buf.word32be(value);
}
var int4recv = function(buf) {
  return buf.readInt32BE(0);
}

// text
var textsend = function(buf, value) {
  var tbuf = new Buffer(value, 'utf-8');
  buf.put(tbuf);
}
var textrecv = function(buf) {
  return buf.toString('utf-8');
}

// json
var json_send = function(buf, value) {
  var jbuf = new Buffer(JSON.stringify(value), 'utf-8');
  buf.put(jbuf);
}
var json_recv = function(buf) {
  return JSON.parse(buf.toString('utf-8'))
}

// float4
var float4send = function(buf, value) {
  var fbuf = new Buffer(4);
  ieee754.write(fbuf, value, 0, false, 23, 4)
  buf.put(fbuf);
}
var float4recv = function(buf) {
  return ieee754.read(buf, 0, false, 23, 4)
}

// float8
var float8send = function(buf, value) {
  var fbuf = new Buffer(8);
  ieee754.write(fbuf, value, 0, false, 52, 8)
  buf.put(fbuf);
}
var float8recv = function(buf) {
  return ieee754.read(buf, 0, false, 52, 8)
}

// timestamptz
// NB: This is only for the HAVE_INT64_TIMESTAMP case
// in PostgreSQL source code so there may be some work needed
// to interact with other architures
var timestamptz_send = function(buf, value) {
  // postgres origin of time is 01/01/2000
  var ts = value.getTime() - 946684800000;
  ts = 1000 * ts; // add unknown usecs
  var tbuf = new Buffer(8);
  int64.Int64BE(tbuf, 0, ts);
  buf.put(tbuf);
}
var timestamptz_recv = function(buf) {
  var ts = int64.Int64BE(buf);
  ts = ts / 1000;
  ts = ts + 946684800000;
  return new Date(ts);
}

// array
var array_send = function(atype, buf, value) {
  var tmp;
  var ndim = 0;
  
  // count # of dimensions
  tmp = value;
  while (Array.isArray(tmp)) {
    ndim++;
    tmp = tmp[0];
  }
  buf.word32be(ndim); // ndim
  buf.word32be(0); // hasnull
  buf.word32be(types[atype].oid); // elem oid

  // for each dimension, declare
  // - size of dimension
  // - index of first item in dimension (1)
  var i;
  var tmp = value;
  for(i=0; i<ndim; i++) {
    buf.word32be(tmp.length);
    buf.word32be(1);
    tmp = tmp[0];
  }
  
  // elems are flattened on 1-dim
  var flat = flatten(value);
  var i, len = flat.length;
  for (i=0; i<len; i++) {
    deparse(buf, atype, flat[i])
  }
}
// note the type is not necessary here because it is embedded inside the
// array
var array_recv = function(buf) {
  var offset = 0;
  var UInt32Len = 4;
  var ndim = buf.readUInt32BE(offset);
  offset += UInt32Len;
  var hasnull = buf.readUInt32BE(offset);
  offset += UInt32Len;
  var typoid = buf.readUInt32BE(offset);
  offset += UInt32Len;
  var type;
  var found = false;
  for (type in types) {
    if (types[type].oid === typoid) {
      found = true;
      break;
    }
  }
  // description of dimensions
  var dims = [];
  var lowers = [];
  var len = 1;
  for (var i=0; i<ndim; i++) {
    var n = buf.readUInt32BE(offset);
    len = len * n;
    dims.push(n);
    offset += UInt32Len;
    lowers.push(buf.readUInt32BE(offset));
    offset += UInt32Len;
  }
  // fetch flattenned data
  var flat = [];
  for (var i=0; i<len; i++) {
    var fieldLen = buf.readUInt32BE(offset);
    offset += UInt32Len;
    flat.push(parse(buf.slice(offset, offset + fieldLen), type));
    offset += fieldLen;
  }
  var size;
  dims.shift();
  while(size = dims.pop()) {
    flat = chunk(flat, size);
  }
  return flat; 
}

function chunk(array, size) {
  var result = []
  for (var i=0;i<array.length;i+=size)
    result.push( array.slice(i,i+size) )
  return result
}


// Note that send function names are kept identical to their names in the PostgreSQL source code.
var types = {
  'bool':           { oid: 16,    send: boolsend,         recv: boolrecv },
  'bytea':          { oid: 17,    send: byteasend,        recv: bytearecv },
  'int2':           { oid: 21,    send: int2send,         recv: int2recv },
  'int4':           { oid: 23,    send: int4send,         recv: int4recv },
  'text':           { oid: 25,    send: textsend,         recv: textrecv },
  'json':           { oid: 114,   send: json_send,        recv: json_recv },
  'float4':         { oid: 700,   send: float4send,       recv: float4recv },
  'float8':         { oid: 701,   send: float8send,       recv: float8recv },
  'timestamptz':    { oid: 1184,  send: timestamptz_send, recv: timestamptz_recv },
  '_bool':          { oid: 1000,  send: array_send.bind(null, 'bool'), recv: array_recv },
  '_bytea':         { oid: 1001,  send: array_send.bind(null, 'bytea'), recv: array_recv },
  '_int2':          { oid: 1005,  send: array_send.bind(null, 'int2'), recv: array_recv },
  '_int4':          { oid: 1007,  send: array_send.bind(null, 'int4'), recv: array_recv },
  '_text':          { oid: 1009,  send: array_send.bind(null, 'text'), recv: array_recv },
  '_json':          { oid: 199,   send: array_send.bind(null, 'json'), recv: array_recv },
  '_float4':        { oid: 1021,  send: array_send.bind(null, 'float4'), recv: array_recv },
  '_float8':        { oid: 1022,  send: array_send.bind(null, 'float8'), recv: array_recv },
  '_timestamptz':    { oid: 1185, send: array_send.bind(null, 'timestamptz'), recv: array_recv },
}

function deparse(buf, type, value) {
    
  // Add a UInt32  placeholder for the field length
  buf.word32be(0);
  var lenField = buf.words[buf.words.length-1];

  // See [1] - Tuples Section
  // As a special case, -1 indicates a NULL field value. No value bytes follow in the NULL case
  if (value === null) {
    lenField.value = -1;

  // Then, repeated for each field in the tuple, there is a 32-bit length word followed by
  // that many bytes of field data.
  } else if (types[type]) {
    var offset = buf.len;
    types[type].send(buf, value);
    lenField.value = buf.len - offset;
  }
  return buf;
}

function parse(buf, type) {
  return types[type].recv(buf);
}


module.exports = {
  types:    types,
  deparse:  deparse,
  parse:    parse
}
