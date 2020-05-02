/**
 * Documentation is extracted from
 * [1] https://www.postgresql.org/docs/current/static/sql-copy.html for the COPY binary format
 * [2] https://github.com/postgres/postgres/tree/master/src/backend/utils/adt for the send/recv binary formats of types
 */

module.exports = function (txt, options) {
  return new CopyStream(txt, options)
}

const Transform = require('stream').Transform
const BP = require('bufferput')
const parse = require('./pg_types').parse

class CopyStream extends Transform {
  constructor(options) {
    options.objectMode = true
    super(options)

    // PGCOPY\n\377\r\n\0 (signature + flags field + Header extension area length)
    this.COPYHeaderFull = new BP()
      .put(Buffer.from([0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00]))
      .word32be(0)
      .word32be(0)
      .buffer()

    this.COPYTrailer = Buffer.from([0xff, 0xff])

    this._headerReceived = false
    this._trailerReceived = false
    this._remainder = false

    this.mapping = options.mapping || false
  }

  _transform(chunk, enc, cb) {
    if (this._remainder && chunk) {
      chunk = Buffer.concat([this._remainder, chunk])
    }

    let offset = 0
    if (!this._headerReceived && chunk.length >= this.COPYHeaderFull.length) {
      if (this.COPYHeaderFull.equals(chunk.slice(0, this.COPYHeaderFull.length))) {
        this._headerReceived = true
        offset += this.COPYHeaderFull.length
      }
    }

    // Copy-out mode (data transfer from the server) is initiated when the backend executes a COPY TO STDOUT SQL statement.
    // The backend sends a CopyOutResponse message to the frontend, followed by zero or more CopyData messages (always one per row)
    const UInt16Len = 2
    while (this._headerReceived && chunk.length - offset >= UInt16Len) {
      const fieldCount = chunk.readUInt16BE(offset)
      offset += 2
      const UInt32Len = 4
      const UInt16_0xff = 65535
      const UInt32_0xffffffff = 4294967295
      if (fieldCount === UInt16_0xff) {
        this._trailerReceived = true
        this.push(null)
        return cb()
      }
      const fields = this.mapping ? {} : []
      for (let i = 0; i < fieldCount; i++) {
        let v
        const fieldLen = chunk.readUInt32BE(offset)
        offset += UInt32Len
        if (fieldLen === UInt32_0xffffffff) {
          v = null
        } else {
          v = chunk.slice(offset, offset + fieldLen)
          if (this.mapping) {
            v = parse(v, this.mapping[i].type)
          }
          offset += fieldLen
        }
        if (this.mapping) {
          fields[this.mapping[i].key] = v
        } else {
          fields.push(v)
        }
      }
      this.push(fields)
    }

    if (chunk.length - offset) {
      const slice = chunk.slice(offset)
      this._remainder = slice
    } else {
      this._remainder = false
    }
    cb()
  }

  _flush(cb) {
    cb()
  }
}
