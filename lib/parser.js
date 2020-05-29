/**
 * Documentation is extracted from
 * [1] https://www.postgresql.org/docs/current/static/sql-copy.html for the COPY binary format
 * [2] https://github.com/postgres/postgres/tree/master/src/backend/utils/adt for the send/recv binary formats of types
 */

module.exports = function (txt, options) {
  return new CopyStream(txt, options)
}

const { Transform } = require('stream')
const BP = require('bufferput')
const { parse } = require('./pg_types')
const BufferList = require('bl/BufferList')

const PG_HEADER = 0
const PG_ROW_START = 1
const PG_FIELD_START = 2
const PG_FIELD_DATA = 3
const PG_FIELD_END = 4
const PG_TRAILER = 5

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

    this._buffer = new BufferList()
    this._state = PG_HEADER
    this._row = null
    this._fieldCount = null
    this._fieldIndex = null
    this._fieldLength = null
    this._fieldBuffer = null

    this.mapping = options.mapping || false
  }

  _transform(chunk, enc, cb) {
    this._buffer.append(chunk)
    while (this._buffer.length > 0) {
      if (PG_HEADER === this._state) {
        if (this._buffer.length < this.COPYHeaderFull.length) break
        if (!this.COPYHeaderFull.equals(this._buffer.slice(0, this.COPYHeaderFull.length))) {
          return cb(new Error('COPY BINARY Header mismatch'))
        }
        this._buffer.consume(this.COPYHeaderFull.length)
        this._state = PG_ROW_START
      }

      if (PG_ROW_START === this._state) {
        if (this._buffer.length < 2) break
        this._fieldCount = this._buffer.readUInt16BE(0)
        this._buffer.consume(2)
        const UInt16_0xffff = 65535
        if (this._fieldCount === UInt16_0xffff) {
          this._state = PG_TRAILER
        } else {
          this._row = this.mapping ? {} : []
          this._state = PG_FIELD_START
          this._fieldIndex = -1
        }
      }

      if (PG_TRAILER === this._state) {
        this.push(null)
        this._row = null
        this._fieldBuffer = null
        return cb()
      }

      if (PG_FIELD_START === this._state) {
        if (this._buffer.length < 4) break
        this._fieldIndex++
        this._fieldLength = this._buffer.readUInt32BE(0)
        this._buffer.consume(4)
        const UInt32_0xffffffff = 4294967295 /* Magic value for NULL */
        if (this._fieldLength === UInt32_0xffffffff) {
          this._fieldBuffer = null
          this._fieldLength = 0
          this._state = PG_FIELD_END
        } else {
          this._fieldBuffer = new BufferList()
          this._state = PG_FIELD_DATA
        }
      }

      if (PG_FIELD_DATA === this._state) {
        if (this._buffer.length === 0) break
        const bl = this._buffer.shallowSlice(0, this._fieldLength)
        this._fieldBuffer.append(bl)
        this._fieldLength -= bl.length
        this._buffer.consume(bl.length)
        if (this._fieldLength === 0) {
          this._state = PG_FIELD_END
        }
      }

      if (PG_FIELD_END === this._state) {
        if (this._fieldBuffer && this.mapping) {
          this._fieldBuffer = parse(this._fieldBuffer.slice(), this.mapping[this._fieldIndex].type)
        }
        if (this.mapping) {
          this._row[this.mapping[this._fieldIndex].key] = this._fieldBuffer
        } else {
          this._row.push(this._fieldBuffer)
        }

        this._state = PG_FIELD_START

        if (this._fieldIndex === this._fieldCount - 1) {
          this.push(this._row)
          this._state = PG_ROW_START
        }
      }
    }

    cb()
  }
}
