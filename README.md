## pg-copy-streams-binary

[![Build Status](https://travis-ci.org/jeromew/node-pg-copy-streams-binary.svg)](https://travis-ci.org/jeromew/node-pg-copy-streams-binary)

Streams for parsing and deparsing the PostgreSQL COPY binary format.
Ingest streaming data into PostgresSQL or Export data from PostgreSQL and transform it into a stream, using the COPY BINARY format.

## what are you talking about ?

Well first you have to know that PostgreSQL has not-so-well-known mechanism that helps when importing into PostgreSQL from a source (*copy-in*)
or exporting to a sink from PostgreSQL (*copy-out*)

You should first go and get familiar with the [pg-copy-streams](https://github.com/brianc/node-pg-copy-streams) module that does
the heavy lifting of handling the COPY part of the protocol flow.

## what does this module do ?

When dealing with the COPY mechanism, you can use different formats for *copy-out* or *copy-in* : text, csv or binary.

The text and csv formats are interesting but they have some limitations due to the fact that they are text based, need field separators, escaping, etc. Have you ever been in the CSV hell ? 

The PostgreSQL documentation states : Many programs produce strange and occasionally perverse CSV files, so the file format is more a convention than a standard. Thus you might encounter some files that cannot be imported using this mechanism, and COPY might produce files that other programs cannot process.

Do you want to go there ? If you take the blue pill, then this module might be for you.

It can be used to parse and deparse the PostgreSQL binary streams that are made available by the `pg-copy-streams` module.

The main API is called `transform` an tries to hide many of those details. It can be used to easily do non trivial things like :
 - transforming rows
 - expanding on the number of rows
 - forking rows into several databases at the same time, with the same of different structures 

## Example

This library is mostly interesting for ETL operations (Extract, Transformation, Load). When you just need Extract+Load, `pg-copy-streams` does the job and you don't need this library.

So Here is an example of Tranformation where you want to Extract data from database A (dsnA) and move it in two databases B (dsnB) and C (dsn C). But there is a twist.

In database A, you have table of items

```sql
CREATE TABLE item (id serial PRIMARY KEY, ref text, description text);
INSERT INTO item VALUES ('1:CTX', 'A little item');
INSERT INTO item VALUES ('2:CTX', 'A BIG item');
```

Now you realise that the references (ref column) has historically been composed of a unique id followed by a label. So the target for database B would be

```sql
CREATE TABLE product (code int4 PRIMARY KEY, label text, description text, ts_creation timestamptz, matrix int2[][]);
```

Where the refs '1:CTX' is split in (code, label).
Moreover, all the descriptions are now required to be lowercase, so you would like to clean things up on this field.
Someone in-the-know has told you that the creation timestamp of the product can be derived from the id ! Simply add `id` days to 1999-01-01T00:00:00Z.
You also need a int2 2-dim array matrix field filled with [[ id, id+1 ], [ id+2, id+3 ]] because that is what the specification says.

Table C has the simple structure

```sql
CREATE TABLE generated (body text);
```

And you want to fill it, for each source row, with a number `id` of rows (expanding the number of rows), with a body of "BODY: " + description. 

After all this is done, you want to add a line in the `generated` table with a body of "COUNT: " + total number of rows inserted (not counting this one)

Here is a code that will do just this.

```js
var pg = require('pg');
var through2 = require('through2');
var pgCopyTransform = require('pg-copy-streams-binary').transform;

var client = function(dsn) {
  var client = new pg.Client(dsn);
  client.connect();
  return client;
}

var dsnA = null; // configure database A connection parameters
var dsnB = null; // configure database B connection parameters
var dsnC = null; // configure database C connection parameters

var clientA = client(dsnA);
var clientB = client(dsnB);
var clientC = client(dsnC);

var AStream = clientA.query(copyOut('COPY item      TO   STDOUT BINARY'))
var BStream = clientB.query(copyIn ('COPY product   FROM STDIN  BINARY'))
var CStream = clientB.query(copyIn ('COPY generated FROM STDIN  BINARY'))

var transform = through2.obj(
  function(row, _, cb) {
    var id = parseInt(row.ref.split(':')[0]);
    var d = new Date('1999-01-01T00:00:00Z');
    d.setDate(d.getDate() + id);
    count++
    this.push([0,
      { type: 'int4', value: id },
      { type: 'text', value: row.ref.split(':')[1] },
      { type: 'text', value: row.description.toLowerCase() },
      { type: 'timestamptz', value: d },
      { type: '_int2', value: [ [ id, id+1 ], [ id+2, id+3 ] ] }
    ])
    while (id > 0) {
      count++
      this.push([1,
        { type: 'text', value: 'BODY: ' + row.description }
      ]);
      id--;
    }
    cb()
  },
  function(cb) {
    this.push([1,
      { type: 'text', value: 'COUNT: ' + count}
    ])
    cb()
  }
);

var count = 0;
var pct = pgCopyTransform({
  mapping: [{key:'id',type:'int4'}, {key:'ref',type:'text'},{key:'description',type:'text'}],
  transform: transform,
  targets: [BStream, CStream],
});

pct.on('close', function() {
  // Done !
  clientA.end();
  clientB.end();
  clientC.end();
})

AStream.pipe(pct);

```

The `test/transform.js` test does something along these lines to check that it works.

## API for `transform(options)`

This method returns a Writable Stream. You should pipe a `pg-copy-streams.to` Stream into it. Make sure the corresponding command is in BINARY format.

There are 3 must-have options and 1 important Event.

### option `mapping`

This is an array of { key:, type: } elements. There MUST be as many elements defined as there are columns that are fetched by the COPY command.

The keys are arbitrary and each database row in the COPY command will be translated into an object with those keys holding the values corresponding to their position in the `mapping` array and their respective position in the COPY command.

### option `targets`

This is an array of `pg-copy-streams.from` Streams. The transform operation can deliver transformed rows to several tables in several databases at the same time. Each target will be referenced by its index in the transform stream.

### option `transform`

This MUST be a classical PassThrough Stream. You can build it with `through2` for example. This stream is a Duplex Stream that takes a serie of rows as input and that outputs another serie of rows.

It can remove rows, add rows, transform rows. Whatever a through stream can do.

When rows are pushed, they should have the format

```js
this.push([index,
    { type: .., value: .. },
    { type: .., value: .. },
    { type: .., value: .. },
])
```

where `index` is an integer that corresponds to the target COPY command in the `targets` option.
The { type: .., value: ..} elements MUST correspond to the number of fields in the target COPY command and the types must correspond to the associated types in the database. The transform operation can change the types of the data in the incoming rows, but it must always adhere to the types of the target table in the COPY target because there will be no coercion in the database and the binary protocol must send the data exactly as it is expected in the table.

### Event `close`

The Writable Stream will emit a `close` event, following the node.js documentation

> The 'close' event is emitted when the stream and any of its underlying resources (a file descriptor, for example) have been closed. The event indicates that no more events will be emitted, and no further computation will occur.

Not all Streams emit a `close` event but this one does because it is necessary to wait for the end of all the underlying COPY FROM STDIN BINARY commands on the targets. `close` is emitted when all the underlying COPY commands have emitted their respective `finish` event.

## API for Deparser

### options.COPY_sendHeader

default: true
This option can be used to not send the header that PostgreSQL expects at the beginning of a COPY session.
You could use this if you want to pipe this stream to an already opened COPY session.

### options.COPY_sendTrailer

default: true
This option can be used to not send the header that PostgreSQL expects at the end of COPY session.
You could use this if you want to unpipe this stream pipe another one that will send more data and maybe finish the COPY session.

### options.mapping

default: false
By default, the Deparser expects a stream of arrays. Each array consists of `{ type: type, value: value }` elements. The length of the array MUST be equal to the number of fields in your target database table. The types MUST be identical to the types of the fields in the database (cf the "currently supported types")

This `mapping` option can be used to transform an stream of objects into such an array. mapping MUST be an array of Function elements. The prototype of the function is `function(row)`. The function can do what it wants with the row and output the necessary `{ type: type, value: value }`


## API for Parser

### options.mapping

default: false
This option can be used to describe the rows that are beeing exported from PostgreSQL. Each and every field MUST be described.
For each index in the array, you MUST put an object `{ key: name, type: type }` where name will be the name given to the field at the corresponding index in the export. Note that it does not need to be equal to the database field name. The COPY protocol does not include the field names.
The type MUST correspond to the type of the column in the database. It must be a type that is implemented in the library.

the Parser will push rows with the corresponding keys.

When `mapping` is not given, the Parser will push rows as arrays of Buffers.

## Currently supported types

For all supported types, their corresponding array version is also supported.

 * bool
 * bytea
 * int2, int4
 * float4, float8
 * text
 * json
 * timestamptz

Note that when types are mentioned in the `mapping` option, it should be stricly equal to one of theses types. pgadmin might sometimes mention aliases (like integer instead of int4) and you should not use these aliases.

The types for array (one or more dimentions) corresponds to the type prefixed with an underscore. So an array of int4, int4[], needs to be referenced as _int4 without any mention of the dimensions. This is because the dimension information is embedded in the binary format.


## Warnings & Disclaimer

There are many details in the binary protocol, and as usual, the devil is in the details.
 * Currently, operations are considered to happen on table WITHOUT OIDS. Usage on table WITH OIDS has not been tested.
 * In Arrays null placeholders are not implemented (no spot in the array can be empty).
 * In Arrays, the first element of a dimension is always at index 1.
 * Errors handling has not yet been tuned so do not expect explicit error messages


The PostgreSQL documentation states it clearly : "a binary-format file is less portable across machine architectures and PostgreSQL versions".
Tests are trying to discover issues that may appear in between PostgreSQL version but it might not work in your specific environment.
You are invited to open your debugger and submit a PR !

Use it at your own risks !

## External references

 * [COPY documentation, including binary format](https://www.postgresql.org/docs/current/static/sql-copy.html)
 * [send/recv implementations for types in PostgreSQL](https://github.com/postgres/postgres/tree/master/src/backend/utils/adt)
 * [default type OIDs in PostgreSQL catalog](https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h)

## Acknowledgments

This would not have been possible without the great work of Brian M. Carlson on the `pg` module and his breakthrough on `pg-copy-streams` showing that node.js can be used as a mediator between COPY-out and COPY-in. Thanks !

## Licence

The MIT License (MIT)

Copyright (c) 2016 Jérôme WAGNER

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


