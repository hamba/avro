![Logo](http://svg.wiersma.co.za/hamba/project?title=avro&tag=Go%20avro%20codec)

[![Go Report Card](https://goreportcard.com/badge/github.com/hamba/avro)](https://goreportcard.com/report/github.com/hamba/avro)
[![Build Status](https://travis-ci.com/hamba/avro.svg?branch=master)](https://travis-ci.com/hamba/avro)
[![Coverage Status](https://coveralls.io/repos/github/hamba/avro/badge.svg?branch=master)](https://coveralls.io/github/hamba/avro?branch=master)
[![GoDoc](https://godoc.org/github.com/hamba/avro?status.svg)](https://godoc.org/github.com/hamba/avro)
[![GitHub release](https://img.shields.io/github/release/hamba/avro.svg)](https://github.com/hamba/avro/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/hamba/avro/master/LICENSE)

A fast Go avro codec

## Overview

Install with:

```shell
go get github.com/hamba/avro
```

## Usage

```go
type SimpleRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

schema, err := avro.Parse(`{
    "type": "record",
    "name": "simple",
    "namespace": "org.hamba.avro",
    "fields" : [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
    ]
}`)
if err != nil {
	log.Fatal(err)
}

in := SimpleRecord{A: 27, B: "foo"}

data, err := avro.Marshal(schema, in)
if err != nil {
	log.Fatal(err)
}

fmt.Println(data)
// Outputs: [54 6 102 111 111]

out := SimpleRecord{}
err = avro.Unmarshal(schema, data, &out)
if err != nil {
	log.Fatal(err)
}

fmt.Println(out)
// Outputs: {27 foo}
```

#### Types Conversions

| Avro     | Go Struct                          | Go Interface              |
| -------- | ---------------------------------- | ------------------------- |
| `null`   | `nil`                              | `nil`                     |
| `boolean`| `bool`                             | `bool`                    |
| `bytes`  | `[]byte`                           | `[]byte`                  |
| `float`  | `float32`                          | `float32`                 |
| `double` | `float64`                          | `float64`                 |
| `long`   | `int64`                            | `int64`                   |
| `int`    | `int`, `int32`, `int16`, `int8`    | `int`                     |
| `string` | `string`                           | `string`                  |
| `array`  | `[]T`                              | `[]interface{}`           |
| `enum`   | `string`                           | `string`                  |
| `fixed`  | `[n]byte`                          | `[]byte`                  |
| `map`    | `map[string]T{}`                   | `map[string]interface{}`  |
| `record` | `struct`                           | `map[string]interface{}`  |
| `union`  | *see below*                        | *see below*               |

##### Unions

In Go structs, the following types are accepted: `map[string]interface{}`, `*T`, 
and a `struct` implementing `avro.UnionType`. When en/decoding to an `interface{}`, a 
`map[string]interface{}` will always be used.

* **map[string]interface{}:** If the union value is `nil`, a `nil` map will be en/decoded. 
When a non-`nil` union value is encountered, a single key is en/decoded. The key is the avro
type name, or scheam full name in the case of a named schema (enum, fixed or record).
* ***T:** This is allowed in a "nullable" union. A nullable union is defined as a two schema union, 
with the first being `null` (ie. `["null", "string"]`), in this case a `*T` is allowed, 
with `T` matching the conversion table above.
* **avro.UnionType:** A `struct` in implementing `avro.UnionType` can be provided, allowing for
strong type encoding. An example can be found in the [godoc](https://godoc.org/github.com/hamba/avro).

## TODO

* Improve test coverage, docs and examples
* Logical Types
* Schema registry
* Schema
    * Refactor parsing to be cleaner
    * Better schema validation
    * Aliases
* Avro Textual Form?
