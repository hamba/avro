![Logo](http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec)

[![Go Report Card](https://goreportcard.com/badge/github.com/hamba/avro)](https://goreportcard.com/report/github.com/hamba/avro)
[![Build Status](https://github.com/hamba/avro/actions/workflows/test.yml/badge.svg)](https://github.com/hamba/avro/actions)
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

More examples in the [godoc](https://godoc.org/github.com/hamba/avro).

#### Types Conversions

| Avro                    | Go Struct                          | Go Interface              |
| ----------------------- | ---------------------------------- | ------------------------- |
| `null`                  | `nil`                              | `nil`                     |
| `boolean`               | `bool`                             | `bool`                    |
| `bytes`                 | `[]byte`                           | `[]byte`                  |
| `float`                 | `float32`                          | `float32`                 |
| `double`                | `float64`                          | `float64`                 |
| `long`                  | `int64`                            | `int64`                   |
| `int`                   | `int`, `int32`, `int16`, `int8`    | `int`                     |
| `string`                | `string`                           | `string`                  |
| `array`                 | `[]T`                              | `[]interface{}`           |
| `enum`                  | `string`                           | `string`                  |
| `fixed`                 | `[n]byte`                          | `[]byte`                  |
| `map`                   | `map[string]T{}`                   | `map[string]interface{}`  |
| `record`                | `struct`                           | `map[string]interface{}`  |
| `union`                 | *see below*                        | *see below*               |
| `int.date`              | `time.Time`                        | `time.Time`               |
| `int.time-millis`       | `time.Duration`                    | `time.Duration`           |
| `long.time-micros`      | `time.Duration`                    | `time.Duration`           |
| `long.timestamp-millis` | `time.Time`                        | `time.Time`               |
| `long.timestamp-micros` | `time.Time`                        | `time.Time`               |
| `bytes.decimal`         | `*big.Rat`                         | `*big.Rat`                |
| `fixed.decimal`         | `*big.Rat`                         | `*big.Rat`                |

##### Unions

The following union types are accepted: `map[string]interface{}`, `*T` and `interface{}`.

* **map[string]interface{}:** If the union value is `nil`, a `nil` map will be en/decoded. 
When a non-`nil` union value is encountered, a single key is en/decoded. The key is the avro
type name, or scheam full name in the case of a named schema (enum, fixed or record).
* ***T:** This is allowed in a "nullable" union. A nullable union is defined as a two schema union, 
with one of the types being `null` (ie. `["null", "string"]` or `["string", "null"]`), in this case 
a `*T` is allowed, with `T` matching the conversion table above.
* **interface{}:** An `interface` can be provided and the type or name resolved. Primitive types
are pre-registered, but named types, maps and slices will need to be registered with the `Register` function. In the 
case of arrays and maps the enclosed schema type or name is postfix to the type
with a `:` separator, e.g `"map:string"`. If any type cannot be resolved the map type above is used unless
`Config.UnionResolutionError` is set to `true` in which case an error is returned.

##### TextMarshaler and TextUnmarshaler

The interfaces `TextMarshaler` and `TextUnmarshaler` are supported for a `string` schema type. The object will
be tested first for implementation of these interfaces, in the case of a `string` schema, before trying regular
encoding and decoding. 

## Benchmark

Benchmark source code can be found at: [https://github.com/nrwiersma/avro-benchmarks](https://github.com/nrwiersma/avro-benchmarks)

```
BenchmarkGoAvroDecode-8     	  326304	      3608 ns/op	     442 B/op	      27 allocs/op
BenchmarkGoAvroEncode-8     	  282902	      4367 ns/op	     856 B/op	      63 allocs/op
BenchmarkHambaDecode-8      	 2133788	       568 ns/op	      64 B/op	       4 allocs/op
BenchmarkHambaEncode-8      	 2635092	       444 ns/op	     112 B/op	       1 allocs/op
BenchmarkLinkedinDecode-8   	  583681	      2138 ns/op	    1728 B/op	      35 allocs/op
BenchmarkLinkedinEncode-8   	 1527082	       785 ns/op	     248 B/op	       5 allocs/op
```

Always benchmark with your own workload. The result depends heavily on the data input.
