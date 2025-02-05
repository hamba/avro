<picture>
  <source media="(prefers-color-scheme: dark)" srcset="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec&mode=dark">
  <source media="(prefers-color-scheme: light)" srcset="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec">
  <img alt="Logo" src="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec">
</picture>

[![Go Report Card](https://goreportcard.com/badge/github.com/hamba/avro/v2)](https://goreportcard.com/report/github.com/hamba/avro/v2)
[![Build Status](https://github.com/hamba/avro/actions/workflows/test.yml/badge.svg)](https://github.com/hamba/avro/actions)
[![Coverage Status](https://coveralls.io/repos/github/hamba/avro/badge.svg?branch=main)](https://coveralls.io/github/hamba/avro?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/hamba/avro/v2.svg)](https://pkg.go.dev/github.com/hamba/avro/v2)
[![GitHub release](https://img.shields.io/github/release/hamba/avro.svg)](https://github.com/hamba/avro/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/hamba/avro/master/LICENSE)

A fast Go avro codec

## Overview

Install with:

```shell
go get github.com/hamba/avro/v2
```

**Note:** This project has renamed the default branch from `master` to `main`. You will need to update your local environment.

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

More examples in the [godoc](https://pkg.go.dev/github.com/hamba/avro/v2).

#### Types Conversions

| Avro                          | Go Struct                                                  | Go Interface             |
|-------------------------------|------------------------------------------------------------|--------------------------|
| `null`                        | `nil`                                                      | `nil`                    |
| `boolean`                     | `bool`                                                     | `bool`                   |
| `bytes`                       | `[]byte`                                                   | `[]byte`                 |
| `float`                       | `float32`                                                  | `float32`                |
| `double`                      | `float64`                                                  | `float64`                |
| `long`                        | `int`\*, `int64`, `uint32`\**                              | `int`, `int64`, `uint32` |
| `int`                         | `int`\*, `int32`, `int16`, `int8`, `uint8`\**, `uint16`\** | `int`, `uint8`, `uint16` |
| `fixed`                       | `uint64`                                                   | `uint64`                 |
| `string`                      | `string`                                                   | `string`                 |
| `array`                       | `[]T`                                                      | `[]any`                  |
| `enum`                        | `string`                                                   | `string`                 |
| `fixed`                       | `[n]byte`                                                  | `[n]byte`                |
| `map`                         | `map[string]T{}`                                           | `map[string]any`         |
| `record`                      | `struct`                                                   | `map[string]any`         |
| `union`                       | *see below*                                                | *see below*              |
| `int.date`                    | `time.Time`                                                | `time.Time`              |
| `int.time-millis`             | `time.Duration`                                            | `time.Duration`          |
| `long.time-micros`            | `time.Duration`                                            | `time.Duration`          |
| `long.timestamp-millis`       | `time.Time`                                                | `time.Time`              |
| `long.timestamp-micros`       | `time.Time`                                                | `time.Time`              |
| `long.local-timestamp-millis` | `time.Time`                                                | `time.Time`              |
| `long.local-timestamp-micros` | `time.Time`                                                | `time.Time`              |
| `bytes.decimal`               | `*big.Rat`                                                 | `*big.Rat`               |
| `fixed.decimal`               | `*big.Rat`                                                 | `*big.Rat`               |
| `string.uuid`                 | `string`                                                   | `string`                 |

\* Please note that the size of the Go type `int` is platform dependent. Decoding an Avro `long` into a Go `int` is
only allowed on 64-bit platforms and will result in an error on 32-bit platforms. Similarly, be careful when encoding a
Go `int` using Avro `int` on a 64-bit platform, as that can result in an integer overflow causing misinterpretation of
the data.

\** Please note that when the Go type is an unsigned integer care must be taken to ensure that information is not lost
when converting between the Avro type and Go type. For example, storing a *negative* number in Avro of `int = -100`
would be interpreted as `uint16 = 65,436` in Go. Another example would be storing numbers in Avro `int = 256` that 
are larger than the Go type `uint8 = 0`. 

##### Unions

The following union types are accepted: `map[string]any`, `*T` and `any`.

* **map[string]any:** If the union value is `nil`, a `nil` map will be en/decoded. 
When a non-`nil` union value is encountered, a single key is en/decoded. The key is the avro
type name, or scheam full name in the case of a named schema (enum, fixed or record).
* ***T:** This is allowed in a "nullable" union. A nullable union is defined as a two schema union, 
with one of the types being `null` (ie. `["null", "string"]` or `["string", "null"]`), in this case 
a `*T` is allowed, with `T` matching the conversion table above. In the case of a slice, the slice can be used
directly.
* **any:** An `interface` can be provided and the type or name resolved. Primitive types
are pre-registered, but named types, maps and slices will need to be registered with the `Register` function. 
In the case of arrays and maps the enclosed schema type or name is postfix to the type with a `:` separator, 
e.g `"map:string"`. Behavior when a type cannot be resolved will depend on your chosen configuation options:
	* !Config.UnionResolutionError && !Config.PartialUnionTypeResolution: the map type above is used
	* Config.UnionResolutionError && !Config.PartialUnionTypeResolution: an error is returned
	* !Config.UnionResolutionError && Config.PartialUnionTypeResolution: any registered type will get resolved while any unregistered type will fallback to the map type above.
	* Config.UnionResolutionError && !Config.PartialUnionTypeResolution: any registered type will get resolved while any unregistered type will return an error.

##### TextMarshaler and TextUnmarshaler

The interfaces `TextMarshaler` and `TextUnmarshaler` are supported for a `string` schema type. The object will
be tested first for implementation of these interfaces, in the case of a `string` schema, before trying regular
encoding and decoding.

Enums may also implement `TextMarshaler` and `TextUnmarshaler`, and must resolve to valid symbols in the given enum schema.

##### Identical Underlying Types

One type can be [ConvertibleTo](https://go.dev/ref/spec#Conversions) another type if they have identical underlying types. 
A non-native type is allowed be used if it can be convertible to *time.Time*, *big.Rat* or *avro.LogicalDuration* for the particular of *LogicalTypes*.

Ex.: `type Timestamp time.Time`

##### Untrusted Input With Bytes and Strings

For security reasons, the configuration `Config.MaxByteSliceSize` restricts the maximum size of `bytes` and `string` types created
by the `Reader`. The default maximum size is `1MiB` and is configurable. This is required to stop untrusted input from consuming all memory and
crashing the application. Should this not be need, setting a negative number will disable the behaviour.

## Benchmark

Benchmark source code can be found at: [https://github.com/nrwiersma/avro-benchmarks](https://github.com/nrwiersma/avro-benchmarks)

```
BenchmarkGoAvroDecode-8      	  788455	      1505 ns/op	     418 B/op	      27 allocs/op
BenchmarkGoAvroEncode-8      	  624343	      1908 ns/op	     806 B/op	      63 allocs/op
BenchmarkGoGenAvroDecode-8   	 1360375	       876.4 ns/op	     320 B/op	      11 allocs/op
BenchmarkGoGenAvroEncode-8   	 2801583	       425.9 ns/op	     240 B/op	       3 allocs/op
BenchmarkHambaDecode-8       	 5046832	       238.7 ns/op	      47 B/op	       0 allocs/op
BenchmarkHambaEncode-8       	 6017635	       196.2 ns/op	     112 B/op	       1 allocs/op
BenchmarkLinkedinDecode-8    	 1000000	      1003 ns/op	    1688 B/op	      35 allocs/op
BenchmarkLinkedinEncode-8    	 3170553	       381.5 ns/op	     248 B/op	       5 allocs/op
```

Always benchmark with your own workload. The result depends heavily on the data input.

## Go structs generation

Go structs can be generated for you from the schema. The types generated follow the same logic in [types conversions](#types-conversions)

Install the struct generator with:

```shell
go install github.com/hamba/avro/v2/cmd/avrogen@<version>
```

Example usage assuming there's a valid schema in `in.avsc`:

```shell
avrogen -pkg avro -o bla.go -tags json:snake,yaml:upper-camel in.avsc
```

**Tip:** Omit `-o FILE` to dump the generated Go structs to stdout instead of a file.

Check the options and usage with `-h`:

```shell
avrogen -h
```

Or use it as a lib in internal commands, it's the `gen` package

## Avro schema validation

### avrosv

A small Avro schema validation command-line utility is also available. This simple tool leverages the
schema parsing functionality of the library, showing validation errors or optionally dumping parsed
schemas to the console. It can be used in CI/CD pipelines to validate schema changes in a repository.

Install the Avro schema validator with:

```shell
go install github.com/hamba/avro/v2/cmd/avrosv@<version>
```

Example usage assuming there's a valid schema in `in.avsc` (exit status code is `0`):

```shell
avrosv in.avsc
```

An invalid schema will result in a diagnostic output and a non-zero exit status code:

```shell
avrosv bad-default-schema.avsc; echo $?
Error: avro: invalid default for field someString. <nil> not a string
2
```

Schemas referencing other schemas can also be validated by providing all of them (schemas are parsed in order):

```shell
avrosv base-schema.avsc schema-withref.avsc
```

Check the options and usage with `-h`:

```shell
avrosv -h
```

### Name Validation

Avro names are validated according to the
[Avro specification](https://avro.apache.org/docs/1.11.1/specification/#names).

However, the official Java library does not validate said names accordingly, resulting to some files out in the wild
to have invalid names. Thus, this library has a configuration option to allow for these invalid names to be parsed.

```go
avro.SkipNameValidation = true
```

Note that this variable is global, so ideally you'd need to unset it after you're done with the invalid schema.

## Go Version Support

This library supports the last two versions of Go. While the minimum Go version is
not guaranteed to increase along side Go, it may jump from time to time to support 
additional features. This will be not be considered a breaking change.

## Who uses hamba/avro?

- [Apache Arrow for Go](https://github.com/apache/arrow-go)
- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
- [pulsar-client-go](https://github.com/apache/pulsar-client-go)
