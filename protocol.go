package avro

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/mapstructure"
)

var (
	protocolReserved = []string{"doc", "types", "messages", "protocol", "namespace"}
	messageReserved  = []string{"doc", "response", "request", "errors", "one-way"}
)

type protocolConfig struct {
	doc   string
	props map[string]any
}

// ProtocolOption is a function that sets a protocol option.
type ProtocolOption func(*protocolConfig)

// WithProtoDoc sets the doc on a protocol.
func WithProtoDoc(doc string) ProtocolOption {
	return func(opts *protocolConfig) {
		opts.doc = doc
	}
}

// WithProtoProps sets the properties on a protocol.
func WithProtoProps(props map[string]any) ProtocolOption {
	return func(opts *protocolConfig) {
		opts.props = props
	}
}

// Protocol is an Avro protocol.
type Protocol struct {
	name
	properties

	types    []NamedSchema
	messages map[string]*Message

	doc string

	hash string
}

// NewProtocol creates a protocol instance.
func NewProtocol(
	name, namepsace string,
	types []NamedSchema,
	messages map[string]*Message,
	opts ...ProtocolOption,
) (*Protocol, error) {
	var cfg protocolConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	n, err := newName(name, namepsace, nil)
	if err != nil {
		return nil, err
	}

	p := &Protocol{
		name:       n,
		properties: newProperties(cfg.props, protocolReserved),
		types:      types,
		messages:   messages,
		doc:        cfg.doc,
	}

	b := md5.Sum([]byte(p.String()))
	p.hash = hex.EncodeToString(b[:])

	return p, nil
}

// Message returns a message with the given name or nil.
func (p *Protocol) Message(name string) *Message {
	return p.messages[name]
}

// Doc returns the protocol doc.
func (p *Protocol) Doc() string {
	return p.doc
}

// Hash returns the MD5 hash of the protocol.
func (p *Protocol) Hash() string {
	return p.hash
}

// Types returns the types of the protocol.
func (p *Protocol) Types() []NamedSchema {
	return p.types
}

// String returns the canonical form of the protocol.
func (p *Protocol) String() string {
	types := ""
	for _, f := range p.types {
		types += f.String() + ","
	}
	if len(types) > 0 {
		types = types[:len(types)-1]
	}

	messages := ""
	for k, m := range p.messages {
		messages += `"` + k + `":` + m.String() + ","
	}
	if len(messages) > 0 {
		messages = messages[:len(messages)-1]
	}

	return `{"protocol":"` + p.Name() +
		`","namespace":"` + p.Namespace() +
		`","types":[` + types + `],"messages":{` + messages + `}}`
}

// Message is an Avro protocol message.
type Message struct {
	properties

	req    *RecordSchema
	resp   Schema
	errs   *UnionSchema
	oneWay bool

	doc string
}

// NewMessage creates a protocol message instance.
func NewMessage(req *RecordSchema, resp Schema, errors *UnionSchema, oneWay bool, opts ...ProtocolOption) *Message {
	var cfg protocolConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Message{
		properties: newProperties(cfg.props, messageReserved),
		req:        req,
		resp:       resp,
		errs:       errors,
		oneWay:     oneWay,
		doc:        cfg.doc,
	}
}

// Request returns the message request schema.
func (m *Message) Request() *RecordSchema {
	return m.req
}

// Response returns the message response schema.
func (m *Message) Response() Schema {
	return m.resp
}

// Errors returns the message errors union schema.
func (m *Message) Errors() *UnionSchema {
	return m.errs
}

// OneWay determines of the message is a one way message.
func (m *Message) OneWay() bool {
	return m.oneWay
}

// Doc returns the message doc.
func (m *Message) Doc() string {
	return m.doc
}

// String returns the canonical form of the message.
func (m *Message) String() string {
	fields := ""
	for _, f := range m.req.fields {
		fields += f.String() + ","
	}
	if len(fields) > 0 {
		fields = fields[:len(fields)-1]
	}

	str := `{"request":[` + fields + `]`
	if m.resp != nil {
		str += `,"response":` + m.resp.String()
	}
	if m.errs != nil && len(m.errs.Types()) > 1 {
		errs, _ := NewUnionSchema(m.errs.Types()[1:])
		str += `,"errors":` + errs.String()
	}
	str += "}"
	return str
}

// ParseProtocolFile parses an Avro protocol from a file.
func ParseProtocolFile(path string) (*Protocol, error) {
	s, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return ParseProtocol(string(s))
}

// MustParseProtocol parses an Avro protocol, panicing if there is an error.
func MustParseProtocol(protocol string) *Protocol {
	parsed, err := ParseProtocol(protocol)
	if err != nil {
		panic(err)
	}

	return parsed
}

// ParseProtocol parses an Avro protocol.
func ParseProtocol(protocol string) (*Protocol, error) {
	cache := &SchemaCache{}

	var m map[string]any
	if err := jsoniter.Unmarshal([]byte(protocol), &m); err != nil {
		return nil, err
	}

	seen := seenCache{}
	return parseProtocol(m, seen, cache)
}

type protocol struct {
	Protocol  string                    `mapstructure:"protocol"`
	Namespace string                    `mapstructure:"namespace"`
	Doc       string                    `mapstructure:"doc"`
	Types     []any                     `mapstructure:"types"`
	Messages  map[string]map[string]any `mapstructure:"messages"`
	Props     map[string]any            `mapstructure:",remain"`
}

func parseProtocol(m map[string]any, seen seenCache, cache *SchemaCache) (*Protocol, error) {
	var (
		p    protocol
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &p, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding protocol: %w", err)
	}

	if err := checkParsedName(p.Protocol, p.Namespace, hasKey(meta.Keys, "namespace")); err != nil {
		return nil, err
	}

	var (
		types []NamedSchema
		err   error
	)
	if len(p.Types) > 0 {
		types, err = parseProtocolTypes(p.Namespace, p.Types, seen, cache)
		if err != nil {
			return nil, err
		}
	}

	messages := map[string]*Message{}
	if len(p.Messages) > 0 {
		for k, msg := range p.Messages {
			message, err := parseMessage(p.Namespace, msg, seen, cache)
			if err != nil {
				return nil, err
			}

			messages[k] = message
		}
	}

	return NewProtocol(p.Protocol, p.Namespace, types, messages, WithProtoDoc(p.Doc), WithProtoProps(p.Props))
}

func parseProtocolTypes(namespace string, types []any, seen seenCache, cache *SchemaCache) ([]NamedSchema, error) {
	ts := make([]NamedSchema, len(types))
	for i, typ := range types {
		schema, err := parseType(namespace, typ, seen, cache)
		if err != nil {
			return nil, err
		}

		namedSchema, ok := schema.(NamedSchema)
		if !ok {
			return nil, errors.New("avro: protocol types must be named schemas")
		}

		ts[i] = namedSchema
	}

	return ts, nil
}

type message struct {
	Doc      string           `mapstructure:"doc"`
	Request  []map[string]any `mapstructure:"request"`
	Response any              `mapstructure:"response"`
	Errors   []any            `mapstructure:"errors"`
	OneWay   bool             `mapstructure:"one-way"`
	Props    map[string]any   `mapstructure:",remain"`
}

func parseMessage(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (*Message, error) {
	var (
		msg  message
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &msg, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding message: %w", err)
	}

	fields := make([]*Field, len(msg.Request))
	for i, f := range msg.Request {
		field, err := parseField(namespace, f, seen, cache)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	request := &RecordSchema{
		name:       name{},
		properties: properties{},
		fields:     fields,
	}

	var response Schema
	if msg.Response != nil {
		schema, err := parseType(namespace, msg.Response, seen, cache)
		if err != nil {
			return nil, err
		}

		if schema.Type() != Null {
			response = schema
		}
	}

	types := []Schema{NewPrimitiveSchema(String, nil)}
	if len(msg.Errors) > 0 {
		for _, e := range msg.Errors {
			schema, err := parseType(namespace, e, seen, cache)
			if err != nil {
				return nil, err
			}

			if rec, ok := schema.(*RecordSchema); ok && !rec.IsError() {
				return nil, errors.New("avro: errors record schema must be of type error")
			}

			types = append(types, schema)
		}
	}
	errs, err := NewUnionSchema(types)
	if err != nil {
		return nil, err
	}

	oneWay := msg.OneWay
	if hasKey(meta.Keys, "one-way") && oneWay && (len(errs.Types()) > 1 || response != nil) {
		return nil, errors.New("avro: one-way messages cannot not have a response or errors")
	}
	if !oneWay && len(errs.Types()) <= 1 && response == nil {
		oneWay = true
	}

	return NewMessage(request, response, errs, oneWay, WithProtoDoc(msg.Doc), WithProtoProps(msg.Props)), nil
}
