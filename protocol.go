package avro

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io/ioutil"

	jsoniter "github.com/json-iterator/go"
)

var (
	protocolReserved = []string{"doc", "types", "messages", "protocol", "namespace"}
	messageReserved  = []string{"doc", "response", "request", "errors", "one-way"}
)

// Protocol is an Avro protocol.
type Protocol struct {
	name
	properties

	types    []NamedSchema
	messages map[string]*Message

	hash string
}

// NewProtocol creates a protocol instance.
func NewProtocol(name, space string, types []NamedSchema, messages map[string]*Message) (*Protocol, error) {
	n, err := newName(name, space)
	if err != nil {
		return nil, err
	}

	p := &Protocol{
		name:       n,
		properties: properties{reserved: protocolReserved},
		types:      types,
		messages:   messages,
	}

	b := md5.Sum([]byte(p.String()))
	p.hash = hex.EncodeToString(b[:])

	return p, nil
}

// Message returns a message with the given name or nil.
func (p *Protocol) Message(name string) *Message {
	return p.messages[name]
}

// Hash returns the MD5 hash of the protocol.
func (p *Protocol) Hash() string {
	return p.hash
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

	return `{"protocol":"` + p.Name() + `","namespace":"` + p.Namespace() + `","types":[` + types + `],"messages":{` + messages + `}}`
}

// Message is an Avro protocol message.
type Message struct {
	properties

	req    *RecordSchema
	resp   Schema
	errs   *UnionSchema
	oneWay bool
}

// NewMessage creates a protocol message instance.
func NewMessage(req *RecordSchema, resp Schema, errors *UnionSchema, oneWay bool) *Message {
	return &Message{
		properties: properties{reserved: messageReserved},
		req:        req,
		resp:       resp,
		errs:       errors,
		oneWay:     oneWay,
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
	s, err := ioutil.ReadFile(path)
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

	var m map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(protocol), &m); err != nil {
		return nil, err
	}

	name, err := resolveProtocolName(m)
	if err != nil {
		return nil, err
	}

	var types []NamedSchema
	if ts, ok := m["types"].([]interface{}); ok {
		types, err = parseProtocolTypes(name.space, ts, cache)
		if err != nil {
			return nil, err
		}
	}

	messages := map[string]*Message{}
	if msgs, ok := m["messages"].(map[string]interface{}); ok {
		for k, msg := range msgs {
			m, ok := msg.(map[string]interface{})
			if !ok {
				return nil, errors.New("avro: message must be an object")
			}

			message, err := parseMessage(name.space, m, cache)
			if err != nil {
				return nil, err
			}

			messages[k] = message
		}
	}

	proto, _ := NewProtocol(name.name, name.space, types, messages)

	for k, v := range m {
		proto.AddProp(k, v)
	}

	return proto, nil
}

func parseProtocolTypes(namespace string, types []interface{}, cache *SchemaCache) ([]NamedSchema, error) {
	ts := make([]NamedSchema, len(types))
	for i, typ := range types {
		schema, err := parseType(namespace, typ, cache)
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

func parseMessage(namespace string, m map[string]interface{}, cache *SchemaCache) (*Message, error) {
	req, ok := m["request"].([]interface{})
	if !ok {
		return nil, errors.New("avro: request must have an array of fields")
	}

	fields := make([]*Field, len(req))
	for i, f := range req {
		field, err := parseField(namespace, f, cache)
		if err != nil {
			return nil, err
		}

		fields[i] = field
	}
	request := &RecordSchema{
		name:       name{},
		properties: properties{reserved: schemaReserved},
		fields:     fields,
	}

	var response Schema
	if res, ok := m["response"]; ok {
		schema, err := parseType(namespace, res, cache)
		if err != nil {
			return nil, err
		}

		if schema.Type() != Null {
			response = schema
		}
	}

	types := []Schema{NewPrimitiveSchema(String)}
	if errs, ok := m["errors"].([]interface{}); ok {
		for _, e := range errs {
			schema, err := parseType(namespace, e, cache)
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

	oneWay := false
	if o, ok := m["one-way"].(bool); ok {
		oneWay = o
		if oneWay && (len(errs.Types()) > 1 || response != nil) {
			return nil, errors.New("avro: one-way messages cannot not have a response or errors")
		}
	}

	if !oneWay && len(errs.Types()) <= 1 && response == nil {
		oneWay = true
	}

	msg := NewMessage(request, response, errs, oneWay)

	for k, v := range m {
		msg.AddProp(k, v)
	}

	return msg, nil
}

func resolveProtocolName(m map[string]interface{}) (name, error) {
	proto, ok := m["protocol"].(string)
	if !ok {
		return name{}, errors.New("avro: protocol key required")
	}

	space := ""
	if namespace, ok := m["namespace"].(string); ok {
		if namespace == "" {
			return name{}, errors.New("avro: namespace key must be non-empty or omitted")
		}

		space = namespace
	}

	return newName(proto, space)
}
