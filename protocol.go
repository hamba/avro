package avro

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io/ioutil"

	jsoniter "github.com/json-iterator/go"
)

var protocolReserved = []string{"doc", "types", "messages", "protocol", "namespace"}

type Protocol struct {
	name
	properties

	types    []NamedSchema
	messages map[string]*Message

	hash string
}

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

func (p *Protocol) Message(name string) *Message {
	return p.messages[name]
}

func (p *Protocol) Hash() string {
	return p.hash
}

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

type Message struct {
	Request  *RecordSchema
	Response Schema
	Errors   *UnionSchema
	OneWay   bool
}

func NewMessage(req *RecordSchema, resp Schema, errors *UnionSchema, oneWay bool) *Message {
	return &Message{
		Request:  req,
		Response: resp,
		Errors:   errors,
		OneWay:   oneWay,
	}
}

func (m *Message) String() string {
	fields := ""
	for _, f := range m.Request.fields {
		fields += f.String() + ","
	}
	if len(fields) > 0 {
		fields = fields[:len(fields)-1]
	}

	return `{"request":[` + fields + `]}`
}

func ParseProtocolFile(path string) (*Protocol, error) {
	s, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return ParseProtocol(string(s))
}

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

	return NewProtocol(name.name, name.space, types, messages)
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

	return NewMessage(request, nil, nil, false), nil
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
