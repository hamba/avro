package avro

import (
	"crypto/md5"
	"encoding/hex"
)

var protocolReserved = []string{"doc", "types", "messages", "protocol", "namespace"}

type Protocol struct {
	name
	properties

	types    []Schema
	messages map[string]*ProtocolMessage

	hash string
}

func NewProtocol(name, space string) (*Protocol, error) {
	n, err := newName(name, space)
	if err != nil {
		return nil, err
	}

	return &Protocol{
		name:       n,
		properties: properties{reserved: protocolReserved},
		messages:   map[string]*ProtocolMessage{},
	}, nil
}

func (p *Protocol) AddType(schema Schema) {
	p.types = append(p.types, schema)
}

func (p *Protocol) AddMessage(name string, message *ProtocolMessage) {
	p.messages[name] = message
}

func (p *Protocol) Hash() string {
	if p.hash != "" {
		return p.hash
	}

	b := md5.Sum([]byte(p.String()))
	p.hash = hex.EncodeToString(b[:])
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

type ProtocolMessage struct {
	Request  Schema
	Response Schema
	Errors   Schema
}

func (m *ProtocolMessage) String() string {
	return ""
}

func ParseProtocol(json string) (*Protocol, error) {
	return &Protocol{}, nil
}
