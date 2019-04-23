/*
Package registry implements a Confluent Schema Registry compliant client.

See the Confluent Schema Registry docs for an understanding of the API: https://docs.confluent.io/current/schema-registry/docs/api.html

*/
package registry

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hamba/avro"
	"github.com/json-iterator/go"
	"github.com/modern-go/concurrent"
)

const (
	contentType = "application/vnd.schemaregistry.v1+json"
)

// Registry represents a schema registry.
type Registry interface {
	// GetSchema returns the schema with the given id.
	GetSchema(id int) (avro.Schema, error)

	// GetSubjects gets the registry subjects.
	GetSubjects() ([]string, error)

	// GetVersions gets the schema versions for a subject.
	GetVersions(subject string) ([]int, error)

	// GetSchemaByVersion gets the schema by version.
	GetSchemaByVersion(subject string, version int) (avro.Schema, error)

	// GetLatestSchema gets the latest schema for a subject.
	GetLatestSchema(subject string) (avro.Schema, error)

	// CreateSchema creates a schema in the registry, returning the schema id.
	CreateSchema(subject string, schema string) (int, avro.Schema, error)

	// IsRegistered determines of the schema is registered.
	IsRegistered(subject string, schema string) (int, avro.Schema, error)
}

type schemaPayload struct {
	Schema string `json:"schema"`
}

type idPayload struct {
	ID int `json:"id"`
}

var defaultClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 90 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 3 * time.Second,
	},
}

// ClientFunc is a function used to customize the Client.
type ClientFunc func(*Client)

// WithHTTPClient sets the http client to make requests with.
func WithHTTPClient(client *http.Client) ClientFunc {
	return func(c *Client) {
		c.client = client
	}
}

// Client is an HTTP registry client
type Client struct {
	client *http.Client
	base   string

	cache *concurrent.Map // map[int]avro.Schema
}

// NewClient creates a schema registry Client with the given base url.
func NewClient(baseURL string, opts ...ClientFunc) (*Client, error) {
	_, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	c := &Client{
		client: defaultClient,
		base:   baseURL,
		cache:  concurrent.NewMap(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// GetSchema returns the schema with the given id.
//
// GetSchema will cache the schema in memory after it is successfully returned,
// allowing it to be used efficiently in a high load situation.
func (c *Client) GetSchema(id int) (avro.Schema, error) {
	if schema, ok := c.cache.Load(id); ok {
		return schema.(avro.Schema), nil
	}

	var payload schemaPayload
	err := c.request(http.MethodGet, "/schemas/ids/"+strconv.Itoa(id), nil, &payload)
	if err != nil {
		return nil, err
	}

	schema, err := avro.Parse(payload.Schema)
	if err != nil {
		return nil, err
	}

	c.cache.Store(id, schema)

	return schema, nil
}

// GetSubjects gets the registry subjects.
func (c *Client) GetSubjects() ([]string, error) {
	var subjects []string
	err := c.request(http.MethodGet, "/subjects", nil, &subjects)
	if err != nil {
		return nil, err
	}

	return subjects, err
}

// GetVersions gets the schema versions for a subject.
func (c *Client) GetVersions(subject string) ([]int, error) {
	var versions []int
	err := c.request(http.MethodGet, "/subjects/"+subject+"/versions", nil, &versions)
	if err != nil {
		return nil, err
	}

	return versions, err
}

// GetSchemaByVersion gets the schema by version.
func (c *Client) GetSchemaByVersion(subject string, version int) (avro.Schema, error) {
	var payload schemaPayload
	err := c.request(http.MethodGet, "/subjects/"+subject+"/versions/"+strconv.Itoa(version), nil, &payload)
	if err != nil {
		return nil, err
	}

	return avro.Parse(payload.Schema)
}

// GetLatestSchema gets the latest schema for a subject.
func (c *Client) GetLatestSchema(subject string) (avro.Schema, error) {
	var payload schemaPayload
	err := c.request(http.MethodGet, "/subjects/"+subject+"/versions/latest", nil, &payload)
	if err != nil {
		return nil, err
	}

	return avro.Parse(payload.Schema)
}

// CreateSchema creates a schema in the registry, returning the schema id.
func (c *Client) CreateSchema(subject string, schema string) (int, avro.Schema, error) {
	var payload idPayload
	err := c.request(http.MethodPost, "/subjects/"+subject+"/versions", schemaPayload{Schema: schema}, &payload)
	if err != nil {
		return 0, nil, err
	}

	sch, err := avro.Parse(schema)
	return payload.ID, sch, err
}

// IsRegistered determines of the schema is registered.
func (c *Client) IsRegistered(subject string, schema string) (int, avro.Schema, error) {
	var payload idPayload
	err := c.request(http.MethodPost, "/subjects/"+subject, schemaPayload{Schema: schema}, &payload)
	if err != nil {
		return 0, nil, err
	}

	sch, err := avro.Parse(schema)
	return payload.ID, sch, err
}

func (c *Client) request(method, uri string, in, out interface{}) error {
	var body io.Reader
	if in != nil {
		b, _ := jsoniter.Marshal(in)
		body = bytes.NewReader(b)
	}

	req, _ := http.NewRequest(method, c.base+uri, body) // This error is not possible as we already parsed the url
	req.Header.Set("Content-Type", contentType)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		err := Error{StatusCode: resp.StatusCode}
		_ = jsoniter.NewDecoder(resp.Body).Decode(&err)
		return err
	}

	return jsoniter.NewDecoder(resp.Body).Decode(out)
}

// Error is returned by the registry when there is an error.
type Error struct {
	StatusCode int `json:"-"`

	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

// Error returns the error message.
func (e Error) Error() string {
	if e.Message != "" {
		return e.Message
	}

	return "registry error: " + strconv.Itoa(e.StatusCode)
}
