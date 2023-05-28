// Package registry implements a Confluent Schema Registry compliant client.
//
// See the Confluent Schema Registry docs for an understanding of the
// API: https://docs.confluent.io/current/schema-registry/docs/api.html
package registry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	jsoniter "github.com/json-iterator/go"
)

const contentType = "application/vnd.schemaregistry.v1+json"

// Registry represents a schema registry.
type Registry interface {
	// GetSchema returns the schema with the given id.
	GetSchema(ctx context.Context, id int) (avro.Schema, error)

	// DeleteSubject delete subject.
	DeleteSubject(ctx context.Context, subject string) ([]int, error)

	// GetSubjects gets the registry subjects.
	GetSubjects(ctx context.Context) ([]string, error)

	// GetVersions gets the schema versions for a subject.
	GetVersions(ctx context.Context, subject string) ([]int, error)

	// GetSchemaByVersion gets the schema by version.
	GetSchemaByVersion(ctx context.Context, subject string, version int) (avro.Schema, error)

	// GetLatestSchema gets the latest schema for a subject.
	GetLatestSchema(ctx context.Context, subject string) (avro.Schema, error)

	// GetSchemaInfo gets the schema and schema metadata for a subject and version.
	GetSchemaInfo(ctx context.Context, subject string, version int) (SchemaInfo, error)

	// GetLatestSchemaInfo gets the latest schema and schema metadata for a subject.
	GetLatestSchemaInfo(ctx context.Context, subject string) (SchemaInfo, error)

	// CreateSchema creates a schema in the registry, returning the schema id.
	CreateSchema(ctx context.Context, subject, schema string, references ...SchemaReference) (int, avro.Schema, error)

	// IsRegistered determines if the schema is registered.
	IsRegistered(ctx context.Context, subject, schema string) (int, avro.Schema, error)

	// IsRegisteredWithRefs determines if the schema is registered, with optional referenced schemas.
	IsRegisteredWithRefs(ctx context.Context, subject, schema string, refs ...SchemaReference) (int, avro.Schema, error)
}

type schemaPayload struct {
	Schema     string            `json:"schema"`
	References []SchemaReference `json:"references,omitempty"`
}

// SchemaReference represents a schema reference.
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type idPayload struct {
	ID int `json:"id"`
}

type credentials struct {
	username string
	password string
}

type schemaInfoPayload struct {
	Schema  string `json:"schema"`
	ID      int    `json:"id"`
	Version int    `json:"version"`
}

// Parse converts the string schema registry response into a
// SchemaInfo object with an avro.Schema schema.
func (s *schemaInfoPayload) Parse() (info SchemaInfo, err error) {
	info = SchemaInfo{
		ID:      s.ID,
		Version: s.Version,
	}
	info.Schema, err = avro.Parse(s.Schema)
	return info, err
}

// SchemaInfo represents a schema and metadata information.
type SchemaInfo struct {
	Schema  avro.Schema
	ID      int
	Version int
}

var defaultClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 90 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 3 * time.Second,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 10 * time.Second,
}

// ClientFunc is a function used to customize the Client.
type ClientFunc func(*Client)

// WithHTTPClient sets the http client to make requests with.
func WithHTTPClient(client *http.Client) ClientFunc {
	return func(c *Client) {
		c.client = client
	}
}

// WithBasicAuth sets the credentials to perform http basic auth.
func WithBasicAuth(username, password string) ClientFunc {
	return func(c *Client) {
		c.creds = credentials{username: username, password: password}
	}
}

// Client is an HTTP registry client.
type Client struct {
	client *http.Client
	base   *url.URL

	creds credentials

	cache sync.Map // map[int]avro.Schema
}

// NewClient creates a schema registry Client with the given base url.
func NewClient(baseURL string, opts ...ClientFunc) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}

	c := &Client{
		client: defaultClient,
		base:   u,
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
func (c *Client) GetSchema(ctx context.Context, id int) (avro.Schema, error) {
	if schema, ok := c.cache.Load(id); ok {
		return schema.(avro.Schema), nil
	}

	var resp schemaPayload
	p := path.Join("schemas", "ids", strconv.Itoa(id))
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return nil, err
	}

	schema, err := avro.Parse(resp.Schema)
	if err != nil {
		return nil, err
	}

	c.cache.Store(id, schema)

	return schema, nil
}

// GetSubjects gets the registry subjects.
func (c *Client) GetSubjects(ctx context.Context) ([]string, error) {
	var subjects []string
	if err := c.request(ctx, http.MethodGet, "subjects", nil, &subjects); err != nil {
		return nil, err
	}
	return subjects, nil
}

// DeleteSubject delete subject.
func (c *Client) DeleteSubject(ctx context.Context, subject string) ([]int, error) {
	var versions []int
	p := path.Join("subjects", subject)
	if err := c.request(ctx, http.MethodDelete, p, nil, &versions); err != nil {
		return nil, err
	}

	return versions, nil
}

// GetVersions gets the schema versions for a subject.
func (c *Client) GetVersions(ctx context.Context, subject string) ([]int, error) {
	var versions []int
	p := path.Join("subjects", subject, "versions")
	if err := c.request(ctx, http.MethodGet, p, nil, &versions); err != nil {
		return nil, err
	}
	return versions, nil
}

// GetSchemaByVersion gets the schema by version.
func (c *Client) GetSchemaByVersion(ctx context.Context, subject string, version int) (avro.Schema, error) {
	var resp schemaPayload
	p := path.Join("subjects", subject, "versions", strconv.Itoa(version))
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return nil, err
	}
	return avro.Parse(resp.Schema)
}

// GetLatestSchema gets the latest schema for a subject.
func (c *Client) GetLatestSchema(ctx context.Context, subject string) (avro.Schema, error) {
	var resp schemaPayload
	p := path.Join("subjects", subject, "versions", "latest")
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return nil, err
	}
	return avro.Parse(resp.Schema)
}

// GetSchemaInfo gets the schema and schema metadata for a subject and version.
func (c *Client) GetSchemaInfo(ctx context.Context, subject string, version int) (SchemaInfo, error) {
	var resp schemaInfoPayload
	p := path.Join("subjects", subject, "versions", strconv.Itoa(version))
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return SchemaInfo{}, err
	}
	return resp.Parse()
}

// GetLatestSchemaInfo gets the latest schema and schema metadata for a subject.
func (c *Client) GetLatestSchemaInfo(ctx context.Context, subject string) (SchemaInfo, error) {
	var resp schemaInfoPayload
	p := path.Join("subjects", subject, "versions", "latest")
	if err := c.request(ctx, http.MethodGet, p, nil, &resp); err != nil {
		return SchemaInfo{}, err
	}
	return resp.Parse()
}

// CreateSchema creates a schema in the registry, returning the schema id.
func (c *Client) CreateSchema(
	ctx context.Context,
	subject, schema string,
	references ...SchemaReference,
) (int, avro.Schema, error) {
	var resp idPayload
	req := schemaPayload{Schema: schema, References: references}
	p := path.Join("subjects", subject, "versions")
	if err := c.request(ctx, http.MethodPost, p, req, &resp); err != nil {
		return 0, nil, err
	}

	sch, err := avro.Parse(schema)
	return resp.ID, sch, err
}

// IsRegistered determines if the schema is registered.
func (c *Client) IsRegistered(ctx context.Context, subject, schema string) (int, avro.Schema, error) {
	return c.IsRegisteredWithRefs(ctx, subject, schema)
}

// IsRegisteredWithRefs determines if the schema is registered, with optional referenced schemas.
func (c *Client) IsRegisteredWithRefs(
	ctx context.Context,
	subject, schema string,
	references ...SchemaReference,
) (int, avro.Schema, error) {
	var resp idPayload
	req := schemaPayload{Schema: schema, References: references}
	if err := c.request(ctx, http.MethodPost, path.Join("subjects", subject), req, &resp); err != nil {
		return 0, nil, err
	}

	sch, err := avro.Parse(schema)
	return resp.ID, sch, err
}

// Compatibility levels.
const (
	BackwardCL           string = "BACKWARD"
	BackwardTransitiveCL string = "BACKWARD_TRANSITIVE"
	ForwardCL            string = "FORWARD"
	ForwardTransitiveCL  string = "FORWARD_TRANSITIVE"
	FullCL               string = "FULL"
	FullTransitiveCL     string = "FULL_TRANSITIVE"
	NoneCL               string = "NONE"
)

func validateCompatibilityLevel(lvl string) error {
	switch lvl {
	case BackwardCL, BackwardTransitiveCL, ForwardCL, ForwardTransitiveCL, FullCL, FullTransitiveCL, NoneCL:
		return nil
	default:
		return fmt.Errorf("invalid compatibility level %s", lvl)
	}
}

type compatPayload struct {
	Compatibility string `json:"compatibility"`
}

// SetGlobalCompatibilityLevel sets the global compatibility level of the registry.
func (c *Client) SetGlobalCompatibilityLevel(ctx context.Context, lvl string) error {
	if err := validateCompatibilityLevel(lvl); err != nil {
		return err
	}

	req := compatPayload{Compatibility: lvl}
	return c.request(ctx, http.MethodPut, "config", req, nil)
}

// SetCompatibilityLevel sets the compatibility level of a subject.
func (c *Client) SetCompatibilityLevel(ctx context.Context, subject, lvl string) error {
	if err := validateCompatibilityLevel(lvl); err != nil {
		return err
	}

	req := compatPayload{Compatibility: lvl}
	return c.request(ctx, http.MethodPut, path.Join("config", subject), req, nil)
}

// GetGlobalCompatibilityLevel gets the global compatibility level.
func (c *Client) GetGlobalCompatibilityLevel(ctx context.Context) (string, error) {
	var resp compatPayload
	if err := c.request(ctx, http.MethodGet, "config", nil, &resp); err != nil {
		return "", err
	}
	return resp.Compatibility, nil
}

// GetCompatibilityLevel gets the compatibility level of a subject.
func (c *Client) GetCompatibilityLevel(ctx context.Context, subject string) (string, error) {
	var resp compatPayload
	if err := c.request(ctx, http.MethodGet, path.Join("config", subject), nil, &resp); err != nil {
		return "", err
	}
	return resp.Compatibility, nil
}

func (c *Client) request(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, _ := jsoniter.Marshal(in)
		body = bytes.NewReader(b)
	}

	// These errors are not possible as we have already parse the base URL.
	u, _ := c.base.Parse(path)
	req, _ := http.NewRequestWithContext(ctx, method, u.String(), body)
	req.Header.Set("Content-Type", contentType)

	if len(c.creds.username) > 0 || len(c.creds.password) > 0 {
		req.SetBasicAuth(c.creds.username, c.creds.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not perform request: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		err := Error{StatusCode: resp.StatusCode}
		_ = jsoniter.NewDecoder(resp.Body).Decode(&err)
		return err
	}

	if out != nil {
		return jsoniter.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// Error is returned by the registry when there is an error.
type Error struct {
	StatusCode int    `json:"-"`
	Code       int    `json:"error_code"`
	Message    string `json:"message"`
}

// Error returns the error message.
func (e Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "registry error: " + strconv.Itoa(e.StatusCode)
}
