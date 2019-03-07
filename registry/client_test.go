package registry_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/registry"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	client, err := registry.NewClient("http://example.com")

	assert.NoError(t, err)
	assert.Implements(t, (*registry.Registry)(nil), client)
}

func TestNewClient_UrlError(t *testing.T) {
	_, err := registry.NewClient("://")

	assert.Error(t, err)
}

func TestClient_GetSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/schemas/ids/5", r.URL.Path)

		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	schema, err := client.GetSchema(5)

	assert.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetSchemaCachesSchema(t *testing.T) {
	count := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _ = client.GetSchema(5)

	_, _ = client.GetSchema(5)

	assert.Equal(t, 1, count)
}

func TestClient_GetSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(5)

	assert.Error(t, err)
}

func TestClient_GetSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(5)

	assert.Error(t, err)
}

func TestClient_GetSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(5)

	assert.Error(t, err)
}

func TestClient_GetSubjects(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects", r.URL.Path)

		_, _ = w.Write([]byte(`["foobar"]`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	subs, err := client.GetSubjects()

	assert.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, "foobar", subs[0])
}

func TestClient_GetSubjectsRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSubjects()

	assert.Error(t, err)
}

func TestClient_GetSubjectsJSONError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`["foobar"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSubjects()

	assert.Error(t, err)
}

func TestClient_GetVersions(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions", r.URL.Path)

		_, _ = w.Write([]byte(`[10]`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	vers, err := client.GetVersions("foobar")

	assert.NoError(t, err)
	assert.Len(t, vers, 1)
	assert.Equal(t, 10, vers[0])
}

func TestClient_GetVersionsRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetVersions("foobar")

	assert.Error(t, err)
}

func TestClient_GetVersionsJSONError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[10`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetVersions("foobar")

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersion(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/5", r.URL.Path)

		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	schema, err := client.GetSchemaByVersion("foobar", 5)

	assert.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetSchemaByVersionRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion("foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersionJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion("foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersionSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion("foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetLatestSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/latest", r.URL.Path)

		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	schema, err := client.GetLatestSchema("foobar")

	assert.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetLatestSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema("foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema("foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema("foobar")

	assert.Error(t, err)
}

func TestClient_CreateSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/subjects/foobar/versions", r.URL.Path)

		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	id, schema, err := client.CreateSchema("foobar", "[\"null\",\"string\",\"int\"]")

	assert.NoError(t, err)
	assert.Equal(t, 10, id)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_CreateSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema("foobar", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_CreateSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema("foobar", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_CreateSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema("foobar", "[\"null\",\"string\",\"int\"")

	assert.Error(t, err)
}

func TestClient_IsRegistered(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/subjects/test", r.URL.Path)

		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	id, schema, err := client.IsRegistered("test", "[\"null\",\"string\",\"int\"]")

	assert.NoError(t, err)
	assert.Equal(t, 10, id)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_IsRegisteredRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered("test", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_IsRegisteredJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered("test", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_IsRegisteredSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered("test", "[\"null\",\"string\",\"int\"")

	assert.Error(t, err)
}

func TestClient_HandlesServerError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(5)

	assert.Error(t, err)
}

func TestError_Error(t *testing.T) {
	err := registry.Error{
		StatusCode: 404,
		Code: 40403,
		Message: "Schema not found",
	}

	str := err.Error()

	assert.Equal(t, "Schema not found", str)
}
