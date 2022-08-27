package registry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	client, err := registry.NewClient("http://example.com")

	require.NoError(t, err)
	assert.Implements(t, (*registry.Registry)(nil), client)
}

func TestNewClient_UrlError(t *testing.T) {
	_, err := registry.NewClient("://")

	assert.Error(t, err)
}

func TestClient_PopulatesError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/vnd.schemaregistry.v1+json")
		w.WriteHeader(422)
		_, _ = w.Write([]byte(`{"error_code": 42202, "message": "schema may not be empty"}"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "test", "")

	assert.Error(t, err)
	assert.IsType(t, registry.Error{}, err)
	assert.Equal(t, "schema may not be empty", err.Error())
	regError := err.(registry.Error)
	assert.Equal(t, 422, regError.StatusCode)
	assert.Equal(t, 42202, regError.Code)
	assert.Equal(t, "schema may not be empty", regError.Message)
}

func TestNewClient_BasicAuth(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "username", username)
		assert.Equal(t, "password", password)

		_, _ = w.Write([]byte(`[]`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL, registry.WithBasicAuth("username", "password"))

	_, err := client.GetSubjects(context.Background())

	assert.NoError(t, err)
}

func TestClient_GetSchema(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/schemas/ids/5", r.URL.Path)

		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	schema, err := client.GetSchema(context.Background(), 5)

	require.NoError(t, err)
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

	_, _ = client.GetSchema(context.Background(), 5)

	_, _ = client.GetSchema(context.Background(), 5)

	assert.Equal(t, 1, count)
}

func TestClient_GetSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

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

	subs, err := client.GetSubjects(context.Background())

	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, "foobar", subs[0])
}

func TestClient_GetSubjectsRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSubjects(context.Background())

	assert.Error(t, err)
}

func TestClient_GetSubjectsJSONError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`["foobar"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSubjects(context.Background())

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

	vers, err := client.GetVersions(context.Background(), "foobar")

	require.NoError(t, err)
	assert.Len(t, vers, 1)
	assert.Equal(t, 10, vers[0])
}

func TestClient_GetVersionsRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetVersions(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetVersionsJSONError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[10`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetVersions(context.Background(), "foobar")

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

	schema, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetSchemaByVersionRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersionJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

	assert.Error(t, err)
}

func TestClient_GetSchemaByVersionSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaByVersion(context.Background(), "foobar", 5)

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

	schema, err := client.GetLatestSchema(context.Background(), "foobar")

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_GetLatestSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchema(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetSchemaInfo(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/1", r.URL.Path)

		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	schemaInfo, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schemaInfo.Schema.String())
	assert.Equal(t, 2, schemaInfo.ID)
	assert.Equal(t, 1, schemaInfo.Version)
}

func TestClient_GetSchemaInfoRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	assert.Error(t, err)
}

func TestClient_GetSchemaInfoJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	assert.Error(t, err)
}

func TestClient_GetSchemaInfoSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchemaInfo(context.Background(), "foobar", 1)

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaInfo(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions/latest", r.URL.Path)

		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	schemaInfo, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

	require.NoError(t, err)
	assert.Equal(t, `["null","string","int"]`, schemaInfo.Schema.String())
	assert.Equal(t, 2, schemaInfo.ID)
	assert.Equal(t, 1, schemaInfo.Version)
}

func TestClient_GetLatestSchemaInfoRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaInfoJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"subject": "foobar", "version": 1, "id": 2, "schema":"[\"null\",\"string\",\"int\"]"`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

	assert.Error(t, err)
}

func TestClient_GetLatestSchemaInfoSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"schema":""}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetLatestSchemaInfo(context.Background(), "foobar")

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

	id, schema, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"]")

	require.NoError(t, err)
	assert.Equal(t, 10, id)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_CreateSchemaRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_CreateSchemaJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_CreateSchemaSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.CreateSchema(context.Background(), "foobar", "[\"null\",\"string\",\"int\"")

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

	id, schema, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"]")

	require.NoError(t, err)
	assert.Equal(t, 10, id)
	assert.Equal(t, `["null","string","int"]`, schema.String())
}

func TestClient_IsRegisteredRequestError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_IsRegisteredJsonError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"]")

	assert.Error(t, err)
}

func TestClient_IsRegisteredSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":10}`))
	}))
	defer s.Close()
	client, _ := registry.NewClient(s.URL)

	_, _, err := client.IsRegistered(context.Background(), "test", "[\"null\",\"string\",\"int\"")

	assert.Error(t, err)
}

func TestClient_HandlesServerError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s.Close()
	client, _ := registry.NewClient(s.URL)

	_, err := client.GetSchema(context.Background(), 5)

	assert.Error(t, err)
}

func TestError_Error(t *testing.T) {
	err := registry.Error{
		StatusCode: 404,
		Code:       40403,
		Message:    "Schema not found",
	}

	str := err.Error()

	assert.Equal(t, "Schema not found", str)
}

func TestError_ErrorEmptyMEssage(t *testing.T) {
	err := registry.Error{
		StatusCode: 404,
	}

	str := err.Error()

	assert.Equal(t, "registry error: 404", str)
}
