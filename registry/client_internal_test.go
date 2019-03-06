package registry

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClient_WithHTTPClient(t *testing.T) {
	httpClient := &http.Client{}

	client, _ := NewClient("http://example.com", WithHTTPClient(httpClient))

	assert.Equal(t, client.client, httpClient)
}
