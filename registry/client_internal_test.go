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

func TestNewClient_WithBasicAuth(t *testing.T) {
	creds := credentials{username: "username", password: "password"}

	client, _ := NewClient("http://example.com", WithBasicAuth("username", "password"))

	assert.Equal(t, client.creds, creds)
}
