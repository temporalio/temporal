package client

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewAwsHttpClientDefaultsServiceToES(t *testing.T) {
	client, err := NewAwsHttpClient(ESAWSRequestSigningConfig{
		Enabled:            true,
		Region:             "us-west-2",
		CredentialProvider: "static",
		Static: ESAWSStaticCredentialProvider{
			AccessKeyID:     "id",
			SecretAccessKey: "secret",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	transport, ok := client.Transport.(*awsSigningTransport)
	require.True(t, ok)
	require.Equal(t, defaultAWSSigningService, transport.service)
}

func TestNewAwsHttpClientUsesConfiguredService(t *testing.T) {
	client, err := NewAwsHttpClient(ESAWSRequestSigningConfig{
		Enabled:            true,
		Region:             "us-west-2",
		Service:            "aoss",
		CredentialProvider: "static",
		Static: ESAWSStaticCredentialProvider{
			AccessKeyID:     "id",
			SecretAccessKey: "secret",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	transport, ok := client.Transport.(*awsSigningTransport)
	require.True(t, ok)
	require.Equal(t, "aoss", transport.service)
}

func TestAwsSigningTransportOmitsPayloadHashHeaderByDefault(t *testing.T) {
	gotAuthorization, gotPayloadHash := roundTripThroughSigningClient(t, ESAWSRequestSigningConfig{
		Enabled:            true,
		Region:             "us-west-2",
		CredentialProvider: "static",
		Static: ESAWSStaticCredentialProvider{
			AccessKeyID:     "id",
			SecretAccessKey: "secret",
		},
	}, `{"query":{"match_all":{}}}`)

	require.Empty(t, gotPayloadHash)
	require.NotContains(t, gotAuthorization, "x-amz-content-sha256")
	require.Contains(t, gotAuthorization, "/us-west-2/es/aws4_request")
}

func TestAwsSigningTransportSignsPayloadHashHeaderWhenEnabled(t *testing.T) {
	body := `{"query":{"match_all":{}}}`

	gotAuthorization, gotPayloadHash := roundTripThroughSigningClient(t, ESAWSRequestSigningConfig{
		Enabled:              true,
		Region:               "us-west-2",
		Service:              "aoss",
		AddPayloadHashHeader: true,
		CredentialProvider:   "static",
		Static: ESAWSStaticCredentialProvider{
			AccessKeyID:     "id",
			SecretAccessKey: "secret",
		},
	}, body)

	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(body))), gotPayloadHash)
	require.Contains(t, gotAuthorization, "x-amz-content-sha256")
	require.Contains(t, gotAuthorization, "/us-west-2/aoss/aws4_request")
}

func roundTripThroughSigningClient(
	t *testing.T,
	config ESAWSRequestSigningConfig,
	body string,
) (authorization string, payloadHash string) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		payloadHash = r.Header.Get("X-Amz-Content-Sha256")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewAwsHttpClient(config)
	require.NoError(t, err)

	resp, err := client.Post(server.URL, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return authorization, payloadHash
}
