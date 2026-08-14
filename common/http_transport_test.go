package common

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewHTTPTransport_ConfiguresHTTP2(t *testing.T) {
	transport, err := NewHTTPTransport(nil)
	require.NoError(t, err)
	require.Contains(t, transport.TLSNextProto, "h2")
	require.NotNil(t, transport.TLSClientConfig)
	require.Contains(t, transport.TLSClientConfig.NextProtos, "h2")
}

func TestNewHTTPTransport_ClonesTLSConfig(t *testing.T) {
	tlsConfig := &tls.Config{ServerName: "some-host"}
	transport, err := NewHTTPTransport(tlsConfig)
	require.NoError(t, err)
	require.Equal(t, "some-host", transport.TLSClientConfig.ServerName)
	require.Contains(t, transport.TLSClientConfig.NextProtos, "h2")
	// The caller's config must not be mutated, it may be shared with other clients.
	require.Empty(t, tlsConfig.NextProtos)
}
