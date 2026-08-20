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
	// The transport must hold a copy, not the caller's config: it may be shared with other
	// clients (both frontend clients pass one cached by localStoreTlsProvider).
	require.NotSame(t, tlsConfig, transport.TLSClientConfig)
	require.Equal(t, "some-host", transport.TLSClientConfig.ServerName)
	require.Contains(t, transport.TLSClientConfig.NextProtos, "h2")
	// ...and the caller's config must come back unmutated.
	require.Empty(t, tlsConfig.NextProtos)
}
