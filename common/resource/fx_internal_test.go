package resource

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
)

func TestGetFrontendConnectionDetails_WithIFE(t *testing.T) {
	cfg := &config.Config{
		Services: map[string]config.Service{
			string(primitives.FrontendService): {
				RPC: config.RPC{
					GRPCPort: 7233,
					HTTPPort: 7243,
				},
			},
			string(primitives.InternalFrontendService): {
				RPC: config.RPC{
					GRPCPort: 7236,
					// No HTTPPort — IFE doesn't run an HTTP server
				},
			},
		},
		PublicClient: config.PublicClient{
			// Empty — rely on defaults
		},
	}

	internodeTLS := &tls.Config{ServerName: "internode"}
	frontendTLS := &tls.Config{ServerName: "frontend"}
	tlsProvider := &encryption.FixedTLSConfigProvider{
		InternodeClientConfig: internodeTLS,
		FrontendClientConfig:  frontendTLS,
	}

	resolver := membership.NewGRPCResolverForTesting(nil)

	frontendURL, frontendHTTPURL, frontendHTTPPort, grpcTLSConfig, httpTLSConfig, err := getFrontendConnectionDetails(cfg, tlsProvider, resolver)
	require.NoError(t, err)

	// gRPC should target internal-frontend
	assert.True(t, strings.Contains(frontendURL, string(primitives.InternalFrontendService)),
		"gRPC URL should contain internal-frontend, got: %s", frontendURL)

	// HTTP should target regular frontend, not internal-frontend
	assert.True(t, strings.Contains(frontendHTTPURL, string(primitives.FrontendService)),
		"HTTP URL should contain frontend, got: %s", frontendHTTPURL)
	assert.False(t, strings.Contains(frontendHTTPURL, string(primitives.InternalFrontendService)),
		"HTTP URL should not contain internal-frontend, got: %s", frontendHTTPURL)

	// HTTP port should come from frontend config
	assert.Equal(t, 7243, frontendHTTPPort)

	// gRPC TLS should be internode (for IFE)
	require.NotNil(t, grpcTLSConfig)
	assert.Equal(t, "internode", grpcTLSConfig.ServerName)

	// HTTP TLS should be frontend client (for frontend HTTP server)
	require.NotNil(t, httpTLSConfig)
	assert.Equal(t, "frontend", httpTLSConfig.ServerName)
}

func TestGetFrontendConnectionDetails_WithoutIFE(t *testing.T) {
	cfg := &config.Config{
		Services: map[string]config.Service{
			string(primitives.FrontendService): {
				RPC: config.RPC{
					GRPCPort: 7233,
					HTTPPort: 7243,
				},
			},
		},
		PublicClient: config.PublicClient{},
	}

	frontendTLS := &tls.Config{ServerName: "frontend"}
	tlsProvider := &encryption.FixedTLSConfigProvider{
		FrontendClientConfig: frontendTLS,
	}

	resolver := membership.NewGRPCResolverForTesting(nil)

	frontendURL, frontendHTTPURL, frontendHTTPPort, grpcTLSConfig, httpTLSConfig, err := getFrontendConnectionDetails(cfg, tlsProvider, resolver)
	require.NoError(t, err)

	// Both gRPC and HTTP should target frontend
	assert.True(t, strings.Contains(frontendURL, string(primitives.FrontendService)),
		"gRPC URL should contain frontend, got: %s", frontendURL)
	assert.True(t, strings.Contains(frontendHTTPURL, string(primitives.FrontendService)),
		"HTTP URL should contain frontend, got: %s", frontendHTTPURL)

	assert.Equal(t, 7243, frontendHTTPPort)

	// Both TLS configs should be the same (frontend client TLS)
	require.NotNil(t, grpcTLSConfig)
	require.NotNil(t, httpTLSConfig)
	assert.Equal(t, grpcTLSConfig, httpTLSConfig, "without IFE, gRPC and HTTP TLS configs should be identical")
}
