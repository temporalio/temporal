package localexecution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testBootstrapToken = "0123456789abcdef0123456789abcdef"

func TestBridgeBootstrapServerBootstrapsOnce(t *testing.T) {
	configuration := validBridgeConfiguration()
	var calls atomic.Int32
	server := startTestBootstrapServer(t, testBootstrapToken, func(
		_ context.Context,
		actual BridgeConfiguration,
	) (BridgeBootstrapResponse, error) {
		calls.Add(1)
		require.Equal(t, configuration, actual)
		return BridgeBootstrapResponse{
			FrontendAddress: "127.0.0.1:7233",
			LocalServerID:   "bridge-id",
		}, nil
	})

	response := bootstrapRequest(t, server.Address(), testBootstrapToken, configuration)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var result BridgeBootstrapResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
	require.NoError(t, response.Body.Close())
	require.Equal(t, "127.0.0.1:7233", result.FrontendAddress)
	require.Equal(t, "bridge-id", result.LocalServerID)

	reused := bootstrapRequest(t, server.Address(), testBootstrapToken, configuration)
	require.Equal(t, http.StatusUnauthorized, reused.StatusCode)
	require.NoError(t, reused.Body.Close())
	require.Equal(t, int32(1), calls.Load())
}

func TestBridgeBootstrapServerAllowsRetryAfterStartupFailure(t *testing.T) {
	var calls atomic.Int32
	server := startTestBootstrapServer(t, testBootstrapToken, func(
		_ context.Context,
		_ BridgeConfiguration,
	) (BridgeBootstrapResponse, error) {
		if calls.Add(1) == 1 {
			return BridgeBootstrapResponse{}, errors.New("startup failed")
		}
		return BridgeBootstrapResponse{
			FrontendAddress: "127.0.0.1:7233",
			LocalServerID:   "bridge-id",
		}, nil
	})

	failed := bootstrapRequest(t, server.Address(), testBootstrapToken, validBridgeConfiguration())
	require.Equal(t, http.StatusServiceUnavailable, failed.StatusCode)
	require.NoError(t, failed.Body.Close())

	retried := bootstrapRequest(t, server.Address(), testBootstrapToken, validBridgeConfiguration())
	require.Equal(t, http.StatusOK, retried.StatusCode)
	require.NoError(t, retried.Body.Close())
	require.Equal(t, int32(2), calls.Load())
}

func TestBridgeBootstrapServerRejectsInvalidConfigurationWithoutConsumingToken(t *testing.T) {
	var calls atomic.Int32
	server := startTestBootstrapServer(t, testBootstrapToken, func(
		_ context.Context,
		_ BridgeConfiguration,
	) (BridgeBootstrapResponse, error) {
		calls.Add(1)
		return BridgeBootstrapResponse{
			FrontendAddress: "127.0.0.1:7233",
			LocalServerID:   "bridge-id",
		}, nil
	})

	configuration := validBridgeConfiguration()
	configuration.Namespace = ""
	invalid := bootstrapRequest(t, server.Address(), testBootstrapToken, configuration)
	require.Equal(t, http.StatusBadRequest, invalid.StatusCode)
	require.NoError(t, invalid.Body.Close())

	valid := bootstrapRequest(t, server.Address(), testBootstrapToken, validBridgeConfiguration())
	require.Equal(t, http.StatusOK, valid.StatusCode)
	require.NoError(t, valid.Body.Close())
	require.Equal(t, int32(1), calls.Load())
}

func TestBridgeBootstrapServerRejectsNonLoopbackListener(t *testing.T) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	_, err = NewBridgeBootstrapServer(
		listener,
		[]byte(testBootstrapToken),
		func(context.Context, BridgeConfiguration) (BridgeBootstrapResponse, error) {
			return BridgeBootstrapResponse{}, nil
		},
	)
	require.EqualError(t, err, "bootstrap listener must use a loopback TCP address")
}

func TestBridgeBootstrapServerBoundsRequestBody(t *testing.T) {
	server := startTestBootstrapServer(t, testBootstrapToken, func(
		_ context.Context,
		_ BridgeConfiguration,
	) (BridgeBootstrapResponse, error) {
		return BridgeBootstrapResponse{}, errors.New("must not be called")
	})
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		server.Address()+BridgeBootstrapPath,
		strings.NewReader(strings.Repeat("x", bridgeBootstrapBodyLimit+1)),
	)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+testBootstrapToken)

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestReadAndRemoveBootstrapToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap-token")
	require.NoError(t, os.WriteFile(path, []byte(testBootstrapToken+"\n"), 0o600))

	token, err := ReadAndRemoveBootstrapToken(path)
	require.NoError(t, err)
	require.Equal(t, testBootstrapToken, string(token))
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestReadAndRemoveBootstrapTokenRejectsBroadPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap-token")
	require.NoError(t, os.WriteFile(path, []byte(testBootstrapToken), 0o600))
	require.NoError(t, os.Chmod(path, 0o640))

	_, err := ReadAndRemoveBootstrapToken(path)
	require.EqualError(t, err, "bootstrap token file must not be accessible by group or other users")
}

func startTestBootstrapServer(
	t *testing.T,
	token string,
	handler BridgeBootstrapHandler,
) *BridgeBootstrapServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server, err := NewBridgeBootstrapServer(listener, []byte(token), handler)
	require.NoError(t, err)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve() }()
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(shutdownContext))
		require.NoError(t, <-serveErrors)
	})
	return server
}

func bootstrapRequest(
	t *testing.T,
	address string,
	token string,
	configuration BridgeConfiguration,
) *http.Response {
	t.Helper()
	body, err := json.Marshal(configuration)
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		address+BridgeBootstrapPath,
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	return response
}

func validBridgeConfiguration() BridgeConfiguration {
	return BridgeConfiguration{
		Namespace: "namespace",
		Upstream: UpstreamConnectionProfile{
			Address:    "upstream:7233",
			ServerName: "upstream.example.test",
			TLS: &UpstreamTLS{
				ServerRootCACertificate: "root certificate",
				ClientCertificate:       "client certificate",
				ClientPrivateKey:        "client private key",
			},
			APIKey:  "secret API key",
			Headers: map[string]string{"x-header": "value"},
		},
		Options: BridgeLocalFirstOptions{
			SyncIntervalMilliseconds:    3_000,
			MaximumUnsynchronizedEvents: 10_240,
			MaximumUnsynchronizedBytes:  8 << 20,
		},
		Registrations: WorkerRegistrationManifest{
			TaskQueue:     "task-queue",
			WorkflowTypes: []string{"workflow"},
			ActivityTypes: []string{"activity"},
		},
	}
}
