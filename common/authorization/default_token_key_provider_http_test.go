package authorization

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

const httpTestWait = 2 * time.Second

func newTestJWKS(t *testing.T) []byte {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	body, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
		{Key: &privateKey.PublicKey, KeyID: "key-1", Algorithm: "RS256", Use: "sig"},
	}})
	require.NoError(t, err)
	return body
}

func shortenKeyFetchTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	original := keyFetchTimeout
	keyFetchTimeout = timeout
	t.Cleanup(func() { keyFetchTimeout = original })
}

func newTestKeyProviderConfig(uri string, refresh time.Duration) *config.Authorization {
	return &config.Authorization{JWTKeyProvider: config.JWTKeyProvider{
		KeySourceURIs:   []string{uri},
		RefreshInterval: refresh,
	}}
}

// A key endpoint that accepts the request and never answers must not block construction, which
// runs during server startup. The request is abandoned once keyFetchTimeout elapses.
func TestKeyProvider_HungEndpointDoesNotBlockConstruction(t *testing.T) {
	shortenKeyFetchTimeout(t, 200*time.Millisecond)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) }) // runs before server.Close and releases the blocked handler

	constructed := make(chan struct{})
	go func() {
		NewDefaultTokenKeyProvider(newTestKeyProviderConfig(server.URL, 0), log.NewNoopLogger())
		close(constructed)
	}()

	select {
	case <-constructed:
	case <-time.After(httpTestWait):
		t.Fatalf("NewDefaultTokenKeyProvider still blocked %v after the key endpoint accepted the request and never answered", httpTestWait)
	}
}

// Close must not wait for a stuck refresh request: the refresh goroutine can only receive on the
// stop channel after the request returns, so Close cancels the request first.
func TestKeyProvider_CloseDoesNotBlockOnInFlightRefresh(t *testing.T) {
	release := make(chan struct{})
	var requests atomic.Int32
	body := newTestJWKS(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			_, _ = w.Write(body) // the initial load succeeds
			return
		}
		<-release // later refresh requests never answer
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	provider := NewDefaultTokenKeyProvider(newTestKeyProviderConfig(server.URL, 50*time.Millisecond), log.NewNoopLogger())
	require.Eventually(t, func() bool { return requests.Load() >= 2 }, httpTestWait, 10*time.Millisecond,
		"refresh request should have been issued")

	closed := make(chan struct{})
	go func() {
		provider.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(httpTestWait):
		t.Fatalf("Close() still blocked %v after a refresh request got stuck", httpTestWait)
	}
}

// A 5xx response with a JSON body decodes into an empty key set. It must be rejected instead of
// replacing the keys that were loaded successfully.
func TestKeyProvider_ErrorStatusKeepsExistingKeys(t *testing.T) {
	var fail atomic.Bool
	body := newTestJWKS(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"message":"Service Unavailable"}`))
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	provider := NewDefaultTokenKeyProvider(newTestKeyProviderConfig(server.URL, 0), log.NewNoopLogger())
	_, err := provider.RsaKey("RS256", "key-1")
	require.NoError(t, err, "key should be loaded from the healthy endpoint")

	fail.Store(true)
	refreshErr := provider.updateKeys()

	_, err = provider.RsaKey("RS256", "key-1")
	require.NoError(t, err, "a 503 response must not discard the previously loaded key (updateKeys err: %v)", refreshErr)
}

func TestKeyProvider_LoadsKeysOverHTTP(t *testing.T) {
	body := newTestJWKS(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	provider := NewDefaultTokenKeyProvider(newTestKeyProviderConfig(server.URL, 0), log.NewNoopLogger())
	t.Cleanup(provider.Close)

	_, err := provider.RsaKey("RS256", "key-1")
	require.NoError(t, err)
}

func TestKeyProvider_ErrorStatusIsReported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	provider := NewDefaultTokenKeyProvider(newTestKeyProviderConfig(server.URL, 0), log.NewNoopLogger())
	t.Cleanup(provider.Close)

	err := provider.updateKeys()
	require.ErrorContains(t, err, "unexpected HTTP status 500")
}
