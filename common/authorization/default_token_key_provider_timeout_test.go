package authorization

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

// TestJWKSFetchTimesOut guards the timeout on the JWKS retrieval. updateKeys runs synchronously
// inside NewDefaultTokenKeyProvider via initialize, so without a bounded fetch an endpoint that
// accepts the connection and never responds blocks construction indefinitely.
func TestJWKSFetchTimesOut(t *testing.T) {
	// Shorten the fetch timeout so the test does not wait out the production default.
	prev := jwksHTTPClient
	jwksHTTPClient = &http.Client{Timeout: 200 * time.Millisecond}
	t.Cleanup(func() { jwksHTTPClient = prev })

	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hang // accept the request, then never respond
	}))
	defer srv.Close()
	defer close(hang) // LIFO: unblock the handler before closing the server

	cfg := &config.Authorization{
		JWTKeyProvider: config.JWTKeyProvider{
			KeySourceURIs: []string{srv.URL},
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		NewDefaultTokenKeyProvider(cfg, log.NewNoopLogger())
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "NewDefaultTokenKeyProvider did not return",
			"construction is still blocked on an unresponsive JWKS endpoint")
	}
}
