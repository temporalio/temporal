package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
)

// nonHTTPTransport is a RoundTripper that is not *http.Transport, used to
// verify that wrapDialLogger bails out on unsupported transport types.
type nonHTTPTransport struct{}

func (t *nonHTTPTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, nil }

// ===== dialFailureCache =====

func TestDialFailureCache_UnknownIP_IsNotBad(t *testing.T) {
	c := newDialFailureCache()
	require.False(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_MarkBad_IsBad(t *testing.T) {
	c := newDialFailureCache()
	c.markBad("10.0.0.1")
	require.True(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_MarkGood_ClearsIP(t *testing.T) {
	c := newDialFailureCache()
	c.markBad("10.0.0.1")
	c.markGood("10.0.0.1")
	require.False(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_MarkGood_UnknownIP_NoOp(t *testing.T) {
	c := newDialFailureCache()
	c.markGood("10.0.0.1") // should not panic
	require.False(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_TTLExpired_IsNotBad(t *testing.T) {
	c := &dialFailureCache{
		badIPs: make(map[string]time.Time),
		ttl:    5 * time.Millisecond,
	}
	c.markBad("10.0.0.1")
	require.True(t, c.isBad("10.0.0.1"))
	await.RequireTrue(t, func() bool {
		return !c.isBad("10.0.0.1")
	}, 100*time.Millisecond, 5*time.Millisecond)
}

func TestDialFailureCache_Cleanup_RemovesExpiredEntries(t *testing.T) {
	c := &dialFailureCache{
		badIPs: make(map[string]time.Time),
		ttl:    5 * time.Millisecond,
	}
	c.markBad("10.0.0.1")
	c.markBad("10.0.0.2")

	c.startCleanup(t.Context())

	// Cleanup interval is 2*ttl (10ms); poll until both entries are evicted.
	await.RequireTrue(t, func() bool {
		c.mu.RLock()
		_, ip1Exists := c.badIPs["10.0.0.1"]
		_, ip2Exists := c.badIPs["10.0.0.2"]
		c.mu.RUnlock()
		return !ip1Exists && !ip2Exists
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestDialFailureCache_Cleanup_DoesNotRemoveUnexpiredEntries(t *testing.T) {
	c := &dialFailureCache{
		badIPs: make(map[string]time.Time),
		ttl:    10 * time.Second, // cleanup interval is 20s — cannot have fired yet
	}
	c.markBad("10.0.0.1")

	c.startCleanup(t.Context())

	c.mu.RLock()
	_, exists := c.badIPs["10.0.0.1"]
	c.mu.RUnlock()
	require.True(t, exists, "unexpired entry should not be evicted")
}

func TestDialFailureCache_Cleanup_StopsOnContextCancel(t *testing.T) {
	c := newDialFailureCache()
	ctx, cancel := context.WithCancel(context.Background())
	c.startCleanup(ctx)
	cancel() // goroutine should exit cleanly
}

// ===== wrapDialLogger =====

func TestWrapDialLogger_NonHTTPTransport_Unchanged(t *testing.T) {
	custom := &nonHTTPTransport{}
	httpClient := &http.Client{Transport: custom}
	wrapDialLogger(httpClient, log.NewNoopLogger())
	require.Equal(t, custom, httpClient.Transport, "non-*http.Transport should not be replaced")
}

func TestWrapDialLogger_NilTransport_SetsHTTPTransport(t *testing.T) {
	httpClient := &http.Client{}
	wrapDialLogger(httpClient, log.NewNoopLogger())
	_, ok := httpClient.Transport.(*http.Transport)
	require.True(t, ok, "expected a *http.Transport to be set on the client")
}

func TestWrapDialLogger_DialSuccess_ReturnsConn(t *testing.T) {
	server, pipe := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })

	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
		return pipe, nil
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	conn, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	require.NoError(t, err)
	_ = conn.Close()
}

func TestWrapDialLogger_DialFailure_ReturnsError(t *testing.T) {
	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	_, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	require.Error(t, err)
}

// TestWrapDialLogger_BaseDialer_CalledWithIP verifies that the wrapped DialContext
// passes a bare IP address (not the original hostname) to the base dialer, so
// the base dialer skips its own DNS resolution.
func TestWrapDialLogger_BaseDialer_CalledWithIP(t *testing.T) {
	server, pipe := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })

	var calledAddr string
	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, addr string) (net.Conn, error) {
		calledAddr = addr
		return pipe, nil
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	conn, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	require.NoError(t, err)
	_ = conn.Close()

	require.Equal(t, "127.0.0.1:9200", calledAddr)
}

// TestWrapDialLogger_FailedDial_MarksIPBad verifies that after a dial failure the
// IP is marked unhealthy and a subsequent dial still attempts it (only candidate)
// but can succeed once the underlying issue is resolved.
func TestWrapDialLogger_FailedDial_MarksIPBad(t *testing.T) {
	callCount := 0
	server, pipe := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })

	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
		callCount++
		if callCount == 1 {
			return nil, errors.New("first dial fails")
		}
		return pipe, nil
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	// First dial fails.
	_, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	require.Error(t, err)

	// Second dial succeeds.
	conn, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	require.NoError(t, err)
	_ = conn.Close()
	require.Equal(t, 2, callCount)
}

func TestWrapDialLogger_MalformedAddr_FallsBackToBaseDialer(t *testing.T) {
	server, pipe := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })

	var calledAddr string
	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, addr string) (net.Conn, error) {
		calledAddr = addr
		return pipe, nil
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	// Malformed addr: wrapper cannot split host/port, falls back to base dialer unchanged.
	conn, err := transport.DialContext(context.Background(), "tcp", "not-valid-addr")
	require.NoError(t, err)
	_ = conn.Close()
	require.Equal(t, "not-valid-addr", calledAddr)
}

func TestWrapDialLogger_CancelledContext_ReturnsError(t *testing.T) {
	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
		return nil, errors.New("dial error")
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := transport.DialContext(ctx, "tcp", "127.0.0.1:9200")
	require.Error(t, err)
}
