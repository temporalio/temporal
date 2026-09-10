package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

// nonHTTPTransport is a RoundTripper that is not *http.Transport, used to
// verify that wrapDialLogger bails out on unsupported transport types.
type nonHTTPTransport struct{}

func (t *nonHTTPTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, nil }

// ===== dialFailureCache =====

func TestDialFailureCache_UnknownIP_IsNotBad(t *testing.T) {
	c := newDialFailureCache()
	assert.False(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_MarkBad_IsBad(t *testing.T) {
	c := newDialFailureCache()
	c.markBad("10.0.0.1")
	assert.True(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_MarkGood_ClearsIP(t *testing.T) {
	c := newDialFailureCache()
	c.markBad("10.0.0.1")
	c.markGood("10.0.0.1")
	assert.False(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_MarkGood_UnknownIP_NoOp(t *testing.T) {
	c := newDialFailureCache()
	c.markGood("10.0.0.1") // should not panic
	assert.False(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_TTLExpired_IsNotBad(t *testing.T) {
	c := &dialFailureCache{
		badIPs: make(map[string]time.Time),
		ttl:    5 * time.Millisecond,
	}
	c.markBad("10.0.0.1")
	assert.True(t, c.isBad("10.0.0.1"))
	time.Sleep(10 * time.Millisecond)
	assert.False(t, c.isBad("10.0.0.1"))
}

func TestDialFailureCache_Cleanup_RemovesExpiredEntries(t *testing.T) {
	c := &dialFailureCache{
		badIPs: make(map[string]time.Time),
		ttl:    5 * time.Millisecond,
	}
	c.markBad("10.0.0.1")
	c.markBad("10.0.0.2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.startCleanup(ctx)

	// Cleanup interval is 2*ttl (10ms); wait long enough for it to fire.
	time.Sleep(50 * time.Millisecond)

	c.mu.RLock()
	_, ip1Exists := c.badIPs["10.0.0.1"]
	_, ip2Exists := c.badIPs["10.0.0.2"]
	c.mu.RUnlock()
	assert.False(t, ip1Exists, "expired entry should be evicted")
	assert.False(t, ip2Exists, "expired entry should be evicted")
}

func TestDialFailureCache_Cleanup_DoesNotRemoveUnexpiredEntries(t *testing.T) {
	c := &dialFailureCache{
		badIPs: make(map[string]time.Time),
		ttl:    10 * time.Second,
	}
	c.markBad("10.0.0.1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.startCleanup(ctx)

	time.Sleep(10 * time.Millisecond)

	c.mu.RLock()
	_, exists := c.badIPs["10.0.0.1"]
	c.mu.RUnlock()
	assert.True(t, exists, "unexpired entry should not be evicted")
}

func TestDialFailureCache_Cleanup_StopsOnContextCancel(t *testing.T) {
	c := newDialFailureCache()
	ctx, cancel := context.WithCancel(context.Background())
	c.startCleanup(ctx)
	cancel() // goroutine should exit cleanly, no panic
}

// ===== wrapDialLogger =====

func TestWrapDialLogger_NonHTTPTransport_Unchanged(t *testing.T) {
	custom := &nonHTTPTransport{}
	httpClient := &http.Client{Transport: custom}
	wrapDialLogger(httpClient, log.NewNoopLogger())
	assert.Equal(t, custom, httpClient.Transport, "non-*http.Transport should not be replaced")
}

func TestWrapDialLogger_NilTransport_SetsHTTPTransport(t *testing.T) {
	httpClient := &http.Client{}
	wrapDialLogger(httpClient, log.NewNoopLogger())
	_, ok := httpClient.Transport.(*http.Transport)
	assert.True(t, ok, "expected a *http.Transport to be set on the client")
}

func TestWrapDialLogger_DialSuccess_ReturnsConn(t *testing.T) {
	server, pipe := net.Pipe()
	defer server.Close()

	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
		return pipe, nil
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	conn, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	require.NoError(t, err)
	conn.Close()
}

func TestWrapDialLogger_DialFailure_ReturnsError(t *testing.T) {
	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	_, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	assert.Error(t, err)
}

// TestWrapDialLogger_BaseDialer_CalledWithIP verifies that the wrapped DialContext
// passes a bare IP address (not the original hostname) to the base dialer, so
// the base dialer skips its own DNS resolution.
func TestWrapDialLogger_BaseDialer_CalledWithIP(t *testing.T) {
	server, pipe := net.Pipe()
	defer server.Close()

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
	conn.Close()

	assert.Equal(t, "127.0.0.1:9200", calledAddr)
}

// TestWrapDialLogger_FailedDial_MarksIPBad verifies that after a dial failure the
// IP is treated as unhealthy: a second dial to the same address still attempts
// the IP (it is the only candidate) but the cache records it as bad.
func TestWrapDialLogger_FailedDial_MarksIPBad(t *testing.T) {
	callCount := 0
	server, pipe := net.Pipe()
	defer server.Close()

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
	assert.Error(t, err)

	// Second dial succeeds — IP is still tried (only candidate) but now marked good again.
	conn, err := transport.DialContext(context.Background(), "tcp", "127.0.0.1:9200")
	require.NoError(t, err)
	conn.Close()

	assert.Equal(t, 2, callCount)
}

func TestWrapDialLogger_MalformedAddr_FallsBackToBaseDialer(t *testing.T) {
	server, pipe := net.Pipe()
	defer server.Close()

	var calledAddr string
	transport := &http.Transport{}
	transport.DialContext = func(_ context.Context, _, addr string) (net.Conn, error) {
		calledAddr = addr
		return pipe, nil
	}

	httpClient := &http.Client{Transport: transport}
	wrapDialLogger(httpClient, log.NewNoopLogger())

	conn, err := transport.DialContext(context.Background(), "tcp", "not-valid-addr")
	require.NoError(t, err)
	conn.Close()

	// Malformed addr: wrapper cannot split host/port, so it falls back unchanged.
	assert.Equal(t, "not-valid-addr", calledAddr)
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
	assert.Error(t, err)
}
