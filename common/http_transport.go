package common

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

const (
	http2ReadIdleTimeout = 15 * time.Second
	http2PingTimeout     = 5 * time.Second
)

// NewHTTPTransport returns an http.Transport with the same field values as http.DefaultTransport
// and HTTP/2 health checks enabled: idle connections are pinged so that unresponsive ones get
// evicted from the pool. Without this, a connection that is silently broken (e.g. the peer dropped
// it without sending a GOAWAY or RST) stays in the pool and stalls every request multiplexed onto
// it until their individual deadlines expire.
//
// tlsConfig may be nil. If set, it is cloned before use, because enabling HTTP/2 appends to the
// config's NextProtos and callers typically pass a config shared with other clients.
func NewHTTPTransport(tlsConfig *tls.Config) (*http.Transport, error) {
	// dialer and transport field values copied from http.DefaultTransport.
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	t := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if tlsConfig != nil {
		t.TLSClientConfig = tlsConfig.Clone()
	}
	// Must come after TLSClientConfig is set: this negotiates h2 via ALPN, and replacing the TLS
	// config afterwards would drop that negotiation and silently fall back to HTTP/1.1.
	h2, err := http2.ConfigureTransports(t)
	if err != nil {
		return nil, fmt.Errorf("configure http2 transport: %w", err)
	}
	h2.ReadIdleTimeout = http2ReadIdleTimeout
	h2.PingTimeout = http2PingTimeout
	return t, nil
}
