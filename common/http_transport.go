package common

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
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
// tlsConfig may be nil. If set, it is cloned so the transport does not alias a config that may
// be shared with other clients.
func NewHTTPTransport(tlsConfig *tls.Config) (*http.Transport, error) {
	// dialer and transport field values copied from http.DefaultTransport.
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	t := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		HTTP2: &http.HTTP2Config{
			SendPingTimeout: http2ReadIdleTimeout,
			PingTimeout:     http2PingTimeout,
		},
		Protocols: protocols,
	}
	if tlsConfig != nil {
		t.TLSClientConfig = tlsConfig.Clone()
	} else {
		t.TLSClientConfig = new(tls.Config)
	}
	return t, nil
}
