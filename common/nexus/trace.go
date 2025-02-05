// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package nexus

import (
	"crypto/tls"
	"net/http/httptrace"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type HTTPClientTraceProvider interface {
	// NewTrace returns a *httptrace.ClientTrace which provides hooks to invoke at each point in the HTTP request
	// lifecycle. This trace must be added to the HTTP request context using httptrace.WithClientTrace for the hooks to
	// be invoked. The provided logger should already be tagged with relevant request information
	// e.g. using log.With(logger, tag.RequestID(id), tag.Operation(op), ...).
	NewTrace(namespace string, attempt int32, logger log.Logger) *httptrace.ClientTrace
}

var HTTPTraceConfig = dynamicconfig.NewNamespaceTypedSetting(
	"system.nexusHTTPTraceConfig",
	HTTPClientTraceConfig{
		Enabled:    false,
		MinAttempt: 2,
		MaxAttempt: 4,
		Hooks:      []string{"GetConn", "GotConn", "ConnectStart", "ConnectDone", "DNSStart", "DNSDone", "TLSHandshakeStart", "TLSHandshakeDone", "WroteRequest", "GotFirstResponseByte"},
	},
	`Configuration options for controlling additional logging for Nexus HTTP requests. Fields: Enabled, MinAttempt, MaxAttempt, Hooks. See HTTPClientTraceConfig comments for more detail.`,
)

type HTTPClientTraceConfig struct {
	// Enabled controls whether any additional logging will be emitted. Default false.
	Enabled bool
	// MinAttempt is the first operation attempt to include additional logging. Default 2. Setting to 0 or 1 will add logging to all requests and may be expensive.
	MinAttempt int32
	// MaxAttempt is the maximum operation attempt to include additional logging. Default 4. Setting to 0 means no maximum.
	MaxAttempt int32
	// Hooks is the list of method names to invoke with extra logging. See httptrace.ClientTrace for more detail.
	// Defaults to all implemented hooks: GetConn, GotConn, ConnectStart, ConnectDone, DNSStart, DNSDone, TLSHandshakeStart, TLSHandshakeDone, WroteRequest, GotFirstResponseByte.
	Hooks []string
}

type LoggedHTTPClientTraceProvider struct {
	Config dynamicconfig.TypedPropertyFnWithNamespaceFilter[HTTPClientTraceConfig]
}

func NewLoggedHTTPClientTraceProvider(dc *dynamicconfig.Collection) HTTPClientTraceProvider {
	return &LoggedHTTPClientTraceProvider{
		Config: HTTPTraceConfig.Get(dc),
	}
}

func (p *LoggedHTTPClientTraceProvider) NewTrace(ns string, attempt int32, logger log.Logger) *httptrace.ClientTrace {
	config := p.Config(ns)
	if !config.Enabled {
		return nil
	}
	if attempt < config.MinAttempt || attempt > config.MaxAttempt {
		return nil
	}

	clientTrace := &httptrace.ClientTrace{}
	for _, h := range config.Hooks {
		switch h {
		case "GetConn":
			clientTrace.GetConn = func(hostPort string) {
				logger.Info("attempting to get HTTP connection for Nexus request",
					tag.Timestamp(time.Now().UTC()),
					tag.Address(hostPort))
			}
		case "GotConn":
			clientTrace.GotConn = func(info httptrace.GotConnInfo) {
				logger.Info("got HTTP connection for Nexus request",
					tag.Timestamp(time.Now().UTC()),
					tag.NewBoolTag("reused", info.Reused),
					tag.NewBoolTag("was-idle", info.WasIdle),
					tag.NewDurationTag("idle-time", info.IdleTime))
			}
		case "ConnectStart":
			clientTrace.ConnectStart = func(network, addr string) {
				logger.Info("starting dial for new connection for Nexus request",
					tag.Timestamp(time.Now().UTC()),
					tag.Address(addr),
					tag.NewStringTag("network", network))
			}
		case "ConnectDone":
			clientTrace.ConnectDone = func(network, addr string, err error) {
				logger.Info("finished dial for new connection for Nexus request",
					tag.Timestamp(time.Now().UTC()),
					tag.Address(addr),
					tag.NewStringTag("network", network),
					tag.Error(err))
			}
		case "DNSStart":
			clientTrace.DNSStart = func(info httptrace.DNSStartInfo) {
				logger.Info("starting DNS lookup for Nexus request",
					tag.Timestamp(time.Now().UTC()),
					tag.Host(info.Host))
			}
		case "DNSDone":
			clientTrace.DNSDone = func(info httptrace.DNSDoneInfo) {
				addresses := make([]string, len(info.Addrs))
				for i, a := range info.Addrs {
					addresses[i] = a.String()
				}
				logger.Info("finished DNS lookup for Nexus request",
					tag.Timestamp(time.Now().UTC()),
					tag.Addresses(addresses),
					tag.Error(info.Err),
					tag.NewBoolTag("coalesced", info.Coalesced))
			}
		case "TLSHandshakeStart":
			clientTrace.TLSHandshakeStart = func() {
				logger.Info("starting TLS handshake for Nexus request", tag.Timestamp(time.Now().UTC()))
			}
		case "TLSHandshakeDone":
			clientTrace.TLSHandshakeDone = func(state tls.ConnectionState, err error) {
				logger.Info("finished TLS handshake for Nexus request",
					tag.Timestamp(time.Now().UTC()),
					// TODO: consider other state info
					tag.NewBoolTag("handshake-complete", state.HandshakeComplete),
					tag.Error(err))
			}
		case "WroteRequest":
			clientTrace.WroteRequest = func(info httptrace.WroteRequestInfo) {
				logger.Info("finished writing Nexus HTTP request",
					tag.Timestamp(time.Now().UTC()),
					tag.Error(info.Err))
			}
		case "GotFirstResponseByte":
			clientTrace.GotFirstResponseByte = func() {
				logger.Info("got response to Nexus HTTP request", tag.AttemptEnd(time.Now().UTC()))
			}
		}
	}
	return clientTrace
}
