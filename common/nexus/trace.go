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

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// NewHTTPClientTrace returns a *httptrace.ClientTrace which adds additional logging information at each point in the
// HTTP request lifecycle. This trace must be added to the HTTP request context using httptrace.WithClientTrace for
// the logging hooks to be invoked. The provided logger should already be tagged with relevant request information
// e.g. using log.With(logger, tag.RequestID(id), tag.Operation(op), ...).
func NewHTTPClientTrace(logger log.Logger) *httptrace.ClientTrace {
	logger.Info("starting trace for Nexus HTTP request")
	return &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			logger.Info("attempting to get HTTP connection for Nexus request",
				tag.Timestamp(time.Now().UTC()),
				tag.Address(hostPort))
		},
		GotConn: func(info httptrace.GotConnInfo) {
			logger.Info("got HTTP connection for Nexus request",
				tag.Timestamp(time.Now().UTC()),
				tag.NewBoolTag("reused", info.Reused),
				tag.NewBoolTag("was-idle", info.WasIdle),
				tag.NewDurationTag("idle-time", info.IdleTime))
		},
		ConnectStart: func(network, addr string) {
			logger.Info("starting dial for new connection for Nexus request",
				tag.Timestamp(time.Now().UTC()),
				tag.Address(addr),
				tag.NewStringTag("network", network))
		},
		ConnectDone: func(network, addr string, err error) {
			logger.Info("finished dial for new connection for Nexus request",
				tag.Timestamp(time.Now().UTC()),
				tag.Address(addr),
				tag.NewStringTag("network", network),
				tag.Error(err))
		},
		DNSStart: func(info httptrace.DNSStartInfo) {
			logger.Info("starting DNS lookup for Nexus request",
				tag.Timestamp(time.Now().UTC()),
				tag.Host(info.Host))
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			addresses := make([]string, len(info.Addrs))
			for i, a := range info.Addrs {
				addresses[i] = a.String()
			}
			logger.Info("finished DNS lookup for Nexus request",
				tag.Timestamp(time.Now().UTC()),
				tag.Addresses(addresses),
				tag.Error(info.Err),
				tag.NewBoolTag("coalesced", info.Coalesced))
		},
		TLSHandshakeStart: func() {
			logger.Info("starting TLS handshake for Nexus request", tag.Timestamp(time.Now().UTC()))
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			logger.Info("finished TLS handshake for Nexus request",
				tag.Timestamp(time.Now().UTC()),
				// TODO: consider other state info
				tag.NewBoolTag("handshake-complete", state.HandshakeComplete),
				tag.Error(err))
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			logger.Info("finished writing Nexus HTTP request",
				tag.Timestamp(time.Now().UTC()),
				tag.Error(info.Err))
		},
		GotFirstResponseByte: func() {
			logger.Info("got response to Nexus HTTP request", tag.AttemptEnd(time.Now().UTC()))
		},
	}
}
