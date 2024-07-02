// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package rpc

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"google.golang.org/grpc"
)

func TestCreateLocalFrontendHTTPClient_UsingMembership(t *testing.T) {
	ctrl := gomock.NewController(t)
	monitor := membership.NewMockMonitor(ctrl)
	resolver := membership.NewMockServiceResolver(ctrl)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr()
	_, portStr, err := net.SplitHostPort(addr.String())
	require.NoError(t, err)
	port, err := strconv.ParseInt(portStr, 10, 64)
	require.NoError(t, err)
	monitor.EXPECT().GetResolver(primitives.FrontendService).Return(resolver, nil)
	resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{membership.NewHostInfoFromAddress(addr.String())})

	fact := rpc.NewFactory(
		nil,
		primitives.HistoryService,
		nil, // No logger
		nil,
		membership.MakeResolverURL(primitives.FrontendService),
		membership.MakeResolverURL(primitives.FrontendService),
		int(port),
		nil, // No TLS
		[]grpc.UnaryClientInterceptor{},
		monitor,
	)

	client, err := fact.CreateLocalFrontendHTTPClient()
	require.NoError(t, err)
	require.Equal(t, "internal", client.Address)
	require.Equal(t, "http", client.Scheme)
	res, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestCreateLocalFrontendHTTPClient_UsingFixedHostPort(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr()

	fact := rpc.NewFactory(
		nil, // unused
		primitives.HistoryService,
		nil, // No logger
		nil,
		membership.MakeResolverURL(primitives.FrontendService),
		addr.String(),
		0, // Port is unused
		nil, // No TLS
		[]grpc.UnaryClientInterceptor{},
		nil, // monitor should not be used
	)

	client, err := fact.CreateLocalFrontendHTTPClient()
	require.NoError(t, err)
	require.Equal(t, addr.String(), client.Address)
	require.Equal(t, "http", client.Scheme)
	res, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestCreateLocalFrontendHTTPClient_UsingFixedHostPort_AndTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr()
	tlsConfig := srv.Client().Transport.(*http.Transport).TLSClientConfig

	fact := rpc.NewFactory(
		nil, // unused
		primitives.HistoryService,
		nil, // No logger
		nil,
		membership.MakeResolverURL(primitives.FrontendService),
		addr.String(),
		0, // Port is unused
		tlsConfig,
		[]grpc.UnaryClientInterceptor{},
		nil, // monitor should not be used
	)

	client, err := fact.CreateLocalFrontendHTTPClient()
	require.NoError(t, err)
	require.Equal(t, addr.String(), client.Address)
	require.Equal(t, "https", client.Scheme)
	res, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
}
