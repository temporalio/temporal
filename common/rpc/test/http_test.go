package rpc

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.uber.org/mock/gomock"
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
		nil, // No metrics handler
		nil,
		membership.GRPCResolverURLForTesting(monitor, primitives.FrontendService),
		membership.GRPCResolverURLForTesting(monitor, primitives.FrontendService),
		int(port),
		nil, // No TLS
		nil,
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
		nil, // No metrics handler
		nil,
		membership.GRPCResolverURLForTesting(nil, primitives.FrontendService),
		addr.String(),
		0,   // Port is unused
		nil, // No TLS
		nil,
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
		nil, // No metrics handler
		nil,
		membership.GRPCResolverURLForTesting(nil, primitives.FrontendService),
		addr.String(),
		0, // Port is unused
		tlsConfig,
		nil,
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
