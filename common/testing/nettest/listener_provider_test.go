package nettest_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/nettest"
)

func TestListenerProvider_GRPCListener(t *testing.T) {
	t.Parallel()

	provider := nettest.NewTestListenerProvider(t)
	testListenerAcceptsConnection(t, provider.GetGRPCListener())
}

func TestListenerProvider_MembershipListener(t *testing.T) {
	t.Parallel()

	provider := nettest.NewTestListenerProvider(t)
	testListenerAcceptsConnection(t, provider.GetMembershipListener())
}

func TestListenerProvider_ConsistentAddrs(t *testing.T) {
	t.Parallel()

	provider := nettest.NewTestListenerProvider(t)

	grpcAddr1 := provider.GetGRPCListener().Addr().String()
	grpcAddr2 := provider.GetGRPCListener().Addr().String()
	assert.Equal(t, grpcAddr1, grpcAddr2)

	membershipAddr1 := provider.GetMembershipListener().Addr().String()
	membershipAddr2 := provider.GetMembershipListener().Addr().String()
	assert.Equal(t, membershipAddr1, membershipAddr2)
}

func TestListenerProvider_DistinctAddrs(t *testing.T) {
	t.Parallel()

	provider := nettest.NewTestListenerProvider(t)
	grpcAddr := provider.GetGRPCListener().Addr().String()
	membershipAddr := provider.GetMembershipListener().Addr().String()
	assert.NotEqual(t, grpcAddr, membershipAddr)
}

func testListenerAcceptsConnection(t *testing.T, listener net.Listener) {
	t.Helper()

	errs := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if conn != nil {
			_ = conn.Close()
		}
		errs <- err
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	assert.NoError(t, conn.Close())
	assert.NoError(t, <-errs)
}
