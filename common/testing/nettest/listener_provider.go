package nettest

import (
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
)

type (
	testListenerProvider struct {
		*testing.T
		grpc       lockedListener
		membership lockedListener
	}
	lockedListener struct {
		listener net.Listener
		sync.Mutex
	}
)

func NewTestListenerProvider(t *testing.T) common.ListenerProvider {
	listenerProvider := &testListenerProvider{T: t}
	t.Cleanup(listenerProvider.close)
	return listenerProvider
}

func (lp *testListenerProvider) GetGRPCListener() net.Listener {
	return lp.grpc.getOrInit(lp.T)
}

func (lp *testListenerProvider) GetMembershipListener() net.Listener {
	return lp.membership.getOrInit(lp.T)
}

func (lp *testListenerProvider) close() {
	lp.grpc.close()
	lp.membership.close()
}

func (ll *lockedListener) getOrInit(t *testing.T) net.Listener {
	ll.Lock()
	defer ll.Unlock()
	if ll.listener == nil {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		ll.listener = listener
	}
	return ll.listener
}

func (ll *lockedListener) close() {
	ll.Lock()
	defer ll.Unlock()
	if ll.listener != nil {
		_ = ll.listener.Close()
	}
}
