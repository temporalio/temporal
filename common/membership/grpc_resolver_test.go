package membership

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/nettest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCBuilder(t *testing.T) {
	t.Parallel()

	// There's a lot of channel stuff in this test, so we use a context to make sure we don't hang forever if something
	// goes wrong.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	sr := NewMockServiceResolver(ctrl)

	// On the first call to [ServiceResolver.Members], return an empty list of members
	sr.EXPECT().AvailableMembers().Return([]HostInfo{})
	// Once our resolver registers a listener to membership changes, get a hold of the channel it's listening on.
	sr.EXPECT().AddListener(gomock.Any(), gomock.Any()).Do(func(_ string, ch chan<- *ChangedEvent) {
		// Return a single member on the next call to [ServiceResolver.Members]. This simulates a temporary network
		// partition where we can't find any hosts for the frontend for a short period of time, but then we get a host.
		sr.EXPECT().AvailableMembers().Return([]HostInfo{
			NewHostInfoFromAddress("localhost:1234"),
		}).MinTimes(1) // MinTimes(1) because we don't control when ResolveNow is called

		// After the first call to [ServiceResolver.Members] returns an empty list, expect our resolver to request a
		// refresh of the members list. When it does, notify the listener that the members list has changed
		sr.EXPECT().RequestRefresh().Do(func() {
			select {
			case <-ctx.Done():
			case ch <- &ChangedEvent{}:
			}
		})
	})

	monitor := NewMockMonitor(ctrl)
	monitor.EXPECT().GetResolver(primitives.FrontendService).Return(sr, nil)

	// Start a fake local server and then dial it.
	serverErrs := make(chan error)
	p := nettest.NewPipe()

	// This is our fake server. It accepts a connection and then immediately closes it.
	go func() {
		conn, _ := p.Accept(ctx.Done())
		serverErrs <- conn.Close()
	}()

	// This is where we invoke the code under test. We dial the frontend service. The URL should use our custom
	// protocol, and then our resolver should resolve this to the localhost:1234 address.
	url := GRPCResolverURLForTesting(monitor, primitives.FrontendService)
	assert.Regexp(t, "membership://frontend~0x[[:xdigit:]]*", url)

	// dialedAddress is the actual address that the gRPC framework dialed after resolving the URL using our resolver.
	var dialedAddress string

	conn, err := grpc.NewClient(
		url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			dialedAddress = s
			return p.Connect(ctx.Done())
		}),
	)
	require.NoError(t, err)
	conn.Connect()
	require.NoError(t, <-serverErrs)

	// The gRPC library calls [resolver.Resolver.Close] when the connection is closed in a background goroutine, so we
	// can't synchronously assert that [ServiceResolver.RemoveListener] was called right after the connection is closed.
	// Instead, we use a channel to signal that the listener was removed.
	listenerRemoved := make(chan struct{})

	sr.EXPECT().RemoveListener(gomock.Any()).Do(func(string) {
		close(listenerRemoved)
	})
	assert.NoError(t, conn.Close())
	select {
	case <-ctx.Done():
		t.Fatal("timed out waiting for resolver to be removed")
	case <-listenerRemoved:
	}

	// Verify that the address we dialed was the address of the single host in the members list.
	assert.Equal(t, "localhost:1234", dialedAddress)
}
