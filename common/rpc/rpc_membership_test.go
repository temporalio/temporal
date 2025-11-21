package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestRPCFactoryRemovesStaleConnectionsOnMembershipUpdate(t *testing.T) {
	const targetAddress = "matching-host:7235"

	// Setup

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockMonitor := membership.NewMockMonitor(ctrl)
	mockResolver := membership.NewMockServiceResolver(ctrl)

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = listener.Close() })

	server := grpc.NewServer()
	t.Cleanup(server.Stop)

	go func() {
		_ = server.Serve(listener)
	}()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return listener.Dial()
		}),
	}

	factory := NewFactory(
		&config.Config{
			Services: map[string]config.Service{
				string(primitives.MatchingService): {
					RPC: config.RPC{},
				},
			},
		},
		primitives.HistoryService,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil,
		"frontend:7233",
		"",
		0,
		nil,
		dialOptions,
		nil,
		mockMonitor,
	)

	// Add expectations

	var listenerChan chan<- *membership.ChangedEvent

	mockMonitor.EXPECT().GetResolver(primitives.MatchingService).Return(mockResolver, nil).Times(1)
	mockResolver.EXPECT().
		AddListener(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ string, ch chan<- *membership.ChangedEvent) error {
			listenerChan = ch
			return nil
		}).
		Times(1)
	mockResolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()

	// Create the first connection and validate it

	conn1 := factory.CreateMatchingGRPCConnection(targetAddress)
	require.NotNil(t, conn1)
	t.Cleanup(func() { _ = conn1.Close() })

	require.NotNil(t, listenerChan, "membership listener channel not captured")
	require.Same(t, conn1, factory.interNodeGrpcConnections.Get(targetAddress))

	// Next pretend the host has been removed

	listenerChan <- &membership.ChangedEvent{
		HostsRemoved: []membership.HostInfo{membership.NewHostInfoFromAddress(targetAddress)},
	}

	// Now we expect that the first connection has been dropped

	require.Eventually(t, func() bool {
		conn := factory.interNodeGrpcConnections.Get(targetAddress)
		return conn == nil
	}, time.Second, 10*time.Millisecond)

	mockMonitor.EXPECT().GetResolver(primitives.MatchingService).Times(0)
	mockResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Times(0)

	// Finally create a new connection to the same address, we expect to get a new one

	conn2 := factory.CreateMatchingGRPCConnection(targetAddress)
	require.NotNil(t, conn2)
	require.NotSame(t, conn1, conn2)
	t.Cleanup(func() { _ = conn2.Close() })
}
