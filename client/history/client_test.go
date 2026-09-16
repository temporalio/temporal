package history_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/testing/nettest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type (
	testHistoryService struct {
		historyservice.UnimplementedHistoryServiceServer
	}
	testRPCFactory struct {
		base            *nettest.RPCFactory
		dialedAddresses []string
	}
)

func TestErrLookup(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	serviceResolver := membership.NewMockServiceResolver(ctrl)
	serviceResolver.EXPECT().Lookup(gomock.Any()).Return(nil, membership.ErrInsufficientHosts).AnyTimes()
	serviceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	serviceResolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
	serviceResolver.EXPECT().Members().Return(nil).AnyTimes()
	client := history.NewClient(
		dynamicconfig.NewNoopCollection(),
		serviceResolver,
		log.NewTestLogger(),
		1,
		nil,
		time.Duration(0),
	)

	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{
			name: "GetDLQTasks",
			fn: func() error {
				_, err := client.GetDLQTasks(context.Background(), &historyservice.GetDLQTasksRequest{})
				return err
			},
		},
		{
			name: "DeleteDLQTasks",
			fn: func() error {
				_, err := client.DeleteDLQTasks(context.Background(), &historyservice.DeleteDLQTasksRequest{})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.fn()
			require.ErrorIs(t, err, membership.ErrInsufficientHosts)
		})
	}
}

// This tests our strategy for getting history hosts to serve shard-agnostic requests, like those interacting with the
// DLQ. For such requests, we should route to a random history shard. In addition, we should re-use any available
// connections if we hit the same host.
func TestShardAgnosticConnectionStrategy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		fn   func(client historyservice.HistoryServiceClient) error
	}{
		{
			name: "GetDLQTasks",
			fn: func(client historyservice.HistoryServiceClient) error {
				_, err := client.GetDLQTasks(context.Background(), &historyservice.GetDLQTasksRequest{})
				return err
			},
		},
		{
			name: "DeleteDLQTasks",
			fn: func(client historyservice.HistoryServiceClient) error {
				_, err := client.DeleteDLQTasks(context.Background(), &historyservice.DeleteDLQTasksRequest{})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			// Create a service resolver that just returns 2 hosts for the first 3 requests. We want to send 3 requests
			// with 2 hosts so that we can verify that we re-use the connection of "test1" on the last request.
			serviceResolver := membership.NewMockServiceResolver(ctrl)
			serviceResolver.EXPECT().Lookup(gomock.Any()).Return(membership.NewHostInfoFromAddress("localhost"), nil)
			serviceResolver.EXPECT().Lookup(gomock.Any()).Return(membership.NewHostInfoFromAddress("127.0.0.1"), nil)
			serviceResolver.EXPECT().Lookup(gomock.Any()).Return(membership.NewHostInfoFromAddress("localhost"), nil)
			serviceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			serviceResolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
			serviceResolver.EXPECT().Members().Return(nil).AnyTimes()

			// Create an in-memory gRPC server.
			listener := nettest.NewListener(nettest.NewPipe())
			rpcFactory := &testRPCFactory{
				base: nettest.NewRPCFactory(listener),
			}
			grpcServer := grpc.NewServer()
			testSvc := &testHistoryService{}
			historyservice.RegisterHistoryServiceServer(grpcServer, testSvc)
			errs := make(chan error)
			go func() {
				errs <- grpcServer.Serve(listener)
			}()
			defer func() {
				grpcServer.Stop()
				require.NoError(t, <-errs)
			}()

			// Send 3 requests to verify that we re-use a connection for the last request.
			client := history.NewClient(
				dynamicconfig.NewNoopCollection(),
				serviceResolver,
				log.NewTestLogger(),
				2,
				rpcFactory,
				time.Second,
			)
			for range 3 {
				err := tc.fn(client)
				require.NoError(t, err)
			}

			// Verify that there are no repeated dialed addresses (indicating that we re-used the connection).
			assert.Equal(
				t,
				[]string{"localhost", "127.0.0.1"},
				rpcFactory.dialedAddresses,
				"Should cache the client connection and reuse it for subsequent requests",
			)
		})
	}
}

func TestIsActivityTaskValidRouting(t *testing.T) {
	const (
		namespaceID    = "test-namespace-id"
		businessID     = "test-activity-id"
		workflowID     = "test-workflow-id"
		numberOfShards = int32(128)
	)
	standaloneShardID := common.WorkflowIDToHistoryShard(namespaceID, businessID, numberOfShards)
	workflowShardID := common.WorkflowIDToHistoryShard(namespaceID, workflowID, numberOfShards)
	require.NotEqual(t, standaloneShardID, workflowShardID)

	for _, tc := range []struct {
		name       string
		routingID  string
		standalone bool
	}{
		{
			name:       "standalone activity uses business ID",
			routingID:  businessID,
			standalone: true,
		},
		{
			name:      "workflow activity uses workflow ID",
			routingID: workflowID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			expectedShardID := common.WorkflowIDToHistoryShard(namespaceID, tc.routingID, numberOfShards)

			serviceResolver := membership.NewMockServiceResolver(ctrl)
			serviceResolver.EXPECT().Lookup(convert.Int32ToString(expectedShardID)).Return(membership.NewHostInfoFromAddress("localhost"), nil)
			serviceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			serviceResolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
			serviceResolver.EXPECT().Members().Return(nil).AnyTimes()

			listener := nettest.NewListener(nettest.NewPipe())
			grpcServer := grpc.NewServer()
			historyservice.RegisterHistoryServiceServer(grpcServer, &testHistoryService{})
			errs := make(chan error)
			go func() {
				errs <- grpcServer.Serve(listener)
			}()
			defer func() {
				grpcServer.Stop()
				require.NoError(t, <-errs)
			}()

			request := &historyservice.IsActivityTaskValidRequest{NamespaceId: namespaceID}
			if tc.standalone {
				componentRef, err := (&persistencespb.ChasmComponentRef{
					NamespaceId: namespaceID,
					BusinessId:  businessID,
				}).Marshal()
				require.NoError(t, err)
				request.ComponentRef = componentRef
			} else {
				request.Execution = &commonpb.WorkflowExecution{WorkflowId: workflowID}
			}

			client := history.NewClient(
				dynamicconfig.NewNoopCollection(),
				serviceResolver,
				log.NewTestLogger(),
				numberOfShards,
				nettest.NewRPCFactory(listener),
				time.Second,
			)
			response, err := client.IsActivityTaskValid(context.Background(), request)
			require.NoError(t, err)
			require.True(t, response.GetIsValid())
		})
	}
}

func TestIsActivityTaskValidInvalidComponentRef(t *testing.T) {
	missingBusinessIDRef, err := (&persistencespb.ChasmComponentRef{NamespaceId: "test-namespace-id"}).Marshal()
	require.NoError(t, err)
	missingNamespaceIDRef, err := (&persistencespb.ChasmComponentRef{BusinessId: "test-activity-id"}).Marshal()
	require.NoError(t, err)

	for _, tc := range []struct {
		name         string
		componentRef []byte
		errMessage   string
	}{
		{
			name:         "malformed",
			componentRef: []byte("not-a-component-ref"),
			errMessage:   "error deserializing component ref",
		},
		{
			name:         "missing business ID",
			componentRef: missingBusinessIDRef,
			errMessage:   "component ref missing namespace ID or business ID",
		},
		{
			name:         "missing namespace ID",
			componentRef: missingNamespaceIDRef,
			errMessage:   "component ref missing namespace ID or business ID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			serviceResolver := membership.NewMockServiceResolver(ctrl)
			serviceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			serviceResolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
			serviceResolver.EXPECT().Members().Return(nil).AnyTimes()

			client := history.NewClient(
				dynamicconfig.NewNoopCollection(),
				serviceResolver,
				log.NewTestLogger(),
				1,
				nil,
				time.Second,
			)
			_, err := client.IsActivityTaskValid(context.Background(), &historyservice.IsActivityTaskValidRequest{
				ComponentRef: tc.componentRef,
			})

			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.ErrorContains(t, err, tc.errMessage)
		})
	}
}

func (s *testHistoryService) GetDLQTasks(
	context.Context,
	*historyservice.GetDLQTasksRequest,
) (*historyservice.GetDLQTasksResponse, error) {
	return &historyservice.GetDLQTasksResponse{}, nil
}

func (s *testHistoryService) DeleteDLQTasks(
	context.Context,
	*historyservice.DeleteDLQTasksRequest,
) (*historyservice.DeleteDLQTasksResponse, error) {
	return &historyservice.DeleteDLQTasksResponse{}, nil
}

func (s *testHistoryService) IsActivityTaskValid(
	context.Context,
	*historyservice.IsActivityTaskValidRequest,
) (*historyservice.IsActivityTaskValidResponse, error) {
	return &historyservice.IsActivityTaskValidResponse{IsValid: true}, nil
}

func (t *testRPCFactory) CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn {
	t.dialedAddresses = append(t.dialedAddresses, rpcAddress)
	return t.base.CreateHistoryGRPCConnection(rpcAddress)
}
