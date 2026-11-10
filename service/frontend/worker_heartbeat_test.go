package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestLivenessHeartbeatRequestOmitsMetadata(t *testing.T) {
	t.Parallel()
	worker := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker-1",
		WorkerIdentity:    "worker-identity",
		TaskQueue:         "task-queue",
		HostInfo: &workerpb.WorkerHostInfo{
			WorkerGroupingKey: "group-1",
			HostName:          "host",
			ProcessId:         "process",
		},
	}
	request := &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: "namespace",
		Identity:  "client-identity",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			worker,
			{WorkerInstanceKey: "worker-2", HostInfo: &workerpb.WorkerHostInfo{WorkerGroupingKey: "group-1"}},
		},
	}
	original := common.CloneProto(request)

	// Forward the worker keys and routing information for the whole batch.
	got := newLivenessHeartbeatRequest(request, "group-1")
	protorequire.ProtoEqual(t, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace:  request.Namespace,
		Identity:   request.Identity,
		ResourceId: "group-1",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{WorkerInstanceKey: "worker-1"},
			{WorkerInstanceKey: "worker-2"},
		},
	}, got)

	// Matching still needs the original metadata for every worker.
	protorequire.ProtoEqual(t, original, request)
}

// TestWorkerHeartbeatIndependentUpdates verifies that each update runs despite
// the other failing, and that either failure prevents acknowledgment.
func TestWorkerHeartbeatIndependentUpdates(t *testing.T) {
	t.Parallel()
	matchingErr := serviceerror.NewUnavailable("matching unavailable")
	durableErr := serviceerror.NewUnavailable("durable heartbeat unavailable")
	for _, tc := range []struct {
		name        string
		matchingErr error
		durableErr  error
		expectedErr error
	}{
		{name: "both succeed"},
		{name: "matching fails", matchingErr: matchingErr, expectedErr: matchingErr},
		{name: "durable update fails", durableErr: durableErr, expectedErr: durableErr},
		{name: "both fail", matchingErr: matchingErr, durableErr: durableErr, expectedErr: matchingErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			registry := namespace.NewMockRegistry(controller)
			registry.EXPECT().GetNamespaceID(namespace.Name("namespace")).Return(namespace.ID("namespace-id"), nil)
			matching := matchingservicemock.NewMockMatchingServiceClient(controller)
			matchingStarted := make(chan struct{})
			durableStarted := make(chan struct{})
			matching.EXPECT().RecordWorkerHeartbeat(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ *matchingservice.RecordWorkerHeartbeatRequest, _ ...grpc.CallOption) (*matchingservice.RecordWorkerHeartbeatResponse, error) {
					close(matchingStarted)
					// Matching waits for the durable call to start, so serial forwarding cannot pass.
					select {
					case <-durableStarted:
						return &matchingservice.RecordWorkerHeartbeatResponse{}, tc.matchingErr
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
			)
			handler := &WorkflowHandler{
				config: &Config{
					WorkerHeartbeatsEnabled:       func(string) bool { return true },
					EnableDurableWorkerHeartbeats: func(string) bool { return true },
				},
				namespaceRegistry: registry,
				matchingClient:    matching,
				logger:            log.NewNoopLogger(),
				workerClient: &heartbeatTestClient{recordHeartbeat: func(ctx context.Context, _ *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
					select {
					case <-matchingStarted:
						close(durableStarted)
						return &workerstatepb.RecordHeartbeatResponse{}, tc.durableErr
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}},
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			response, err := handler.RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
				Namespace: "namespace",
				WorkerHeartbeat: []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker",
					HostInfo:          &workerpb.WorkerHostInfo{WorkerGroupingKey: "group"},
				}},
			})
			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.NotNil(t, response)
			}
			// A Matching failure must not skip the durable heartbeat.
			select {
			case <-durableStarted:
			default:
				t.Fatal("durable heartbeat was not sent")
			}
		})
	}
}

type heartbeatTestClient struct {
	workerstatepb.WorkerServiceClient
	recordHeartbeat func(context.Context, *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error)
}

func (c *heartbeatTestClient) RecordHeartbeat(ctx context.Context, req *workerstatepb.RecordHeartbeatRequest, _ ...grpc.CallOption) (*workerstatepb.RecordHeartbeatResponse, error) {
	return c.recordHeartbeat(ctx, req)
}
