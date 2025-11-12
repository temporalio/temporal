package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
)

const (
	testWorkerInstanceKey = "test-worker-1"
)

func TestRecordHeartbeatHandler(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	mockEngine := chasm.NewMockEngine(controller)
	ctx := chasm.NewEngineContext(context.Background(), mockEngine)

	tv := testvars.New(t)

	handler := newHandler()

	req := &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: tv.NamespaceID().String(),
		FrontendRequest: &workflowservice.RecordWorkerHeartbeatRequest{
			Namespace: tv.NamespaceName().String(),
			Identity:  "test-identity",
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
				{
					WorkerInstanceKey: testWorkerInstanceKey,
				},
			},
		},
	}

	// Mock successful UpdateWithNewEntity
	mockEngine.EXPECT().
		UpdateWithNewEntity(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			chasm.EntityKey{
				NamespaceID: tv.NamespaceID().String(),
				BusinessID:  testWorkerInstanceKey,
			},
			[]byte("serialized-ref"),
			nil,
		)

	// Call the handler
	resp, err := handler.RecordHeartbeat(ctx, req)

	// Verify results
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestRecordHeartbeatHandlerInvalidHeartbeatCount(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	mockEngine := chasm.NewMockEngine(controller)
	ctx := chasm.NewEngineContext(context.Background(), mockEngine)

	tv := testvars.New(t)

	handler := newHandler()

	t.Run("ZeroHeartbeats", func(t *testing.T) {
		req := &workerstatepb.RecordHeartbeatRequest{
			NamespaceId: tv.NamespaceID().String(),
			FrontendRequest: &workflowservice.RecordWorkerHeartbeatRequest{
				Namespace:       tv.NamespaceName().String(),
				Identity:        "test-identity",
				WorkerHeartbeat: []*workerpb.WorkerHeartbeat{}, // Empty array
			},
		}

		resp, err := handler.RecordHeartbeat(ctx, req)

		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "exactly one worker heartbeat must be present")
	})

	t.Run("MultipleHeartbeats", func(t *testing.T) {
		req := &workerstatepb.RecordHeartbeatRequest{
			NamespaceId: tv.NamespaceID().String(),
			FrontendRequest: &workflowservice.RecordWorkerHeartbeatRequest{
				Namespace: tv.NamespaceName().String(),
				Identity:  "test-identity",
				WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
					{WorkerInstanceKey: "worker-1"},
					{WorkerInstanceKey: "worker-2"}, // Multiple workers
				},
			},
		}

		resp, err := handler.RecordHeartbeat(ctx, req)

		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "exactly one worker heartbeat must be present")
	})
}
