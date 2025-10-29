package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
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

	// Create a static client with the config enabled for this namespace
	staticClient := dynamicconfig.StaticClient{
		dynamicconfig.EnableWorkerStateTracking.Key(): []dynamicconfig.ConstrainedValue{
			{
				Constraints: dynamicconfig.Constraints{Namespace: tv.NamespaceID().String()},
				Value:       true,
			},
		},
	}
	dc := dynamicconfig.NewCollection(staticClient, log.NewNoopLogger())
	handler := newHandler(dc)

	req := &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: tv.NamespaceID().String(),
		WorkerHeartbeat: &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: testWorkerInstanceKey,
		},
		LeaseDuration: durationpb.New(30 * time.Second),
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

func TestRecordHeartbeatHandlerDisabled(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	mockEngine := chasm.NewMockEngine(controller)
	ctx := chasm.NewEngineContext(context.Background(), mockEngine)

	tv := testvars.New(t)

	// Create handler with worker state tracking disabled (default is false)
	dc := dynamicconfig.NewNoopCollection()
	handler := newHandler(dc)

	req := &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: tv.NamespaceID().String(),
		WorkerHeartbeat: &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: testWorkerInstanceKey,
		},
		LeaseDuration: durationpb.New(30 * time.Second),
	}

	// Call the handler - should fail because setting is disabled
	resp, err := handler.RecordHeartbeat(ctx, req)

	// Verify error is returned
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "worker state tracking is disabled for namespace")
}
