package localexecution

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHistoryClosesWorkflow(t *testing.T) {
	serializer := serialization.NewSerializer()
	replicator := &HistoryReplicator{eventSerializer: serializer}

	for _, eventType := range []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
	} {
		batch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{{EventId: 3, EventType: eventType}})
		require.NoError(t, err)
		require.True(t, replicator.historyClosesWorkflow([]*commonpb.DataBlob{batch}))
	}

	openBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId:   3,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
	}})
	require.NoError(t, err)
	require.False(t, replicator.historyClosesWorkflow([]*commonpb.DataBlob{openBatch}))
	require.False(t, replicator.historyClosesWorkflow(nil))
}

func TestIsRetryReplicationError(t *testing.T) {
	typed := serviceerrors.NewRetryReplication(
		"resend",
		"namespace-id",
		"workflow-id",
		"run-id",
		1,
		1,
		2,
		1,
	)

	require.True(t, isRetryReplicationError(typed))
	require.True(t, isRetryReplicationError(status.Convert(typed.(*serviceerrors.RetryReplication).Status().Err()).Err()))
	require.False(t, isRetryReplicationError(status.Error(codes.Aborted, "unrelated conflict")))
	require.False(t, isRetryReplicationError(errors.New("not a gRPC status")))
}
