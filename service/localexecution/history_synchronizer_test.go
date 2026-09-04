package localexecution

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/persistence/serialization"
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

func TestHistoryAfterCursorTrimsReturnedPrefix(t *testing.T) {
	serializer := serialization.NewSerializer()
	firstBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: 1},
		{EventId: 2},
	})
	require.NoError(t, err)
	secondBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: 3},
		{EventId: 4},
	})
	require.NoError(t, err)
	replicator := &HistoryReplicator{
		cursor:          SyncCursor{EventID: 3},
		eventSerializer: serializer,
	}

	delta, err := replicator.historyAfterCursor([]*commonpb.DataBlob{firstBatch, secondBatch})
	require.NoError(t, err)
	require.Len(t, delta, 1)
	events, err := serializer.DeserializeEvents(delta[0])
	require.NoError(t, err)
	require.Equal(t, []int64{4}, []int64{events[0].GetEventId()})
}
