package replication

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestEmitBufferedEventsSkipped(t *testing.T) {
	const targetClusterID = int32(3)
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey:  definition.NewWorkflowKey(tests.NamespaceID.String(), "wf-id", "run-id"),
		TaskID:       1444,
		FirstEventID: 13,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 101,
			TransitionCount:          7,
		},
	}

	setup := func(t *testing.T, state enumspb.ReplicationState, emitEnabled bool, startedWFT *historyi.WorkflowTaskInfo) *eventCaptureLogger {
		ctrl := gomock.NewController(t)
		capture := &eventCaptureLogger{}

		config := tests.NewDynamicConfig()
		config.EmitReplicationLifecycleEvents = func() bool { return emitEnabled }

		nsEntry := namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
			nil,
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "source_cluster",
				Clusters:          []string{"source_cluster", "target_cluster"},
				State:             state,
			},
			101,
		)
		registry := namespace.NewMockRegistry(ctrl)
		registry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(nsEntry, nil).AnyTimes()

		clusterMetadata := cluster.NewMockMetadata(ctrl)
		clusterMetadata.EXPECT().GetCurrentClusterName().Return("source_cluster").AnyTimes()
		clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
			"source_cluster": {InitialFailoverVersion: 1},
			"target_cluster": {InitialFailoverVersion: int64(targetClusterID)},
		}).AnyTimes()

		shardContext := historyi.NewMockShardContext(ctrl)
		shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
		shardContext.EXPECT().GetEventLogger().Return(capture).AnyTimes()
		shardContext.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
		shardContext.EXPECT().GetClusterMetadata().Return(clusterMetadata).AnyTimes()
		shardContext.EXPECT().GetShardID().Return(int32(874)).AnyTimes()

		mutableState := historyi.NewMockMutableState(ctrl)
		mutableState.EXPECT().GetNextEventID().Return(int64(40)).AnyTimes()
		mutableState.EXPECT().NumBufferedEvents().Return(3).AnyTimes()
		mutableState.EXPECT().GetStartedWorkflowTask().Return(startedWFT).AnyTimes()

		emitBufferedEventsSkipped(shardContext, task, targetClusterID, mutableState)
		return capture
	}

	t.Run("handover records the handover state", func(t *testing.T) {
		wftStarted := time.Date(2026, 11, 17, 6, 17, 33, 195000000, time.UTC)
		capture := setup(t, enumspb.REPLICATION_STATE_HANDOVER, true, &historyi.WorkflowTaskInfo{
			ScheduledEventID: 11,
			StartedEventID:   12,
			StartedTime:      wftStarted,
			Attempt:          1,
		})
		require.Len(t, capture.records, 1)
		rec := capture.records[0]
		require.Equal(t, wideevents.ReplicationLifecycleEventName, rec.EventName())

		fields := map[string]otellog.Value{}
		rec.WalkAttributes(func(kv otellog.KeyValue) bool {
			fields[kv.Key] = kv.Value
			return true
		})
		require.Equal(t, string(wideevents.ReplicationError), fields["phase"].AsString())
		require.Equal(t, wideevents.ReplTaskSyncVersionedTransition, fields["task_type"].AsString())
		require.Equal(t, "wf-id", fields["workflow_id"].AsString())
		require.Equal(t, "run-id", fields["run_id"].AsString())
		require.Equal(t, int64(1444), fields["source_task_id"].AsInt64())
		require.Equal(t, "source_cluster", fields["source_cluster"].AsString())
		require.Equal(t, int64(101), fields["failover_version"].AsInt64())
		require.Equal(t, int64(7), fields["transition_count"].AsInt64())

		var details map[string]any
		require.NoError(t, json.Unmarshal([]byte(fields["details"].AsString()), &details))
		require.Equal(t, wideevents.ReplOperationTaskConversion, details["operation"])
		require.Equal(t, string(wideevents.ReplDispositionDiscarded), details["disposition"])
		require.Equal(t, string(wideevents.ReplErrorClassificationBufferedEvents), details["classification"])
		require.Equal(t, enumspb.REPLICATION_STATE_HANDOVER.String(), details["replication_state"])
		require.Equal(t, "target_cluster", details["target_cluster"])
		require.EqualValues(t, 3, details["buffered_events_count"])
		require.EqualValues(t, 40, details["source_next_event_id"])
		require.EqualValues(t, 13, details["task_first_event_id"])
		require.EqualValues(t, 11, details["started_wft_scheduled_event_id"])
		require.EqualValues(t, 12, details["started_wft_started_event_id"])
		require.Equal(t, wftStarted.Format(time.RFC3339Nano), details["started_wft_started_time"])
		require.EqualValues(t, 1, details["started_wft_attempt"])
	})

	t.Run("normal replication state also emits and records the state", func(t *testing.T) {
		capture := setup(t, enumspb.REPLICATION_STATE_NORMAL, true, nil)
		require.Len(t, capture.records, 1)
		var details map[string]any
		capture.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
			if kv.Key == "details" {
				require.NoError(t, json.Unmarshal([]byte(kv.Value.AsString()), &details))
			}
			return true
		})
		require.Equal(t, enumspb.REPLICATION_STATE_NORMAL.String(), details["replication_state"])
		require.NotContains(t, details, "started_wft_started_event_id")
	})

	t.Run("lifecycle events disabled emits nothing", func(t *testing.T) {
		capture := setup(t, enumspb.REPLICATION_STATE_HANDOVER, false, nil)
		require.Empty(t, capture.records)
	})
}
