package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/wideevents"
)

func eventBatchBlob(t *testing.T, serializer serialization.Serializer, eventIDs ...int64) *commonpb.DataBlob {
	t.Helper()
	return eventBatchBlobAtVersion(t, serializer, 5, eventIDs...)
}

func eventBatchBlobAtVersion(t *testing.T, serializer serialization.Serializer, version int64, eventIDs ...int64) *commonpb.DataBlob {
	t.Helper()
	events := make([]*historypb.HistoryEvent, 0, len(eventIDs))
	for _, id := range eventIDs {
		events = append(events, &historypb.HistoryEvent{EventId: id, Version: version})
	}
	blob, err := serializer.SerializeEvents(events)
	require.NoError(t, err)
	return blob
}

func versionedTransitionArtifactTask(art *replicationspb.VersionedTransitionArtifact) *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				VersionedTransitionArtifact: art,
			},
		},
	}
}

// A mutation must report both bounds of the state diff: the exclusive lower bound comes from the
// sender's progress cache and exists nowhere else, and the inclusive upper bound (hydrated) is the
// versioned transition of the state actually serialized.
func TestPopulateSentArtifactInfo_Mutation(t *testing.T) {
	serializer := serialization.NewSerializer()
	task := versionedTransitionArtifactTask(&replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				ExclusiveStartVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 5, TransitionCount: 4,
				},
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 3},
							{NamespaceFailoverVersion: 5, TransitionCount: 7},
						},
						ParentWorkflowId: "p-wf",
						ParentRunId:      "p-run",
					},
				},
			},
		},
		EventBatches: []*commonpb.DataBlob{
			eventBatchBlob(t, serializer, 19, 20),
			eventBatchBlob(t, serializer, 21, 22, 23),
		},
	})

	var payload wideevents.ReplicationLifecyclePayload
	populateSentArtifactInfo(&payload, task, serializer, log.NewNoopLogger())

	require.Equal(t, wideevents.ArtifactKindMutation, payload.ArtifactKind)
	require.Equal(t, &wideevents.VersionedTransitionEntry{FailoverVersion: 5, TransitionCount: 4}, payload.ExclusiveStartVT)
	// hydrated is the LAST transition history entry, not the first.
	require.Equal(t, &wideevents.VersionedTransitionEntry{FailoverVersion: 5, TransitionCount: 7}, payload.HydratedVT)
	// bounds span the first and last batch, not just the first.
	require.Equal(t, int64(19), payload.ShippedFirstEventID)
	require.Equal(t, int64(23), payload.ShippedLastEventID)
	require.Equal(t, int64(5), payload.ShippedFirstEventVersion)
	require.Equal(t, int64(5), payload.ShippedLastEventVersion)
	require.Equal(t, "p-wf", payload.ParentWorkflowID)
}

// A snapshot carries whole mutable state, so it has no lower bound. The absence of ExclusiveStartVT
// is what distinguishes it from a diff.
func TestPopulateSentArtifactInfo_Snapshot(t *testing.T) {
	serializer := serialization.NewSerializer()
	task := versionedTransitionArtifactTask(&replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 5, TransitionCount: 9},
						},
					},
				},
			},
		},
	})

	var payload wideevents.ReplicationLifecyclePayload
	populateSentArtifactInfo(&payload, task, serializer, log.NewNoopLogger())

	require.Equal(t, wideevents.ArtifactKindSnapshot, payload.ArtifactKind)
	require.Nil(t, payload.ExclusiveStartVT)
	require.Equal(t, &wideevents.VersionedTransitionEntry{FailoverVersion: 5, TransitionCount: 9}, payload.HydratedVT)
	// no event batches shipped, so the bounds stay unset.
	require.Zero(t, payload.ShippedFirstEventID)
	require.Zero(t, payload.ShippedLastEventID)
}

// Verify tasks carry no mutable state, so they contribute no artifact detail at all.
func TestPopulateSentArtifactInfo_VerifyContributesNothing(t *testing.T) {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NextEventId: 30,
			},
		},
	}

	var payload wideevents.ReplicationLifecyclePayload
	populateSentArtifactInfo(&payload, task, serialization.NewSerializer(), log.NewNoopLogger())

	require.Empty(t, payload.ArtifactKind)
	require.Nil(t, payload.ExclusiveStartVT)
	require.Nil(t, payload.HydratedVT)
	require.Zero(t, payload.ShippedFirstEventID)
}

// Transition history is absent when state-based replication is disabled or the workflow is in an
// unknown versioned transition. That must not fabricate a join key.
func TestPopulateSentArtifactInfo_NoTransitionHistory(t *testing.T) {
	serializer := serialization.NewSerializer()
	task := versionedTransitionArtifactTask(&replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{},
				},
			},
		},
	})

	var payload wideevents.ReplicationLifecyclePayload
	populateSentArtifactInfo(&payload, task, serializer, log.NewNoopLogger())

	require.Equal(t, wideevents.ArtifactKindMutation, payload.ArtifactKind)
	require.Nil(t, payload.HydratedVT)
}

// Instrumentation must never fail the send path, so undecodable batches degrade to zero bounds.
func TestShippedEventRange_Degrades(t *testing.T) {
	serializer := serialization.NewSerializer()

	first, firstVer, last, lastVer := shippedEventRange(serializer, nil, log.NewNoopLogger())
	require.Zero(t, first)
	require.Zero(t, last)
	require.Zero(t, firstVer)
	require.Zero(t, lastVer)

	first, firstVer, last, lastVer = shippedEventRange(nil, []*commonpb.DataBlob{eventBatchBlob(t, serializer, 1)}, log.NewNoopLogger())
	require.Zero(t, first, "a nil serializer must not panic")
	require.Zero(t, last)
	require.Zero(t, firstVer)
	require.Zero(t, lastVer)

	garbage := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("not a proto")}
	first, firstVer, last, lastVer = shippedEventRange(serializer, []*commonpb.DataBlob{garbage}, log.NewNoopLogger())
	require.Zero(t, first)
	require.Zero(t, last)
	require.Zero(t, firstVer)
	require.Zero(t, lastVer)

	// A single batch reports the same batch for both bounds without a second deserialization.
	first, firstVer, last, lastVer = shippedEventRange(serializer, []*commonpb.DataBlob{eventBatchBlob(t, serializer, 7, 8, 9)}, log.NewNoopLogger())
	require.Equal(t, int64(7), first)
	require.Equal(t, int64(9), last)
	require.Equal(t, int64(5), firstVer)
	require.Equal(t, int64(5), lastVer)
}

// A shipped range that crosses a failover carries a different version at each bound, so each bound
// needs its own: the pair (event id, version) is what identifies a branch, not the id alone.
func TestShippedEventRange_VersionPerBound(t *testing.T) {
	serializer := serialization.NewSerializer()
	batches := []*commonpb.DataBlob{
		eventBatchBlobAtVersion(t, serializer, 1101290, 871, 872),
		eventBatchBlobAtVersion(t, serializer, 2101156, 873, 874),
	}

	first, firstVer, last, lastVer := shippedEventRange(serializer, batches, log.NewNoopLogger())

	require.Equal(t, int64(871), first)
	require.Equal(t, int64(1101290), firstVer)
	require.Equal(t, int64(874), last)
	require.Equal(t, int64(2101156), lastVer, "the last bound must report its own version, not the first's")
}

// A continue-as-new ships the successor's event 1 outside the shipped bounds; the run id locates it.
func TestPopulateSentArtifactInfo_NewRunInfo(t *testing.T) {
	serializer := serialization.NewSerializer()
	task := versionedTransitionArtifactTask(&replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{},
				},
			},
		},
		EventBatches: []*commonpb.DataBlob{eventBatchBlob(t, serializer, 19, 20)},
		NewRunInfo: &replicationspb.NewRunInfo{
			RunId:      "successor-run",
			EventBatch: eventBatchBlob(t, serializer, 1),
		},
	})

	var payload wideevents.ReplicationLifecyclePayload
	populateSentArtifactInfo(&payload, task, serializer, log.NewNoopLogger())

	require.Equal(t, "successor-run", payload.NewRunID)
	require.Equal(t, int64(19), payload.ShippedFirstEventID, "the range describes this run only")
	require.Equal(t, int64(20), payload.ShippedLastEventID)
}

func TestPopulateSentArtifactInfo_NoNewRunInfo(t *testing.T) {
	serializer := serialization.NewSerializer()
	task := versionedTransitionArtifactTask(&replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{},
				},
			},
		},
	})

	var payload wideevents.ReplicationLifecyclePayload
	populateSentArtifactInfo(&payload, task, serializer, log.NewNoopLogger())

	require.Empty(t, payload.NewRunID)
}

// A delete task carries no state or events, so it contributes only identity.
func TestPopulateSentArtifactInfo_DeleteExecution(t *testing.T) {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK,
	}

	var payload wideevents.ReplicationLifecyclePayload
	populateSentArtifactInfo(&payload, task, serialization.NewSerializer(), log.NewNoopLogger())

	require.Empty(t, payload.ArtifactKind)
	require.Nil(t, payload.HydratedVT)
	require.Zero(t, payload.ShippedFirstEventID)
	require.Empty(t, payload.NewRunID)
}
