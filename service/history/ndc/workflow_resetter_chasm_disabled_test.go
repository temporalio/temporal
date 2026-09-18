package ndc

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

// TestReapplyEventsChasmDisabledWithPersistedNodes builds the state that makes ChasmEnabled insufficient on its
// own: a mutable state loaded from a record that holds CHASM nodes, in a namespace where EnableChasm is off.
// NewMutableStateFromDB reads dbRecord.ChasmNodes regardless of the config but hydrates the tree only when the
// config is on, so the tree is the noop one -- ChasmEnabled reports false -- while the operation's node is still
// there. Skipping on ChasmEnabled alone would silently drop the completion of an operation that exists, so
// reapply fails instead and lets the replication task retry.
func (s *workflowResetterSuite) TestReapplyEventsChasmDisabledWithPersistedNodes() {
	const scheduledEventID = int64(5)
	const eventType = enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED

	dbRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      string(tests.NamespaceID),
			WorkflowId:       tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{}}},
			ExecutionStats:   &persistencespb.ExecutionStats{},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 10,
		// The workflow holds CHASM state: a root node and a node for the Nexus operation the event addresses.
		ChasmNodes: map[string]*persistencespb.ChasmNode{
			"": {Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
				},
			}},
			"Operations/5": {Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
				},
			}},
		},
	}

	hsmReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(hsmReg))
	s.mockShard.SetStateMachineRegistry(hsmReg)
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()

	// EnableChasm is off for this namespace, which is what leaves the tree unhydrated.
	s.mockShard.GetConfig().EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)

	ms, err := workflow.NewMutableStateFromDB(
		s.mockShard, events.NewMockCache(s.controller), s.logger, tests.LocalNamespaceEntry, dbRecord, 123)
	s.NoError(err)

	// The two signals disagree: no hydrated tree, but the workflow does carry CHASM nodes.
	s.False(ms.ChasmEnabled(), "the tree must be the noop one when EnableChasm is off")
	s.True(ms.HasChasmNodes(), "the persisted CHASM nodes are tracked regardless of the config")

	event := &historypb.HistoryEvent{
		EventId:   9,
		EventType: eventType,
		Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
			NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}

	applied, err := reapplyEvents(
		context.Background(), ms, nil,
		newHSMRegistryWithEvent(eventType, hsm.ErrStateMachineNotFound),
		s.newChasmRegistryWithEvent(eventType, nil),
		[]*historypb.HistoryEvent{event}, nil, "", false, s.logger,
	)

	s.ErrorIs(err, errChasmDisabledWithNodes,
		"the operation may be among the unhydrated nodes, so the event must not be silently skipped")
	s.Empty(applied)
}
