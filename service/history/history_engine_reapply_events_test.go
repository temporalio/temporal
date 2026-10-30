package history

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type reapplyNexusEventsLibrary struct{}

func (reapplyNexusEventsLibrary) CommandHandlers() map[enumspb.CommandType]chasmworkflow.CommandHandler {
	return nil
}

func (reapplyNexusEventsLibrary) EventDefinitions() []chasmworkflow.EventDefinition {
	return []chasmworkflow.EventDefinition{
		chasmworkflow.CompletedEventDefinition{},
		chasmworkflow.ScheduledEventDefinition{},
	}
}

func (s *engine2Suite) TestReapplyEvents_SkippedChasmEvents() {
	for _, tc := range []struct {
		name        string
		event       *historypb.HistoryEvent
		wantPersist bool
	}{
		{
			name: "missing Nexus operation",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
				Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
					NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{ScheduledEventId: 9},
				},
			},
			wantPersist: true,
		},
		{
			name: "Nexus command event",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
				Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
					NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{},
				},
			},
			wantPersist: true,
		},
		{
			name: "unregistered event stays clean",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
				Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
					TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{},
				},
			},
		},
	} {
		s.Run(tc.name, func() {
			ctx := metrics.AddMetricsContext(context.Background())
			s.config.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
			s.config.EnableCHASMWorkflowRootOnStart = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
			registry := chasmworkflow.NewRegistry()
			s.Require().NoError(registry.Register(reapplyNexusEventsLibrary{}))
			s.Require().NoError(s.mockShard.ChasmRegistry().Register(&chasm.CoreLibrary{}))
			s.Require().NoError(s.mockShard.ChasmRegistry().Register(chasmworkflow.NewLibrary(registry)))
			s.mockShard.SetChasmWorkflowRegistry(registry)
			s.historyEngine.eventsReapplier = ndc.NewEventsReapplier(
				s.mockShard.StateMachineRegistry(), registry, metrics.NoopMetricsHandler, s.logger,
			)

			const eventVersion int64 = 100
			s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, eventVersion).
				Return(cluster.TestAlternativeClusterName).AnyTimes()
			we := &commonpb.WorkflowExecution{WorkflowId: "reapply-skipped-event", RunId: tests.RunID}
			ms := workflow.TestGlobalMutableState(s.mockShard, s.mockEventsCache, s.logger, tests.Version, we.WorkflowId, we.RunId)
			addWorkflowExecutionStartedEvent(ms, we, "workflow-type", "task-queue", payloads.EncodeString("input"),
				time.Hour, time.Minute, time.Hour, "test-identity")
			_, _, err := ms.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
			s.Require().NoError(err)
			s.Require().False(ms.IsDirty())
			state := ms.CloneToProto()
			s.Require().NotEmpty(state.ChasmNodes)
			nextEventID := ms.GetNextEventID()

			s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).
				Return(&persistence.GetCurrentExecutionResponse{RunID: we.RunId}, nil)
			s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
				Return(&persistence.GetWorkflowExecutionResponse{State: state}, nil)
			if tc.wantPersist {
				s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						s.Equal(nextEventID, request.UpdateWorkflowMutation.NextEventID)
						return tests.UpdateWorkflowExecutionResponse, nil
					},
				)
			}
			s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).
				Return(searchattribute.TestNameTypeMap(), nil).AnyTimes()
			s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
				Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

			tc.event.EventId = 10
			tc.event.Version = eventVersion
			err = s.historyEngine.ReapplyEvents(ctx, tests.NamespaceID, we.WorkflowId, "source-run", []*historypb.HistoryEvent{tc.event})
			s.Require().NoError(err)

			wfCtx, release, err := s.workflowCache.GetOrCreateWorkflowExecution(ctx, s.mockShard, tests.NamespaceID, we, locks.PriorityHigh)
			s.Require().NoError(err)
			defer release(nil)
			loadedMS, err := wfCtx.LoadMutableState(ctx, s.mockShard)
			s.Require().NoError(err)
			s.False(loadedMS.IsDirty())
			s.Equal(nextEventID, loadedMS.GetNextEventID())
			s.False(loadedMS.HasPendingWorkflowTask())
		})
	}
}
