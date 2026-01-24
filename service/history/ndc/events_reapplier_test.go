package ndc

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

type (
	nDCEventReapplicationSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		nDCReapplication EventsReapplier

		hsmNode *hsm.Node
	}
)

func TestNDCEventReapplicationSuite(t *testing.T) {
	s := new(nDCEventReapplicationSuite)
	suite.Run(t, s)
}

func (s *nDCEventReapplicationSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler
	s.nDCReapplication = NewEventsReapplier(
		hsm.NewRegistry(),
		metricsHandler,
		logger,
	)

	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	s.NoError(err)
	s.hsmNode = root
}

func (s *nDCEventReapplicationSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_WorkflowExecutionOptionsUpdated() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	event := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		WorkflowExecutionOptionsUpdatedEventAttributes: historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:          nil,
			UnsetVersioningOverride:     false,
			AttachedRequestId:           "test-attached-request-id",
			AttachedCompletionCallbacks: nil,
		}.Build(),
		Links: []*commonpb.Link{
			commonpb.Link_builder{
				WorkflowEvent: commonpb.Link_WorkflowEvent_builder{
					Namespace:  "whatever",
					WorkflowId: "abc",
					RunId:      uuid.NewString(),
				}.Build(),
			}.Build(),
		},
	}.Build()
	attr := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
		attr.GetVersioningOverride(),
		attr.GetUnsetVersioningOverride(),
		attr.GetAttachedRequestId(),
		attr.GetAttachedCompletionCallbacks(),
		event.GetLinks(),
		attr.GetIdentity(),
		attr.GetPriority(),
	).Return(event, nil)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_Signal() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	event := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
		Links: []*commonpb.Link{
			commonpb.Link_builder{
				WorkflowEvent: commonpb.Link_WorkflowEvent_builder{
					Namespace:  "whatever",
					WorkflowId: "abc",
					RunId:      uuid.NewString(),
				}.Build(),
			}.Build(),
		},
	}.Build()
	attr := event.GetWorkflowExecutionSignaledEventAttributes()

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
		attr.GetHeader(),
		event.GetLinks(),
	).Return(event, nil)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_Update() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	for _, event := range []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId:   105,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
			WorkflowExecutionUpdateAdmittedEventAttributes: historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
				Request: updatepb.Request_builder{Input: updatepb.Input_builder{Args: payloads.EncodeString("update-request-payload")}.Build(), Meta: updatepb.Meta_builder{UpdateId: "update-1"}.Build()}.Build(),
				Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
			}.Build(),
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId:   105,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
			WorkflowExecutionUpdateAcceptedEventAttributes: historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
				AcceptedRequest:    updatepb.Request_builder{Input: updatepb.Input_builder{Args: payloads.EncodeString("update-request-payload")}.Build(), Meta: updatepb.Meta_builder{UpdateId: "update-2"}.Build()}.Build(),
				ProtocolInstanceId: "update-2",
			}.Build(),
		}.Build(),
	} {

		msCurrent := historyi.NewMockMutableState(s.controller)
		msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
		msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
		updateRegistry := update.NewRegistry(msCurrent)
		msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
		msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
			attr := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
			msCurrent.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(
				attr.GetRequest(),
				enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
			).Return(event, nil)
			msCurrent.EXPECT().GetUpdateOutcome(gomock.Any(), attr.GetRequest().GetMeta().GetUpdateId()).Return(nil, serviceerror.NewNotFound(""))
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			attr := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
			msCurrent.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(
				attr.GetAcceptedRequest(),
				enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY,
			).Return(event, nil)
			msCurrent.EXPECT().GetUpdateOutcome(gomock.Any(), attr.GetProtocolInstanceId()).Return(nil, serviceerror.NewNotFound(""))
		}
		msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
		msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
		dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
		msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
		msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
		events := []*historypb.HistoryEvent{
			historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
			event,
		}
		appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
		s.NoError(err)
		s.Equal(1, len(appliedEvent))
	}
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_Noop() {
	runID := uuid.NewString()
	event := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
		}.Build(),
	}.Build()

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(true)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(0, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_PartialAppliedEvent() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	event1 := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
	}.Build()
	event2 := historypb.HistoryEvent_builder{
		EventId:   2,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
	}.Build()
	attr1 := event1.GetWorkflowExecutionSignaledEventAttributes()

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr1.GetSignalName(),
		attr1.GetInput(),
		attr1.GetIdentity(),
		attr1.GetHeader(),
		event1.GetLinks(),
	).Return(event1, nil)
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	dedupResource1 := definition.NewEventReappliedID(runID, event1.GetEventId(), event1.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource1).Return(false)
	dedupResource2 := definition.NewEventReappliedID(runID, event2.GetEventId(), event2.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource2).Return(true)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource1)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event1,
		event2,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_Error() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	event := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
	}.Build()
	attr := event.GetWorkflowExecutionSignaledEventAttributes()

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
		attr.GetHeader(),
		event.GetLinks(),
	).Return(nil, fmt.Errorf("test"))
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.Error(err)
	s.Equal(0, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_Termination() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	event := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		WorkflowExecutionTerminatedEventAttributes: historypb.WorkflowExecutionTerminatedEventAttributes_builder{
			Reason:   "test",
			Details:  payloads.EncodeBytes([]byte{}),
			Identity: "test",
		}.Build(),
	}.Build()
	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	gomock.InOrder(
		msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true),
		msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(false),
	)
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
	msCurrent.EXPECT().GetNextEventID().Return(int64(2))
	msCurrent.EXPECT().GetStartedWorkflowTask().Return(nil)
	msCurrent.EXPECT().AddWorkflowExecutionTerminatedEvent(
		int64(2),
		event.GetWorkflowExecutionTerminatedEventAttributes().GetReason(),
		event.GetWorkflowExecutionTerminatedEventAttributes().GetDetails(),
		event.GetWorkflowExecutionTerminatedEventAttributes().GetIdentity(),
		false,
		nil,
	).Return(nil, nil)
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_NoPendingWorkflowTask() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	event := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
		Links: []*commonpb.Link{
			commonpb.Link_builder{
				WorkflowEvent: commonpb.Link_WorkflowEvent_builder{
					Namespace:  "whatever",
					WorkflowId: "abc",
					RunId:      uuid.NewString(),
				}.Build(),
			}.Build(),
		},
	}.Build()
	attr := event.GetWorkflowExecutionSignaledEventAttributes()

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
		attr.GetHeader(),
		event.GetLinks(),
	).Return(event, nil)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(false)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
	msCurrent.EXPECT().HasPendingWorkflowTask().Return(false)
	msCurrent.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	msCurrent.EXPECT().AddWorkflowTaskScheduledEvent(
		false,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	).Return(nil, nil)
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

// Reapplies a signal event to a paused workflow
// Asserts that AddWorkflowTaskScheduledEvent() is NOT called

func (s *nDCEventReapplicationSuite) TestReapplyEvents_PausedWorkflow_NoWorkflowTaskScheduled() {
	runID := uuid.NewString()
	execution := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId: uuid.NewString(),
	}.Build()
	event := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
	}.Build()
	attr := event.GetWorkflowExecutionSignaledEventAttributes()

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
		attr.GetHeader(),
		event.GetLinks(),
	).Return(event, nil)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(false)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
	msCurrent.EXPECT().HasPendingWorkflowTask().Return(false)
	// Workflow is paused, so AddWorkflowTaskScheduledEvent should NOT be called.
	msCurrent.EXPECT().IsWorkflowExecutionStatusPaused().Return(true)
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}.Build(),
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Len(appliedEvent, 1)
}
