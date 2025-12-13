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
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				VersioningOverride:          nil,
				UnsetVersioningOverride:     false,
				AttachedRequestId:           "test-attached-request-id",
				AttachedCompletionCallbacks: nil,
			},
		},
		Links: []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "whatever",
						WorkflowId: "abc",
						RunId:      uuid.NewString(),
					},
				},
			},
		},
	}
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
		event.Links,
		attr.GetIdentity(),
		attr.GetPriority(),
	).Return(event, nil)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_Signal() {
	runID := uuid.NewString()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
		Links: []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "whatever",
						WorkflowId: "abc",
						RunId:      uuid.NewString(),
					},
				},
			},
		},
	}
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
		event.Links,
	).Return(event, nil)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource)
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_Update() {
	runID := uuid.NewString()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	for _, event := range []*historypb.HistoryEvent{
		{
			EventId:   105,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
				Request: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("update-request-payload")}, Meta: &updatepb.Meta{UpdateId: "update-1"}},
				Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
			}},
		},
		{
			EventId:   105,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
				AcceptedRequest:    &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("update-request-payload")}, Meta: &updatepb.Meta{UpdateId: "update-2"}},
				ProtocolInstanceId: "update-2",
			}},
		},
	} {

		msCurrent := historyi.NewMockMutableState(s.controller)
		msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
		msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
		updateRegistry := update.NewRegistry(msCurrent)
		msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
		msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
		switch event.EventType {
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
			{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
			event,
		}
		appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
		s.NoError(err)
		s.Equal(1, len(appliedEvent))
	}
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_Noop() {
	runID := uuid.NewString()
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
		}},
	}

	msCurrent := historyi.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(true)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(0, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_PartialAppliedEvent() {
	runID := uuid.NewString()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	event1 := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:   2,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
	}
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
		event1.Links,
	).Return(event1, nil)
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	dedupResource1 := definition.NewEventReappliedID(runID, event1.GetEventId(), event1.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource1).Return(false)
	dedupResource2 := definition.NewEventReappliedID(runID, event2.GetEventId(), event2.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource2).Return(true)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource1)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event1,
		event2,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_Error() {
	runID := uuid.NewString()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
	}
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
		event.Links,
	).Return(nil, fmt.Errorf("test"))
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	msCurrent.EXPECT().HSM().Return(s.hsmNode).AnyTimes()
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.Error(err)
	s.Equal(0, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_Termination() {
	runID := uuid.NewString()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
			Reason:   "test",
			Details:  payloads.EncodeBytes([]byte{}),
			Identity: "test",
		}},
	}
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
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_NoPendingWorkflowTask() {
	runID := uuid.NewString()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
		Links: []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "whatever",
						WorkflowId: "abc",
						RunId:      uuid.NewString(),
					},
				},
			},
		},
	}
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
		event.Links,
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
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
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
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.NewString(),
	}
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
	}
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
		event.Links,
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
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Len(appliedEvent, 1)
}
