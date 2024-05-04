// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ndc

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	updatepb "go.temporal.io/api/update/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

type (
	nDCEventReapplicationSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		nDCReapplication EventsReapplier
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
		metricsHandler,
		logger,
	)
}

func (s *nDCEventReapplicationSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent_Signal() {
	runID := uuid.New()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.New(),
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

	msCurrent := workflow.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msCurrent.EXPECT().GetLastWriteVersion().Return(int64(1), nil).AnyTimes()
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
		attr.GetHeader(),
		attr.GetSkipGenerateWorkflowTask(),
	).Return(event, nil)
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
	runID := uuid.New()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.New(),
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
				AcceptedRequest: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("update-request-payload")}, Meta: &updatepb.Meta{UpdateId: "update-2"}},
			}},
		},
	} {

		msCurrent := workflow.NewMockMutableState(s.controller)
		msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
		msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
		updateRegistry := update.NewRegistry(msCurrent)
		msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
		msCurrent.EXPECT().GetLastWriteVersion().Return(int64(1), nil).AnyTimes()
		msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
			attr := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
			msCurrent.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(
				attr.GetRequest(),
				enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
			).Return(event, nil)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			attr := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
			msCurrent.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(
				attr.GetAcceptedRequest(),
				enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY,
			).Return(event, nil)
		}
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
	runID := uuid.New()
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      payloads.EncodeBytes([]byte{}),
		}},
	}

	msCurrent := workflow.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(true)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.NoError(err)
	s.Equal(0, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_PartialAppliedEvent() {
	runID := uuid.New()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.New(),
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

	msCurrent := workflow.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msCurrent.EXPECT().GetLastWriteVersion().Return(int64(1), nil).AnyTimes()
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr1.GetSignalName(),
		attr1.GetInput(),
		attr1.GetIdentity(),
		attr1.GetHeader(),
		attr1.GetSkipGenerateWorkflowTask(),
	).Return(event1, nil)
	msCurrent.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	dedupResource1 := definition.NewEventReappliedID(runID, event1.GetEventId(), event1.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource1).Return(false)
	dedupResource2 := definition.NewEventReappliedID(runID, event2.GetEventId(), event2.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource2).Return(true)
	msCurrent.EXPECT().UpdateDuplicatedResource(dedupResource1)
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
	runID := uuid.New()
	execution := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.New(),
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

	msCurrent := workflow.NewMockMutableState(s.controller)
	msCurrent.EXPECT().VisitUpdates(gomock.Any()).Return()
	msCurrent.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(msCurrent)
	msCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msCurrent.EXPECT().GetLastWriteVersion().Return(int64(1), nil).AnyTimes()
	msCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
		attr.GetHeader(),
		attr.GetSkipGenerateWorkflowTask(),
	).Return(nil, fmt.Errorf("test"))
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false)
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		event,
	}
	appliedEvent, err := s.nDCReapplication.ReapplyEvents(context.Background(), msCurrent, updateRegistry, events, runID)
	s.Error(err)
	s.Equal(0, len(appliedEvent))
}
