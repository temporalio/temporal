// Copyright (c) 2019 Uber Technologies, Inc.
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

package history

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	nDCEventReapplicationSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		nDCReapplication nDCEventsReapplier
	}
)

func TestNDCEventReapplicationSuite(t *testing.T) {
	s := new(nDCEventReapplicationSuite)
	suite.Run(t, s)
}

func (s *nDCEventReapplicationSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	logger := loggerimpl.NewDevelopmentForTest(s.Suite)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.nDCReapplication = newNDCEventsReapplier(
		metricsClient,
		logger,
	)
}

func (s *nDCEventReapplicationSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_AppliedEvent() {
	runID := uuid.New()
	execution := &persistence.WorkflowExecutionInfo{
		DomainID: uuid.New(),
	}
	event := &commonproto.HistoryEvent{
		EventId:   1,
		EventType: enums.EventTypeWorkflowExecutionSignaled,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      []byte{},
		}},
	}
	attr := event.GetWorkflowExecutionSignaledEventAttributes()

	msBuilderCurrent := NewMockmutableState(s.controller)
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(int64(1), nil).AnyTimes()
	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
	).Return(event, nil).Times(1)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msBuilderCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false).Times(1)
	msBuilderCurrent.EXPECT().UpdateDuplicatedResource(dedupResource).Times(1)
	events := []*commonproto.HistoryEvent{
		{EventType: enums.EventTypeWorkflowExecutionStarted},
		event,
	}
	appliedEvent, err := s.nDCReapplication.reapplyEvents(context.Background(), msBuilderCurrent, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_Noop() {
	runID := uuid.New()
	event := &commonproto.HistoryEvent{
		EventId:   1,
		EventType: enums.EventTypeWorkflowExecutionSignaled,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      []byte{},
		}},
	}

	msBuilderCurrent := NewMockmutableState(s.controller)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msBuilderCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(true).Times(1)
	events := []*commonproto.HistoryEvent{
		{EventType: enums.EventTypeWorkflowExecutionStarted},
		event,
	}
	appliedEvent, err := s.nDCReapplication.reapplyEvents(context.Background(), msBuilderCurrent, events, runID)
	s.NoError(err)
	s.Equal(0, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_PartialAppliedEvent() {
	runID := uuid.New()
	execution := &persistence.WorkflowExecutionInfo{
		DomainID: uuid.New(),
	}
	event1 := &commonproto.HistoryEvent{
		EventId:   1,
		EventType: enums.EventTypeWorkflowExecutionSignaled,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      []byte{},
		}},
	}
	event2 := &commonproto.HistoryEvent{
		EventId:   2,
		EventType: enums.EventTypeWorkflowExecutionSignaled,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      []byte{},
		}},
	}
	attr1 := event1.GetWorkflowExecutionSignaledEventAttributes()

	msBuilderCurrent := NewMockmutableState(s.controller)
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(int64(1), nil).AnyTimes()
	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr1.GetSignalName(),
		attr1.GetInput(),
		attr1.GetIdentity(),
	).Return(event1, nil).Times(1)
	dedupResource1 := definition.NewEventReappliedID(runID, event1.GetEventId(), event1.GetVersion())
	msBuilderCurrent.EXPECT().IsResourceDuplicated(dedupResource1).Return(false).Times(1)
	dedupResource2 := definition.NewEventReappliedID(runID, event2.GetEventId(), event2.GetVersion())
	msBuilderCurrent.EXPECT().IsResourceDuplicated(dedupResource2).Return(true).Times(1)
	msBuilderCurrent.EXPECT().UpdateDuplicatedResource(dedupResource1).Times(1)
	events := []*commonproto.HistoryEvent{
		{EventType: enums.EventTypeWorkflowExecutionStarted},
		event1,
		event2,
	}
	appliedEvent, err := s.nDCReapplication.reapplyEvents(context.Background(), msBuilderCurrent, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_Error() {
	runID := uuid.New()
	execution := &persistence.WorkflowExecutionInfo{
		DomainID: uuid.New(),
	}
	event := &commonproto.HistoryEvent{
		EventId:   1,
		EventType: enums.EventTypeWorkflowExecutionSignaled,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			Identity:   "test",
			SignalName: "signal",
			Input:      []byte{},
		}},
	}
	attr := event.GetWorkflowExecutionSignaledEventAttributes()

	msBuilderCurrent := NewMockmutableState(s.controller)
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true)
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(int64(1), nil).AnyTimes()
	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(
		attr.GetSignalName(),
		attr.GetInput(),
		attr.GetIdentity(),
	).Return(nil, fmt.Errorf("test")).Times(1)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msBuilderCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(false).Times(1)
	events := []*commonproto.HistoryEvent{
		{EventType: enums.EventTypeWorkflowExecutionStarted},
		event,
	}
	appliedEvent, err := s.nDCReapplication.reapplyEvents(context.Background(), msBuilderCurrent, events, runID)
	s.Error(err)
	s.Equal(0, len(appliedEvent))
}
