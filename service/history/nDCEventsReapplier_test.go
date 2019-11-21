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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
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
	event := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(1),
		EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionSignaled),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			Identity:   common.StringPtr("test"),
			SignalName: common.StringPtr("signal"),
			Input:      []byte{},
		},
	}
	attr := event.WorkflowExecutionSignaledEventAttributes

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
	events := []*shared.HistoryEvent{
		{EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionStarted)},
		event,
	}
	appliedEvent, err := s.nDCReapplication.reapplyEvents(context.Background(), msBuilderCurrent, events, runID)
	s.NoError(err)
	s.Equal(1, len(appliedEvent))
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents_Noop() {
	runID := uuid.New()
	event := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(1),
		EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionSignaled),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			Identity:   common.StringPtr("test"),
			SignalName: common.StringPtr("signal"),
			Input:      []byte{},
		},
	}

	msBuilderCurrent := NewMockmutableState(s.controller)
	dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
	msBuilderCurrent.EXPECT().IsResourceDuplicated(dedupResource).Return(true).Times(1)
	events := []*shared.HistoryEvent{
		{EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionStarted)},
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
	event1 := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(1),
		EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionSignaled),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			Identity:   common.StringPtr("test"),
			SignalName: common.StringPtr("signal"),
			Input:      []byte{},
		},
	}
	event2 := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(2),
		EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionSignaled),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			Identity:   common.StringPtr("test"),
			SignalName: common.StringPtr("signal"),
			Input:      []byte{},
		},
	}
	attr1 := event1.WorkflowExecutionSignaledEventAttributes

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
	events := []*shared.HistoryEvent{
		{EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionStarted)},
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
	event := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(1),
		EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionSignaled),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			Identity:   common.StringPtr("test"),
			SignalName: common.StringPtr("signal"),
			Input:      []byte{},
		},
	}
	attr := event.WorkflowExecutionSignaledEventAttributes

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
	events := []*shared.HistoryEvent{
		{EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionStarted)},
		event,
	}
	appliedEvent, err := s.nDCReapplication.reapplyEvents(context.Background(), msBuilderCurrent, events, runID)
	s.Error(err)
	s.Equal(0, len(appliedEvent))
}
