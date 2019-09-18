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
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCEventReapplicationSuite struct {
		suite.Suite
		nDCReapplication nDCEventsReapplier
	}
)

func TestNDCEventReapplicationSuite(t *testing.T) {
	s := new(nDCEventReapplicationSuite)
	suite.Run(t, s)
}

func (s *nDCEventReapplicationSuite) SetupTest() {
	logger := loggerimpl.NewDevelopmentForTest(s.Suite)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.nDCReapplication = newNDCEventsReapplier(
		metricsClient,
		logger,
	)
}

func (s *nDCEventReapplicationSuite) TestReapplyEvents() {
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
	msBuilderCurrent := &mockMutableState{}
	msBuilderCurrent.On("GetLastWriteVersion").Return(int64(1), nil)
	msBuilderCurrent.On("UpdateReplicationStateVersion", int64(1), true).Return()
	msBuilderCurrent.On("GetExecutionInfo").Return(execution)
	msBuilderCurrent.On("AddWorkflowExecutionSignaled", mock.Anything, mock.Anything, mock.Anything).Return(event, nil).Once()
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)

	events := []*shared.HistoryEvent{
		{EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionStarted)},
		event,
	}
	err := s.nDCReapplication.reapplyEvents(context.Background(), msBuilderCurrent, events)
	s.NoError(err)
}
