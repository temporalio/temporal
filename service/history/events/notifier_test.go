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

package events

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

type (
	notifierSuite struct {
		suite.Suite
		*require.Assertions

		notifier *NotifierImpl
	}
)

func TestHistoryEventNotifierSuite(t *testing.T) {
	s := new(notifierSuite)
	suite.Run(t, s)
}

func (s *notifierSuite) SetupSuite() {

}

func (s *notifierSuite) TearDownSuite() {

}

func (s *notifierSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.notifier = NewNotifier(
		clock.NewRealTimeSource(),
		metrics.NoopClient,
		func(namespaceID namespace.ID, workflowID string) int32 {
			key := namespaceID.String() + "_" + workflowID
			return int32(len(key))
		},
	)
	s.notifier.Start()
}

func (s *notifierSuite) TearDownTest() {
	s.notifier.Stop()
}

func (s *notifierSuite) TestSingleSubscriberWatchingEvents() {
	namespaceID := "namespace ID"
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "workflow ID",
		RunId:      "run ID",
	}
	lastFirstEventID := int64(3)
	lastFirstEventTxnID := int64(398)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	workflowStatus := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	branchToken := make([]byte, 0)
	historyEvent := NewNotification(namespaceID, execution, lastFirstEventID, lastFirstEventTxnID, nextEventID, previousStartedEventID, branchToken, workflowState, workflowStatus)
	timerChan := time.NewTimer(time.Second * 2).C

	subscriberID, channel, err := s.notifier.WatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()))
	s.Nil(err)

	go func() {
		<-timerChan
		s.notifier.NotifyNewHistoryEvent(historyEvent)
	}()

	msg := <-channel
	s.Equal(historyEvent, msg)

	err = s.notifier.UnwatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID)
	s.Nil(err)
}

func (s *notifierSuite) TestMultipleSubscriberWatchingEvents() {
	namespaceID := "namespace ID"
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "workflow ID",
		RunId:      "run ID",
	}

	lastFirstEventID := int64(3)
	lastFirstEventTxnID := int64(3980)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	workflowStatus := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	branchToken := make([]byte, 0)
	historyEvent := NewNotification(namespaceID, execution, lastFirstEventID, lastFirstEventTxnID, nextEventID, previousStartedEventID, branchToken, workflowState, workflowStatus)
	timerChan := time.NewTimer(time.Second * 5).C

	subscriberCount := 100
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(subscriberCount)

	watchFunc := func() {
		subscriberID, channel, err := s.notifier.WatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()))
		s.Nil(err)

		timeourChan := time.NewTimer(time.Second * 10).C

		select {
		case msg := <-channel:
			s.Equal(historyEvent, msg)
		case <-timeourChan:
			s.Fail("subscribe to new events timeout")
		}
		err = s.notifier.UnwatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID)
		s.Nil(err)
		waitGroup.Done()
	}

	for count := 0; count < subscriberCount; count++ {
		go watchFunc()
	}

	<-timerChan
	s.notifier.NotifyNewHistoryEvent(historyEvent)
	waitGroup.Wait()
}
