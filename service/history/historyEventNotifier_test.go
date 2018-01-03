// Copyright (c) 2017 Uber Technologies, Inc.
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
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
)

type (
	historyEventNotifierSuite struct {
		suite.Suite
		historyEventNotifier *historyEventNotifierImpl
	}
)

func TestHistoryEventNotifierSuite(t *testing.T) {
	s := new(historyEventNotifierSuite)
	suite.Run(t, s)
}

func (s *historyEventNotifierSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *historyEventNotifierSuite) TearDownSuite() {

}

func (s *historyEventNotifierSuite) SetupTest() {
	s.historyEventNotifier = newHistoryEventNotifier(
		metrics.NewClient(tally.NoopScope, metrics.History),
		func(workflowID string) int {
			return len(workflowID)
		},
	)
	s.historyEventNotifier.Start()
}

func (s *historyEventNotifierSuite) TearDownTest() {
	s.historyEventNotifier.Stop()
}

func (s *historyEventNotifierSuite) TestSingleSubscriberWatchingEvents() {
	domainID := "domain ID"
	execution := &gen.WorkflowExecution{
		WorkflowId: common.StringPtr("workflow ID"),
		RunId:      common.StringPtr("run ID"),
	}
	var lastFirstEventID int64 = 3
	var nextEventID int64 = 18
	isRunning := true
	historyEvent := newHistoryEventNotification(domainID, execution, lastFirstEventID, nextEventID, isRunning)
	timerChan := time.NewTimer(time.Second * 2).C

	subscriberID, channel, err := s.historyEventNotifier.WatchHistoryEvent(newWorkflowIdentifier(domainID, execution))
	s.Nil(err)

	go func() {
		<-timerChan
		s.historyEventNotifier.NotifyNewHistoryEvent(historyEvent)
	}()

	select {
	case msg := <-channel:
		s.Equal(historyEvent.domainID, msg.domainID)
		s.Equal(historyEvent.workflowID, msg.workflowID)
		s.Equal(historyEvent.runID, msg.runID)
		s.Equal(historyEvent.nextEventID, msg.nextEventID)
		s.Equal(historyEvent.isWorkflowRunning, msg.isWorkflowRunning)
	}

	err = s.historyEventNotifier.UnwatchHistoryEvent(newWorkflowIdentifier(domainID, execution), subscriberID)
	s.Nil(err)
}

func (s *historyEventNotifierSuite) TestMultipleSubscriberWatchingEvents() {
	domainID := "domain ID"
	execution := &gen.WorkflowExecution{
		WorkflowId: common.StringPtr("workflow ID"),
		RunId:      common.StringPtr("run ID"),
	}

	var lastFirstEventID int64 = 3
	var nextEventID int64 = 18
	isRunning := true
	historyEvent := newHistoryEventNotification(domainID, execution, lastFirstEventID, nextEventID, isRunning)
	timerChan := time.NewTimer(time.Second * 5).C

	subscriberCount := 100
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(subscriberCount)

	watchFunc := func() {
		subscriberID, channel, err := s.historyEventNotifier.WatchHistoryEvent(newWorkflowIdentifier(domainID, execution))
		s.Nil(err)

		timeourChan := time.NewTimer(time.Second * 10).C

		select {
		case msg := <-channel:
			s.Equal(historyEvent.domainID, msg.domainID)
			s.Equal(historyEvent.workflowID, msg.workflowID)
			s.Equal(historyEvent.runID, msg.runID)
			s.Equal(historyEvent.nextEventID, msg.nextEventID)
			s.Equal(historyEvent.isWorkflowRunning, msg.isWorkflowRunning)
		case <-timeourChan:
			s.Fail("subscribe to new events timeout")
		}
		err = s.historyEventNotifier.UnwatchHistoryEvent(newWorkflowIdentifier(domainID, execution), subscriberID)
		s.Nil(err)
		waitGroup.Done()
	}

	for count := 0; count < subscriberCount; count++ {
		go watchFunc()
	}

	<-timerChan
	s.historyEventNotifier.NotifyNewHistoryEvent(historyEvent)
	waitGroup.Wait()
}
