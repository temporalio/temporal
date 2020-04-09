package history

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	executionpb "go.temporal.io/temporal-proto/execution"

	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	historyEventNotifierSuite struct {
		suite.Suite
		*require.Assertions

		historyEventNotifier *historyEventNotifierImpl
	}
)

func TestHistoryEventNotifierSuite(t *testing.T) {
	s := new(historyEventNotifierSuite)
	suite.Run(t, s)
}

func (s *historyEventNotifierSuite) SetupSuite() {

}

func (s *historyEventNotifierSuite) TearDownSuite() {

}

func (s *historyEventNotifierSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.historyEventNotifier = newHistoryEventNotifier(
		clock.NewRealTimeSource(),
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
	namespaceID := "namespace ID"
	execution := &executionpb.WorkflowExecution{
		WorkflowId: "workflow ID",
		RunId:      "run ID",
	}
	lastFirstEventID := int64(3)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := persistence.WorkflowStateCreated
	workflowStatus := executionpb.WorkflowExecutionStatus_Running
	branchToken := make([]byte, 0)
	historyEvent := newHistoryEventNotification(namespaceID, execution, lastFirstEventID, nextEventID, previousStartedEventID, branchToken, workflowState, workflowStatus)
	timerChan := time.NewTimer(time.Second * 2).C

	subscriberID, channel, err := s.historyEventNotifier.WatchHistoryEvent(definition.NewWorkflowIdentifier(namespaceID, execution.GetWorkflowId(), execution.GetRunId()))
	s.Nil(err)

	go func() {
		<-timerChan
		s.historyEventNotifier.NotifyNewHistoryEvent(historyEvent)
	}()

	select {
	case msg := <-channel:
		s.Equal(historyEvent, msg)
	}

	err = s.historyEventNotifier.UnwatchHistoryEvent(definition.NewWorkflowIdentifier(namespaceID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID)
	s.Nil(err)
}

func (s *historyEventNotifierSuite) TestMultipleSubscriberWatchingEvents() {
	namespaceID := "namespace ID"
	execution := &executionpb.WorkflowExecution{
		WorkflowId: "workflow ID",
		RunId:      "run ID",
	}

	lastFirstEventID := int64(3)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := persistence.WorkflowStateCreated
	workflowStatus := executionpb.WorkflowExecutionStatus_Running
	branchToken := make([]byte, 0)
	historyEvent := newHistoryEventNotification(namespaceID, execution, lastFirstEventID, nextEventID, previousStartedEventID, branchToken, workflowState, workflowStatus)
	timerChan := time.NewTimer(time.Second * 5).C

	subscriberCount := 100
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(subscriberCount)

	watchFunc := func() {
		subscriberID, channel, err := s.historyEventNotifier.WatchHistoryEvent(definition.NewWorkflowIdentifier(namespaceID, execution.GetWorkflowId(), execution.GetRunId()))
		s.Nil(err)

		timeourChan := time.NewTimer(time.Second * 10).C

		select {
		case msg := <-channel:
			s.Equal(historyEvent, msg)
		case <-timeourChan:
			s.Fail("subscribe to new events timeout")
		}
		err = s.historyEventNotifier.UnwatchHistoryEvent(definition.NewWorkflowIdentifier(namespaceID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID)
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
