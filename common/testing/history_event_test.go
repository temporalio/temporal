package testing

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
)

type (
	historyEventTestSuit struct {
		suite.Suite
		generator Generator
	}
)

func TestHistoryEventTestSuite(t *testing.T) {
	suite.Run(t, new(historyEventTestSuit))
}

func (s *historyEventTestSuit) SetupSuite() {
	s.generator = InitializeHistoryEventGenerator("namespace", "ns-id", 1)
}

func (s *historyEventTestSuit) SetupTest() {
	s.generator.Reset()
}

// This is a sample about how to use the generator
func (s *historyEventTestSuit) Test_HistoryEvent_Generator() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
		}
	}()
	maxEventID := int64(0)
	maxVersion := int64(1)
	maxTaskID := int64(1)
	for i := 0; i < 10 && s.generator.HasNextVertex(); i++ {
		events := s.generator.GetNextVertices()

		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*historypb.HistoryEvent)
			if maxEventID != event.GetEventId()-1 {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.GetEventId()
			if maxVersion > event.GetVersion() {
				s.Fail("event version is incorrect")
			}
			maxVersion = event.GetVersion()
			if maxTaskID > event.GetTaskId() {
				s.Fail("event task id is incorrect")
			}
			maxTaskID = event.GetTaskId()
			fmt.Println(e.GetName())
			fmt.Println(event.GetEventId())
		}
	}
	s.NotEmpty(s.generator.ListGeneratedVertices())
	fmt.Println("==========================")
	branchGenerator1 := s.generator.DeepCopy()
	for i := 0; i < 10 && branchGenerator1.HasNextVertex(); i++ {
		events := branchGenerator1.GetNextVertices()
		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*historypb.HistoryEvent)
			if maxEventID != event.GetEventId()-1 {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.GetEventId()
			if maxVersion > event.GetVersion() {
				s.Fail("event version is incorrect")
			}
			maxVersion = event.GetVersion()
			if maxTaskID > event.GetTaskId() {
				s.Fail("event task id is incorrect")
			}
			maxTaskID = event.GetTaskId()
			fmt.Println(e.GetName())
			fmt.Println(event.GetEventId())
		}
	}
	fmt.Println("==========================")
	history := s.generator.ListGeneratedVertices()
	maxEventID = history[len(history)-1].GetData().(*historypb.HistoryEvent).GetEventId()
	for i := 0; i < 10 && s.generator.HasNextVertex(); i++ {
		events := s.generator.GetNextVertices()
		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*historypb.HistoryEvent)
			if maxEventID != event.GetEventId()-1 {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.GetEventId()
			if maxVersion > event.GetVersion() {
				s.Fail("event version is incorrect")
			}
			maxVersion = event.GetVersion()
			if maxTaskID > event.GetTaskId() {
				s.Fail("event task id is incorrect")
			}
			maxTaskID = event.GetTaskId()
			fmt.Println(e.GetName())
			fmt.Println(event.GetEventId())
		}
	}
}
