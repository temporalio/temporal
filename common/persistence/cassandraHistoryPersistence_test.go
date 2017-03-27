package persistence

import (
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	historyPersistenceSuite struct {
		suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestHistoryPersistenceSuite(t *testing.T) {
	s := new(historyPersistenceSuite)
	suite.Run(t, s)
}

func (s *historyPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.SetupWorkflowStore()
}

func (s *historyPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *historyPersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *historyPersistenceSuite) TestAppendHistoryEvents() {
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("append-history-events-test"),
		RunId:      common.StringPtr("986fc9cd-4a2d-4964-bf9f-5130116d5851"),
	}

	events1 := []byte("event1;event2")
	err0 := s.AppendHistoryEvents(workflowExecution, 1, 1, 1, events1, false)
	s.Nil(err0)

	events2 := []byte("event3;")
	err1 := s.AppendHistoryEvents(workflowExecution, 3, 1, 1, events2, false)
	s.Nil(err1)

	events2New := []byte("event3new;")
	err2 := s.AppendHistoryEvents(workflowExecution, 3, 1, 1, events2New, false)
	s.NotNil(err2)
	s.IsType(&ConditionFailedError{}, err2)

	err3 := s.AppendHistoryEvents(workflowExecution, 3, 1, 2, events2New, true)
	s.Nil(err3)
}

func (s *historyPersistenceSuite) TestGetHistoryEvents() {
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-history-events-test"),
		RunId:      common.StringPtr("26fa29f6-af41-4b70-9a3b-8b1b35eed82a"),
	}

	events := []byte("event1;event2")
	err0 := s.AppendHistoryEvents(workflowExecution, 1, 1, 1, events, false)
	s.Nil(err0)

	data, token, err1 := s.GetWorkflowExecutionHistory(workflowExecution, 2, 10, nil)
	s.Nil(err1)
	s.Equal([]byte{}, token)
	s.Equal(1, len(data))
	s.Equal(events, data[0])
}

func (s *historyPersistenceSuite) TestDeleteHistoryEvents() {
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("delete-history-events-test"),
		RunId:      common.StringPtr("2122fd8d-f583-459e-a2e2-d1fb273a43cb"),
	}

	events := [][]byte{
		[]byte("event1;event2"),
		[]byte("event3"),
		[]byte("event4;event5"),
		[]byte("event6"),
		[]byte("event7"),
		[]byte("event8;event9"),
		[]byte("event10"),
		[]byte("event11;event12"),
		[]byte("event13"),
		[]byte("event14"),
	}
	for i := 0; i < 10; i++ {
		err0 := s.AppendHistoryEvents(workflowExecution, int64(i), 1, int64(i), events[i], false)
		s.Nil(err0)
	}

	data, token, err1 := s.GetWorkflowExecutionHistory(workflowExecution, 10, 11, nil)
	s.Nil(err1)
	s.Equal([]byte{}, token)
	s.Equal(events, data)

	err2 := s.DeleteWorkflowExecutionHistory(workflowExecution)
	s.Nil(err2)

	data1, token1, err3 := s.GetWorkflowExecutionHistory(workflowExecution, 10, 11, nil)
	s.NotNil(err3)
	s.IsType(&gen.EntityNotExistsError{}, err3)
	s.Nil(token1)
	s.Nil(data1)
}

func (s *historyPersistenceSuite) AppendHistoryEvents(workflowExecution gen.WorkflowExecution, firstEventID, rangeID,
txID int64, events []byte, overwrite bool) error {

	return s.HistoryMgr.AppendHistoryEvents(&AppendHistoryEventsRequest{
		Execution:     workflowExecution,
		FirstEventID:  firstEventID,
		RangeID:       rangeID,
		TransactionID: txID,
		Events:        events,
		Overwrite:     overwrite,
	})
}

func (s *historyPersistenceSuite) GetWorkflowExecutionHistory(workflowExecution gen.WorkflowExecution,
	nextEventID int64, pageSize int, token []byte) ([][]byte, []byte, error) {

	response, err := s.HistoryMgr.GetWorkflowExecutionHistory(&GetWorkflowExecutionHistoryRequest{
		Execution:     workflowExecution,
		NextEventID:   nextEventID,
		PageSize:      pageSize,
		NextPageToken: token,
	})

	if err != nil {
		return nil, nil, err
	}

	return response.Events, response.NextPageToken, nil
}

func (s *historyPersistenceSuite) DeleteWorkflowExecutionHistory(workflowExecution gen.WorkflowExecution) error {

	return s.HistoryMgr.DeleteWorkflowExecutionHistory(&DeleteWorkflowExecutionHistoryRequest{
		Execution: workflowExecution,
	})
}
