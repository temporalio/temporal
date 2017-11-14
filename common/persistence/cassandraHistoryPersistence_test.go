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

package persistence

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"fmt"
	"github.com/pborman/uuid"
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
	domainID := "ff03c29f-fcf1-4aea-893d-1a7ec421e3ec"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("append-history-events-test"),
		RunId:      common.StringPtr("986fc9cd-4a2d-4964-bf9f-5130116d5851"),
	}

	events1 := []byte("event1;event2")
	serializedHistory := &SerializedHistoryEventBatch{Version: 1, EncodingType: common.EncodingTypeJSON, Data: events1}
	err0 := s.AppendHistoryEvents(domainID, workflowExecution, 1, 1, 1, serializedHistory, false)
	s.Nil(err0)

	events2 := []byte("event3;")
	serializedHistory.Data = events2
	err1 := s.AppendHistoryEvents(domainID, workflowExecution, 3, 1, 1, serializedHistory, false)
	s.Nil(err1)

	events2New := []byte("event3new;")
	serializedHistory.Data = events2New
	err2 := s.AppendHistoryEvents(domainID, workflowExecution, 3, 1, 1, serializedHistory, false)
	s.NotNil(err2)
	s.IsType(&ConditionFailedError{}, err2)

	err3 := s.AppendHistoryEvents(domainID, workflowExecution, 3, 1, 2, serializedHistory, true)
	s.Nil(err3)
}

func (s *historyPersistenceSuite) TestGetHistoryEvents() {
	domainID := "0fdc53ef-b890-4870-a944-b9b028ac9742"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-history-events-test"),
		RunId:      common.StringPtr("26fa29f6-af41-4b70-9a3b-8b1b35eed82a"),
	}

	events := []byte("event1;event2")
	serializedHistory := &SerializedHistoryEventBatch{Version: 1, EncodingType: common.EncodingTypeJSON, Data: events}
	err0 := s.AppendHistoryEvents(domainID, workflowExecution, 1, 1, 1, serializedHistory, false)
	s.Nil(err0)

	history, token, err1 := s.GetWorkflowExecutionHistory(domainID, workflowExecution, 0, 2, 10, nil)
	s.Nil(err1)
	s.Equal([]byte{}, token)
	s.Equal(1, len(history))
	s.Equal(1, history[0].Version)
	s.Equal(common.EncodingTypeJSON, history[0].EncodingType)
	s.Equal(events, history[0].Data)
}

func (s *historyPersistenceSuite) TestGetHistoryEventsCompatibility() {
	domainID := "373de9d6-e41e-42d4-bee9-9e06968e4d0d"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-history-events-compatibility-test"),
		RunId:      common.StringPtr(uuid.New()),
	}

	events := []*SerializedHistoryEventBatch{
		NewSerializedHistoryEventBatch([]byte("event1;event2"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event3"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event4;event5"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event6"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event7"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event8;event9"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event10"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event11;event12"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event13"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event14"), common.EncodingTypeGob, 1),
	}
	for i := 0; i < 10; i++ {
		err0 := s.AppendHistoryEvents(domainID, workflowExecution, int64(i), 1, int64(i), events[i], false)
		s.Nil(err0)
	}

	var firstEventID int64
	for firstEventID = 0; firstEventID < 10; firstEventID++ {

		var eventsResult []SerializedHistoryEventBatch
		var nexttoken []byte
		for {
			fmt.Println(int(firstEventID))
			gotHistoryList, token, err1 := s.GetWorkflowExecutionHistory(domainID, workflowExecution, firstEventID, 10, 3, nexttoken)
			s.Nil(err1)
			eventsResult = append(eventsResult, gotHistoryList...)
			if len(token) == 0 {
				break
			}
			nexttoken = token
		}

		s.Equal(len(events)-int(firstEventID), len(eventsResult))
		for i := 0; i < len(eventsResult); i++ {
			var eventsIndex = i + int(firstEventID)
			s.Equal(events[eventsIndex].Data, eventsResult[i].Data)
			s.Equal(events[eventsIndex].Version, eventsResult[i].Version)
			s.Equal(events[eventsIndex].EncodingType, eventsResult[i].EncodingType)
		}
	}
}

func (s *historyPersistenceSuite) TestDeleteHistoryEvents() {
	domainID := "373de9d6-e41e-42d4-bee9-9e06968e4d0d"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("delete-history-events-test"),
		RunId:      common.StringPtr("2122fd8d-f583-459e-a2e2-d1fb273a43cb"),
	}

	events := []*SerializedHistoryEventBatch{
		NewSerializedHistoryEventBatch([]byte("event1;event2"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event3"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event4;event5"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event6"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event7"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event8;event9"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event10"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event11;event12"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event13"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event14"), common.EncodingTypeGob, 1),
	}
	for i := 0; i < 10; i++ {
		err0 := s.AppendHistoryEvents(domainID, workflowExecution, int64(i), 1, int64(i), events[i], false)
		s.Nil(err0)
	}

	gotHistoryList, token, err1 := s.GetWorkflowExecutionHistory(domainID, workflowExecution, 0, 10, 11, nil)
	s.Nil(err1)
	s.Equal([]byte{}, token)
	s.Equal(len(events), len(gotHistoryList))
	for i := 0; i < len(gotHistoryList); i++ {
		s.Equal(events[i].Data, gotHistoryList[i].Data)
		s.Equal(events[i].Version, gotHistoryList[i].Version)
		s.Equal(events[i].EncodingType, gotHistoryList[i].EncodingType)
	}

	err2 := s.DeleteWorkflowExecutionHistory(domainID, workflowExecution)
	s.Nil(err2)

	data1, token1, err3 := s.GetWorkflowExecutionHistory(domainID, workflowExecution, 0, 10, 11, nil)
	s.NotNil(err3)
	s.IsType(&gen.EntityNotExistsError{}, err3)
	s.Nil(token1)
	s.Nil(data1)
}

func (s *historyPersistenceSuite) TestAppendAndGet() {
	domainID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("append-and-get-test"),
		RunId:      common.StringPtr(uuid.New()),
	}
	historyList := []*SerializedHistoryEventBatch{
		NewSerializedHistoryEventBatch([]byte("event1;event2"), common.EncodingTypeJSON, 0),
		NewSerializedHistoryEventBatch([]byte("event3;event4"), common.EncodingTypeGob, 1),
		NewSerializedHistoryEventBatch([]byte("event5;event6"), common.EncodingTypeJSON, 2),
		NewSerializedHistoryEventBatch([]byte("event7;event8"), common.EncodingTypeGob, 3),
	}

	for i := 0; i < len(historyList); i++ {

		err0 := s.AppendHistoryEvents(domainID, workflowExecution, int64(i), 1, int64(i), historyList[i], false)
		s.Nil(err0)

		gotHistoryList, token, err1 := s.GetWorkflowExecutionHistory(domainID, workflowExecution, 0, 10, 11, nil)
		s.Nil(err1)
		s.Equal([]byte{}, token)
		s.Equal(i+1, len(gotHistoryList))

		for j := 0; j < len(gotHistoryList); j++ {
			s.Equal(historyList[i].Data, gotHistoryList[i].Data)
			s.Equal(historyList[i].Version, gotHistoryList[i].Version)
			s.Equal(historyList[i].EncodingType, gotHistoryList[i].EncodingType)
		}
	}
}

func (s *historyPersistenceSuite) AppendHistoryEvents(domainID string, workflowExecution gen.WorkflowExecution,
	firstEventID, rangeID, txID int64, eventsBatch *SerializedHistoryEventBatch, overwrite bool) error {

	return s.HistoryMgr.AppendHistoryEvents(&AppendHistoryEventsRequest{
		DomainID:      domainID,
		Execution:     workflowExecution,
		FirstEventID:  firstEventID,
		RangeID:       rangeID,
		TransactionID: txID,
		Events:        eventsBatch,
		Overwrite:     overwrite,
	})
}

func (s *historyPersistenceSuite) GetWorkflowExecutionHistory(domainID string, workflowExecution gen.WorkflowExecution,
	firstEventID, nextEventID int64, pageSize int, token []byte) ([]SerializedHistoryEventBatch, []byte, error) {

	response, err := s.HistoryMgr.GetWorkflowExecutionHistory(&GetWorkflowExecutionHistoryRequest{
		DomainID:      domainID,
		Execution:     workflowExecution,
		FirstEventID:  firstEventID,
		NextEventID:   nextEventID,
		PageSize:      pageSize,
		NextPageToken: token,
	})

	if err != nil {
		return nil, nil, err
	}

	return response.Events, response.NextPageToken, nil
}

func (s *historyPersistenceSuite) DeleteWorkflowExecutionHistory(domainID string,
	workflowExecution gen.WorkflowExecution) error {

	return s.HistoryMgr.DeleteWorkflowExecutionHistory(&DeleteWorkflowExecutionHistoryRequest{
		DomainID:  domainID,
		Execution: workflowExecution,
	})
}
