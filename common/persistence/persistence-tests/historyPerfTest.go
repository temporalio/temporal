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

package persistencetests

import (
	"os"
	"testing"

	"math/rand"
	"time"

	"fmt"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
)

type (
	// HistoryPerfSuite contains history persistence tests
	HistoryPerfSuite struct {
		suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// SetupSuite implementation
func (s *HistoryPerfSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// SetupTest implementation
func (s *HistoryPerfSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TearDownSuite implementation
func (s *HistoryPerfSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *HistoryPerfSuite) genRandomUUIDString() string {
	at := time.Unix(rand.Int63(), rand.Int63())
	uuid := gocql.UUIDFromTime(at)
	return uuid.String()
}

func (s *HistoryPerfSuite) startProfile() int64 {
	return time.Now().UnixNano()
}

func (s *HistoryPerfSuite) stopProfile(startT int64, name string) {
	du := time.Now().UnixNano() - startT
	fmt.Printf("%v , time eslapsed: %v milliseconds\n", name, float64(du)/float64(1000000))
}

/*
TestPerf is the test entry
=== RUN   TestCassandraHistoryPerformance/TestPerf
appendV1-batch size: 1 , time eslapsed: 3454.098 milliseconds
appendV2-batch size: 1 , time eslapsed: 1022.303 milliseconds
appendV1-batch size: 2 , time eslapsed: 1579.684 milliseconds
appendV2-batch size: 2 , time eslapsed: 457.522 milliseconds
appendV1-batch size: 5 , time eslapsed: 627.084 milliseconds
appendV2-batch size: 5 , time eslapsed: 191.902 milliseconds
appendV1-batch size: 10 , time eslapsed: 324.444 milliseconds
appendV2-batch size: 10 , time eslapsed: 106.51 milliseconds
appendV1-batch size: 100 , time eslapsed: 45.617 milliseconds
appendV2-batch size: 100 , time eslapsed: 18.488 milliseconds
appendV1-batch size: 500 , time eslapsed: 28.697 milliseconds
appendV2-batch size: 500 , time eslapsed: 14.168 milliseconds
appendV1-batch size: 1000 , time eslapsed: 28.188 milliseconds
appendV2-batch size: 1000 , time eslapsed: 12.643 milliseconds
readV1-batch size: 1 , time eslapsed: 31.842 milliseconds
readv2-batch size: 1 , time eslapsed: 23.431 milliseconds
readV1-batch size: 2 , time eslapsed: 26.428 milliseconds
readv2-batch size: 2 , time eslapsed: 19.373 milliseconds
readV1-batch size: 5 , time eslapsed: 18.031 milliseconds
readv2-batch size: 5 , time eslapsed: 11.139 milliseconds
readV1-batch size: 10 , time eslapsed: 13.673 milliseconds
readv2-batch size: 10 , time eslapsed: 8.602 milliseconds
readV1-batch size: 100 , time eslapsed: 11.497 milliseconds
readv2-batch size: 100 , time eslapsed: 6.315 milliseconds
readV1-batch size: 500 , time eslapsed: 11.975 milliseconds
readv2-batch size: 500 , time eslapsed: 5.927 milliseconds
readV1-batch size: 1000 , time eslapsed: 9.965 milliseconds
readv2-batch size: 1000 , time eslapsed: 5.758 milliseconds
time="2018-10-21T19:33:02-07:00" level=info msg="dropped namespace" keyspace=test_wofofrfwrw
--- PASS: TestCassandraHistoryPerformance (10.07s)
    --- PASS: TestCassandraHistoryPerformance/TestPerf (8.13s)
PASS
*/
func (s *HistoryPerfSuite) TestPerf() {
	treeID := s.genRandomUUIDString()

	//for v1
	domainID := treeID

	total := 5000
	allBatches := []int{1, 2, 5, 10, 100, 500, 1000}
	//1. test append different batch allBatches of events:
	wfs := [7]workflow.WorkflowExecution{}
	brs := [7][]byte{}

	for idx, batchSize := range allBatches {

		uuid := s.genRandomUUIDString()
		wfs[idx] = workflow.WorkflowExecution{
			WorkflowId: &uuid,
			RunId:      &uuid,
		}

		br, err := s.newHistoryBranch(treeID)
		s.Nil(err)
		s.NotNil(br)
		brs[idx] = br

		firstIDV1 := int64(1)
		firstIDV2 := int64(1)
		st := s.startProfile()
		for i := 0; i < total/batchSize; i++ {
			lastID := firstIDV1 + int64(batchSize)
			events := s.genRandomEvents(firstIDV1, lastID)
			history := &workflow.History{
				Events: events,
			}

			err := s.appendV1(domainID, wfs[idx], firstIDV1, 0, 0, 0, history, false)
			s.Nil(err)
			firstIDV1 = lastID
		}
		s.stopProfile(st, fmt.Sprintf("appendV1-batch size: %v", batchSize))

		st = s.startProfile()
		for i := 0; i < total/batchSize; i++ {
			lastID := firstIDV2 + int64(batchSize)
			events := s.genRandomEvents(firstIDV2, lastID)

			err := s.appendV2(brs[idx], events, 0)

			s.Nil(err)
			firstIDV2 = lastID
		}
		s.stopProfile(st, fmt.Sprintf("appendV2-batch size: %v", batchSize))

	}

	pageSize := 1000
	//2. test read events:
	for idx, batchSize := range allBatches {

		st := s.startProfile()

		var err error
		token := []byte{}
		events := make([]*workflow.HistoryEvent, 0, total)
		for {
			var historyR *workflow.History
			historyR, token, err = s.readV1(domainID, wfs[idx], 1, int64(total+1), pageSize, token)
			s.Nil(err)
			events = append(events, historyR.Events...)
			if len(token) == 0 {
				break
			}
		}
		s.Equal(total, len(events))
		s.Equal(int64(1), *events[0].EventId)
		s.Equal(int64(total), *events[total-1].EventId)
		s.stopProfile(st, fmt.Sprintf("readV1-batch size: %v", batchSize))

		st = s.startProfile()
		token = []byte{}
		events = make([]*workflow.HistoryEvent, 0, total)
		for {
			var events2 []*workflow.HistoryEvent
			events2, token, err = s.readv2(brs[idx], 1, int64(total+1), 0, pageSize, token)
			s.Nil(err)
			events = append(events, events2...)
			if len(token) == 0 {
				break
			}
		}
		s.Equal(total, len(events))
		s.Equal(int64(1), *events[0].EventId)
		s.Equal(int64(total), *events[total-1].EventId)

		s.stopProfile(st, fmt.Sprintf("readv2-batch size: %v", batchSize))

	}
}

// lastID is exclusive
func (s *HistoryPerfSuite) genRandomEvents(firstID, lastID int64) []*workflow.HistoryEvent {
	events := make([]*workflow.HistoryEvent, 0, lastID-firstID+1)

	timestamp := time.Now().UnixNano()
	for eid := firstID; eid < lastID; eid++ {
		e := &workflow.HistoryEvent{EventId: common.Int64Ptr(eid), Version: common.Int64Ptr(timestamp), Timestamp: int64Ptr(timestamp)}
		events = append(events, e)
	}

	return events
}

// persistence helper
func (s *HistoryPerfSuite) newHistoryBranch(treeID string) ([]byte, error) {

	resp, err := s.HistoryV2Mgr.NewHistoryBranch(&p.NewHistoryBranchRequest{
		TreeID: treeID,
	})

	return resp.BranchToken, err
}

// persistence helper
func (s *HistoryPerfSuite) readv2(branch []byte, minID, maxID, lastVersion int64, pageSize int, token []byte) ([]*workflow.HistoryEvent, []byte, error) {

	resp, err := s.HistoryV2Mgr.ReadHistoryBranch(&p.ReadHistoryBranchRequest{
		BranchToken:      branch,
		MinEventID:       minID,
		MaxEventID:       maxID,
		PageSize:         pageSize,
		NextPageToken:    token,
		LastEventVersion: lastVersion,
	})
	if err != nil {
		return nil, nil, err
	}
	if len(resp.History) > 0 {
		s.True(resp.Size > 0)
	}
	return resp.History, resp.NextPageToken, nil
}

// persistence helper
func (s *HistoryPerfSuite) appendV2(br []byte, events []*workflow.HistoryEvent, txnID int64) error {

	var resp *p.AppendHistoryNodesResponse
	var err error

	resp, err = s.HistoryV2Mgr.AppendHistoryNodes(&p.AppendHistoryNodesRequest{
		BranchToken:   br,
		Events:        events,
		TransactionID: txnID,
		Encoding:      common.EncodingTypeThriftRW,
	})
	if err != nil {
		s.True(resp.Size > 0)
	}
	return err
}

// AppendHistoryEvents helper
func (s *HistoryPerfSuite) appendV1(domainID string, workflowExecution workflow.WorkflowExecution,
	firstEventID, eventBatchVersion int64, rangeID, txID int64, eventsBatch *workflow.History, overwrite bool) error {

	_, err := s.HistoryMgr.AppendHistoryEvents(&p.AppendHistoryEventsRequest{
		DomainID:          domainID,
		Execution:         workflowExecution,
		FirstEventID:      firstEventID,
		EventBatchVersion: eventBatchVersion,
		RangeID:           rangeID,
		TransactionID:     txID,
		Events:            eventsBatch.Events,
		Overwrite:         overwrite,
		Encoding:          common.EncodingTypeThriftRW,
	})
	return err
}

// GetWorkflowExecutionHistory helper
func (s *HistoryPerfSuite) readV1(domainID string, workflowExecution workflow.WorkflowExecution,
	firstEventID, nextEventID int64, pageSize int, token []byte) (*workflow.History, []byte, error) {

	response, err := s.HistoryMgr.GetWorkflowExecutionHistory(&p.GetWorkflowExecutionHistoryRequest{
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

	return response.History, response.NextPageToken, nil
}
