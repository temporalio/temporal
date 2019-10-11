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

package archiver

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

const (
	testDomainID                     = "test-domain-id"
	testDomainName                   = "test-domain-name"
	testWorkflowID                   = "test-workflow-id"
	testRunID                        = "test-run-id"
	testNextEventID                  = 1800
	testCloseFailoverVersion         = 100
	testDefaultPersistencePageSize   = 250
	testDefaultTargetHistoryBlobSize = 2 * 1024 * 124
	testDefaultHistoryEventSize      = 50
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type (
	HistoryIteratorSuite struct {
		*require.Assertions
		suite.Suite
	}

	page struct {
		firstbatchIdx             int
		numBatches                int
		firstEventFailoverVersion int64
		lastEventFailoverVersion  int64
	}

	testSizeEstimator struct{}
)

func (e *testSizeEstimator) EstimateSize(v interface{}) (int, error) {
	historyBatch, ok := v.(*shared.History)
	if !ok {
		return -1, errors.New("test size estimator only estimate the size of history batches")
	}
	return testDefaultHistoryEventSize * len(historyBatch.Events), nil
}

func newTestSizeEstimator() SizeEstimator {
	return &testSizeEstimator{}
}

func TestHistoryIteratorSuite(t *testing.T) {
	suite.Run(t, new(HistoryIteratorSuite))
}

func (s *HistoryIteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HistoryIteratorSuite) TestReadHistory_Failed_EventsV2() {
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	mockHistoryV2Manager.On("ReadHistoryBranchByBatch", mock.Anything).Return(nil, errors.New("got error reading history branch"))
	itr := s.constructTestHistoryIterator(mockHistoryV2Manager, testDefaultTargetHistoryBlobSize, nil)
	history, err := itr.readHistory(common.FirstEventID)
	s.Error(err)
	s.Nil(history)
	mockHistoryV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestReadHistory_Success_EventsV2() {
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	resp := persistence.ReadHistoryBranchByBatchResponse{
		History:       []*shared.History{},
		NextPageToken: []byte{},
	}
	mockHistoryV2Manager.On("ReadHistoryBranchByBatch", mock.Anything).Return(&resp, nil)
	itr := s.constructTestHistoryIterator(mockHistoryV2Manager, testDefaultTargetHistoryBlobSize, nil)
	history, err := itr.readHistory(common.FirstEventID)
	s.NoError(err)
	s.NotNil(history)
	mockHistoryV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestNewIteratorWithState() {
	itr := s.constructTestHistoryIterator(nil, testDefaultTargetHistoryBlobSize, nil)
	testIteratorState := historyIteratorState{
		FinishedIteration: true,
		NextEventID:       4,
	}
	itr.historyIteratorState = testIteratorState
	stateToken, err := itr.GetState()
	s.NoError(err)

	newItr := s.constructTestHistoryIterator(nil, testDefaultTargetHistoryBlobSize, stateToken)
	s.assertStateMatches(testIteratorState, newItr)
}

func (s *HistoryIteratorSuite) copyIteratorState(itr *historyIterator) historyIteratorState {
	return itr.historyIteratorState
}

func (s *HistoryIteratorSuite) assertStateMatches(expected historyIteratorState, itr *historyIterator) {
	s.Equal(expected.NextEventID, itr.NextEventID)
	s.Equal(expected.FinishedIteration, itr.FinishedIteration)
}

func (s *HistoryIteratorSuite) constructHistoryBatches(batchInfo []int, page page, firstEventID int64) []*shared.History {
	batches := []*shared.History{}
	eventsID := firstEventID
	for batchIdx, numEvents := range batchInfo[page.firstbatchIdx : page.firstbatchIdx+page.numBatches] {
		events := []*shared.HistoryEvent{}
		for i := 0; i < numEvents; i++ {
			event := &shared.HistoryEvent{
				EventId: common.Int64Ptr(eventsID),
				Version: common.Int64Ptr(page.firstEventFailoverVersion),
			}
			eventsID++
			if batchIdx == page.numBatches-1 {
				event.Version = common.Int64Ptr(page.lastEventFailoverVersion)
			}
			events = append(events, event)
		}
		batches = append(batches, &shared.History{
			Events: events,
		})
	}
	return batches
}

func (s *HistoryIteratorSuite) constructTestHistoryIterator(
	mockHistoryV2Manager *mocks.HistoryV2Manager,
	targetHistoryBlobSize int,
	initialState []byte,
) *historyIterator {

	request := &ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	itr := newHistoryIterator(request, mockHistoryV2Manager, targetHistoryBlobSize)
	if initialState != nil {
		err := itr.reset(initialState)
		s.NoError(err)
	}
	itr.sizeEstimator = newTestSizeEstimator()
	return itr
}
