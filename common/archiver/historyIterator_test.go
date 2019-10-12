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
	testShardID                      = 1
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

// In the following test:
//   batchInfo represents # of events for each history batch.
//   page represents the metadata of the set of history batches that should be requested by the iterator
//   and returned by the history manager. Each page specifies the index of the first history batch it should
//   return, # of batches to return and first/last event failover version for the set of batches returned.
//   Note that is possible that a history batch is contained in multiple pages.

func (s *HistoryIteratorSuite) TestReadHistoryBatches_Fail_FirstCallToReadHistoryGivesError() {
	batchInfo := []int{1}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, 0, false, pages...)
	itr := s.constructTestHistoryIterator(historyV2Manager, testDefaultTargetHistoryBlobSize, nil)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextIterState, err := itr.readHistoryBatches(common.FirstEventID)
	s.Nil(events)
	s.False(nextIterState.FinishedIteration)
	s.Zero(nextIterState.NextEventID)
	s.Error(err)
	s.assertStateMatches(startingIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestReadHistoryBatches_Fail_NonFirstCallToReadHistoryGivesError() {
	batchInfo := []int{1, 1}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             1,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, 1, false, pages...)
	itr := s.constructTestHistoryIterator(historyV2Manager, testDefaultTargetHistoryBlobSize, nil)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextIterState, err := itr.readHistoryBatches(common.FirstEventID)
	s.Nil(events)
	s.False(nextIterState.FinishedIteration)
	s.Zero(nextIterState.NextEventID)
	s.Error(err)
	s.assertStateMatches(startingIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestReadHistoryBatches_Success_ReadToHistoryEnd() {
	batchInfo := []int{1, 2, 1, 1, 1, 3, 3, 1, 3}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                3,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             3,
			numBatches:                2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             5,
			numBatches:                4,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, -1, true, pages...)
	// ensure target history batches size is greater than total history length to ensure all of history is read
	itr := s.constructTestHistoryIterator(historyV2Manager, 20*testDefaultHistoryEventSize, nil)
	startingIteratorState := s.copyIteratorState(itr)
	history, nextIterState, err := itr.readHistoryBatches(common.FirstEventID)
	s.NotNil(history)
	s.Len(history, 9)
	s.True(nextIterState.FinishedIteration)
	s.Zero(nextIterState.NextEventID)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestReadHistoryBatches_Success_TargetSizeSatisfiedWithoutReadingToEnd() {
	batchInfo := []int{1, 2, 1, 1, 1, 3, 3, 1, 3}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                3,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             3,
			numBatches:                2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             5,
			numBatches:                4,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, -1, false, pages...)
	// ensure target history batches is smaller than full length of history so that not all of history is read
	itr := s.constructTestHistoryIterator(historyV2Manager, 11*testDefaultHistoryEventSize, nil)
	startingIteratorState := s.copyIteratorState(itr)
	history, nextIterState, err := itr.readHistoryBatches(common.FirstEventID)
	s.NotNil(history)
	s.Len(history, 7)
	s.False(nextIterState.FinishedIteration)
	s.Equal(int64(13), nextIterState.NextEventID)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestReadHistoryBatches_Success_ReadExactlyToHistoryEnd() {
	batchInfo := []int{1, 2, 1, 1, 1, 3, 3, 1, 3}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                3,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             3,
			numBatches:                2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             5,
			numBatches:                4,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, -1, true, pages...)
	// ensure target history batches size is equal to the full length of history so that all of history is read
	itr := s.constructTestHistoryIterator(historyV2Manager, 16*testDefaultHistoryEventSize, nil)
	startingIteratorState := s.copyIteratorState(itr)
	history, nextIterState, err := itr.readHistoryBatches(common.FirstEventID)
	s.NotNil(history)
	s.Len(history, 9)
	s.True(nextIterState.FinishedIteration)
	s.Zero(nextIterState.NextEventID)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestReadHistoryBatches_Success_ReadPageMultipleTimes() {
	batchInfo := []int{1, 3, 2}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                3,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             2,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, -1, true, pages...)
	// ensure target history batches is very small so that one page needs multiple read
	itr := s.constructTestHistoryIterator(historyV2Manager, 2*testDefaultHistoryEventSize, nil)
	startingIteratorState := s.copyIteratorState(itr)
	history, nextIterState, err := itr.readHistoryBatches(common.FirstEventID)
	s.NotNil(history)
	s.Len(history, 2)
	s.False(nextIterState.FinishedIteration)
	s.Equal(int64(5), nextIterState.NextEventID)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)

	history, nextIterState, err = itr.readHistoryBatches(nextIterState.NextEventID)
	s.NotNil(history)
	s.Len(history, 1)
	s.True(nextIterState.FinishedIteration)
	s.Zero(nextIterState.NextEventID)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestNext_Fail_IteratorDepleted() {
	batchInfo := []int{1, 3, 2, 1, 2, 3, 4}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             2,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  2,
		},
		{
			firstbatchIdx:             3,
			numBatches:                4,
			firstEventFailoverVersion: 2,
			lastEventFailoverVersion:  5,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, -1, true, pages...)
	// set target history batches such that a single call to next will read all of history
	itr := s.constructTestHistoryIterator(historyV2Manager, 16*testDefaultHistoryEventSize, nil)
	blob, err := itr.Next()
	s.Nil(err)

	expectedIteratorState := historyIteratorState{
		// when iteration is finished page token is not advanced
		FinishedIteration: true,
		NextEventID:       0,
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.NotNil(blob)
	expectedHeader := &HistoryBlobHeader{
		DomainName:           common.StringPtr(testDomainName),
		DomainID:             common.StringPtr(testDomainID),
		WorkflowID:           common.StringPtr(testWorkflowID),
		RunID:                common.StringPtr(testRunID),
		IsLast:               common.BoolPtr(true),
		FirstFailoverVersion: common.Int64Ptr(1),
		LastFailoverVersion:  common.Int64Ptr(5),
		FirstEventID:         common.Int64Ptr(common.FirstEventID),
		LastEventID:          common.Int64Ptr(16),
		EventCount:           common.Int64Ptr(16),
	}
	s.Equal(expectedHeader, blob.Header)
	s.Len(blob.Body, 7)
	s.NoError(err)
	s.False(itr.HasNext())

	blob, err = itr.Next()
	s.Equal(err, errIteratorDepleted)
	s.Nil(blob)
	s.assertStateMatches(expectedIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestNext_Fail_ReturnErrOnSecondCallToNext() {
	batchInfo := []int{1, 3, 2, 1, 3, 2}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             2,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             3,
			numBatches:                2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             5,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, 3, false, pages...)
	// set target blob size such that the first two pages are read for blob one without error, third page will return error
	itr := s.constructTestHistoryIterator(historyV2Manager, 6*testDefaultHistoryEventSize, nil)
	blob, err := itr.Next()
	expectedIteratorState := historyIteratorState{
		FinishedIteration: false,
		NextEventID:       7,
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.NotNil(blob)
	expectedHeader := &HistoryBlobHeader{
		DomainName:           common.StringPtr(testDomainName),
		DomainID:             common.StringPtr(testDomainID),
		WorkflowID:           common.StringPtr(testWorkflowID),
		RunID:                common.StringPtr(testRunID),
		IsLast:               common.BoolPtr(false),
		FirstFailoverVersion: common.Int64Ptr(1),
		LastFailoverVersion:  common.Int64Ptr(1),
		FirstEventID:         common.Int64Ptr(common.FirstEventID),
		LastEventID:          common.Int64Ptr(6),
		EventCount:           common.Int64Ptr(6),
	}
	s.Equal(expectedHeader, blob.Header)
	s.NoError(err)
	s.True(itr.HasNext())

	blob, err = itr.Next()
	s.Error(err)
	s.Nil(blob)
	s.assertStateMatches(expectedIteratorState, itr)
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestNext_Success_TenCallsToNext() {
	batchInfo := []int{}
	for i := 0; i < 100; i++ {
		batchInfo = append(batchInfo, []int{1, 2, 3, 4, 4, 3, 2, 1}...)
	}
	var pages []page
	for i := 0; i < 100; i++ {
		p := page{
			firstbatchIdx:             i * 8,
			numBatches:                8,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		}
		pages = append(pages, p)
	}
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, -1, true, pages...)
	// set target blob size size such that every 10 persistence pages is one group of history batches
	itr := s.constructTestHistoryIterator(historyV2Manager, 20*10*testDefaultHistoryEventSize, nil)
	expectedIteratorState := historyIteratorState{
		FinishedIteration: false,
		NextEventID:       common.FirstEventID,
	}
	for i := 0; i < 10; i++ {
		s.assertStateMatches(expectedIteratorState, itr)
		s.True(itr.HasNext())
		blob, err := itr.Next()
		s.NoError(err)
		s.NotNil(blob)
		expectedHeader := &HistoryBlobHeader{
			DomainName:           common.StringPtr(testDomainName),
			DomainID:             common.StringPtr(testDomainID),
			WorkflowID:           common.StringPtr(testWorkflowID),
			RunID:                common.StringPtr(testRunID),
			IsLast:               common.BoolPtr(false),
			FirstFailoverVersion: common.Int64Ptr(1),
			LastFailoverVersion:  common.Int64Ptr(1),
			FirstEventID:         common.Int64Ptr(common.FirstEventID + int64(i*200)),
			LastEventID:          common.Int64Ptr(int64(200 + (i * 200))),
			EventCount:           common.Int64Ptr(200),
		}
		if i == 9 {
			expectedHeader.IsLast = common.BoolPtr(true)
		}
		s.Equal(expectedHeader, blob.Header)

		if i < 9 {
			expectedIteratorState.FinishedIteration = false
			expectedIteratorState.NextEventID = int64(200*(i+1) + 1)
		} else {
			expectedIteratorState.NextEventID = 0
			expectedIteratorState.FinishedIteration = true
		}
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.False(itr.HasNext())
	historyV2Manager.AssertExpectations(s.T())
}

func (s *HistoryIteratorSuite) TestNext_Success_SameHistoryDifferentPage() {
	batchInfo := []int{2, 4, 4, 3, 2, 1, 1, 2}
	pages := []page{
		{
			firstbatchIdx:             0,
			numBatches:                3,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             2,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             3,
			numBatches:                2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             4,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             5,
			numBatches:                3,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	eventsPerRead := 6
	targetBlobSize := eventsPerRead * testDefaultHistoryEventSize
	historyV2Manager := s.constructMockHistoryV2Manager(batchInfo, -1, true, pages...)
	itr1 := s.constructTestHistoryIterator(historyV2Manager, targetBlobSize, nil)

	pages = []page{
		{
			firstbatchIdx:             0,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             1,
			numBatches:                3,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             2,
			numBatches:                1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             3,
			numBatches:                5,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			firstbatchIdx:             4,
			numBatches:                4,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyV2Manager = s.constructMockHistoryV2Manager(batchInfo, -1, true, pages...)
	itr2 := s.constructTestHistoryIterator(historyV2Manager, targetBlobSize, nil)

	totalPages := 3
	expectedFirstEventID := []int64{1, 7, 14}
	for i := 0; i != totalPages; i++ {
		s.True(itr1.HasNext())
		history1, err := itr1.Next()
		s.NoError(err)

		s.True(itr2.HasNext())
		history2, err := itr2.Next()
		s.NoError(err)

		s.Equal(history1.Header, history2.Header)
		s.Equal(len(history1.Body), len(history2.Body))
		s.Equal(expectedFirstEventID[i], history1.Body[0].Events[0].GetEventId())
		s.Equal(expectedFirstEventID[i], history2.Body[0].Events[0].GetEventId())
	}
	expectedIteratorState := historyIteratorState{
		NextEventID:       0,
		FinishedIteration: true,
	}
	s.assertStateMatches(expectedIteratorState, itr1)
	s.assertStateMatches(expectedIteratorState, itr2)
	s.False(itr1.HasNext())
	s.False(itr2.HasNext())
	historyV2Manager.AssertExpectations(s.T())
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

func (s *HistoryIteratorSuite) constructMockHistoryV2Manager(batchInfo []int, returnErrorOnPage int, addNotExistCall bool, pages ...page) *mocks.HistoryV2Manager {
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}

	firstEventIDs := []int64{common.FirstEventID}
	for i, batchSize := range batchInfo {
		firstEventIDs = append(firstEventIDs, firstEventIDs[i]+int64(batchSize))
	}

	for i, p := range pages {
		req := &persistence.ReadHistoryBranchRequest{
			BranchToken: testBranchToken,
			MinEventID:  firstEventIDs[p.firstbatchIdx],
			MaxEventID:  common.EndEventID,
			PageSize:    testDefaultPersistencePageSize,
			ShardID:     common.IntPtr(testShardID),
		}
		if returnErrorOnPage == i {
			mockHistoryV2Manager.On("ReadHistoryBranchByBatch", req).Return(nil, errors.New("got error getting workflow execution history"))
			return mockHistoryV2Manager
		}

		resp := &persistence.ReadHistoryBranchByBatchResponse{
			History: s.constructHistoryBatches(batchInfo, p, firstEventIDs[p.firstbatchIdx]),
		}
		mockHistoryV2Manager.On("ReadHistoryBranchByBatch", req).Return(resp, nil)
	}

	if addNotExistCall {
		req := &persistence.ReadHistoryBranchRequest{
			BranchToken: testBranchToken,
			MinEventID:  firstEventIDs[len(firstEventIDs)-1],
			MaxEventID:  common.EndEventID,
			PageSize:    testDefaultPersistencePageSize,
			ShardID:     common.IntPtr(testShardID),
		}
		mockHistoryV2Manager.On("ReadHistoryBranchByBatch", req).Return(nil, &shared.EntityNotExistsError{Message: "Reach the end"})
	}

	return mockHistoryV2Manager
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
		ShardID:              testShardID,
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
