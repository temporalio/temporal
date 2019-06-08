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
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

const (
	testDomainID                      = "test-domain-id"
	testDomainName                    = "test-domain-name"
	testWorkflowID                    = "test-workflow-id"
	testRunID                         = "test-run-id"
	testNextEventID                   = 1800
	testDomain                        = "test-domain"
	testClusterName                   = "test-cluster-name"
	testCloseFailoverVersion          = 100
	testDefaultPersistencePageSize    = 250
	testDefaultTargetArchivalBlobSize = 2 * 1024 * 124
	testDefaultHistoryEventSize       = 50
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type (
	HistoryBlobIteratorSuite struct {
		*require.Assertions
		suite.Suite
	}

	iteratorState struct {
		blobPageToken        int
		persistencePageToken []byte
		finishedIteration    bool
		numEventsToSkip      int
	}

	page struct {
		numEvents                 int
		firstEventID              int64
		firstEventFailoverVersion int64
		lastEventFailoverVersion  int64
	}

	testSizeEstimator struct{}
)

func (e *testSizeEstimator) EstimateSize(v interface{}) (int, error) {
	return testDefaultHistoryEventSize, nil
}

func newTestSizeEstimator() SizeEstimator {
	return &testSizeEstimator{}
}

func TestHistoryBlobIteratorSuite(t *testing.T) {
	suite.Run(t, new(HistoryBlobIteratorSuite))
}

func (s *HistoryBlobIteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Failed_EventsV2() {
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	mockHistoryV2Manager.On("ReadHistoryBranch", mock.Anything).Return(nil, errors.New("got error reading history branch"))
	itr := s.constructTestHistoryBlobIterator(nil, mockHistoryV2Manager, nil)
	events, nextPageToken, err := itr.readHistory([]byte{})
	s.Error(err)
	s.Nil(events)
	s.Nil(nextPageToken)
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Success_EventsV2() {
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	resp := persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*shared.HistoryEvent{},
		NextPageToken: []byte{},
	}
	mockHistoryV2Manager.On("ReadHistoryBranch", mock.Anything).Return(&resp, nil)
	itr := s.constructTestHistoryBlobIterator(nil, mockHistoryV2Manager, nil)
	events, nextPageToken, err := itr.readHistory([]byte{})
	s.NoError(err)
	s.NotNil(events)
	s.Empty(nextPageToken)
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Failed_EventsV1() {
	mockHistoryManager := &mocks.HistoryManager{}
	mockHistoryManager.On("GetWorkflowExecutionHistory", mock.Anything).Return(nil, errors.New("error getting workflow execution history"))
	itr := s.constructTestHistoryBlobIterator(mockHistoryManager, nil, nil)
	events, nextPageToken, err := itr.readHistory([]byte{})
	s.Error(err)
	s.Nil(events)
	s.Nil(nextPageToken)
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Success_EventsV1() {
	mockHistoryManager := &mocks.HistoryManager{}
	resp := persistence.GetWorkflowExecutionHistoryResponse{
		History: &shared.History{
			Events: []*shared.HistoryEvent{},
		},
	}
	mockHistoryManager.On("GetWorkflowExecutionHistory", mock.Anything).Return(&resp, nil)
	itr := s.constructTestHistoryBlobIterator(mockHistoryManager, nil, nil)
	events, nextPageToken, err := itr.readHistory([]byte{})
	s.NotNil(events)
	s.Empty(nextPageToken)
	s.NoError(err)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Fail_FirstCallToReadHistoryGivesError() {
	pages := []page{
		{
			numEvents:                 1,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(0, false, pages...)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, nil)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, numEventsToSkip, err := itr.readBlobEvents(pageTokens[0], 0)
	s.Nil(events)
	s.Nil(nextPageToken)
	s.False(historyEndReached)
	s.Zero(numEventsToSkip)
	s.Error(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Fail_NonFirstCallToReadHistoryGivesError() {
	pages := []page{
		{
			numEvents:                 1,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 1,
			firstEventID:              2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(1, false, pages...)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, nil)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, numEventsToSkip, err := itr.readBlobEvents(pageTokens[0], 0)
	s.Nil(events)
	s.Nil(nextPageToken)
	s.False(historyEndReached)
	s.Zero(numEventsToSkip)
	s.Error(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_ReadToHistoryEnd() {
	pages := []page{
		{
			numEvents:                 4,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 2,
			firstEventID:              5,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 10,
			firstEventID:              7,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, false, pages...)
	// ensure target blob size is greater than total history length to ensure all of history is read
	config := constructConfig(testDefaultPersistencePageSize, 20*testDefaultHistoryEventSize)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, numEventsToSkip, err := itr.readBlobEvents(pageTokens[0], 0)
	s.NotNil(events)
	s.Len(events, 16)
	s.Len(nextPageToken, 0)
	s.True(historyEndReached)
	s.Zero(numEventsToSkip)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_TargetSizeSatisfiedWithoutReadingToEnd() {
	pages := []page{
		{
			numEvents:                 4,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 2,
			firstEventID:              5,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 10,
			firstEventID:              7,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, false, pages...)
	// ensure target blob size is smaller than full length of history so that not all of history is read
	config := constructConfig(testDefaultPersistencePageSize, 11*testDefaultHistoryEventSize)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, numEventsToSkip, err := itr.readBlobEvents(pageTokens[0], 0)
	s.NotNil(events)
	s.Len(events, 11)
	s.NotEmpty(nextPageToken)
	s.Equal(pageTokens[len(pageTokens)-1], nextPageToken)
	s.False(historyEndReached)
	s.Equal(5, numEventsToSkip)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_ReadExactlyToHistoryEnd() {
	pages := []page{
		{
			numEvents:                 4,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 2,
			firstEventID:              5,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 10,
			firstEventID:              7,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, true, pages...)
	// ensure target blob size is equal to the full length of history so that all of history is read
	config := constructConfig(testDefaultPersistencePageSize, 16*testDefaultHistoryEventSize)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, numEventsToSkip, err := itr.readBlobEvents(pageTokens[0], 0)
	s.NotNil(events)
	s.Len(events, 16)
	s.Len(nextPageToken, 0)
	s.True(historyEndReached)
	s.Zero(numEventsToSkip)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_ReadPageMultipleTimes() {
	pages := []page{
		{
			numEvents:                 6,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, false, pages...)
	// ensure target blob size is very small so that one page needs multiple read
	config := constructConfig(testDefaultPersistencePageSize, 2*testDefaultHistoryEventSize)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, numEventsToSkip, err := itr.readBlobEvents(pageTokens[0], 0)
	s.NotNil(events)
	s.Len(events, 2)
	s.Len(nextPageToken, 0)
	s.False(historyEndReached)
	s.Equal(2, numEventsToSkip)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)

	events, nextPageToken, historyEndReached, numEventsToSkip, err = itr.readBlobEvents(nextPageToken, numEventsToSkip)
	s.NotNil(events)
	s.Len(events, 2)
	s.Len(nextPageToken, 0)
	s.False(historyEndReached)
	s.Equal(4, numEventsToSkip)
	s.NoError(err)
	s.Equal(int64(3), events[0].GetEventId())
	s.assertStateMatches(startingIteratorState, itr)

	events, nextPageToken, historyEndReached, numEventsToSkip, err = itr.readBlobEvents(nextPageToken, numEventsToSkip)
	s.NotNil(events)
	s.Len(events, 2)
	s.Len(nextPageToken, 0)
	s.True(historyEndReached)
	s.Zero(numEventsToSkip)
	s.NoError(err)
	s.Equal(int64(5), events[0].GetEventId())
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestNext_Fail_IteratorDepleted() {
	pages := []page{
		{
			numEvents:                 4,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 2,
			firstEventID:              5,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  2,
		},
		{
			numEvents:                 10,
			firstEventID:              7,
			firstEventFailoverVersion: 2,
			lastEventFailoverVersion:  5,
		},
	}
	historyManager, _ := s.constructMockHistoryManager(-1, true, pages...)
	// set target blob size such that a single call to next will read all of history
	config := constructConfig(testDefaultPersistencePageSize, 16*testDefaultHistoryEventSize)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	blob, err := itr.Next()
	expectedIteratorState := iteratorState{
		// when iteration is finished page token is not advanced
		blobPageToken:        startingIteratorState.blobPageToken,
		persistencePageToken: nil,
		finishedIteration:    true,
		numEventsToSkip:      0,
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.NotNil(blob)
	s.Equal(common.FirstBlobPageToken, *blob.Header.CurrentPageToken)
	s.Equal(common.LastBlobNextPageToken, *blob.Header.NextPageToken)
	s.True(*blob.Header.IsLast)
	s.Equal(int64(1), *blob.Header.FirstFailoverVersion)
	s.Equal(int64(5), *blob.Header.LastFailoverVersion)
	s.Equal(int64(1), *blob.Header.FirstEventID)
	s.Equal(int64(16), *blob.Header.LastEventID)
	s.NoError(err)
	s.False(itr.HasNext())

	blob, err = itr.Next()
	s.Error(err)
	s.Nil(blob)
	s.assertStateMatches(expectedIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestNext_Fail_ReturnErrOnSecondCallToNext() {
	pages := []page{
		{
			numEvents:                 4,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 2,
			firstEventID:              5,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 4,
			firstEventID:              7,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 2,
			firstEventID:              11,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  2,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(3, false, pages...)
	// set target blob size such that the first two pages are read for blob one without error, third page will return error
	config := constructConfig(testDefaultPersistencePageSize, 6*testDefaultHistoryEventSize)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	blob, err := itr.Next()
	expectedIteratorState := iteratorState{
		blobPageToken:        startingIteratorState.blobPageToken + 1,
		persistencePageToken: pageTokens[2],
		finishedIteration:    false,
		numEventsToSkip:      0,
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.NotNil(blob)
	s.Equal(common.FirstBlobPageToken, *blob.Header.CurrentPageToken)
	s.Equal(common.FirstBlobPageToken+1, *blob.Header.NextPageToken)
	s.Equal(int64(1), *blob.Header.FirstFailoverVersion)
	s.Equal(int64(1), *blob.Header.LastFailoverVersion)
	s.Equal(int64(1), *blob.Header.FirstEventID)
	s.Equal(int64(6), *blob.Header.LastEventID)
	s.NoError(err)
	s.True(itr.HasNext())

	blob, err = itr.Next()
	s.Error(err)
	s.Nil(blob)
	s.assertStateMatches(expectedIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestNext_Success_TenCallsToNext() {
	var pages []page
	for i := 0; i < 100; i++ {
		p := page{
			numEvents:                 20,
			firstEventID:              common.FirstEventID + int64(i*20),
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		}
		pages = append(pages, p)
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, false, pages...)
	// set config such that every 10 persistence pages is one blob
	config := constructConfig(testDefaultPersistencePageSize, 20*10*testDefaultHistoryEventSize)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	expectedIteratorState := iteratorState{
		blobPageToken:        common.FirstBlobPageToken,
		persistencePageToken: nil,
		finishedIteration:    false,
		numEventsToSkip:      0,
	}
	for i := 0; i < 10; i++ {
		s.assertStateMatches(expectedIteratorState, itr)
		s.True(itr.HasNext())
		blob, err := itr.Next()
		s.NoError(err)
		s.NotNil(blob)
		s.Equal(common.FirstEventID+int64(i*200), *blob.Header.FirstEventID)
		s.Equal(int64(200+(i*200)), *blob.Header.LastEventID)
		s.Equal(i+1, *blob.Header.CurrentPageToken)
		if i == 9 {
			s.Equal(common.LastBlobNextPageToken, *blob.Header.NextPageToken)
			s.True(*blob.Header.IsLast)
		} else {
			s.Equal(i+2, *blob.Header.NextPageToken)
			s.False(*blob.Header.IsLast)
		}
		s.Equal(int64(200), *blob.Header.EventCount)
		if i < 9 {
			expectedIteratorState.blobPageToken = expectedIteratorState.blobPageToken + 1
			expectedIteratorState.persistencePageToken = pageTokens[10+(i*10)]
		} else {
			expectedIteratorState.persistencePageToken = nil
			expectedIteratorState.finishedIteration = true
		}
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.False(itr.HasNext())
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_SameHistoryDifferentPage() {
	pages := []page{
		{
			numEvents:                 10,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 5,
			firstEventID:              11,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 4,
			firstEventID:              16,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	eventsPerBlob := 6
	targetBlobSize := eventsPerBlob * testDefaultHistoryEventSize
	historyManager, _ := s.constructMockHistoryManager(-1, false, pages...)
	config := constructConfig(testDefaultPersistencePageSize, targetBlobSize)
	itr1 := s.constructTestHistoryBlobIterator(historyManager, nil, config)

	pages = []page{
		{
			numEvents:                 5,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 7,
			firstEventID:              6,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 3,
			firstEventID:              13,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			numEvents:                 4,
			firstEventID:              16,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, _ = s.constructMockHistoryManager(-1, false, pages...)
	itr2 := s.constructTestHistoryBlobIterator(historyManager, nil, config)

	totalPages := 4
	for i := 0; i != totalPages; i++ {
		s.True(itr1.HasNext())
		blob1, err := itr1.Next()
		s.NoError(err)

		s.True(itr2.HasNext())
		blob2, err := itr2.Next()
		s.NoError(err)

		s.Equal(len(blob1.Body.Events), len(blob2.Body.Events))
		s.Equal(common.FirstEventID+int64(i*eventsPerBlob), *blob1.Header.FirstEventID)
		s.Equal(*blob1.Header.FirstEventID, *blob2.Header.FirstEventID)
		s.Equal(*blob1.Header.LastEventID, *blob2.Header.LastEventID)
	}
	expectedIteratorState := iteratorState{
		blobPageToken:        totalPages,
		persistencePageToken: nil,
		finishedIteration:    true,
		numEventsToSkip:      0,
	}
	s.assertStateMatches(expectedIteratorState, itr1)
	s.assertStateMatches(expectedIteratorState, itr2)
	s.False(itr1.HasNext())
	s.False(itr2.HasNext())
}

func (s *HistoryBlobIteratorSuite) constructMockHistoryManager(returnErrorOnPage int, lastPageHasNextPageToken bool, pages ...page) (*mocks.HistoryManager, [][]byte) {
	mockHistoryManager := &mocks.HistoryManager{}
	var pageTokens [][]byte
	for i, p := range pages {
		var pageToken []byte
		if i != 0 {
			pageToken = []byte(fmt.Sprintf("%v", i))
		}
		pageTokens = append(pageTokens, pageToken)
		req := &persistence.GetWorkflowExecutionHistoryRequest{
			DomainID: testDomainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(testWorkflowID),
				RunId:      common.StringPtr(testRunID),
			},
			FirstEventID:  common.FirstEventID,
			NextEventID:   testNextEventID,
			PageSize:      testDefaultPersistencePageSize,
			NextPageToken: pageToken,
		}
		if returnErrorOnPage == i {
			mockHistoryManager.On("GetWorkflowExecutionHistory", req).Return(nil, errors.New("got error getting workflow execution history"))
			return mockHistoryManager, pageTokens
		}
		var nextPageToken []byte
		if i != len(pages)-1 || lastPageHasNextPageToken {
			nextPageToken = []byte(fmt.Sprintf("%v", i+1))
		}
		resp := &persistence.GetWorkflowExecutionHistoryResponse{
			History: &shared.History{
				Events: s.constructHistoryEvents(p),
			},
			NextPageToken: nextPageToken,
		}
		mockHistoryManager.On("GetWorkflowExecutionHistory", req).Return(resp, nil)
	}
	if lastPageHasNextPageToken {
		req := &persistence.GetWorkflowExecutionHistoryRequest{
			DomainID: testDomainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(testWorkflowID),
				RunId:      common.StringPtr(testRunID),
			},
			FirstEventID:  common.FirstEventID,
			NextEventID:   testNextEventID,
			PageSize:      testDefaultPersistencePageSize,
			NextPageToken: []byte(fmt.Sprintf("%v", len(pages))),
		}
		resp := &persistence.GetWorkflowExecutionHistoryResponse{
			History:       &shared.History{},
			NextPageToken: nil,
			Size:          0,
		}
		mockHistoryManager.On("GetWorkflowExecutionHistory", req).Return(resp, nil)
	}
	return mockHistoryManager, pageTokens
}

func (s *HistoryBlobIteratorSuite) copyIteratorState(itr *historyBlobIterator) iteratorState {
	return iteratorState{
		blobPageToken:        itr.blobPageToken,
		persistencePageToken: itr.persistencePageToken,
		finishedIteration:    itr.finishedIteration,
		numEventsToSkip:      itr.numEventsToSkip,
	}
}

func (s *HistoryBlobIteratorSuite) assertStateMatches(expected iteratorState, itr *historyBlobIterator) {
	s.Equal(expected.blobPageToken, itr.blobPageToken)
	s.Equal(expected.persistencePageToken, itr.persistencePageToken)
	s.Equal(expected.finishedIteration, itr.finishedIteration)
	s.Equal(expected.numEventsToSkip, itr.numEventsToSkip)
}

func (s *HistoryBlobIteratorSuite) constructHistoryEvents(page page) []*shared.HistoryEvent {
	var events []*shared.HistoryEvent
	for i := 0; i < page.numEvents; i++ {
		event := &shared.HistoryEvent{
			EventId: common.Int64Ptr(page.firstEventID + int64(i)),
			Version: common.Int64Ptr(page.firstEventFailoverVersion),
		}
		if i == page.numEvents-1 {
			event.Version = common.Int64Ptr(page.lastEventFailoverVersion)
		}
		events = append(events, event)
	}
	return events
}

func (s *HistoryBlobIteratorSuite) constructTestHistoryBlobIterator(
	mockHistoryManager *mocks.HistoryManager,
	mockHistoryV2Manager *mocks.HistoryV2Manager,
	config *Config,
) *historyBlobIterator {
	var eventStoreVersion int32
	if mockHistoryV2Manager != nil {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
	if config == nil {
		config = constructConfig(testDefaultPersistencePageSize, testDefaultTargetArchivalBlobSize)
	}

	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		EventStoreVersion:    int32(eventStoreVersion),
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	container := &BootstrapContainer{
		HistoryManager:       mockHistoryManager,
		HistoryV2Manager:     mockHistoryV2Manager,
		Config:               config,
		HistorySizeEstimator: newTestSizeEstimator(),
	}
	return NewHistoryBlobIterator(request, container, testDomain, testClusterName).(*historyBlobIterator)
}

func constructConfig(historyPageSize, targetArchivalBlobSize int) *Config {
	return &Config{
		HistoryPageSize:           dynamicconfig.GetIntPropertyFilteredByDomain(historyPageSize),
		TargetArchivalBlobSize:    dynamicconfig.GetIntPropertyFilteredByDomain(targetArchivalBlobSize),
		EnableArchivalCompression: dynamicconfig.GetBoolPropertyFnFilteredByDomain(true),
	}
}
