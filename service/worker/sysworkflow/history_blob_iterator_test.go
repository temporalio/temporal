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

package sysworkflow

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	metricsMocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"testing"
)

const (
	testDomainID                      = "test-domain-id"
	testWorkflowID                    = "test-workflow-id"
	testRunID                         = "test-run-id"
	testNextEventID                   = 1800
	testDomain                        = "test-domain"
	testClusterName                   = "test-cluster-name"
	testCloseFailoverVersion          = 100
	testDefaultPersistencePageSize    = 250
	testDefaultTargetArchivalBlobSize = 2 * 1024 * 124
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type (
	HistoryBlobIteratorSuite struct {
		*require.Assertions
		suite.Suite
		logger        bark.Logger
		metricsClient *metricsMocks.Client
	}

	iteratorState struct {
		blobPageToken        int
		persistencePageToken []byte
		finishedIteration    bool
	}

	page struct {
		size                      int
		numEvents                 int
		firstEventID              int64
		firstEventFailoverVersion int64
		lastEventFailoverVersion  int64
	}
)

func TestHistoryBlobIteratorSuite(t *testing.T) {
	suite.Run(t, new(HistoryBlobIteratorSuite))
}

func (s *HistoryBlobIteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.logger = bark.NewNopLogger()
	s.metricsClient = &metricsMocks.Client{}
	s.metricsClient.On("IncCounter", mock.Anything, mock.Anything)
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Failed_EventsV2() {
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	mockHistoryV2Manager.On("ReadHistoryBranch", mock.Anything).Return(nil, errors.New("got error reading history branch"))
	itr := s.constructTestHistoryBlobIterator(nil, mockHistoryV2Manager, nil)
	events, size, nextPageToken, err := itr.readHistory([]byte{})
	s.Error(err)
	s.Nil(events)
	s.Zero(size)
	s.Nil(nextPageToken)
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Success_EventsV2() {
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	resp := persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*shared.HistoryEvent{},
		NextPageToken: []byte{},
		Size:          100,
	}
	mockHistoryV2Manager.On("ReadHistoryBranch", mock.Anything).Return(&resp, nil)
	itr := s.constructTestHistoryBlobIterator(nil, mockHistoryV2Manager, nil)
	events, size, nextPageToken, err := itr.readHistory([]byte{})
	s.NoError(err)
	s.NotNil(events)
	s.Equal(100, size)
	s.Empty(nextPageToken)
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Failed_EventsV1() {
	mockHistoryManager := &mocks.HistoryManager{}
	mockHistoryManager.On("GetWorkflowExecutionHistory", mock.Anything).Return(nil, errors.New("error getting workflow execution history"))
	itr := s.constructTestHistoryBlobIterator(mockHistoryManager, nil, nil)
	events, size, nextPageToken, err := itr.readHistory([]byte{})
	s.Error(err)
	s.Nil(events)
	s.Zero(size)
	s.Nil(nextPageToken)
}

func (s *HistoryBlobIteratorSuite) TestReadHistory_Success_EventsV1() {
	mockHistoryManager := &mocks.HistoryManager{}
	resp := persistence.GetWorkflowExecutionHistoryResponse{
		History: &shared.History{
			Events: []*shared.HistoryEvent{},
		},
		Size: 100,
	}
	mockHistoryManager.On("GetWorkflowExecutionHistory", mock.Anything).Return(&resp, nil)
	itr := s.constructTestHistoryBlobIterator(mockHistoryManager, nil, nil)
	events, size, nextPageToken, err := itr.readHistory([]byte{})
	s.NotNil(events)
	s.Equal(100, size)
	s.Empty(nextPageToken)
	s.NoError(err)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Fail_FirstCallToReadHistoryGivesError() {
	pages := []page{
		{
			size:                      1,
			numEvents:                 1,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(0, pages...)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, nil)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, err := itr.readBlobEvents(pageTokens[0])
	s.Nil(events)
	s.Nil(nextPageToken)
	s.False(historyEndReached)
	s.Error(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Fail_NonFirstCallToReadHistoryGivesError() {
	pages := []page{
		{
			size:                      1,
			numEvents:                 1,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      1,
			numEvents:                 1,
			firstEventID:              2,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(1, pages...)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, nil)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, err := itr.readBlobEvents(pageTokens[0])
	s.Nil(events)
	s.Nil(nextPageToken)
	s.False(historyEndReached)
	s.Error(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_ReadToHistoryEnd() {
	pages := []page{
		{
			size:                      200,
			numEvents:                 10,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      100,
			numEvents:                 15,
			firstEventID:              11,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      500,
			numEvents:                 50,
			firstEventID:              26,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, pages...)
	// ensure target blob size is greater than total history length to ensure all of history is read
	config := constructConfig(testDefaultPersistencePageSize, 1000)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, err := itr.readBlobEvents(pageTokens[0])
	s.NotNil(events)
	s.Len(events, 75)
	s.Len(nextPageToken, 0)
	s.True(historyEndReached)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_TargetSizeSatisfiedWithoutReadingToEnd() {
	pages := []page{
		{
			size:                      200,
			numEvents:                 10,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      100,
			numEvents:                 15,
			firstEventID:              11,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      500,
			numEvents:                 50,
			firstEventID:              26,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, pages...)
	// ensure target blob size is smaller than full length of history so that not all of history is read
	config := constructConfig(testDefaultPersistencePageSize, 250)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, err := itr.readBlobEvents(pageTokens[0])
	s.NotNil(events)
	s.Len(events, 25)
	s.NotEmpty(nextPageToken)
	s.Equal(pageTokens[len(pageTokens)-1], nextPageToken)
	s.False(historyEndReached)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestReadBlobEvents_Success_TargetSizeSatisfiedWithReadingToEnd() {
	pages := []page{
		{
			size:                      200,
			numEvents:                 10,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      100,
			numEvents:                 15,
			firstEventID:              11,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      500,
			numEvents:                 50,
			firstEventID:              26,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, pages...)
	// set target blob size such that all of history is read and the target blob size becomes satisfied upon reading last blob
	config := constructConfig(testDefaultPersistencePageSize, 301)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	events, nextPageToken, historyEndReached, err := itr.readBlobEvents(pageTokens[0])
	s.NotNil(events)
	s.Len(events, 75)
	s.Len(nextPageToken, 0)
	s.True(historyEndReached)
	s.NoError(err)
	s.assertStateMatches(startingIteratorState, itr)
}

func (s *HistoryBlobIteratorSuite) TestNext_Fail_IteratorDepleted() {
	pages := []page{
		{
			size:                      200,
			numEvents:                 10,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      100,
			numEvents:                 15,
			firstEventID:              11,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  2,
		},
		{
			size:                      500,
			numEvents:                 50,
			firstEventID:              26,
			firstEventFailoverVersion: 2,
			lastEventFailoverVersion:  5,
		},
	}
	historyManager, _ := s.constructMockHistoryManager(-1, pages...)
	// set target blob size such that a single call to next will read all of history
	config := constructConfig(testDefaultPersistencePageSize, 301)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	blob, err := itr.Next()
	expectedIteratorState := iteratorState{
		// when iteration is finished page token is not advanced
		blobPageToken:        startingIteratorState.blobPageToken,
		persistencePageToken: nil,
		finishedIteration:    true,
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.NotNil(blob)
	s.Equal(common.FirstBlobPageToken, *blob.Header.CurrentPageToken)
	s.Equal(common.LastBlobNextPageToken, *blob.Header.NextPageToken)
	s.Equal(int64(1), *blob.Header.FirstFailoverVersion)
	s.Equal(int64(5), *blob.Header.LastFailoverVersion)
	s.Equal(int64(1), *blob.Header.FirstEventID)
	s.Equal(int64(75), *blob.Header.LastEventID)
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
			size:                      200,
			numEvents:                 10,
			firstEventID:              1,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		},
		{
			size:                      100,
			numEvents:                 15,
			firstEventID:              11,
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  2,
		},
		{
			size:                      500,
			numEvents:                 50,
			firstEventID:              26,
			firstEventFailoverVersion: 2,
			lastEventFailoverVersion:  5,
		},
	}
	historyManager, pageTokens := s.constructMockHistoryManager(2, pages...)
	// set target blob size such that the first two pages are read for blob one without error, third page will return error
	config := constructConfig(testDefaultPersistencePageSize, 250)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	startingIteratorState := s.copyIteratorState(itr)
	blob, err := itr.Next()
	expectedIteratorState := iteratorState{
		blobPageToken:        startingIteratorState.blobPageToken + 1,
		persistencePageToken: pageTokens[2],
		finishedIteration:    false,
	}
	s.assertStateMatches(expectedIteratorState, itr)
	s.NotNil(blob)
	s.Equal(common.FirstBlobPageToken, *blob.Header.CurrentPageToken)
	s.Equal(common.FirstBlobPageToken+1, *blob.Header.NextPageToken)
	s.Equal(int64(1), *blob.Header.FirstFailoverVersion)
	s.Equal(int64(2), *blob.Header.LastFailoverVersion)
	s.Equal(int64(1), *blob.Header.FirstEventID)
	s.Equal(int64(25), *blob.Header.LastEventID)
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
			size:                      1000,
			numEvents:                 10,
			firstEventID:              common.FirstEventID + int64(i*10),
			firstEventFailoverVersion: 1,
			lastEventFailoverVersion:  1,
		}
		pages = append(pages, p)
	}
	historyManager, pageTokens := s.constructMockHistoryManager(-1, pages...)
	// set config such that every 10 persistence pages is one blob
	config := constructConfig(testDefaultPersistencePageSize, 10000)
	itr := s.constructTestHistoryBlobIterator(historyManager, nil, config)
	expectedIteratorState := iteratorState{
		blobPageToken:        common.FirstBlobPageToken,
		persistencePageToken: nil,
		finishedIteration:    false,
	}
	for i := 0; i < 10; i++ {
		s.assertStateMatches(expectedIteratorState, itr)
		s.True(itr.HasNext())
		blob, err := itr.Next()
		s.NoError(err)
		s.NotNil(blob)
		s.Equal(common.FirstEventID+int64(i*100), *blob.Header.FirstEventID)
		s.Equal(int64(100+(i*100)), *blob.Header.LastEventID)
		s.Equal(i+1, *blob.Header.CurrentPageToken)
		if i == 9 {
			s.Equal(common.LastBlobNextPageToken, *blob.Header.NextPageToken)
		} else {
			s.Equal(i+2, *blob.Header.NextPageToken)
		}
		s.Equal(int64(100), *blob.Header.EventCount)
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

func (s *HistoryBlobIteratorSuite) constructMockHistoryManager(returnErrorOnPage int, pages ...page) (*mocks.HistoryManager, [][]byte) {
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
		if i != len(pages)-1 {
			nextPageToken = []byte(fmt.Sprintf("%v", i+1))
		}
		resp := &persistence.GetWorkflowExecutionHistoryResponse{
			History: &shared.History{
				Events: s.constructHistoryEvents(p),
			},
			NextPageToken: nextPageToken,
			Size:          p.size,
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
	}
}

func (s *HistoryBlobIteratorSuite) assertStateMatches(expected iteratorState, itr *historyBlobIterator) {
	s.Equal(expected.blobPageToken, itr.blobPageToken)
	s.Equal(expected.persistencePageToken, itr.persistencePageToken)
	s.Equal(expected.finishedIteration, itr.finishedIteration)
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
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		EventStoreVersion:    int32(eventStoreVersion),
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	container := &SysWorkerContainer{
		HistoryManager:   mockHistoryManager,
		HistoryV2Manager: mockHistoryV2Manager,
		Config:           config,
	}
	return NewHistoryBlobIterator(
		s.logger,
		s.metricsClient,
		request,
		container,
		testDomain,
		testClusterName,
	).(*historyBlobIterator)
}

func constructConfig(historyPageSize, targetArchivalBlobSize int) *Config {
	return &Config{
		HistoryPageSize:           dynamicconfig.GetIntPropertyFilteredByDomain(historyPageSize),
		TargetArchivalBlobSize:    dynamicconfig.GetIntPropertyFilteredByDomain(targetArchivalBlobSize),
		EnableArchivalCompression: dynamicconfig.GetBoolPropertyFnFilteredByDomain(true),
	}
}
