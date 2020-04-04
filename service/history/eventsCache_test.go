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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	eventpb "go.temporal.io/temporal-proto/event"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	eventsCacheSuite struct {
		suite.Suite
		*require.Assertions

		logger log.Logger

		mockEventsV2Mgr *mocks.HistoryV2Manager

		cache *eventsCacheImpl
	}
)

func TestEventsCacheSuite(t *testing.T) {
	s := new(eventsCacheSuite)
	suite.Run(t, s)
}

func (s *eventsCacheSuite) SetupSuite() {

}

func (s *eventsCacheSuite) TearDownSuite() {

}

func (s *eventsCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.mockEventsV2Mgr = &mocks.HistoryV2Manager{}
	s.cache = s.newTestEventsCache()
}

func (s *eventsCacheSuite) TearDownTest() {
	s.mockEventsV2Mgr.AssertExpectations(s.T())
}

func (s *eventsCacheSuite) newTestEventsCache() *eventsCacheImpl {
	shardId := 10
	return newEventsCacheWithOptions(16, 32, time.Minute, s.mockEventsV2Mgr, false, s.logger,
		metrics.NewClient(tally.NoopScope, metrics.History), &shardId)
}

func (s *eventsCacheSuite) TestEventsCacheHitSuccess() {
	namespaceID := "events-cache-hit-success-namespace"
	workflowID := "events-cache-hit-success-workflow-id"
	runID := "events-cache-hit-success-run-id"
	eventID := int64(23)
	event := &eventpb.HistoryEvent{
		EventId:    eventID,
		EventType:  eventpb.EventType_ActivityTaskStarted,
		Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{}},
	}

	s.cache.putEvent(namespaceID, workflowID, runID, eventID, event)
	actualEvent, err := s.cache.getEvent(namespaceID, workflowID, runID, eventID, eventID, nil)
	s.Nil(err)
	s.Equal(event, actualEvent)
}

func (s *eventsCacheSuite) TestEventsCacheMissMultiEventsBatchV2Success() {
	namespaceID := "events-cache-miss-multi-events-batch-v2-success-namespace"
	workflowID := "events-cache-miss-multi-events-batch-v2-success-workflow-id"
	runID := "events-cache-miss-multi-events-batch-v2-success-run-id"
	event1 := &eventpb.HistoryEvent{
		EventId:    11,
		EventType:  eventpb.EventType_DecisionTaskCompleted,
		Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{}},
	}
	event2 := &eventpb.HistoryEvent{
		EventId:    12,
		EventType:  eventpb.EventType_ActivityTaskScheduled,
		Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{}},
	}
	event3 := &eventpb.HistoryEvent{
		EventId:    13,
		EventType:  eventpb.EventType_ActivityTaskScheduled,
		Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{}},
	}
	event4 := &eventpb.HistoryEvent{
		EventId:    14,
		EventType:  eventpb.EventType_ActivityTaskScheduled,
		Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{}},
	}
	event5 := &eventpb.HistoryEvent{
		EventId:    15,
		EventType:  eventpb.EventType_ActivityTaskScheduled,
		Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{}},
	}
	event6 := &eventpb.HistoryEvent{
		EventId:    16,
		EventType:  eventpb.EventType_ActivityTaskScheduled,
		Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{}},
	}

	shardId := 10
	s.mockEventsV2Mgr.On("ReadHistoryBranch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("store_token"),
		MinEventID:    event1.GetEventId(),
		MaxEventID:    event6.GetEventId() + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents:    []*eventpb.HistoryEvent{event1, event2, event3, event4, event5, event6},
		NextPageToken:    nil,
		LastFirstEventID: event1.GetEventId(),
	}, nil)

	s.cache.putEvent(namespaceID, workflowID, runID, event2.GetEventId(), event2)
	actualEvent, err := s.cache.getEvent(namespaceID, workflowID, runID, event1.GetEventId(), event6.GetEventId(),
		[]byte("store_token"))
	s.Nil(err)
	s.Equal(event6, actualEvent)
}

func (s *eventsCacheSuite) TestEventsCacheMissV2Failure() {
	namespaceID := "events-cache-miss-failure-namespace"
	workflowID := "events-cache-miss-failure-workflow-id"
	runID := "events-cache-miss-failure-run-id"

	shardId := 10
	expectedErr := errors.New("persistence call failed")
	s.mockEventsV2Mgr.On("ReadHistoryBranch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("store_token"),
		MinEventID:    int64(11),
		MaxEventID:    int64(15),
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(nil, expectedErr)

	actualEvent, err := s.cache.getEvent(namespaceID, workflowID, runID, int64(11), int64(14),
		[]byte("store_token"))
	s.Nil(actualEvent)
	s.Equal(expectedErr, err)
}

func (s *eventsCacheSuite) TestEventsCacheDisableSuccess() {
	namespaceID := "events-cache-disable-success-namespace"
	workflowID := "events-cache-disable-success-workflow-id"
	runID := "events-cache-disable-success-run-id"
	event1 := &eventpb.HistoryEvent{
		EventId:    23,
		EventType:  eventpb.EventType_ActivityTaskStarted,
		Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{}},
	}
	event2 := &eventpb.HistoryEvent{
		EventId:    32,
		EventType:  eventpb.EventType_ActivityTaskStarted,
		Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{}},
	}

	shardId := 10
	s.mockEventsV2Mgr.On("ReadHistoryBranch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("store_token"),
		MinEventID:    event2.GetEventId(),
		MaxEventID:    event2.GetEventId() + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents:    []*eventpb.HistoryEvent{event2},
		NextPageToken:    nil,
		LastFirstEventID: event2.GetEventId(),
	}, nil)

	s.cache.putEvent(namespaceID, workflowID, runID, event1.GetEventId(), event1)
	s.cache.putEvent(namespaceID, workflowID, runID, event2.GetEventId(), event2)
	s.cache.disabled = true
	actualEvent, err := s.cache.getEvent(namespaceID, workflowID, runID, event2.GetEventId(), event2.GetEventId(),
		[]byte("store_token"))
	s.Nil(err)
	s.Equal(event2, actualEvent)
}
