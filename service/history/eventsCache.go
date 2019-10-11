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
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	eventsCache interface {
		getEvent(domainID, workflowID, runID string, firstEventID, eventID int64,
			branchToken []byte) (*shared.HistoryEvent, error)
		putEvent(domainID, workflowID, runID string, eventID int64, event *shared.HistoryEvent)
		deleteEvent(domainID, workflowID, runID string, eventID int64)
	}

	eventsCacheImpl struct {
		cache.Cache
		eventsV2Mgr   persistence.HistoryV2Manager
		disabled      bool
		logger        log.Logger
		metricsClient metrics.Client
		shardID       *int
	}

	eventKey struct {
		domainID   string
		workflowID string
		runID      string
		eventID    int64
	}
)

var (
	errEventNotFoundInBatch = &shared.InternalServiceError{Message: "History event not found within expected batch"}
)

var _ eventsCache = (*eventsCacheImpl)(nil)

func newEventsCache(shardCtx ShardContext) eventsCache {
	config := shardCtx.GetConfig()
	shardID := common.IntPtr(shardCtx.GetShardID())
	return newEventsCacheWithOptions(config.EventsCacheInitialSize(), config.EventsCacheMaxSize(), config.EventsCacheTTL(),
		shardCtx.GetHistoryV2Manager(), false, shardCtx.GetLogger(), shardCtx.GetMetricsClient(), shardID)
}

func newEventsCacheWithOptions(initialSize, maxSize int, ttl time.Duration,
	eventsV2Mgr persistence.HistoryV2Manager, disabled bool, logger log.Logger, metrics metrics.Client, shardID *int) *eventsCacheImpl {
	opts := &cache.Options{}
	opts.InitialCapacity = initialSize
	opts.TTL = ttl

	return &eventsCacheImpl{
		Cache:         cache.New(maxSize, opts),
		eventsV2Mgr:   eventsV2Mgr,
		disabled:      disabled,
		logger:        logger.WithTags(tag.ComponentEventsCache),
		metricsClient: metrics,
		shardID:       shardID,
	}
}

func newEventKey(domainID, workflowID, runID string, eventID int64) eventKey {
	return eventKey{
		domainID:   domainID,
		workflowID: workflowID,
		runID:      runID,
		eventID:    eventID,
	}
}

func (e *eventsCacheImpl) getEvent(domainID, workflowID, runID string, firstEventID, eventID int64,
	branchToken []byte) (*shared.HistoryEvent, error) {
	e.metricsClient.IncCounter(metrics.EventsCacheGetEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheGetEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(domainID, workflowID, runID, eventID)
	// Test hook for disabling cache
	if !e.disabled {
		event, cacheHit := e.Cache.Get(key).(*shared.HistoryEvent)
		if cacheHit {
			return event, nil
		}
	}

	e.metricsClient.IncCounter(metrics.EventsCacheGetEventScope, metrics.CacheMissCounter)
	event, err := e.getHistoryEventFromStore(domainID, workflowID, runID, firstEventID, eventID, branchToken)
	if err != nil {
		e.metricsClient.IncCounter(metrics.EventsCacheGetEventScope, metrics.CacheFailures)
		e.logger.Error("EventsCache unable to retrieve event from store",
			tag.Error(err),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.WorkflowDomainID(domainID),
			tag.WorkflowEventID(eventID))
		return nil, err
	}

	e.Put(key, event)
	return event, nil
}

func (e *eventsCacheImpl) putEvent(domainID, workflowID, runID string, eventID int64, event *shared.HistoryEvent) {
	e.metricsClient.IncCounter(metrics.EventsCachePutEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCachePutEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(domainID, workflowID, runID, eventID)
	e.Put(key, event)
}

func (e *eventsCacheImpl) deleteEvent(domainID, workflowID, runID string, eventID int64) {
	e.metricsClient.IncCounter(metrics.EventsCacheDeleteEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheDeleteEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(domainID, workflowID, runID, eventID)
	e.Delete(key)
}

func (e *eventsCacheImpl) getHistoryEventFromStore(domainID, workflowID, runID string, firstEventID, eventID int64,
	branchToken []byte) (*shared.HistoryEvent, error) {
	e.metricsClient.IncCounter(metrics.EventsCacheGetFromStoreScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheGetFromStoreScope, metrics.CacheLatency)
	defer sw.Stop()

	var historyEvents []*shared.HistoryEvent

	response, err := e.eventsV2Mgr.ReadHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    eventID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       e.shardID,
	})

	if err != nil {
		e.metricsClient.IncCounter(metrics.EventsCacheGetFromStoreScope, metrics.CacheFailures)
		return nil, err
	}

	historyEvents = response.HistoryEvents

	// find history event from batch and return back single event to caller
	for _, e := range historyEvents {
		if e.GetEventId() == eventID {
			return e, nil
		}
	}

	return nil, errEventNotFoundInBatch
}
