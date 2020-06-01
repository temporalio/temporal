// Copyright (c) 2020 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination cache_mock.go

package events

import (
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
)

type (
	// Cache caches workflow history event
	Cache interface {
		GetEvent(
			shardID int,
			domainID string,
			workflowID string,
			runID string,
			firstEventID int64,
			eventID int64,
			branchToken []byte,
		) (*shared.HistoryEvent, error)
		PutEvent(
			domainID string,
			workflowID string,
			runID string,
			eventID int64,
			event *shared.HistoryEvent,
		)
		DeleteEvent(
			domainID string,
			workflowID string,
			runID string,
			eventID int64,
		)
	}

	cacheImpl struct {
		cache.Cache
		historyManager persistence.HistoryManager
		disabled       bool
		logger         log.Logger
		metricsClient  metrics.Client
		shardID        *int
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

var _ Cache = (*cacheImpl)(nil)

// NewGlobalCache creates a new global events cache
func NewGlobalCache(
	initialCount int,
	maxCount int,
	ttl time.Duration,
	historyManager persistence.HistoryManager,
	logger log.Logger,
	metricsClient metrics.Client,
	maxSize uint64,
) Cache {
	return newCacheWithOption(
		nil,
		initialCount,
		maxCount,
		ttl,
		historyManager,
		false,
		logger,
		metricsClient,
		maxSize,
	)
}

// NewCache creates a new events cache
func NewCache(
	shardID int,
	historyManager persistence.HistoryManager,
	config *config.Config,
	logger log.Logger,
	metricsClient metrics.Client,
) Cache {
	return newCacheWithOption(
		&shardID,
		config.EventsCacheInitialCount(),
		config.EventsCacheMaxCount(),
		config.EventsCacheTTL(),
		historyManager,
		false,
		logger,
		metricsClient,
		0,
	)
}

func newCacheWithOption(
	shardID *int,
	initialCount int,
	maxCount int,
	ttl time.Duration,
	historyManager persistence.HistoryManager,
	disabled bool,
	logger log.Logger,
	metrics metrics.Client,
	maxSize uint64,
) *cacheImpl {
	opts := &cache.Options{}
	opts.InitialCapacity = initialCount
	opts.TTL = ttl
	opts.MaxCount = maxCount

	if maxSize > 0 {
		opts.MaxSize = maxSize
		opts.GetCacheItemSizeFunc = func(event interface{}) uint64 {
			return common.GetSizeOfHistoryEvent(event.(*shared.HistoryEvent))
		}
	}

	return &cacheImpl{
		Cache:          cache.New(opts),
		historyManager: historyManager,
		disabled:       disabled,
		logger:         logger.WithTags(tag.ComponentEventsCache),
		metricsClient:  metrics,
		shardID:        shardID,
	}
}

func newEventKey(
	domainID,
	workflowID,
	runID string,
	eventID int64,
) eventKey {
	return eventKey{
		domainID:   domainID,
		workflowID: workflowID,
		runID:      runID,
		eventID:    eventID,
	}
}

func (e *cacheImpl) GetEvent(
	shardID int,
	domainID string,
	workflowID string,
	runID string, firstEventID int64,
	eventID int64,
	branchToken []byte,
) (*shared.HistoryEvent, error) {
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
	// use local id to preserve old logic before full migration to global event cache
	if e.shardID != nil {
		shardID = *e.shardID
	}
	event, err := e.getHistoryEventFromStore(firstEventID, eventID, branchToken, shardID)
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

func (e *cacheImpl) PutEvent(
	domainID, workflowID,
	runID string,
	eventID int64,
	event *shared.HistoryEvent,
) {
	e.metricsClient.IncCounter(metrics.EventsCachePutEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCachePutEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(domainID, workflowID, runID, eventID)
	e.Put(key, event)
}

func (e *cacheImpl) DeleteEvent(
	domainID, workflowID,
	runID string,
	eventID int64,
) {
	e.metricsClient.IncCounter(metrics.EventsCacheDeleteEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheDeleteEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(domainID, workflowID, runID, eventID)
	e.Delete(key)
}

func (e *cacheImpl) getHistoryEventFromStore(
	firstEventID,
	eventID int64,
	branchToken []byte,
	shardID int,
) (*shared.HistoryEvent, error) {
	e.metricsClient.IncCounter(metrics.EventsCacheGetFromStoreScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheGetFromStoreScope, metrics.CacheLatency)
	defer sw.Stop()

	var historyEvents []*shared.HistoryEvent

	response, err := e.historyManager.ReadHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    eventID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       common.IntPtr(shardID),
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
