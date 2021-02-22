// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination events_cache_mock.go

package events

import (
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

type (
	Cache interface {
		GetEvent(
			namespaceID string,
			workflowID string,
			runID string,
			firstEventID int64,
			eventID int64,
			branchToken []byte,
		) (*historypb.HistoryEvent, error)
		PutEvent(
			namespaceID string,
			workflowID string,
			runID string,
			eventID int64,
			event *historypb.HistoryEvent,
		)
		DeleteEvent(
			namespaceID string,
			workflowID string,
			runID string,
			eventID int64,
		)
	}

	CacheImpl struct {
		cache.Cache
		eventsMgr     persistence.HistoryManager
		disabled      bool
		logger        log.Logger
		metricsClient metrics.Client
		shardID       *int32
	}

	eventKey struct {
		namespaceID string
		workflowID  string
		runID       string
		eventID     int64
	}
)

var (
	errEventNotFoundInBatch = serviceerror.NewInternal("History event not found within expected batch")
)

var _ Cache = (*CacheImpl)(nil)

func NewEventsCache(
	shardID *int32,
	initialCount int,
	maxCount int,
	ttl time.Duration,
	eventsMgr persistence.HistoryManager,
	disabled bool,
	logger log.Logger,
	metrics metrics.Client,
) *CacheImpl {
	opts := &cache.Options{}
	opts.InitialCapacity = initialCount
	opts.TTL = ttl

	return &CacheImpl{
		Cache:         cache.New(maxCount, opts),
		eventsMgr:     eventsMgr,
		disabled:      disabled,
		logger:        logger.WithTags(tag.ComponentEventsCache),
		metricsClient: metrics,
		shardID:       shardID,
	}
}

func newEventKey(namespaceID, workflowID, runID string, eventID int64) eventKey {
	return eventKey{
		namespaceID: namespaceID,
		workflowID:  workflowID,
		runID:       runID,
		eventID:     eventID,
	}
}

func (e *CacheImpl) GetEvent(namespaceID, workflowID, runID string, firstEventID, eventID int64,
	branchToken []byte) (*historypb.HistoryEvent, error) {
	e.metricsClient.IncCounter(metrics.EventsCacheGetEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheGetEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(namespaceID, workflowID, runID, eventID)
	// Test hook for disabling cache
	if !e.disabled {
		event, cacheHit := e.Cache.Get(key).(*historypb.HistoryEvent)
		if cacheHit {
			return event, nil
		}
	}

	e.metricsClient.IncCounter(metrics.EventsCacheGetEventScope, metrics.CacheMissCounter)
	event, err := e.getHistoryEventFromStore(namespaceID, workflowID, runID, firstEventID, eventID, branchToken)
	if err != nil {
		e.metricsClient.IncCounter(metrics.EventsCacheGetEventScope, metrics.CacheFailures)
		e.logger.Error("Cache unable to retrieve event from store",
			tag.Error(err),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowEventID(eventID))
		return nil, err
	}

	e.Put(key, event)
	return event, nil
}

func (e *CacheImpl) PutEvent(namespaceID, workflowID, runID string, eventID int64, event *historypb.HistoryEvent) {
	e.metricsClient.IncCounter(metrics.EventsCachePutEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCachePutEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(namespaceID, workflowID, runID, eventID)
	e.Put(key, event)
}

func (e *CacheImpl) DeleteEvent(namespaceID, workflowID, runID string, eventID int64) {
	e.metricsClient.IncCounter(metrics.EventsCacheDeleteEventScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheDeleteEventScope, metrics.CacheLatency)
	defer sw.Stop()

	key := newEventKey(namespaceID, workflowID, runID, eventID)
	e.Delete(key)
}

func (e *CacheImpl) getHistoryEventFromStore(
	namespaceID string,
	workflowID string,
	runID string,
	firstEventID int64,
	eventID int64,
	branchToken []byte,
) (*historypb.HistoryEvent, error) {

	e.metricsClient.IncCounter(metrics.EventsCacheGetFromStoreScope, metrics.CacheRequests)
	sw := e.metricsClient.StartTimer(metrics.EventsCacheGetFromStoreScope, metrics.CacheLatency)
	defer sw.Stop()

	response, err := e.eventsMgr.ReadHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    eventID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       e.shardID,
	})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		e.logger.Error("encounter data loss event", tag.WorkflowNamespaceID(namespaceID), tag.WorkflowID(workflowID), tag.WorkflowRunID(runID))
		e.metricsClient.IncCounter(metrics.EventsCacheGetFromStoreScope, metrics.CacheFailures)
		return nil, err
	default:
		e.metricsClient.IncCounter(metrics.EventsCacheGetFromStoreScope, metrics.CacheFailures)
		return nil, err
	}

	// find history event from batch and return back single event to caller
	for _, e := range response.HistoryEvents {
		if e.GetEventId() == eventID {
			return e, nil
		}
	}

	return nil, errEventNotFoundInBatch
}
