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
	"context"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
)

type (
	EventKey struct {
		NamespaceID namespace.ID
		WorkflowID  string
		RunID       string
		EventID     int64
		Version     int64
	}

	Cache interface {
		GetEvent(ctx context.Context, shardID int32, key EventKey, firstEventID int64, branchToken []byte) (*historypb.HistoryEvent, error)
		PutEvent(key EventKey, event *historypb.HistoryEvent)
		DeleteEvent(key EventKey)
	}

	CacheImpl struct {
		cache.Cache
		executionManager persistence.ExecutionManager
		metricsHandler   metrics.Handler
		logger           log.Logger
		disabled         bool
	}

	historyEventCacheItemImpl struct {
		event *historypb.HistoryEvent
	}
)

var (
	errEventNotFoundInBatch = serviceerror.NewInternal("History event not found within expected batch")
)

var _ Cache = (*CacheImpl)(nil)

func NewHostLevelEventsCache(
	executionManager persistence.ExecutionManager,
	config *configs.Config,
	handler metrics.Handler,
	logger log.Logger,
	disabled bool,
) Cache {
	return newEventsCache(executionManager, handler, logger, config.EventsHostLevelCacheMaxSizeBytes(), config.EventsCacheTTL(), disabled)
}

func NewShardLevelEventsCache(
	executionManager persistence.ExecutionManager,
	config *configs.Config,
	handler metrics.Handler,
	logger log.Logger,
	disabled bool,
) Cache {
	return newEventsCache(executionManager, handler, logger, config.EventsShardLevelCacheMaxSizeBytes(), config.EventsCacheTTL(), disabled)
}

func newEventsCache(
	executionManager persistence.ExecutionManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
	maxSize int,
	ttl time.Duration,
	disabled bool,
) *CacheImpl {
	opts := &cache.Options{}
	opts.TTL = ttl

	taggedMetricHandler := metricsHandler.WithTags(metrics.StringTag(metrics.CacheTypeTagName, metrics.EventsCacheTypeTagValue))
	return &CacheImpl{
		Cache:            cache.New(maxSize, opts, taggedMetricHandler),
		executionManager: executionManager,
		metricsHandler:   taggedMetricHandler,
		logger:           logger,
		disabled:         disabled,
	}
}

func (e *CacheImpl) validateKey(key EventKey) bool {
	if len(key.NamespaceID) == 0 || len(key.WorkflowID) == 0 || len(key.RunID) == 0 || key.EventID < common.FirstEventID {
		// This is definitely a bug, but just warn and don't crash so we can find anywhere this happens.
		e.logger.Warn("one or more ids is invalid in event cache",
			tag.WorkflowID(key.WorkflowID),
			tag.WorkflowRunID(key.RunID),
			tag.WorkflowNamespaceID(key.NamespaceID.String()),
			tag.WorkflowEventID(key.EventID))
		return false
	}
	return true
}

func (e *CacheImpl) GetEvent(ctx context.Context, shardID int32, key EventKey, firstEventID int64, branchToken []byte) (*historypb.HistoryEvent, error) {
	handler := e.metricsHandler.WithTags(metrics.OperationTag(metrics.EventsCacheGetEventScope))
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	validKey := e.validateKey(key)

	// Test hook for disabling cache
	if !e.disabled {
		eventItem, cacheHit := e.Cache.Get(key).(*historyEventCacheItemImpl)
		if cacheHit {
			return eventItem.event, nil
		}
	}

	metrics.CacheMissCounter.With(handler).Record(1)
	event, err := e.getHistoryEventFromStore(ctx, shardID, key, firstEventID, branchToken)
	if err != nil {
		metrics.CacheFailures.With(handler).Record(1)
		e.logger.Error("Cache unable to retrieve event from store",
			tag.Error(err),
			tag.WorkflowID(key.WorkflowID),
			tag.WorkflowRunID(key.RunID),
			tag.WorkflowNamespaceID(key.NamespaceID.String()),
			tag.WorkflowEventID(key.EventID))
		return nil, err
	}

	// If invalid, return event anyway, but don't store in cache
	if validKey {
		e.put(key, event)
	}
	return event, nil
}

func (e *CacheImpl) PutEvent(key EventKey, event *historypb.HistoryEvent) {
	handler := e.metricsHandler.WithTags(metrics.OperationTag(metrics.EventsCachePutEventScope))
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	if !e.validateKey(key) {
		return
	}
	e.put(key, event)
}

func (e *CacheImpl) DeleteEvent(key EventKey) {
	handler := e.metricsHandler.WithTags(metrics.OperationTag(metrics.EventsCacheDeleteEventScope))
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	e.validateKey(key) // just for log message, delete anyway
	e.Delete(key)
}

func (e *CacheImpl) getHistoryEventFromStore(
	ctx context.Context,
	shardID int32,
	key EventKey,
	firstEventID int64,
	branchToken []byte,
) (*historypb.HistoryEvent, error) {

	handler := e.metricsHandler.WithTags(metrics.OperationTag(metrics.EventsCacheGetFromStoreScope))
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	response, err := e.executionManager.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    key.EventID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
	})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		e.logger.Error("encounter data loss event",
			tag.WorkflowNamespaceID(key.NamespaceID.String()),
			tag.WorkflowID(key.WorkflowID),
			tag.WorkflowRunID(key.RunID))
		metrics.CacheFailures.With(handler).Record(1)
		return nil, err
	default:
		metrics.CacheFailures.With(handler).Record(1)
		return nil, err
	}

	// find history event from batch and return back single event to caller
	for _, e := range response.HistoryEvents {
		if e.EventId == key.EventID && e.Version == key.Version {
			return e, nil
		}
	}

	return nil, errEventNotFoundInBatch
}

func (e *CacheImpl) put(key EventKey, event *historypb.HistoryEvent) interface{} {
	return e.Put(key, newHistoryEventCacheItem(event))
}

var _ cache.SizeGetter = (*historyEventCacheItemImpl)(nil)

func newHistoryEventCacheItem(
	event *historypb.HistoryEvent,
) *historyEventCacheItemImpl {
	return &historyEventCacheItemImpl{
		event: event,
	}
}

func (h *historyEventCacheItemImpl) CacheSize() int {
	return h.event.Size()
}
