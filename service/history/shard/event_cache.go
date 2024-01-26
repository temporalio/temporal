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

package shard

import (
	"context"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
)

type (
	EventsCacheKey struct {
		NamespaceID namespace.ID
		WorkflowID  string
		RunID       string
		EventID     int64
		Version     int64
	}

	EventsCache interface {
		GetEvent(ctx context.Context, shardContext Context, key EventsCacheKey, firstEventID int64, branchToken []byte) (*historypb.HistoryEvent, error)
		PutEvent(shardContext Context, key EventsCacheKey, event *historypb.HistoryEvent)
		DeleteEvent(shardContext Context, key EventsCacheKey)
	}

	CacheImpl struct {
		cache.Cache
		disabled bool
	}

	historyEventCacheItemImpl struct {
		event *historypb.HistoryEvent
	}
	NewEventsCacheFn func(config *configs.Config) EventsCache
)

var (
	errEventNotFoundInBatch = serviceerror.NewInternal("History event not found within expected batch")
)

var _ EventsCache = (*CacheImpl)(nil)

func NewHostLevelEventsCache(
	config *configs.Config,
	disabled bool,
) *CacheImpl {
	return NewEventsCache(config.EventsCacheMaxSizeBytes(), config.EventsCacheTTL(), disabled)
}

func NewShardLevelEventsCache(
	config *configs.Config,
	disabled bool,
) *CacheImpl {
	return NewEventsCache(config.EventsShardLevelCacheMaxSizeBytes(), config.EventsCacheTTL(), disabled)
}

func NewEventsCache(
	maxSize int,
	ttl time.Duration,
	disabled bool,
) *CacheImpl {
	opts := &cache.Options{}
	opts.TTL = ttl

	return &CacheImpl{
		Cache:    cache.New(maxSize, opts),
		disabled: disabled,
	}
}

func (e *CacheImpl) validateKey(shardContext Context, key EventsCacheKey) bool {
	if len(key.NamespaceID) == 0 || len(key.WorkflowID) == 0 || len(key.RunID) == 0 || key.EventID < common.FirstEventID {
		// This is definitely a bug, but just warn and don't crash so we can find anywhere this happens.
		shardContext.GetLogger().Warn("one or more ids is invalid in event cache",
			tag.WorkflowID(key.WorkflowID),
			tag.WorkflowRunID(key.RunID),
			tag.WorkflowNamespaceID(key.NamespaceID.String()),
			tag.WorkflowEventID(key.EventID))
		return false
	}
	return true
}

func (e *CacheImpl) GetEvent(ctx context.Context, shardContext Context, key EventsCacheKey, firstEventID int64, branchToken []byte) (*historypb.HistoryEvent, error) {
	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.StringTag(metrics.CacheTypeTagName, metrics.EventsCacheTypeTagValue),
		metrics.OperationTag(metrics.EventsCacheGetEventScope),
	)
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	validKey := e.validateKey(shardContext, key)

	// Test hook for disabling cache
	if !e.disabled {
		eventItem, cacheHit := e.Cache.Get(key).(*historyEventCacheItemImpl)
		if cacheHit {
			return eventItem.event, nil
		}
	}

	handler.Counter(metrics.CacheMissCounter.Name()).Record(1)
	event, err := e.getHistoryEventFromStore(ctx, shardContext, key, firstEventID, branchToken)
	if err != nil {
		handler.Counter(metrics.CacheFailures.Name()).Record(1)
		shardContext.GetLogger().Error("EventsCache unable to retrieve event from store",
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

func (e *CacheImpl) PutEvent(shardContext Context, key EventsCacheKey, event *historypb.HistoryEvent) {
	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.StringTag(metrics.CacheTypeTagName, metrics.EventsCacheTypeTagValue),
		metrics.OperationTag(metrics.EventsCachePutEventScope),
	)
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	if !e.validateKey(shardContext, key) {
		return
	}
	e.put(key, event)
}

func (e *CacheImpl) DeleteEvent(shardContext Context, key EventsCacheKey) {
	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.StringTag(metrics.CacheTypeTagName, metrics.EventsCacheTypeTagValue),
		metrics.OperationTag(metrics.EventsCacheDeleteEventScope),
	)
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	e.validateKey(shardContext, key) // just for log message, delete anyway
	e.Delete(key)
}

func (e *CacheImpl) getHistoryEventFromStore(
	ctx context.Context,
	shardContext Context,
	key EventsCacheKey,
	firstEventID int64,
	branchToken []byte,
) (*historypb.HistoryEvent, error) {

	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.StringTag(metrics.CacheTypeTagName, metrics.EventsCacheTypeTagValue),
		metrics.OperationTag(metrics.EventsCacheGetFromStoreScope),
	)
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	response, err := shardContext.GetExecutionManager().ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    key.EventID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardContext.GetShardID(),
	})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		shardContext.GetLogger().Error("encounter data loss event", tag.WorkflowNamespaceID(key.NamespaceID.String()), tag.WorkflowID(key.WorkflowID), tag.WorkflowRunID(key.RunID))
		//handler.Counter(metrics.CacheFailures.Name()).Record(1)
		return nil, err
	default:
		//handler.Counter(metrics.CacheFailures.Name()).Record(1)
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

func (e *CacheImpl) put(key EventsCacheKey, event *historypb.HistoryEvent) interface{} {
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
