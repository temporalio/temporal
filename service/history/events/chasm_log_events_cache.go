//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination chasm_log_events_cache_mock.go

package events

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/configs"
	"google.golang.org/protobuf/proto"
)

type (
	ChasmLogMessagesKey struct {
		NamespaceID namespace.ID
		BusinessID  string
		RunID       string
		BatchID     int64
		Version     int64
		ShardUUID   string
	}

	ChasmLogMessagesCache interface {
		GetLogMessageBatch(
			ctx context.Context,
			shardID int32,
			key ChasmLogMessagesKey,
			branchToken []byte,
		) (*persistencespb.ChasmLogMessageBatch, error)

		PutLogMessageBatch(key ChasmLogMessagesKey, batch *persistencespb.ChasmLogMessageBatch)

		DeleteLogMessageBatch(key ChasmLogMessagesKey)
	}

	ChasmLogMessagesCacheImpl struct {
		cache.Cache
		executionManager persistence.ExecutionManager
		serializer       serialization.Serializer
		metricsHandler   metrics.Handler
		logger           log.Logger
		disabled         bool
	}

	chasmLogMessagesCacheItemImpl struct {
		batch *persistencespb.ChasmLogMessageBatch
	}
)

var _ ChasmLogMessagesCache = (*ChasmLogMessagesCacheImpl)(nil)

func NewHostLevelChasmLogMessagesCache(
	executionManager persistence.ExecutionManager,
	serializer serialization.Serializer,
	config *configs.Config,
	handler metrics.Handler,
	logger log.Logger,
	disabled bool,
) ChasmLogMessagesCache {
	return newChasmLogMessagesCache(
		executionManager,
		serializer,
		handler,
		logger,
		config.EventsHostLevelCacheMaxSizeBytes(),
		config.EventsCacheTTL(),
		disabled,
	)
}

func NewShardLevelChasmLogMessagesCache(
	executionManager persistence.ExecutionManager,
	serializer serialization.Serializer,
	config *configs.Config,
	handler metrics.Handler,
	logger log.Logger,
	disabled bool,
) ChasmLogMessagesCache {
	return newChasmLogMessagesCache(
		executionManager,
		serializer,
		handler,
		logger,
		config.EventsShardLevelCacheMaxSizeBytes(),
		config.EventsCacheTTL(),
		disabled,
	)
}

func newChasmLogMessagesCache(
	executionManager persistence.ExecutionManager,
	serializer serialization.Serializer,
	metricsHandler metrics.Handler,
	logger log.Logger,
	maxSize int,
	ttl time.Duration,
	disabled bool,
) *ChasmLogMessagesCacheImpl {
	opts := &cache.Options{}
	opts.TTL = ttl

	taggedMetricHandler := metricsHandler.WithTags(
		metrics.CacheTypeTag(metrics.ChasmLogMessagesCacheTypeTagValue),
	)
	return &ChasmLogMessagesCacheImpl{
		Cache:            cache.NewWithMetrics(maxSize, opts, taggedMetricHandler),
		executionManager: executionManager,
		serializer:       serializer,
		metricsHandler:   taggedMetricHandler,
		logger:           logger,
		disabled:         disabled,
	}
}

func (e *ChasmLogMessagesCacheImpl) validateKey(key ChasmLogMessagesKey) bool {
	if len(key.NamespaceID) == 0 || len(key.BusinessID) == 0 || len(key.RunID) == 0 ||
		len(key.ShardUUID) == 0 || key.BatchID < common.FirstEventID {
		// This is definitely a bug, but just warn and don't crash so we can find anywhere this happens.
		e.logger.Warn("one or more ids is invalid in chasm log messages cache",
			tag.WorkflowID(key.BusinessID),
			tag.WorkflowRunID(key.RunID),
			tag.WorkflowNamespaceID(key.NamespaceID.String()),
			tag.WorkflowEventID(key.BatchID),
			tag.NewStringTag("shard-uuid", key.ShardUUID))
		return false
	}
	return true
}

func (e *ChasmLogMessagesCacheImpl) GetLogMessageBatch(
	ctx context.Context,
	shardID int32,
	key ChasmLogMessagesKey,
	branchToken []byte,
) (*persistencespb.ChasmLogMessageBatch, error) {
	handler := e.metricsHandler.WithTags(
		metrics.OperationTag(metrics.EventsCacheGetEventScope),
		metrics.NamespaceIDTag(key.NamespaceID.String()),
	)
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	validKey := e.validateKey(key)

	// Test hook for disabling cache
	if !e.disabled {
		batchItem, cacheHit := e.Get(key).(*chasmLogMessagesCacheItemImpl)
		if cacheHit {
			return batchItem.batch, nil
		}
	}

	metrics.CacheMissCounter.With(handler).Record(1)
	batches, err := e.getLogEventBatchesFromStore(ctx, shardID, key, branchToken)
	if err != nil {
		metrics.CacheFailures.With(handler).Record(1)
		e.logger.Error("Cache unable to retrieve chasm log message batch from store",
			tag.Error(err),
			tag.WorkflowID(key.BusinessID),
			tag.WorkflowRunID(key.RunID),
			tag.WorkflowNamespaceID(key.NamespaceID.String()),
			tag.WorkflowEventID(key.BatchID))
		return nil, err
	}

	// If invalid, return batch anyway, but don't store in cache
	if validKey {
		batchKey := key
		for _, batch := range batches {
			batchKey.BatchID = batch.Messages[0].GetSequenceId()
			e.put(batchKey, batch)
		}
	}
	return batches[0], nil
}

func (e *ChasmLogMessagesCacheImpl) PutLogMessageBatch(
	key ChasmLogMessagesKey,
	batch *persistencespb.ChasmLogMessageBatch,
) {
	handler := e.metricsHandler.WithTags(metrics.OperationTag(metrics.EventsCachePutEventScope), metrics.NamespaceIDTag(key.NamespaceID.String()))
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	if !e.validateKey(key) {
		return
	}
	e.put(key, batch)
}

func (e *ChasmLogMessagesCacheImpl) DeleteLogMessageBatch(key ChasmLogMessagesKey) {
	handler := e.metricsHandler.WithTags(metrics.OperationTag(metrics.EventsCacheDeleteEventScope), metrics.NamespaceIDTag(key.NamespaceID.String()))
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	e.validateKey(key) // just for log message, delete anyway
	e.Delete(key)
}

func (e *ChasmLogMessagesCacheImpl) getLogEventBatchesFromStore(
	ctx context.Context,
	shardID int32,
	key ChasmLogMessagesKey,
	branchToken []byte,
) ([]*persistencespb.ChasmLogMessageBatch, error) {
	handler := e.metricsHandler.WithTags(
		metrics.OperationTag(metrics.EventsCacheGetFromStoreScope),
		metrics.NamespaceIDTag(key.NamespaceID.String()),
	)
	metrics.CacheRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(startTime)) }()

	response, err := e.executionManager.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    key.BatchID,
		MaxEventID:    key.BatchID + 64, // limit number of batches in case each batch size is very large
		PageSize:      64,
		NextPageToken: nil,
		ShardID:       shardID,
	})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss, *serialization.DeserializationError, *serialization.SerializationError:
		// log event
		e.logger.Error("encounter data corruption event",
			tag.WorkflowNamespaceID(key.NamespaceID.String()),
			tag.WorkflowID(key.BusinessID),
			tag.WorkflowRunID(key.RunID))
		metrics.CacheFailures.With(handler).Record(1)
		return nil, err
	default:
		metrics.CacheFailures.With(handler).Record(1)
		return nil, err
	}

	ret := make([]*persistencespb.ChasmLogMessageBatch, 0, len(response.HistoryEventBlobs))
	for _, blob := range response.HistoryEventBlobs {
		batch, err := e.serializer.ChasmLogMessagesFromBlob(blob)
		if err != nil {
			return nil, err
		}
		if batch[0].GetVersion() != key.Version {
			continue
		}
		ret = append(ret, &persistencespb.ChasmLogMessageBatch{
			Messages: batch,
		})
	}

	if len(ret) == 0 {
		return nil, errEventNotFoundInBatch
	}
	return ret, nil
}

func (e *ChasmLogMessagesCacheImpl) put(
	key ChasmLogMessagesKey,
	batch *persistencespb.ChasmLogMessageBatch,
) any {
	return e.Put(key, newChasmLogEventsCacheItem(batch))
}

var _ cache.SizeGetter = (*chasmLogMessagesCacheItemImpl)(nil)

func newChasmLogEventsCacheItem(
	batch *persistencespb.ChasmLogMessageBatch,
) *chasmLogMessagesCacheItemImpl {
	return &chasmLogMessagesCacheItemImpl{
		batch: batch,
	}
}

func (h *chasmLogMessagesCacheItemImpl) CacheSize() int {
	return proto.Size(h.batch)
}
