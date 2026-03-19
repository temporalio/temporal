package temporalfs

import (
	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// chunkGCTaskExecutor handles periodic garbage collection of orphaned chunks.
type chunkGCTaskExecutor struct {
	config        *Config
	logger        log.Logger
	storeProvider FSStoreProvider
}

func newChunkGCTaskExecutor(config *Config, logger log.Logger, storeProvider FSStoreProvider) *chunkGCTaskExecutor {
	return &chunkGCTaskExecutor{config: config, logger: logger, storeProvider: storeProvider}
}

func (e *chunkGCTaskExecutor) Validate(
	_ chasm.Context,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.ChunkGCTask,
) (bool, error) {
	return fs.Status == temporalfspb.FILESYSTEM_STATUS_RUNNING, nil
}

func (e *chunkGCTaskExecutor) Execute(
	ctx chasm.MutableContext,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	task *temporalfspb.ChunkGCTask,
) error {
	key := ctx.ExecutionKey()

	s, err := e.storeProvider.GetStore(0, key.NamespaceID, key.BusinessID)
	if err != nil {
		e.logger.Error("GC: failed to get store", tag.Error(err))
		return e.rescheduleGC(ctx, fs, task.GetLastProcessedTxnId())
	}

	f, err := tfs.Open(s)
	if err != nil {
		_ = s.Close()
		e.logger.Error("GC: failed to open FS", tag.Error(err))
		return e.rescheduleGC(ctx, fs, task.GetLastProcessedTxnId())
	}

	gcStats := f.RunGC(tfs.GCConfig{
		BatchSize:         100,
		MaxChunksPerRound: 10000,
	})
	if closeErr := f.Close(); closeErr != nil {
		e.logger.Warn("GC: failed to close FS", tag.Error(closeErr))
	}

	e.logger.Info("GC completed",
		tag.NewStringTag("filesystem_id", key.BusinessID),
		tag.NewInt64("tombstones_processed", int64(gcStats.TombstonesProcessed)),
		tag.NewInt64("chunks_deleted", int64(gcStats.ChunksDeleted)),
	)

	// Update CHASM state stats from FS metrics.
	if fs.Stats == nil {
		fs.Stats = &temporalfspb.FSStats{}
	}
	fs.Stats.TransitionCount++
	fs.Stats.ChunkCount -= uint64(gcStats.ChunksDeleted)

	return e.rescheduleGC(ctx, fs, task.GetLastProcessedTxnId())
}

func (e *chunkGCTaskExecutor) rescheduleGC(ctx chasm.MutableContext, fs *Filesystem, lastTxnID uint64) error {
	gcInterval := fs.Config.GetGcInterval().AsDuration()
	if gcInterval > 0 {
		ctx.AddTask(fs, chasm.TaskAttributes{
			ScheduledTime: ctx.Now(fs).Add(gcInterval),
		}, &temporalfspb.ChunkGCTask{
			LastProcessedTxnId: lastTxnID,
		})
	}
	return nil
}

// manifestCompactTaskExecutor handles compaction of the underlying PebbleDB store.
type manifestCompactTaskExecutor struct {
	config        *Config
	logger        log.Logger
	storeProvider FSStoreProvider
}

func newManifestCompactTaskExecutor(config *Config, logger log.Logger, storeProvider FSStoreProvider) *manifestCompactTaskExecutor {
	return &manifestCompactTaskExecutor{config: config, logger: logger, storeProvider: storeProvider}
}

func (e *manifestCompactTaskExecutor) Validate(
	_ chasm.Context,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.ManifestCompactTask,
) (bool, error) {
	return fs.Status == temporalfspb.FILESYSTEM_STATUS_RUNNING, nil
}

func (e *manifestCompactTaskExecutor) Execute(
	_ chasm.MutableContext,
	_ *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.ManifestCompactTask,
) error {
	// Compaction is handled at the PebbleDB level per shard, not per filesystem.
	// This task is a placeholder for future per-FS compaction triggers.
	return nil
}

// quotaCheckTaskExecutor enforces storage quotas.
type quotaCheckTaskExecutor struct {
	config        *Config
	logger        log.Logger
	storeProvider FSStoreProvider
}

func newQuotaCheckTaskExecutor(config *Config, logger log.Logger, storeProvider FSStoreProvider) *quotaCheckTaskExecutor {
	return &quotaCheckTaskExecutor{config: config, logger: logger, storeProvider: storeProvider}
}

func (e *quotaCheckTaskExecutor) Validate(
	_ chasm.Context,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.QuotaCheckTask,
) (bool, error) {
	return fs.Status == temporalfspb.FILESYSTEM_STATUS_RUNNING, nil
}

func (e *quotaCheckTaskExecutor) Execute(
	ctx chasm.MutableContext,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.QuotaCheckTask,
) error {
	key := ctx.ExecutionKey()

	s, err := e.storeProvider.GetStore(0, key.NamespaceID, key.BusinessID)
	if err != nil {
		e.logger.Error("QuotaCheck: failed to get store", tag.Error(err))
		return err
	}

	f, err := tfs.Open(s)
	if err != nil {
		_ = s.Close()
		e.logger.Error("QuotaCheck: failed to open FS", tag.Error(err))
		return err
	}

	m := f.Metrics()
	f.Close()

	if fs.Stats == nil {
		fs.Stats = &temporalfspb.FSStats{}
	}

	// Update stats from FS metrics.
	fs.Stats.TotalSize = uint64(m.BytesWritten.Load())
	fs.Stats.FileCount = uint64(m.FilesCreated.Load() - m.FilesDeleted.Load())
	fs.Stats.DirCount = uint64(m.DirsCreated.Load() - m.DirsDeleted.Load())

	maxSize := fs.Config.GetMaxSize()
	if maxSize > 0 && fs.Stats.TotalSize > maxSize {
		e.logger.Warn("Filesystem exceeds size quota",
			tag.NewStringTag("filesystem_id", key.BusinessID),
			tag.NewInt64("total_size", int64(fs.Stats.TotalSize)),
			tag.NewInt64("max_size", int64(maxSize)),
		)
	}

	return nil
}
