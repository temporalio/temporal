package temporalfs

import (
	"context"
	"time"

	tzfs "github.com/temporalio/temporal-zfs/pkg/fs"
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

	f, err := tzfs.Open(s)
	if err != nil {
		_ = s.Close()
		e.logger.Error("GC: failed to open FS", tag.Error(err))
		return e.rescheduleGC(ctx, fs, task.GetLastProcessedTxnId())
	}

	gcStats := f.RunGC(tzfs.GCConfig{
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
	if deleted := uint64(gcStats.ChunksDeleted); deleted >= fs.Stats.ChunkCount {
		fs.Stats.ChunkCount = 0
	} else {
		fs.Stats.ChunkCount -= deleted
	}

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

	f, err := tzfs.Open(s)
	if err != nil {
		_ = s.Close()
		e.logger.Error("QuotaCheck: failed to open FS", tag.Error(err))
		return err
	}

	m := f.Metrics()
	if closeErr := f.Close(); closeErr != nil {
		e.logger.Warn("QuotaCheck: failed to close FS", tag.Error(closeErr))
	}

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

// WorkflowExistenceChecker checks whether a workflow execution still exists.
// Used by ownerCheckTaskExecutor to detect dead owners.
type WorkflowExistenceChecker interface {
	WorkflowExists(ctx context.Context, namespaceID string, workflowID string) (bool, error)
}

// noopWorkflowExistenceChecker is the default OSS implementation.
// SaaS can override this via fx.Decorate with a real implementation
// that queries the history service.
type noopWorkflowExistenceChecker struct{}

func newNoopWorkflowExistenceChecker() *noopWorkflowExistenceChecker {
	return &noopWorkflowExistenceChecker{}
}

func (n *noopWorkflowExistenceChecker) WorkflowExists(_ context.Context, _ string, _ string) (bool, error) {
	// Default: assume all workflows exist. The push path (DetachWorkflow)
	// handles cleanup in OSS. SaaS overrides with a real checker.
	return true, nil
}

// ownerCheckTaskExecutor is the pull-based safety net for GC.
// It periodically checks all owner workflow IDs and removes dead ones.
type ownerCheckTaskExecutor struct {
	logger           log.Logger
	existenceChecker WorkflowExistenceChecker
}

func newOwnerCheckTaskExecutor(logger log.Logger, existenceChecker WorkflowExistenceChecker) *ownerCheckTaskExecutor {
	return &ownerCheckTaskExecutor{logger: logger, existenceChecker: existenceChecker}
}

func (e *ownerCheckTaskExecutor) Validate(
	_ chasm.Context,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.OwnerCheckTask,
) (bool, error) {
	return fs.Status == temporalfspb.FILESYSTEM_STATUS_RUNNING && len(fs.OwnerWorkflowIds) > 0, nil
}

func (e *ownerCheckTaskExecutor) Execute(
	ctx chasm.MutableContext,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	task *temporalfspb.OwnerCheckTask,
) error {
	key := ctx.ExecutionKey()
	notFoundCounts := task.GetNotFoundCounts()

	var surviving []string
	updatedCounts := make(map[string]int32)

	for _, wfID := range fs.OwnerWorkflowIds {
		exists, err := e.existenceChecker.WorkflowExists(context.TODO(), key.NamespaceID, wfID)
		if err != nil {
			// Transient error — keep this owner, reset its counter.
			surviving = append(surviving, wfID)
			e.logger.Warn("OwnerCheck: transient error checking workflow",
				tag.NewStringTag("workflow_id", wfID),
				tag.Error(err),
			)
			continue
		}
		if exists {
			surviving = append(surviving, wfID)
			continue
		}
		// Not found — increment counter.
		count := notFoundCounts[wfID] + 1
		if count < ownerCheckNotFoundThreshold {
			surviving = append(surviving, wfID)
			updatedCounts[wfID] = count
		} else {
			e.logger.Info("OwnerCheck: removing dead owner",
				tag.NewStringTag("filesystem_id", key.BusinessID),
				tag.NewStringTag("workflow_id", wfID),
			)
		}
	}

	fs.OwnerWorkflowIds = surviving

	if len(surviving) == 0 {
		// All owners gone — transition to DELETED.
		e.logger.Info("OwnerCheck: all owners gone, deleting filesystem",
			tag.NewStringTag("filesystem_id", key.BusinessID),
		)
		return TransitionDelete.Apply(fs, ctx, nil)
	}

	// Reschedule next check.
	return e.rescheduleOwnerCheck(ctx, fs, updatedCounts)
}

func (e *ownerCheckTaskExecutor) rescheduleOwnerCheck(
	ctx chasm.MutableContext,
	fs *Filesystem,
	notFoundCounts map[string]int32,
) error {
	interval := fs.Config.GetOwnerCheckInterval().AsDuration()
	if interval <= 0 {
		interval = defaultOwnerCheckInterval
	}
	ctx.AddTask(fs, chasm.TaskAttributes{
		ScheduledTime: ctx.Now(fs).Add(interval),
	}, &temporalfspb.OwnerCheckTask{
		NotFoundCounts: notFoundCounts,
	})
	return nil
}

// dataCleanupTaskExecutor deletes all FS data from the store when a filesystem
// transitions to DELETED. This is a SideEffectTask because it performs
// irreversible external I/O (store deletion).
type dataCleanupTaskExecutor struct {
	logger        log.Logger
	storeProvider FSStoreProvider
}

func newDataCleanupTaskExecutor(logger log.Logger, storeProvider FSStoreProvider) *dataCleanupTaskExecutor {
	return &dataCleanupTaskExecutor{logger: logger, storeProvider: storeProvider}
}

func (e *dataCleanupTaskExecutor) Validate(
	_ chasm.Context,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.DataCleanupTask,
) (bool, error) {
	return fs.Status == temporalfspb.FILESYSTEM_STATUS_DELETED, nil
}

func (e *dataCleanupTaskExecutor) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *temporalfspb.DataCleanupTask,
) error {
	key := ref.ExecutionKey
	e.logger.Info("DataCleanup: deleting FS store data",
		tag.NewStringTag("filesystem_id", key.BusinessID),
		tag.NewInt32("attempt", task.GetAttempt()),
	)

	if err := e.storeProvider.DeleteStore(0, key.NamespaceID, key.BusinessID); err != nil {
		e.logger.Error("DataCleanup: failed to delete store",
			tag.NewStringTag("filesystem_id", key.BusinessID),
			tag.Error(err),
		)
		// Reschedule with exponential backoff.
		nextAttempt := task.GetAttempt() + 1
		backoff := time.Duration(1<<min(nextAttempt, 10)) * time.Second
		if backoff > dataCleanupMaxBackoff {
			backoff = dataCleanupMaxBackoff
		}

		_, _, schedErr := chasm.UpdateComponent(
			ctx,
			ref,
			func(fs *Filesystem, mCtx chasm.MutableContext, _ any) (chasm.NoValue, error) {
				mCtx.AddTask(fs, chasm.TaskAttributes{
					ScheduledTime: mCtx.Now(fs).Add(backoff),
				}, &temporalfspb.DataCleanupTask{
					Attempt: nextAttempt,
				})
				return nil, nil
			},
			nil,
		)
		if schedErr != nil {
			e.logger.Error("DataCleanup: failed to reschedule",
				tag.NewStringTag("filesystem_id", key.BusinessID),
				tag.Error(schedErr),
			)
		}
		return err
	}

	e.logger.Info("DataCleanup: FS store data deleted successfully",
		tag.NewStringTag("filesystem_id", key.BusinessID),
	)
	return nil
}
