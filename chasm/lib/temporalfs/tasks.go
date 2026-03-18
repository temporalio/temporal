package temporalfs

import (
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
)

// chunkGCTaskExecutor handles periodic garbage collection of orphaned chunks.
type chunkGCTaskExecutor struct {
	config *Config
}

func newChunkGCTaskExecutor(config *Config) *chunkGCTaskExecutor {
	return &chunkGCTaskExecutor{config: config}
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
	// TODO: Implement GC using temporal-fs store when available.
	// For now, reschedule the next GC run.
	if gcInterval := fs.Config.GetGcInterval().AsDuration(); gcInterval > 0 {
		ctx.AddTask(fs, chasm.TaskAttributes{
			ScheduledTime: ctx.Now(fs).Add(gcInterval),
		}, &temporalfspb.ChunkGCTask{
			LastProcessedTxnId: task.GetLastProcessedTxnId(),
		})
	}

	return nil
}

// manifestCompactTaskExecutor handles compaction of manifest diff chains.
type manifestCompactTaskExecutor struct {
	config *Config
}

func newManifestCompactTaskExecutor(config *Config) *manifestCompactTaskExecutor {
	return &manifestCompactTaskExecutor{config: config}
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
	// TODO: Implement manifest compaction using temporal-fs store when available.
	return nil
}

// quotaCheckTaskExecutor enforces storage quotas.
type quotaCheckTaskExecutor struct {
	config *Config
}

func newQuotaCheckTaskExecutor(config *Config) *quotaCheckTaskExecutor {
	return &quotaCheckTaskExecutor{config: config}
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
	_ chasm.MutableContext,
	fs *Filesystem,
	_ chasm.TaskAttributes,
	_ *temporalfspb.QuotaCheckTask,
) error {
	// TODO: Implement quota enforcement using temporal-fs store when available.
	// Check fs.Stats.TotalSize against fs.Config.MaxSize.
	// Check fs.Stats.InodeCount against fs.Config.MaxFiles.
	return nil
}
