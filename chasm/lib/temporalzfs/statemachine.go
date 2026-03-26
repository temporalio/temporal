package temporalzfs

import (
	"go.temporal.io/server/chasm"
	temporalzfspb "go.temporal.io/server/chasm/lib/temporalzfs/gen/temporalzfspb/v1"
)

var _ chasm.StateMachine[temporalzfspb.FilesystemStatus] = (*Filesystem)(nil)

// StateMachineState returns the current filesystem status.
func (f *Filesystem) StateMachineState() temporalzfspb.FilesystemStatus {
	if f.FilesystemState == nil {
		return temporalzfspb.FILESYSTEM_STATUS_UNSPECIFIED
	}
	return f.Status
}

// SetStateMachineState sets the filesystem status.
func (f *Filesystem) SetStateMachineState(state temporalzfspb.FilesystemStatus) {
	if f.FilesystemState == nil {
		f.FilesystemState = &temporalzfspb.FilesystemState{}
	}
	f.Status = state
}

// CreateEvent carries the configuration for creating a new filesystem.
type CreateEvent struct {
	Config           *temporalzfspb.FilesystemConfig
	OwnerWorkflowIDs []string
}

// TransitionCreate transitions from UNSPECIFIED → RUNNING.
var TransitionCreate = chasm.NewTransition(
	[]temporalzfspb.FilesystemStatus{
		temporalzfspb.FILESYSTEM_STATUS_UNSPECIFIED,
	},
	temporalzfspb.FILESYSTEM_STATUS_RUNNING,
	func(fs *Filesystem, ctx chasm.MutableContext, event CreateEvent) error {
		fs.Config = event.Config
		if fs.Config == nil {
			fs.Config = defaultConfig()
		}
		fs.NextInodeId = 2 // root inode = 1
		fs.NextTxnId = 1
		fs.Stats = &temporalzfspb.FSStats{}

		// Build deduplicated owner set.
		owners := make(map[string]struct{})
		for _, id := range event.OwnerWorkflowIDs {
			if id != "" {
				owners[id] = struct{}{}
			}
		}
		for id := range owners {
			fs.OwnerWorkflowIds = append(fs.OwnerWorkflowIds, id)
		}

		// Schedule periodic GC task.
		if gcInterval := fs.Config.GetGcInterval().AsDuration(); gcInterval > 0 {
			ctx.AddTask(fs, chasm.TaskAttributes{
				ScheduledTime: ctx.Now(fs).Add(gcInterval),
			}, &temporalzfspb.ChunkGCTask{})
		}

		// Schedule periodic owner check task if there are owners.
		if len(fs.OwnerWorkflowIds) > 0 {
			interval := fs.Config.GetOwnerCheckInterval().AsDuration()
			if interval <= 0 {
				interval = defaultOwnerCheckInterval
			}
			ctx.AddTask(fs, chasm.TaskAttributes{
				ScheduledTime: ctx.Now(fs).Add(interval),
			}, &temporalzfspb.OwnerCheckTask{})
		}

		return nil
	},
)

// TransitionArchive transitions from RUNNING → ARCHIVED.
var TransitionArchive = chasm.NewTransition(
	[]temporalzfspb.FilesystemStatus{
		temporalzfspb.FILESYSTEM_STATUS_RUNNING,
	},
	temporalzfspb.FILESYSTEM_STATUS_ARCHIVED,
	func(_ *Filesystem, _ chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionDelete transitions from RUNNING or ARCHIVED → DELETED.
// Schedules a DataCleanupTask to delete all FS data from the store.
var TransitionDelete = chasm.NewTransition(
	[]temporalzfspb.FilesystemStatus{
		temporalzfspb.FILESYSTEM_STATUS_RUNNING,
		temporalzfspb.FILESYSTEM_STATUS_ARCHIVED,
	},
	temporalzfspb.FILESYSTEM_STATUS_DELETED,
	func(fs *Filesystem, ctx chasm.MutableContext, _ any) error {
		ctx.AddTask(fs, chasm.TaskAttributes{
			ScheduledTime: chasm.TaskScheduledTimeImmediate,
		}, &temporalzfspb.DataCleanupTask{})
		return nil
	},
)
