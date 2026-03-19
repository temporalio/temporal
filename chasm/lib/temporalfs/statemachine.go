package temporalfs

import (
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
)

var _ chasm.StateMachine[temporalfspb.FilesystemStatus] = (*Filesystem)(nil)

// StateMachineState returns the current filesystem status.
func (f *Filesystem) StateMachineState() temporalfspb.FilesystemStatus {
	if f.FilesystemState == nil {
		return temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED
	}
	return f.Status
}

// SetStateMachineState sets the filesystem status.
func (f *Filesystem) SetStateMachineState(state temporalfspb.FilesystemStatus) {
	if f.FilesystemState == nil {
		f.FilesystemState = &temporalfspb.FilesystemState{}
	}
	f.Status = state
}

// CreateEvent carries the configuration for creating a new filesystem.
type CreateEvent struct {
	Config          *temporalfspb.FilesystemConfig
	OwnerWorkflowID string
}

// TransitionCreate transitions from UNSPECIFIED → RUNNING.
var TransitionCreate = chasm.NewTransition(
	[]temporalfspb.FilesystemStatus{
		temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED,
	},
	temporalfspb.FILESYSTEM_STATUS_RUNNING,
	func(fs *Filesystem, ctx chasm.MutableContext, event CreateEvent) error {
		fs.Config = event.Config
		if fs.Config == nil {
			fs.Config = defaultConfig()
		}
		fs.NextInodeId = 2 // root inode = 1
		fs.NextTxnId = 1
		fs.Stats = &temporalfspb.FSStats{}
		fs.OwnerWorkflowId = event.OwnerWorkflowID

		// Schedule periodic GC task.
		if gcInterval := fs.Config.GetGcInterval().AsDuration(); gcInterval > 0 {
			ctx.AddTask(fs, chasm.TaskAttributes{
				ScheduledTime: ctx.Now(fs).Add(gcInterval),
			}, &temporalfspb.ChunkGCTask{})
		}

		return nil
	},
)

// TransitionArchive transitions from RUNNING → ARCHIVED.
var TransitionArchive = chasm.NewTransition(
	[]temporalfspb.FilesystemStatus{
		temporalfspb.FILESYSTEM_STATUS_RUNNING,
	},
	temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
	func(_ *Filesystem, _ chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionDelete transitions from RUNNING or ARCHIVED → DELETED.
var TransitionDelete = chasm.NewTransition(
	[]temporalfspb.FilesystemStatus{
		temporalfspb.FILESYSTEM_STATUS_RUNNING,
		temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
	},
	temporalfspb.FILESYSTEM_STATUS_DELETED,
	func(_ *Filesystem, _ chasm.MutableContext, _ any) error {
		return nil
	},
)
