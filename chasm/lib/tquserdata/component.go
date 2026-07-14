package tquserdata

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
)

// SetInput is the input to a create-or-update of a task-queue family's user
// data. KnownVersion is the version the caller last observed; the stored
// version becomes KnownVersion+1, matching the optimistic-version contract of
// the legacy task_queue_user_data table.
type SetInput struct {
	Data         *persistencespb.TaskQueueUserData
	KnownVersion int64
}

// UserData is the CHASM root component that holds one task-queue family's user
// data (worker versioning and worker-deployment routing config). It reuses the
// existing persistence proto verbatim; the payload shape does not change.
//
// The component is long-lived: task queue user data is durable configuration,
// not a job, so it never reaches a terminal lifecycle state.
type UserData struct {
	chasm.UnimplementedComponent

	// State holds the versioned user data and is persisted as the component's
	// data. The int64 version stands in for the legacy storage version so the
	// Matching user-data manager's version semantics are preserved.
	State *persistencespb.VersionedTaskQueueUserData
}

// NewUserData constructs a UserData component on first write.
func NewUserData(_ chasm.MutableContext, in SetInput) (*UserData, error) {
	return &UserData{
		State: &persistencespb.VersionedTaskQueueUserData{
			Data:    in.Data,
			Version: in.KnownVersion + 1,
		},
	}, nil
}

// Get returns a clone of the stored versioned user data.
func (u *UserData) Get(_ chasm.Context, _ chasm.NoValue) (*persistencespb.VersionedTaskQueueUserData, error) {
	return common.CloneProto(u.State), nil
}

// Set replaces the stored user data, advancing the version to KnownVersion+1,
// and returns a clone of the new versioned value.
func (u *UserData) Set(_ chasm.MutableContext, in SetInput) (*persistencespb.VersionedTaskQueueUserData, error) {
	u.State = &persistencespb.VersionedTaskQueueUserData{
		Data:    in.Data,
		Version: in.KnownVersion + 1,
	}
	return common.CloneProto(u.State), nil
}

// LifecycleState is always Running: the component holds durable configuration
// and does not complete.
func (u *UserData) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// Terminate satisfies the RootComponent interface. User data has no work to
// wind down, so this is a no-op.
func (u *UserData) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

// ContextMetadata satisfies the RootComponent interface. No metadata is
// propagated for user data.
func (u *UserData) ContextMetadata(chasm.Context) map[string]string {
	return nil
}
