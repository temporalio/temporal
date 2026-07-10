package tquserdata

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
)

// UserData is the CHASM root component that holds one task-queue family's user
// data (worker versioning and worker-deployment routing config). It reuses the
// existing persistence proto verbatim; the payload shape does not change.
//
// The component is long-lived: task queue user data is durable configuration,
// not a job, so it never reaches a terminal lifecycle state.
type UserData struct {
	chasm.UnimplementedComponent

	// Data is persisted as a CHASM data field.
	Data *persistencespb.TaskQueueUserData
}

// NewUserData constructs a UserData component. Used as the start function when
// a component is created on first write.
func NewUserData(_ chasm.MutableContext, data *persistencespb.TaskQueueUserData) (*UserData, error) {
	return &UserData{Data: data}, nil
}

// Get returns a clone of the stored user data.
func (u *UserData) Get(_ chasm.Context, _ chasm.NoValue) (*persistencespb.TaskQueueUserData, error) {
	return common.CloneProto(u.Data), nil
}

// Set replaces the stored user data and returns a clone of the new value.
//
// For the POC this is a whole-value replace. Optimistic-version reconciliation
// against the CHASM VersionedTransition is deferred to the production design.
func (u *UserData) Set(_ chasm.MutableContext, data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, error) {
	u.Data = data
	return common.CloneProto(u.Data), nil
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
