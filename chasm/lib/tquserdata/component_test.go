package tquserdata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testvars"
)

// newTestContext builds a minimal CHASM tree and mutable context for exercising
// the component in isolation, mirroring the pattern used by other CHASM libs.
func newTestContext(t *testing.T) chasm.MutableContext {
	t.Helper()

	logger := log.NewTestLogger()

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(Library))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	tv := testvars.New(t)
	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      tv.Any().WorkflowKey,
		HandleIsWorkflow:          func() bool { return false },
		HandleGetNamespaceEntry:   tv.Namespace,
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
		},
	}

	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	return chasm.NewMutableContext(context.Background(), node)
}

func TestUserData_SetAndGet(t *testing.T) {
	ctx := newTestContext(t)

	data := &persistencespb.TaskQueueUserData{
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			1: {},
		},
	}

	comp, err := NewUserData(ctx, SetInput{Data: data, KnownVersion: 0})
	require.NoError(t, err)

	got, err := comp.Get(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Contains(t, got.GetData().GetPerType(), int32(1))
	require.Equal(t, int64(1), got.GetVersion())

	// Get returns a clone: mutating it must not affect stored state.
	got.Data.PerType = nil
	got2, err := comp.Get(ctx, nil)
	require.NoError(t, err)
	require.Contains(t, got2.GetData().GetPerType(), int32(1))
}

func TestUserData_Set_Replaces(t *testing.T) {
	ctx := newTestContext(t)

	comp, err := NewUserData(ctx, SetInput{Data: &persistencespb.TaskQueueUserData{}, KnownVersion: 0})
	require.NoError(t, err)

	updated := &persistencespb.TaskQueueUserData{
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{2: {}},
	}
	out, err := comp.Set(ctx, SetInput{Data: updated, KnownVersion: 1})
	require.NoError(t, err)
	require.Contains(t, out.GetData().GetPerType(), int32(2))
	require.Equal(t, int64(2), out.GetVersion())

	got, err := comp.Get(ctx, nil)
	require.NoError(t, err)
	require.Contains(t, got.GetData().GetPerType(), int32(2))
	require.Equal(t, int64(2), got.GetVersion())
}

func TestUserData_LifecycleAlwaysRunning(t *testing.T) {
	ctx := newTestContext(t)
	comp, err := NewUserData(ctx, SetInput{Data: &persistencespb.TaskQueueUserData{}})
	require.NoError(t, err)
	require.Equal(t, chasm.LifecycleStateRunning, comp.LifecycleState(ctx))
}
