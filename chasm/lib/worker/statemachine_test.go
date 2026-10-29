package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
)

func TestLeaseExpiry(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	group, err := newWorkerGroup(ctx, "group-1", []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-1", "group-1")}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, chasm.LifecycleStateRunning, group.LifecycleState(ctx))
	require.NoError(t, transitionLeaseExpired.Apply(group, ctx, eventLeaseExpired{}))
	require.Equal(t, chasm.LifecycleStateCompleted, group.LifecycleState(ctx))
	require.ErrorIs(t, transitionLeaseExpired.Apply(group, ctx, eventLeaseExpired{}), chasm.ErrInvalidTransition)
}
