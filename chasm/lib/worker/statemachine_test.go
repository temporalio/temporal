package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
)

func TestLeaseExpiry(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	group := newWorkerGroup()
	require.Equal(t, chasm.LifecycleStateRunning, group.LifecycleState(ctx))
	require.NoError(t, transitionLeaseExpired.Apply(group, ctx, eventLeaseExpired{}))
	require.Equal(t, chasm.LifecycleStateCompleted, group.LifecycleState(ctx))
}
