package fc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/matching/simplelimiter"
)

func TestCheckWithoutCallbackRollsBackWithoutRegisteringWaiters(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource().Update(time.Unix(1, 0))
	ts.UseAsyncTimers(true)
	r := NewReadiness(ts)
	t.Cleanup(r.Stop)
	const testNsID = namespace.ID("nsid")
	task := &testTask{limiters: []Limiter{
		{Type: enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT, Key: "first", Source: LimiterSourceConfig, Config: new(simplelimiter.MakeParams(1, 0))},
		{Type: enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT, Key: "second", Source: LimiterSourceConfig, Config: new(simplelimiter.MakeParams(0, 0))},
	}}
	tx := r.NewTx(testNsID, task, nil)
	require.ErrorIs(t, tx.Check(), ErrLocalLimiterBlocked)
	require.True(t, tx.closed)
	rs := r.getShard(testNsID)
	func() {
		rs.lock.Lock()
		defer rs.lock.Unlock()
		require.Empty(t, rs.fwd)
		require.Empty(t, rs.rev)
	}()

	// The first limiter's token must have been returned when the second blocked.
	task.limiters = task.limiters[:1]
	tx = r.NewTx(testNsID, task, nil)
	require.NoError(t, tx.Check())
	tx.Close()
}
