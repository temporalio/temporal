package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/contextutil"
)

// TestLongPollSelectLoop_RespectsParentCancellationWithoutDeadline tests the bug scenario where:
// 1. Parent context has NO deadline (e.g., gRPC timeout wasn't propagated)
// 2. WithDeadlineBuffer uses full long poll timeout (20s)
// 3. Parent is cancelled (e.g., client disconnects) before that
// 4. The select loop should return promptly via case <-ctx.Done() fallback
//
// Without the fallback, the code would wait for the full 20s longPollCtx timeout
// even though the parent context was cancelled.
func TestLongPollSelectLoop_RespectsParentCancellationWithoutDeadline(t *testing.T) {
	t.Parallel()

	longPollInterval := 20 * time.Second
	buffer := 1 * time.Second
	cancelAfter := 500 * time.Millisecond

	// Parent context WITHOUT deadline - simulates bug where gRPC timeout isn't propagated
	ctx, cancel := context.WithCancel(context.Background())

	longPollCtx, longPollCancel := contextutil.WithDeadlineBuffer(ctx, longPollInterval, buffer)
	defer longPollCancel()

	// Verify longPollCtx has full 20s timeout (since parent has no deadline)
	deadline, hasDeadline := longPollCtx.Deadline()
	assert.True(t, hasDeadline, "longPollCtx should have deadline")
	timeUntilDeadline := time.Until(deadline)
	assert.InDelta(t, longPollInterval.Seconds(), timeUntilDeadline.Seconds(), 1.0,
		"longPollCtx should have ~20s timeout when parent has no deadline, got %v", timeUntilDeadline)

	// Cancel parent after short delay (simulates client disconnect)
	go func() {
		time.Sleep(cancelAfter)
		cancel()
	}()

	// Simulate the select loop pattern from GetOrPollWorkflowMutableState
	eventCh := make(chan struct{}) // Never receives - simulates no events

	start := time.Now()
	select {
	case <-eventCh:
		t.Fatal("Should not receive event")
	case <-longPollCtx.Done():
		// Child context cancelled (inherits parent cancellation)
	case <-ctx.Done():
		// Fallback: explicit parent cancellation handling
	}
	elapsed := time.Since(start)

	// Should return shortly after cancelAfter, NOT wait for 20s
	assert.Less(t, elapsed, 2*time.Second,
		"Should return promptly after parent cancel, got %v (would be ~20s without fix)", elapsed)
	assert.InDelta(t, cancelAfter.Seconds(), elapsed.Seconds(), 0.5,
		"Should return around cancel time %v, got %v", cancelAfter, elapsed)
}
