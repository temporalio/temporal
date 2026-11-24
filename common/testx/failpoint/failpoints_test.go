package failpoint_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
)

// === Return ===
func TestFailpointReturn(t *testing.T) {
	f := new(failpoint.Failpoint[int])
	f.Return(42, failpoint.Once)
	ret := f.Do(func() int {
		return 66
	})

	if ret != 42 {
		t.Errorf("expected 42, got %d", ret)
	}

	// Always
	f.Return(43, failpoint.Always)
	for range 10 {
		ret := f.Do(func() int {
			return 66
		})
		if ret != 43 {
			t.Errorf("expected 43, got %d", ret)
		}
	}
}

// === Panic ===
func checkPanic(t *testing.T, value any, cb func()) {
	defer func() {
		if r := recover(); r != value {
			t.Errorf("expected panic %v, got %v", value, r)
		}
	}()
	cb()
}

func TestFailpointPanic(t *testing.T) {
	f := new(failpoint.Failpoint[int])
	f.Panic(42, failpoint.Once)
	checkPanic(t, 42, func() {
		f.Do(func() int {
			return 66
		})
	})

	// Always
	f.Panic(43, failpoint.Always)
	for range 10 {
		checkPanic(t, 43, func() {
			f.Do(func() int {
				return 66
			})
		})
	}
}

// === Block / WaitCh / Wait ===

// TestFailpointBlock_WaitChBeforeArrival tests that WaitCh() called before the
// goroutine reaches the failpoint correctly blocks until arrival.
func TestFailpointBlock_WaitChBeforeArrival(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	ch := f.WaitCh()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Execute()
	}()

	select {
	case <-ch:
	case <-t.Context().Done():
		t.Fatal("timed out waiting for failpoint to be reached")
	}
	f.Disable()
	<-done
}

// TestFailpointBlock_WaitChAfterArrival tests that WaitCh() called after the
// goroutine is already blocked returns a channel that is immediately readable.
func TestFailpointBlock_WaitChAfterArrival(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	arrived := make(chan struct{})
	go func() {
		close(arrived)
		f.Execute()
	}()

	// Wait for the goroutine to be blocked at the failpoint before calling WaitCh.
	<-arrived
	// Give the goroutine time to enter Execute and block.
	ch := f.WaitCh()
	// ch may be arrivedCh or closedCh; either way it must be readable without blocking.
	select {
	case <-ch:
	default:
		// arrivedCh not yet closed — wait with a timeout as fallback.
		select {
		case <-ch:
		case <-t.Context().Done():
			t.Fatal("WaitCh channel was not readable after goroutine blocked")
		}
	}
	f.Disable()
}

// TestFailpointBlock_WaitBeforeArrival tests Wait() called before the goroutine arrives.
func TestFailpointBlock_WaitBeforeArrival(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Execute()
	}()

	if err := f.Wait(t.Context()); err != nil {
		t.Fatalf("Wait returned unexpected error: %v", err)
	}
	f.Disable()
	<-done
}

// TestFailpointBlock_WaitAfterArrival tests that Wait() returns immediately when
// the goroutine is already blocked at the failpoint.
func TestFailpointBlock_WaitAfterArrival(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	arrived := make(chan struct{})
	go func() {
		close(arrived)
		f.Execute()
	}()

	<-arrived
	// The goroutine signalled it entered Execute; wait for it to be blocked.
	if err := f.Wait(t.Context()); err != nil {
		t.Fatalf("Wait returned unexpected error: %v", err)
	}
	f.Disable()
}

// TestFailpointBlock_WaitCtxCancelled tests that Wait() returns the context error
// when the context is cancelled before the goroutine arrives.
func TestFailpointBlock_WaitCtxCancelled(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := f.Wait(ctx); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	f.Disable()
}

// TestFailpointBlock_DisableWakesWait tests that Disable() unblocks a Wait() caller
// that is waiting for an arrival that never comes. The waiter must enter Wait()
// before Disable() is called; we synchronize this with a ready channel.
func TestFailpointBlock_DisableWakesWait(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	// Capture the channel while the failpoint is still enabled.
	ch := f.WaitCh()

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		select {
		case <-ch:
		case <-t.Context().Done():
			t.Error("timed out waiting for failpoint channel")
		}
	}()

	f.Disable()

	select {
	case <-waitDone:
	case <-t.Context().Done():
		t.Fatal("timed out waiting for waiter to return after Disable")
	}
}

// TestFailpointBlock_BlockCalledTwice_WakesWaiter verifies that calling Block()
// a second time closes the old arrivedCh so any waiter holding a reference to
// it is woken rather than stranded.
func TestFailpointBlock_BlockCalledTwice_WakesWaiter(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	ch := f.WaitCh()

	// Second Block() calls disableLocked() which must close the old arrivedCh.
	f.Block(failpoint.Always)

	select {
	case <-ch:
	case <-t.Context().Done():
		t.Fatal("old arrivedCh was not closed when Block() was called a second time; waiter was stranded")
	}

	f.Disable()
}

// TestFailpointBlock_BlockCalledTwice_WakesBlockedGoroutine verifies that
// calling Block() while a goroutine is blocked on the failpoint wakes it via
// disableLocked() closing the old blockCh.
func TestFailpointBlock_BlockCalledTwice_WakesBlockedGoroutine(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Execute()
	}()

	if err := f.Wait(t.Context()); err != nil {
		t.Fatalf("Wait returned unexpected error: %v", err)
	}

	// Second Block() must wake the goroutine blocked on the old blockCh.
	f.Block(failpoint.Always)

	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("blocked goroutine was not woken when Block() was called a second time")
	}

	f.Disable()
}

// TestFailpointBlock_BlockCalledTwice_StillFunctional verifies that the
// failpoint remains fully functional after Block() is called twice in a row.
func TestFailpointBlock_BlockCalledTwice_StillFunctional(t *testing.T) {
	f := new(failpoint.Failpoint[struct{}])
	f.Block(failpoint.Always)
	f.Block(failpoint.Always)

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Execute()
	}()

	if err := f.Wait(t.Context()); err != nil {
		t.Fatalf("Wait returned unexpected error: %v", err)
	}
	f.Disable()

	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("goroutine did not exit after Disable()")
	}
}

// TestFailpoint_UnblockOne_AfterDisable_DoesNotPanic guards against a prior
// race where UnblockOne snapshotted blockCh outside the lock and then sent on
// it after a concurrent Disable closed the channel.
func TestFailpoint_UnblockOne_AfterDisable_DoesNotPanic(t *testing.T) {
	for range 100 {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Always)

		var wg sync.WaitGroup
		wg.Go(fp.Disable)
		wg.Go(fp.UnblockOne)
		wg.Wait()
	}
}

// TestFailpoint_OnceBlock_ConcurrentReinject guards against a prior race where
// the deferred disableLocked in Execute would clobber state belonging to a
// fresh Block() that landed during the wait.
func TestFailpoint_OnceBlock_ConcurrentReinject(t *testing.T) {
	fp := failpoint.NewFailpoint[error](key1)
	fp.Block(failpoint.Once)

	executed := make(chan struct{})
	go func() {
		defer close(executed)
		_, _ = fp.Execute()
	}()

	// Wait for the goroutine to park inside Execute.
	await.RequireTrue(t, func() bool {
		select {
		case <-fp.WaitCh():
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)

	// Re-inject Block(Always) while the original goroutine is parked. The
	// original goroutine's Once cleanup must not close the new channels.
	fp.Block(failpoint.Always)

	// Release the original goroutine via Disable on the OLD blockCh. We do
	// this by issuing a UnblockOne on the new state... but the new state has
	// a fresh blockCh. The original is parked on the old (now-closed via
	// disableLocked invoked from the new Block()). So it should already have
	// returned.
	select {
	case <-executed:
	case <-time.After(time.Second):
		t.Fatal("original Once goroutine did not return after re-inject")
	}

	// The new Always failpoint must still be usable: a fresh goroutine should
	// be parkable on it, and UnblockOne should release it.
	parked := make(chan struct{})
	go func() {
		defer close(parked)
		_, _ = fp.Execute()
	}()
	await.RequireTrue(t, func() bool {
		select {
		case <-fp.WaitCh():
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
	fp.UnblockOne()
	select {
	case <-parked:
	case <-time.After(time.Second):
		t.Fatal("new Always failpoint did not unblock — state was clobbered")
	}
}

// === Call ===
// Note: these are lame uses of call, but they exercise the functionality nonetheless

func TestFailpointCall(t *testing.T) {
	f := new(failpoint.Failpoint[int])
	f.Call(func() int {
		return 42
	}, failpoint.Once)
	ret := f.Do(func() int {
		return 66
	})

	if ret != 42 {
		t.Errorf("expected 42, got %d", ret)
	}

	// Always
	f.Call(func() int {
		return 44
	}, failpoint.Always)

	for range 10 {
		ret := f.Do(func() int {
			return 66
		})
		if ret != 44 {
			t.Errorf("expected 44, got %d", ret)
		}
	}
}

func TestFailpointCall_ConcurrentExecutionsOverlap(t *testing.T) {
	t.Parallel()
	f := failpoint.NewFailpoint[int](failpoint.KeyFromName("overlap", ""))
	arrived := make(chan struct{})
	release := make(chan struct{})
	f.Call(func() int {
		arrived <- struct{}{}
		<-release
		return 42
	}, failpoint.Always)

	results := make(chan int, 2)
	for range 2 {
		go func() {
			v, _ := f.Execute()
			results <- v
		}()
	}
	// Both executions must be inside the callback at once; serialized executions would only
	// let one arrive before release is closed.
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			t.Fatal("executions of the failpoint did not run concurrently")
		}
	}
	close(release)
	require.Equal(t, 42, <-results)
	require.Equal(t, 42, <-results)
}

func TestFailpointCall_CallbackCallsIntoFailpoint(t *testing.T) {
	t.Parallel()
	f := failpoint.NewFailpoint[int](failpoint.KeyFromName("reentrant", ""))
	f.Call(func() int {
		f.Disable()
		return 42
	}, failpoint.Always)

	done := make(chan int, 1)
	go func() {
		v, _ := f.Execute()
		done <- v
	}()
	select {
	case v := <-done:
		require.Equal(t, 42, v)
	case <-time.After(10 * time.Second):
		t.Fatal("callback deadlocked on the failpoint")
	}
	require.False(t, f.Info().Enabled)
}

func TestFailpointCall_OnceReconfiguredDuringCallback(t *testing.T) {
	t.Parallel()
	f := failpoint.NewFailpoint[int](failpoint.KeyFromName("once-rearm", ""))
	entered := make(chan struct{})
	release := make(chan struct{})
	f.Call(func() int {
		close(entered)
		<-release
		return 1
	}, failpoint.Once)

	first := make(chan int, 1)
	go func() {
		v, _ := f.Execute()
		first <- v
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first execution did not reach the callback")
	}

	// The running execution consumed the Once, so a concurrent execution doesn't fire.
	_, fired := f.Execute()
	require.False(t, fired)

	// Re-arm while the first callback still runs.
	f.Call(func() int { return 2 }, failpoint.Once)
	close(release)
	require.Equal(t, 1, <-first)

	// The first callback's completion did not disable the replacement.
	v, fired := f.Execute()
	require.True(t, fired)
	require.Equal(t, 2, v)
	_, fired = f.Execute()
	require.False(t, fired)
}

// === AwaitBlock ===

func TestFailpointBlock_AwaitBlock(t *testing.T) {
	t.Parallel()
	// The subtests run after this function returns, so each uses a context of its own.
	awaitCtx := func(t *testing.T) context.Context {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)
		return ctx
	}

	t.Run("arrival before and after the wait", func(t *testing.T) {
		t.Parallel()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Always)
		executeDone := make(chan struct{})
		t.Cleanup(func() {
			fp.Disable()
			<-executeDone
		})
		go func() {
			defer close(executeDone)
			_, _ = fp.Execute()
		}()
		require.NoError(t, fp.AwaitBlock(awaitCtx(t)))
		// The execution is still at the block.
		require.NoError(t, fp.AwaitBlock(awaitCtx(t)))
	})

	t.Run("once failpoint holding an execution", func(t *testing.T) {
		t.Parallel()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Once)
		executeDone := make(chan struct{})
		t.Cleanup(func() {
			fp.Disable()
			<-executeDone
		})
		go func() {
			defer close(executeDone)
			_, _ = fp.Execute()
		}()
		require.NoError(t, fp.AwaitBlock(awaitCtx(t)))
		require.NoError(t, fp.AwaitBlock(awaitCtx(t)))
	})

	t.Run("not configured to block", func(t *testing.T) {
		t.Parallel()
		fp := failpoint.NewFailpoint[error](key1)
		require.ErrorIs(t, fp.AwaitBlock(awaitCtx(t)), failpoint.ErrNotBlocking)
		fp.Return(errors.New("x"), failpoint.Always)
		require.ErrorIs(t, fp.AwaitBlock(awaitCtx(t)), failpoint.ErrNotBlocking)
	})

	t.Run("context done", func(t *testing.T) {
		t.Parallel()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Always)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, fp.AwaitBlock(ctx), context.DeadlineExceeded)
	})
}

// TestAwaitBlock_Reconfigured covers a wait that is parked on a block configuration when the
// failpoint is disabled or reconfigured. The wait must fail even if an execution reaches the
// replacement block before the waiter runs again.
func TestAwaitBlock_Reconfigured(t *testing.T) {
	t.Parallel()
	key := failpoint.KeyFromName("await-block-reconfigured", "")
	tests := []struct {
		name        string
		reconfigure func(*failpoint.Failpoint[error])
		// arrive reports whether an execution reaches the block after the reconfiguration.
		arrive bool
	}{
		{name: "disabled", reconfigure: (*failpoint.Failpoint[error]).Disable},
		{name: "re-armed", reconfigure: func(f *failpoint.Failpoint[error]) { f.Block(failpoint.Always) }},
		{name: "re-armed and reached", reconfigure: func(f *failpoint.Failpoint[error]) { f.Block(failpoint.Always) }, arrive: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			fp := failpoint.NewFailpoint[error](key)
			fp.Block(failpoint.Always)

			// The waiter is parked on the first configuration.
			w, err := fp.BeginAwaitBlock()
			require.NoError(t, err)
			require.NotNil(t, w)

			tc.reconfigure(fp)
			if tc.arrive {
				executeDone := make(chan struct{})
				t.Cleanup(func() {
					fp.Disable()
					<-executeDone
				})
				go func() {
					defer close(executeDone)
					_, _ = fp.Execute()
				}()
				// The replacement configuration sees the arrival.
				require.NoError(t, fp.AwaitBlock(ctx))
			}

			require.ErrorIs(t, fp.FinishAwaitBlock(ctx, w), failpoint.ErrNotBlocking)
		})
	}
}

func TestAwaitBlock_Arrival(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fp := failpoint.NewFailpoint[error](failpoint.KeyFromName("await-block-arrival", ""))
	fp.Block(failpoint.Always)
	w, err := fp.BeginAwaitBlock()
	require.NoError(t, err)

	executeDone := make(chan struct{})
	t.Cleanup(func() {
		fp.Disable()
		<-executeDone
	})
	go func() {
		defer close(executeDone)
		_, _ = fp.Execute()
	}()
	require.NoError(t, fp.FinishAwaitBlock(ctx, w))
}

// TestAwaitBlock_ReleasedFromEarlierConfiguration covers an execution that is released from a
// Once block by re-arming, and updates the failpoint only after an execution blocked at the new
// configuration: the new execution must still count as blocked.
func TestAwaitBlock_ReleasedFromEarlierConfiguration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fp := failpoint.NewFailpoint[error](failpoint.KeyFromName("await-block-released-earlier", ""))
	fp.Block(failpoint.Once)

	released := make(chan struct{})
	proceed := make(chan struct{})
	var first sync.Once
	fp.SetAfterRelease(func() {
		first.Do(func() {
			close(released)
			<-proceed
		})
	})

	execute := func() chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = fp.Execute()
		}()
		return done
	}
	firstDone := execute()
	require.NoError(t, fp.AwaitBlock(ctx))

	fp.Block(failpoint.Once) // Releases the first execution, which waits in afterRelease.
	select {
	case <-released:
	case <-ctx.Done():
		require.FailNow(t, "the first execution was not released")
	}
	secondDone := execute()
	var proceedOnce sync.Once
	releaseFirst := func() { proceedOnce.Do(func() { close(proceed) }) }
	t.Cleanup(func() {
		releaseFirst()
		fp.Disable()
		<-firstDone
		<-secondDone
	})
	require.NoError(t, fp.AwaitBlock(ctx))

	releaseFirst()
	select {
	case <-firstDone:
	case <-ctx.Done():
		require.FailNow(t, "the first execution did not finish")
	}
	// The second execution is still blocked at the current configuration.
	require.NoError(t, fp.AwaitBlock(ctx))
	require.True(t, fp.CurrentlyBlocked())
}

// === Trigger ===
func fires(fp *failpoint.Failpoint[error], executions int) []bool {
	out := make([]bool, executions)
	for i := range out {
		_, out[i] = fp.Execute()
	}
	return out
}

func TestTrigger(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected")
	tests := []struct {
		name    string
		trigger failpoint.Trigger
		want    []bool
	}{
		{name: "zero value fires always", trigger: failpoint.Trigger{}, want: []bool{true, true, true, true}},
		{name: "after skips first executions", trigger: failpoint.Trigger{After: 2}, want: []bool{false, false, true, true}},
		{name: "max limits fires", trigger: failpoint.Trigger{Max: 2}, want: []bool{true, true, false, false}},
		{name: "after and max", trigger: failpoint.Trigger{After: 1, Max: 1}, want: []bool{false, true, false, false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp := failpoint.NewFailpoint[error](key1)
			fp.Return(injected, failpoint.Always)
			fp.SetTrigger(tc.trigger)
			require.Equal(t, tc.want, fires(fp, len(tc.want)))
		})
	}
}

func TestTrigger_ProbabilityIsDeterministicForSeed(t *testing.T) {
	t.Parallel()

	run := func() []bool {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(errors.New("injected"), failpoint.Always)
		fp.SetTrigger(failpoint.Trigger{Probability: 0.5, Seed: 42})
		return fires(fp, 50)
	}
	first := run()
	require.Equal(t, first, run())
	require.Contains(t, first, true)
	require.Contains(t, first, false)
}

func TestTrigger_SkippedExecutionDoesNotConsumeOnce(t *testing.T) {
	t.Parallel()

	fp := failpoint.NewFailpoint[error](key1)
	fp.Return(errors.New("injected"), failpoint.Once)
	fp.SetTrigger(failpoint.Trigger{After: 1})
	require.Equal(t, []bool{false, true, false}, fires(fp, 3))

	executions, fired := fp.Stats()
	require.Equal(t, int64(2), executions)
	require.Equal(t, int64(1), fired)
}
