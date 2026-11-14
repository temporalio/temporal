//go:build test_dep

package failpoint_test

import (
	"context"
	"testing"

	"go.temporal.io/server/common/testing/failpoint"
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

func TestFailpoint2Return(t *testing.T) {
	f := new(failpoint.Failpoint2[int, int])
	f.Return(42, 43, failpoint.Once)
	ret1, ret2 := f.Do(func() (int, int) {
		return 66, 67
	})

	if ret1 != 42 || ret2 != 43 {
		t.Errorf("expected (42, 43), got (%d, %d)", ret1, ret2)
	}

	// Always
	f.Return(44, 45, failpoint.Always)
	for range 10 {
		ret1, ret2 := f.Do(func() (int, int) {
			return 66, 67
		})
		if ret1 != 44 || ret2 != 45 {
			t.Errorf("expected (44, 45), got (%d, %d)", ret1, ret2)
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

func TestFailpoint2Panic(t *testing.T) {
	f := new(failpoint.Failpoint2[int, int])
	f.Panic(42, failpoint.Once)
	checkPanic(t, 42, func() {
		f.Do(func() (int, int) {
			return 66, 67
		})
	})

	// Always
	f.Panic(44, failpoint.Always)
	for range 10 {
		checkPanic(t, 44, func() {
			f.Do(func() (int, int) {
				return 66, 67
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

// TestFailpoint2Block_WaitBeforeArrival mirrors the Failpoint[T] Block test for Failpoint2.
func TestFailpoint2Block_WaitBeforeArrival(t *testing.T) {
	f := new(failpoint.Failpoint2[struct{}, struct{}])
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

// TestFailpoint2Block_WaitChBeforeArrival mirrors the Failpoint[T] WaitCh test for Failpoint2.
func TestFailpoint2Block_WaitChBeforeArrival(t *testing.T) {
	f := new(failpoint.Failpoint2[struct{}, struct{}])
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

// TestFailpoint2Block_BlockCalledTwice_WakesWaiter mirrors the Failpoint[T]
// double-Block waiter test for Failpoint2.
func TestFailpoint2Block_BlockCalledTwice_WakesWaiter(t *testing.T) {
	f := new(failpoint.Failpoint2[struct{}, struct{}])
	f.Block(failpoint.Always)

	ch := f.WaitCh()

	f.Block(failpoint.Always)

	select {
	case <-ch:
	case <-t.Context().Done():
		t.Fatal("old arrivedCh was not closed when Block() was called a second time; waiter was stranded")
	}

	f.Disable()
}

// === Call ===
// Note: these are lame uses of call, but they exercise the functionality nonetheless

func TestFailpointCall(t *testing.T) {
	f := new(failpoint.Failpoint2[int, int])
	f.Call(func() (int, int) {
		return 42, 43
	}, failpoint.Once)
	ret1, ret2 := f.Do(func() (int, int) {
		return 66, 67
	})

	if ret1 != 42 || ret2 != 43 {
		t.Errorf("expected (42, 43), got (%d, %d)", ret1, ret2)
	}

	// Always
	f.Call(func() (int, int) {
		return 44, 45
	}, failpoint.Always)

	for range 10 {
		ret1, ret2 := f.Do(func() (int, int) {
			return 66, 67
		})
		if ret1 != 44 || ret2 != 45 {
			t.Errorf("expected (44, 45), got (%d, %d)", ret1, ret2)
		}
	}
}

func TestFailpointCall2(t *testing.T) {
	f := new(failpoint.Failpoint2[int, int])
	f.Call(func() (int, int) {
		return 42, 43
	}, failpoint.Once)
	ret1, ret2 := f.Do(func() (int, int) {
		return 66, 67
	})

	if ret1 != 42 || ret2 != 43 {
		t.Errorf("expected (42, 43), got (%d, %d)", ret1, ret2)
	}

	// Always
	f.Call(func() (int, int) {
		return 44, 45
	}, failpoint.Always)
	for range 10 {
		ret1, ret2 := f.Do(func() (int, int) {
			return 66, 67
		})
		if ret1 != 44 || ret2 != 45 {
			t.Errorf("expected (44, 45), got (%d, %d)", ret1, ret2)
		}
	}
}
