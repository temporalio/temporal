package failpoint

import "context"

// Exports of internals for the black-box tests in package failpoint_test.

// BeginAwaitBlock starts AwaitBlock. It returns a nil waiter if an execution is already at the
// block.
func (f *Failpoint[T]) BeginAwaitBlock() (*blockWaiter, error) { return f.beginAwaitBlock() }

// FinishAwaitBlock waits for the arrival that w waits for.
func (f *Failpoint[T]) FinishAwaitBlock(ctx context.Context, w *blockWaiter) error {
	return f.finishAwaitBlock(ctx, w)
}

// SetAfterRelease sets the hook that an execution calls after its block is released, before
// it updates the failpoint.
func (f *Failpoint[T]) SetAfterRelease(fn func()) { f.afterRelease = fn }

// CurrentlyBlocked reports whether an execution is blocked at the current block
// configuration.
func (f *Failpoint[T]) CurrentlyBlocked() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentlyBlocked()
}
