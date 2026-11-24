//go:build test_dep

package failpoint

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAwaitBlock_Reconfigured covers a wait that is parked on a block configuration when the
// failpoint is disabled or reconfigured. The wait must fail even if an execution reaches the
// replacement block before the waiter runs again.
func TestAwaitBlock_Reconfigured(t *testing.T) {
	t.Parallel()
	key := Key{name: "await-block-reconfigured"}
	tests := []struct {
		name        string
		reconfigure func(*Failpoint[error])
		// arrive reports whether an execution reaches the block after the reconfiguration.
		arrive bool
	}{
		{name: "disabled", reconfigure: (*Failpoint[error]).Disable},
		{name: "re-armed", reconfigure: func(f *Failpoint[error]) { f.Block(Always) }},
		{name: "re-armed and reached", reconfigure: func(f *Failpoint[error]) { f.Block(Always) }, arrive: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			fp := NewFailpoint[error](key)
			fp.Block(Always)

			// The waiter is parked on the first configuration.
			w, err := fp.beginAwaitBlock()
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

			require.ErrorIs(t, fp.finishAwaitBlock(ctx, w), ErrNotBlocking)
		})
	}
}

func TestAwaitBlock_Arrival(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fp := NewFailpoint[error](Key{name: "await-block-arrival"})
	fp.Block(Always)
	w, err := fp.beginAwaitBlock()
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
	require.NoError(t, fp.finishAwaitBlock(ctx, w))
}

// TestAwaitBlock_ReleasedFromEarlierConfiguration covers an execution that is released from a
// Once block by re-arming, and updates the failpoint only after an execution blocked at the new
// configuration: the new execution must still count as blocked.
func TestAwaitBlock_ReleasedFromEarlierConfiguration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fp := NewFailpoint[error](Key{name: "await-block-released-earlier"})
	fp.Block(Once)

	released := make(chan struct{})
	proceed := make(chan struct{})
	var first sync.Once
	fp.afterRelease = func() {
		first.Do(func() {
			close(released)
			<-proceed
		})
	}

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

	fp.Block(Once) // Releases the first execution, which waits in afterRelease.
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
	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.True(t, fp.currentlyBlocked())
}
