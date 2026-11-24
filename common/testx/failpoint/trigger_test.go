//go:build test_dep

package failpoint_test

import (
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
)

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

func TestExecute_OtherTypes(t *testing.T) {
	t.Parallel()

	t.Run("return value of another type does not fire", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(errors.New("injected"), failpoint.Always)
		registry.Set(key1, fp)
		var events []failpoint.Event
		registry.Subscribe(func(e failpoint.Event) { events = append(events, e) })

		got, fired := failpoint.Execute[int](registrySource{registry: registry}, key1)
		require.False(t, fired)
		require.Zero(t, got)
		require.Equal(t, []failpoint.Event{
			{Key: key1, Action: failpoint.Return, Fired: true, Execution: 1},
			{Key: key1, Action: failpoint.Return, Fired: true, TypeMismatch: true},
		}, events)
	})

	t.Run("nil return value fires with zero value", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(nil, failpoint.Always)
		registry.Set(key1, fp)

		_, fired := failpoint.Execute[struct{}](registrySource{registry: registry}, key1)
		require.True(t, fired)
	})

	t.Run("block holds call sites of another type", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Always)
		registry.Set(key1, fp)

		done := make(chan bool, 1)
		go func() {
			_, fired := failpoint.Execute[struct{}](registrySource{registry: registry}, key1)
			done <- fired
		}()
		require.NoError(t, fp.Wait(t.Context()))
		fp.Disable()
		require.False(t, <-done)
	})
}

func TestRegistry_Subscribe(t *testing.T) {
	t.Parallel()

	t.Run("reports fired and skipped executions", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(errors.New("injected"), failpoint.Always)
		fp.SetTrigger(failpoint.Trigger{After: 1})
		registry.Set(key1, fp)
		var events []failpoint.Event
		unsubscribe := registry.Subscribe(func(e failpoint.Event) { events = append(events, e) })

		src := registrySource{registry: registry}
		_, _ = failpoint.Execute[error](src, key1)
		_, _ = failpoint.Execute[error](src, key1)
		unsubscribe()
		_, _ = failpoint.Execute[error](src, key1)

		require.Equal(t, []failpoint.Event{
			{Key: key1, Action: failpoint.Return, Fired: false, Execution: 1},
			{Key: key1, Action: failpoint.Return, Fired: true, Execution: 2},
		}, events)
	})

	t.Run("reports a block before it waits", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Always)
		registry.Set(key1, fp)
		var mu sync.Mutex
		var events []failpoint.Event
		registry.Subscribe(func(e failpoint.Event) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, e)
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = failpoint.Execute[error](registrySource{registry: registry}, key1)
		}()
		await.RequireTrue(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(events) == 1
		}, 5*time.Second, 10*time.Millisecond)
		fp.Disable()
		<-done
	})
}

func TestErrorCatalog(t *testing.T) {
	t.Parallel()

	injected := errors.New("base")
	// The catalog is global, so each run registers a name of its own.
	name := "TestErrorCatalog_" + rand.Text()
	failpoint.RegisterError(name, func(msg string) error {
		return errors.Join(injected, errors.New(msg))
	})

	newError, ok := failpoint.LookupError(name)
	require.True(t, ok)
	err := newError("detail")
	require.ErrorIs(t, err, injected)
	require.ErrorContains(t, err, "detail")

	_, ok = failpoint.LookupError(name + "_Unknown")
	require.False(t, ok)

	require.Contains(t, failpoint.ErrorNames(), name)
	require.Panics(t, func() {
		failpoint.RegisterError(name, func(string) error { return nil })
	})
}
