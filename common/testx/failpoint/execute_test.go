package failpoint_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

func TestExecute(t *testing.T) {
	t.Parallel()

	fromUnscoped := errors.New("unscoped")
	fromScoped := errors.New("scoped")
	newSource := func(keys map[failpoint.Key]error) failpoint.Source {
		registry := failpoint.NewRegistry()
		for key, err := range keys {
			fp := failpoint.NewFailpoint[error](key)
			fp.Return(err, failpoint.Always)
			registry.Set(key, fp)
		}
		return registrySource{registry: registry}
	}

	tests := []struct {
		name       string
		registered map[failpoint.Key]error
		key        failpoint.Key
		want       error
		wantFired  bool
	}{
		{name: "not registered", registered: nil, key: key1, want: nil, wantFired: false},
		{name: "unscoped", registered: map[failpoint.Key]error{key1: fromUnscoped}, key: key1, want: fromUnscoped, wantFired: true},
		{
			name:       "scoped",
			registered: map[failpoint.Key]error{key1.ForScope("ns-1"): fromScoped},
			key:        key1.ForScope("ns-1"),
			want:       fromScoped,
			wantFired:  true,
		},
		{
			name:       "scoped key falls back to unscoped",
			registered: map[failpoint.Key]error{key1: fromUnscoped},
			key:        key1.ForScope("ns-1"),
			want:       fromUnscoped,
			wantFired:  true,
		},
		{
			name:       "scoped takes precedence over unscoped",
			registered: map[failpoint.Key]error{key1: fromUnscoped, key1.ForScope("ns-1"): fromScoped},
			key:        key1.ForScope("ns-1"),
			want:       fromScoped,
			wantFired:  true,
		},
		{
			name:       "other scope does not fire",
			registered: map[failpoint.Key]error{key1.ForScope("ns-1"): fromScoped},
			key:        key1.ForScope("ns-2"),
			want:       nil,
			wantFired:  false,
		},
		{
			name:       "unscoped key does not see scoped failpoints",
			registered: map[failpoint.Key]error{key1.ForScope("ns-1"): fromScoped},
			key:        key1,
			want:       nil,
			wantFired:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, fired := failpoint.Execute[error](newSource(tc.registered), tc.key)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantFired, fired)
		})
	}
}

func TestExecute_AcceptsFailpointOfAnotherType(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	registry.Set(key1, failpoint.NewFailpoint[int](key1))
	_, fired := failpoint.Execute[error](registrySource{registry: registry}, key1)
	require.False(t, fired)
}

func TestExecute_PanicsOnNonFailpoint(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	registry.Set(key1, "not a failpoint")
	require.Panics(t, func() { _, _ = failpoint.Execute[error](registrySource{registry: registry}, key1) })
}

func TestCheckpoint(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected")
	tests := []struct {
		name      string
		configure func(fp *failpoint.Failpoint[error])
		want      error
	}{
		{name: "not configured", configure: func(*failpoint.Failpoint[error]) {}, want: nil},
		{name: "return error", configure: func(fp *failpoint.Failpoint[error]) { fp.Return(injected, failpoint.Always) }, want: injected},
		{name: "return nil", configure: func(fp *failpoint.Failpoint[error]) { fp.Return(nil, failpoint.Always) }, want: nil},
		{name: "call returning nil", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Call(func() error { return nil }, failpoint.Always)
		}, want: nil},
		{name: "call returning error", configure: func(fp *failpoint.Failpoint[error]) {
			fp.Call(func() error { return injected }, failpoint.Always)
		}, want: injected},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := failpoint.NewRegistry()
			fp := failpoint.NewFailpoint[error](key1)
			tc.configure(fp)
			registry.Set(key1, fp)
			require.Equal(t, tc.want, failpoint.Checkpoint(registrySource{registry: registry}, key1))
		})
	}
}

func TestExecute_SourceWithoutRegistry(t *testing.T) {
	got, fired := failpoint.Execute[error](registrySource{}, key1)
	require.False(t, fired)
	require.NoError(t, got)
	require.NoError(t, failpoint.Checkpoint(registrySource{}, key1))
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

func TestActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configure  func(*failpoint.Failpoint[int])
		wantActive bool
	}{
		{name: "on", configure: func(fp *failpoint.Failpoint[int]) { fp.On(failpoint.Always) }, wantActive: true},
		{name: "return", configure: func(fp *failpoint.Failpoint[int]) { fp.Return(7, failpoint.Always) }, wantActive: true},
		{name: "pause", configure: func(fp *failpoint.Failpoint[int]) { fp.Pause(time.Nanosecond, failpoint.Always) }, wantActive: false},
		{name: "off", configure: func(fp *failpoint.Failpoint[int]) {}, wantActive: false},
		{name: "suspended", configure: func(fp *failpoint.Failpoint[int]) {
			fp.On(failpoint.Always)
			fp.SetEnabled(false)
		}, wantActive: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp := failpoint.NewFailpoint[int](key1)
			tc.configure(fp)

			registry := failpoint.NewRegistry()
			registry.Set(key1, fp)
			require.Equal(t, tc.wantActive, failpoint.Active(registrySource{registry: registry}, key1))

			ctx := failpoint.InjectRegistry(t.Context(), registry)
			require.Equal(t, tc.wantActive, failpoint.ActiveCtx(ctx, key1))
		})
	}

	t.Run("not registered", func(t *testing.T) {
		t.Parallel()
		require.False(t, failpoint.Active(registrySource{registry: failpoint.NewRegistry()}, key1))
		require.False(t, failpoint.ActiveCtx(t.Context(), key1))
	})
}
