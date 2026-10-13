package temporal

import (
	"context"
	"errors"
	"math"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/fx"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const initialGoMemoryLimit = int64(8 << 30)

func TestGoMemoryLimit(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cfg          config.GoMemoryLimit
		env          map[string]string
		providerErr  error
		want         int64
		calls        int32
		warnings     int
		wantStartErr bool
	}{
		{name: "disabled", cfg: config.GoMemoryLimit{RefreshInterval: time.Millisecond}, env: map[string]string{"AUTOMEMLIMIT": "invalid"}, want: initialGoMemoryLimit},
		{name: "fraction", cfg: config.GoMemoryLimit{Ratio: 0.5}, want: 2 << 30, calls: 1},
		{name: "explicit limit", cfg: config.GoMemoryLimit{Ratio: 0.5, RefreshInterval: time.Millisecond}, env: map[string]string{"GOMEMLIMIT": "1GiB"}, want: initialGoMemoryLimit},
		{name: "empty explicit limit", cfg: config.GoMemoryLimit{Ratio: 0.5, RefreshInterval: time.Millisecond}, env: map[string]string{"GOMEMLIMIT": ""}, want: initialGoMemoryLimit},
		{name: "explicit limit wins over invalid override", cfg: config.GoMemoryLimit{Ratio: 0.5}, env: map[string]string{"GOMEMLIMIT": "1GiB", "AUTOMEMLIMIT": "invalid"}, want: initialGoMemoryLimit},
		{name: "native off", cfg: config.GoMemoryLimit{Ratio: 0.5, RefreshInterval: time.Millisecond}, env: map[string]string{"AUTOMEMLIMIT": "off"}, want: initialGoMemoryLimit},
		{name: "native ratio", cfg: config.GoMemoryLimit{Ratio: 0.5}, env: map[string]string{"AUTOMEMLIMIT": "0.75"}, want: 3 << 30, calls: 1},
		{name: "invalid native ratio", cfg: config.GoMemoryLimit{Ratio: 0.5, RefreshInterval: time.Millisecond}, env: map[string]string{"AUTOMEMLIMIT": "invalid"}, want: initialGoMemoryLimit, wantStartErr: true},
		{name: "empty native ratio", cfg: config.GoMemoryLimit{Ratio: 0.5}, env: map[string]string{"AUTOMEMLIMIT": ""}, want: initialGoMemoryLimit, wantStartErr: true},
		{name: "zero native ratio", cfg: config.GoMemoryLimit{Ratio: 0.5}, env: map[string]string{"AUTOMEMLIMIT": "0"}, want: initialGoMemoryLimit, wantStartErr: true},
		{name: "nan native ratio", cfg: config.GoMemoryLimit{Ratio: 0.5}, env: map[string]string{"AUTOMEMLIMIT": "NaN"}, want: initialGoMemoryLimit, wantStartErr: true},
		{name: "native ratio above one", cfg: config.GoMemoryLimit{Ratio: 0.5}, env: map[string]string{"AUTOMEMLIMIT": "2"}, want: initialGoMemoryLimit, wantStartErr: true},
		{name: "detection failure", cfg: config.GoMemoryLimit{Ratio: 0.5}, providerErr: errors.New("cannot read cgroup"), want: initialGoMemoryLimit, calls: 1, warnings: 1},
		{name: "unlimited", cfg: config.GoMemoryLimit{Ratio: 0.5}, providerErr: memlimit.ErrNoLimit, want: math.MaxInt64, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepareGoMemoryLimitTest(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			var calls atomic.Int32
			core, entries := observer.New(zap.WarnLevel)
			hook := newGoMemoryLimitHook(tc.cfg, log.NewZapLogger(zap.New(core)), func() (uint64, error) {
				calls.Add(1)
				return 4 << 30, tc.providerErr
			})
			app := newGoMemoryLimitTestApp(t, hook)
			require.Equal(t, initialGoMemoryLimit, debug.SetMemoryLimit(-1), "construction must not change the limit")
			err := app.Start(t.Context())
			if tc.wantStartErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, app.Stop(t.Context()))
			require.Equal(t, tc.want, debug.SetMemoryLimit(-1))
			require.Equal(t, tc.calls, calls.Load())
			require.Equal(t, tc.warnings, entries.Len())
		})
	}
}

func TestGoMemoryLimitRefresh(t *testing.T) {
	prepareGoMemoryLimitTest(t)
	var mu sync.Mutex
	limit := uint64(4 << 30)
	providerErr := errors.New("cannot read cgroup")
	reads := 0
	provider := func() (uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		reads++
		return limit, providerErr
	}
	app := newGoMemoryLimitTestApp(t, newGoMemoryLimitHook(
		config.GoMemoryLimit{Ratio: 0.5, RefreshInterval: time.Millisecond}, log.NewNoopLogger(), provider))
	startCtx, cancelStart := context.WithCancel(t.Context())
	t.Cleanup(cancelStart)
	require.NoError(t, app.Start(startCtx))
	cancelStart()
	require.Equal(t, initialGoMemoryLimit, debug.SetMemoryLimit(-1))

	for _, tc := range []struct {
		name  string
		limit uint64
		err   error
		want  int64
	}{
		{name: "recover after failed first read", limit: 4 << 30, want: 2 << 30},
		{name: "changed limit", limit: 6 << 30, want: 3 << 30},
		{name: "failed reads keep last limit", err: errors.New("cannot read cgroup"), want: 3 << 30},
		{name: "limit removed", err: memlimit.ErrNoLimit, want: math.MaxInt64},
		{name: "limit restored", limit: 4 << 30, want: 2 << 30},
	} {
		mu.Lock()
		limit, providerErr, reads = tc.limit, tc.err, 0
		mu.Unlock()
		await.RequireTruef(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return reads >= 3 && debug.SetMemoryLimit(-1) == tc.want
		}, 5*time.Second, time.Millisecond, "%s", tc.name)
	}
	require.NoError(t, app.Stop(t.Context()))
}

func TestGoMemoryLimitLifecycle(t *testing.T) {
	prepareGoMemoryLimitTest(t)
	var events []string
	app := newGoMemoryLimitTestApp(t,
		newGoMemoryLimitHook(config.GoMemoryLimit{Ratio: 0.5}, log.NewNoopLogger(), func() (uint64, error) {
			events = append(events, "memory limit")
			return 4 << 30, nil
		}),
		fx.Hook{OnStart: func(context.Context) error {
			require.Equal(t, int64(2<<30), debug.SetMemoryLimit(-1))
			events = append(events, "service one")
			return nil
		}},
		fx.Hook{OnStart: func(context.Context) error {
			events = append(events, "service two")
			return nil
		}},
	)
	require.Empty(t, events)
	require.NoError(t, app.Start(t.Context()))
	require.Equal(t, []string{"memory limit", "service one", "service two"}, events)
	debug.SetMemoryLimit(7 << 30)
	require.NoError(t, app.Stop(t.Context()))
	require.Equal(t, int64(7<<30), debug.SetMemoryLimit(-1), "stop must not restore an earlier process-wide limit")
}

func TestGoMemoryLimitRefreshUntilServicesStop(t *testing.T) {
	prepareGoMemoryLimitTest(t)
	var calls atomic.Int32
	app := newGoMemoryLimitTestApp(t,
		newGoMemoryLimitHook(config.GoMemoryLimit{Ratio: 0.5, RefreshInterval: time.Millisecond}, log.NewNoopLogger(), func() (uint64, error) {
			calls.Add(1)
			return 4 << 30, nil
		}),
		fx.Hook{OnStop: func(context.Context) error {
			before := calls.Load()
			await.RequireTrue(t, func() bool { return calls.Load() > before }, 5*time.Second, time.Millisecond)
			return nil
		}},
	)
	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
}

func TestGoMemoryLimitStartupRollback(t *testing.T) {
	prepareGoMemoryLimitTest(t)
	baseline := goleak.IgnoreCurrent()
	serviceErr := errors.New("service startup failed")
	app := newGoMemoryLimitTestApp(t,
		newGoMemoryLimitHook(config.GoMemoryLimit{Ratio: 0.5, RefreshInterval: time.Millisecond}, log.NewNoopLogger(), memlimit.Limit(4<<30)),
		fx.Hook{OnStart: func(context.Context) error { return serviceErr }},
	)
	require.ErrorIs(t, app.Start(t.Context()), serviceErr)
	// Verify rollback itself cancels refresh, without relying on explicit Stop.
	require.NoError(t, goleak.Find(baseline))
}

func prepareGoMemoryLimitTest(t *testing.T) {
	t.Helper()
	for _, key := range []string{"GOMEMLIMIT", "AUTOMEMLIMIT"} {
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key))
	}
	previous := debug.SetMemoryLimit(initialGoMemoryLimit)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })
	baseline := goleak.IgnoreCurrent()
	t.Cleanup(func() { require.NoError(t, goleak.Find(baseline)) })
}

func newGoMemoryLimitTestApp(t *testing.T, memoryHook fx.Hook, serviceHooks ...fx.Hook) *fx.App {
	t.Helper()
	app := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle) {
		lc.Append(memoryHook)
		for _, hook := range serviceHooks {
			lc.Append(hook)
		}
	}))
	require.NoError(t, app.Err())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Stop(ctx))
	})
	return app
}
