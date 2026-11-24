//go:build test_dep

package buggify

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func newCollection(t *testing.T, client dynamicconfig.Client) *dynamicconfig.Collection {
	collection := dynamicconfig.NewCollection(client, log.NewNoopLogger())
	collection.Start()
	t.Cleanup(collection.Stop)
	return collection
}

// lockedBuffer collects gomad lines that executions on several goroutines write.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// alwaysValue buggifies every execution of the key "always" with an Internal error.
var alwaysValue = map[string]any{
	"seed":     5,
	"include":  []any{"always"},
	"prefixes": []any{map[string]any{"prefix": "always", "activation": 1, "fire": 1, "error": "Internal"}},
}

var alwaysKey = failpoint.KeyFromName("always", "")

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("configures BUGGIFY before the server starts and disables it when it stops", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, alwaysValue)
		testHooks := testhooks.NewTestHooks()
		var errAtStart error
		app := fxtest.New(t,
			fx.Supply(testHooks, newCollection(t, client), fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
			Module,
			fx.Invoke(func(lc fx.Lifecycle) {
				lc.Append(fx.StartHook(func() { errAtStart = failpoint.Checkpoint(testHooks, alwaysKey) }))
			}),
		)

		app.RequireStart()
		require.ErrorContains(t, errAtStart, "BUGGIFY: injected by failpoint always")
		require.Equal(t, uint64(5), testHooks.Failpoints().BuggifyConfig().Seed)
		app.RequireStop()
		require.False(t, testHooks.Failpoints().BuggifyConfig().Enabled())
	})

	t.Run("fails to start with an invalid initial value", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, map[string]any{"sead": 5})
		testHooks := testhooks.NewTestHooks()
		app := fxtest.New(t,
			fx.Supply(testHooks, newCollection(t, client), fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
			Module,
		)

		require.ErrorContains(t, app.Start(t.Context()), "invalid dynamic config setting testing.buggify")
		require.False(t, testHooks.Failpoints().Armed())
	})
}

func TestSource(t *testing.T) {
	t.Parallel()

	t.Run("seeds from the environment and follows changes of the setting", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		registry := failpoint.NewRegistry()
		fatalErrs := make(chan error, 1)
		s := newSource(registry, newCollection(t, client), log.NewNoopLogger(), env(map[string]string{failpoint.BuggifySeedEnv: "9"}), nil,
			func(err error) { fatalErrs <- err })
		require.NoError(t, s.start(t.Context()))
		t.Cleanup(func() { _ = s.stop(context.Background()) })
		want := Defaults()
		want.Seed = 9
		require.Equal(t, want, registry.BuggifyConfig())

		client.OverrideSetting(Setting, map[string]any{"disabled": true})
		await.RequireTrue(t, func() bool { return !registry.BuggifyConfig().Enabled() }, 5*time.Second, 10*time.Millisecond)

		client.OverrideSetting(Setting, map[string]any{"seed": "x"})
		require.ErrorContains(t, await.Rcv(t, fatalErrs), "invalid dynamic config setting testing.buggify")
	})

	t.Run("prints gomad lines", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, alwaysValue)
		registry := failpoint.NewRegistry()
		var gomad lockedBuffer
		s := newSource(registry, newCollection(t, client), log.NewNoopLogger(), env(nil), &gomad,
			func(err error) { t.Error(err) })
		require.NoError(t, s.start(t.Context()))
		t.Cleanup(func() { _ = s.stop(context.Background()) })

		src := failpoint.Source(testSource{registry})
		require.Error(t, failpoint.Checkpoint(src, alwaysKey.ForScope("ns")))
		require.Error(t, failpoint.Checkpoint(src, alwaysKey))
		require.NoError(t, failpoint.Checkpoint(src, failpoint.KeyFromName("other", "")))
		require.Equal(t, strings.Join([]string{
			`GOMAD failpoint buggify-activation key="always" active=true`,
			`GOMAD failpoint hit key="always" scope="ns" n=1 action=Return buggify=true`,
			`GOMAD failpoint hit key="always" scope="" n=1 action=Return buggify=true`,
			``,
		}, "\n"), gomad.String())
	})
}

type testSource struct{ registry failpoint.Registry }

func (s testSource) Failpoints() failpoint.Registry { return s.registry }

func TestWrapDynamicConfigClient(t *testing.T) {
	t.Parallel()

	// Randomizes every marked setting.
	allKnobs := map[string]any{
		"seed":     3,
		"prefixes": []any{map[string]any{"prefix": KnobPrefix, "activation": 1, "fire": 0}},
	}

	t.Run("returns the client without BUGGIFY", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		wrapped, startup, err := wrapDynamicConfigClient(client, log.NewNoopLogger(), env(nil))
		require.NoError(t, err)
		require.Same(t, client, wrapped)
		require.Equal(t, Startup{Resolved: true}, startup)
	})

	t.Run("randomizes marked settings unless they are set", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, allKnobs)
		client.OverrideSetting(dynamicconfig.TimerTaskBatchSize, 77)
		wrapped, startup, err := wrapDynamicConfigClient(client, log.NewNoopLogger(), env(nil))
		require.NoError(t, err)
		require.Equal(t, uint64(3), startup.Config.Seed)
		collection := newCollection(t, wrapped)

		require.Contains(t, []int{1, 2, 10}, dynamicconfig.TransferTaskBatchSize.Get(collection)())
		require.Contains(t, []int{1, 2, 10}, dynamicconfig.MatchingMaxTaskBatchSize.Get(collection)("ns", "tq", 1))
		require.Equal(t, 77, dynamicconfig.TimerTaskBatchSize.Get(collection)())
	})

	t.Run("fails with an invalid setting", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, map[string]any{"seed": -1})
		_, _, err := wrapDynamicConfigClient(client, log.NewNoopLogger(), env(nil))
		require.ErrorContains(t, err, "is not an unsigned integer")
	})

	t.Run("fails with an invalid configuration", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, map[string]any{"seed": 1, "error": "Oops"})
		_, _, err := wrapDynamicConfigClient(client, log.NewNoopLogger(), env(nil))
		require.ErrorContains(t, err, `unknown error "Oops"`)
	})
}

func TestSource_StartsWithTheStartupConfiguration(t *testing.T) {
	t.Parallel()

	start := func(t *testing.T, client *dynamicconfig.MemoryClient, startup Startup) (failpoint.Registry, error) {
		registry := failpoint.NewRegistry()
		s := newSource(registry, newCollection(t, client), log.NewNoopLogger(), env(map[string]string{failpoint.BuggifySeedEnv: "9"}), nil,
			func(err error) { t.Error(err) })
		s.startup = startup
		err := s.start(t.Context())
		t.Cleanup(func() { _ = s.stop(context.Background()) })
		return registry, err
	}
	resolve := func(t *testing.T, client *dynamicconfig.MemoryClient) Startup {
		// The environment changes after the dynamic config client resolved the configuration.
		_, startup, err := wrapDynamicConfigClient(client, log.NewNoopLogger(), env(map[string]string{failpoint.BuggifySeedEnv: "8"}))
		require.NoError(t, err)
		return startup
	}

	t.Run("uses the startup configuration if the setting is unchanged", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		registry, err := start(t, client, resolve(t, client))
		require.NoError(t, err)
		require.Equal(t, uint64(8), registry.BuggifyConfig().Seed)

		client.OverrideSetting(Setting, map[string]any{"seed": 5})
		await.RequireTrue(t, func() bool { return registry.BuggifyConfig().Seed == 5 }, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("applies a change between resolution and start", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, map[string]any{"seed": 3})
		startup := resolve(t, client)
		client.OverrideSetting(Setting, map[string]any{"seed": 4})
		registry, err := start(t, client, startup)
		require.NoError(t, err)
		require.Equal(t, uint64(4), registry.BuggifyConfig().Seed)
	})

	t.Run("applies a disable between resolution and start", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, map[string]any{"seed": 3})
		startup := resolve(t, client)
		client.OverrideSetting(Setting, map[string]any{"disabled": true})
		registry, err := start(t, client, startup)
		require.NoError(t, err)
		require.False(t, registry.BuggifyConfig().Enabled())
	})

	t.Run("fails to start with a malformed value set between resolution and start", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		startup := resolve(t, client)
		client.OverrideSetting(Setting, map[string]any{"sead": 3})
		_, err := start(t, client, startup)
		require.ErrorContains(t, err, "invalid dynamic config setting testing.buggify")
	})
}

func TestSource_InvalidValueReplacedBeforeItsCallback(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	fatalErrs := make(chan error, 1)
	s := newSource(failpoint.NewRegistry(), newCollection(t, client), log.NewNoopLogger(), env(nil), nil,
		func(err error) { fatalErrs <- err })
	require.NoError(t, s.start(t.Context()))
	t.Cleanup(func() { _ = s.stop(context.Background()) })

	// Well-formed, but invalid: the probability is out of range, even though BUGGIFY is disabled.
	client.OverrideSetting(Setting, map[string]any{"disabled": true, "fire": 2})
	client.OverrideSetting(Setting, map[string]any{"seed": 1})
	require.ErrorContains(t, await.Rcv(t, fatalErrs), "probability 2")
}
