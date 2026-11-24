package buggify_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/protobuf/proto"
)

// buggifyErr is the error that the BUGGIFY tests configure. The catalog is global, so each run
// registers a name of its own.
var (
	errBuggify      = errors.New("buggified")
	buggifyErrName  = "TestBuggify_" + rand.Text()
	registerErrOnce sync.Once
)

func registerBuggifyError() {
	registerErrOnce.Do(func() {
		failpoint.RegisterError(buggifyErrName, func(msg string) error {
			return fmt.Errorf("%w: %s", errBuggify, msg)
		})
	})
}

// alwaysConfig activates every selected key and fires every execution with errBuggify.
func alwaysConfig() buggify.Config {
	registerBuggifyError()
	return buggify.Config{Seed: 1, Activation: 1, Fire: 1, Error: buggifyErrName}
}

// newBuggifyRegistry returns a registry with BUGGIFY configured, and the events it reports.
func newBuggifyRegistry(t *testing.T, config buggify.Config) (failpoint.Registry, func() []failpoint.Event) {
	t.Helper()
	registry := failpoint.NewRegistry()
	require.NoError(t, buggify.For(registry).Set(config))
	var mu sync.Mutex
	var events []failpoint.Event
	registry.Subscribe(func(e failpoint.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	})
	return registry, func() []failpoint.Event {
		mu.Lock()
		defer mu.Unlock()
		return append([]failpoint.Event(nil), events...)
	}
}

func TestBuggify_Determinism(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := buggify.Config{Seed: 99, Activation: 0.5, Fire: 0.5, Error: buggifyErrName}

	// run executes each key a few times and returns which executions failed.
	run := func() (failed []string, events []failpoint.Event) {
		registry, getEvents := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}
		for i := range 20 {
			key := failpoint.KeyFromName(fmt.Sprintf("key.%d", i), "")
			for n := range 5 {
				if failpoint.Checkpoint(src, key) != nil {
					failed = append(failed, fmt.Sprintf("%s#%d", key, n+1))
				}
			}
		}
		return failed, getEvents()
	}

	failed, events := run()
	require.NotEmpty(t, failed)
	againFailed, againEvents := run()
	require.Equal(t, failed, againFailed)
	require.Equal(t, events, againEvents)

	for _, e := range events {
		if e.Kind == failpoint.FallbackActivationEvent {
			require.Equal(t, config.Activated(e.Key.Name()), e.Activated)
		}
	}
}

func TestBuggify_Execute(t *testing.T) {
	t.Parallel()

	t.Run("returns the configured error for a key with nothing registered", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, alwaysConfig())
		src := registrySource{registry: registry}
		key := key1.ForScope("ns")

		err := failpoint.Checkpoint(src, key)
		require.ErrorIs(t, err, errBuggify)
		require.ErrorContains(t, err, "BUGGIFY: injected by failpoint 1")
		value, set, fired := failpoint.ExecuteWithFired[error](src, key, nil)
		require.ErrorIs(t, value, errBuggify)
		require.True(t, set)
		require.True(t, fired)
		ctxErr, set := failpoint.ExecuteCtx[error](failpoint.InjectRegistry(t.Context(), registry), key)
		require.ErrorIs(t, ctxErr, errBuggify)
		require.True(t, set)

		require.Equal(t, []failpoint.Event{
			{Kind: failpoint.FallbackActivationEvent, Key: failpoint.KeyFromName("1", ""), Activated: true},
			{Kind: failpoint.FallbackFireEvent, Key: key, Action: failpoint.Return, Fired: true, Execution: 1},
			{Kind: failpoint.FallbackFireEvent, Key: key, Action: failpoint.Return, Fired: true, Execution: 2},
			{Kind: failpoint.FallbackFireEvent, Key: key, Action: failpoint.Return, Fired: true, Execution: 3},
		}, events())
	})

	t.Run("only injects into error call sites", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, alwaysConfig())
		src := registrySource{registry: registry}

		_, set := failpoint.Execute[any](src, key1)
		require.False(t, set)
		require.False(t, failpoint.Active(src, key1))
		_, set = failpoint.Execute[string](src, key1)
		require.False(t, set)
		require.Empty(t, events())
	})

	t.Run("does not inject without an error for the key", func(t *testing.T) {
		t.Parallel()
		config := alwaysConfig()
		config.Prefixes = []buggify.Prefix{{Prefix: "2", Activation: 1, Fire: 1}}
		registry, _ := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}

		require.NoError(t, failpoint.Checkpoint(src, key2))
		require.ErrorIs(t, failpoint.Checkpoint(src, key1), errBuggify)
		require.True(t, buggify.Fires(src, key2))
	})

	t.Run("ExecuteRegistered ignores BUGGIFY", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, alwaysConfig())
		_, set, fired := failpoint.ExecuteRegistered[error](registrySource{registry: registry}, key1, nil)
		require.False(t, set)
		require.False(t, fired)
		require.Empty(t, events())
	})
}

func TestBuggify_Precedence(t *testing.T) {
	t.Parallel()
	registered := errors.New("registered")

	for _, tc := range []struct {
		name          string
		registeredKey failpoint.Key
	}{
		{name: "same key", registeredKey: key1.ForScope("ns")},
		{name: "unscoped key", registeredKey: key1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry, events := newBuggifyRegistry(t, alwaysConfig())
			src := registrySource{registry: registry}
			key := key1.ForScope("ns")
			fp := failpoint.NewFailpoint[error](tc.registeredKey)
			fp.Return(registered, failpoint.Always)
			registry.Set(tc.registeredKey, fp)

			require.ErrorIs(t, failpoint.Checkpoint(src, key), registered)
			require.True(t, buggify.Fires(src, key))

			// A disabled failpoint still keeps BUGGIFY away from the key.
			fp.Disable()
			require.NoError(t, failpoint.Checkpoint(src, key))
			require.NoError(t, failpoint.FallbackError(src, key))
			require.False(t, buggify.Fires(src, key))

			for _, e := range events() {
				require.Equal(t, failpoint.ExecutionEvent, e.Kind)
			}

			registry.Delete(tc.registeredKey)
			require.ErrorIs(t, failpoint.Checkpoint(src, key), errBuggify)
		})
	}

	t.Run("a failpoint of another scope does not", func(t *testing.T) {
		t.Parallel()
		registry, _ := newBuggifyRegistry(t, alwaysConfig())
		fp := failpoint.NewFailpoint[error](key1.ForScope("other"))
		fp.Return(registered, failpoint.Always)
		registry.Set(key1.ForScope("other"), fp)

		require.ErrorIs(t, failpoint.Checkpoint(registrySource{registry: registry}, key1.ForScope("ns")), errBuggify)
	})
}

func TestBuggify_Branch(t *testing.T) {
	t.Parallel()

	t.Run("fires with the configured probability", func(t *testing.T) {
		t.Parallel()
		config := buggify.Config{Seed: 5, Activation: 1, Fire: 0.25}
		registry, events := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}

		var fired int
		for range 2000 {
			if buggify.Fires(src, key1) {
				fired++
			}
		}
		require.InDelta(t, 500, fired, 75)
		require.Len(t, events(), fired+1)
		require.Equal(t, failpoint.Event{Kind: failpoint.FallbackActivationEvent, Key: key1, Activated: true}, events()[0])
	})

	t.Run("FiresWithProb overrides the fire probability", func(t *testing.T) {
		t.Parallel()
		registry, _ := newBuggifyRegistry(t, buggify.Config{Seed: 5, Activation: 1, Fire: 0})
		src := registrySource{registry: registry}

		require.False(t, buggify.Fires(src, key1))
		require.True(t, buggify.FiresWithProb(src, key1, 1))
		require.False(t, buggify.FiresWithProb(src, key1, 0))
	})

	t.Run("never fires for a key that is not activated", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, buggify.Config{Seed: 5, Activation: 0, Fire: 1})
		src := registrySource{registry: registry}

		require.False(t, buggify.FiresWithProb(src, key1, 1))
		require.False(t, buggify.Fires(src, key1))
		require.Equal(t, []failpoint.Event{{Kind: failpoint.FallbackActivationEvent, Key: key1, Activated: false}}, events())
	})

	t.Run("is off without a registry", func(t *testing.T) {
		t.Parallel()
		require.False(t, buggify.Fires(registrySource{}, key1))
		require.NoError(t, failpoint.FallbackError(registrySource{}, key1))
	})
}

func TestBuggify_Suppress(t *testing.T) {
	t.Parallel()
	registry, _ := newBuggifyRegistry(t, alwaysConfig())
	src := registrySource{registry: registry}
	require.True(t, registry.Armed())

	restore := buggify.For(registry).Suppress()
	restoreNested := buggify.For(registry).Suppress()
	require.False(t, registry.Armed())
	require.NoError(t, failpoint.Checkpoint(src, key1))
	require.False(t, buggify.Fires(src, key1))
	require.Equal(t, alwaysConfig(), buggify.For(registry).Config())

	restoreNested()
	restoreNested()
	require.False(t, registry.Armed())
	restore()
	require.True(t, registry.Armed())
	require.ErrorIs(t, failpoint.Checkpoint(src, key1), errBuggify)
}

func TestBuggify_Set(t *testing.T) {
	t.Parallel()

	t.Run("arms the registry while enabled", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		require.False(t, registry.Armed())
		require.Equal(t, buggify.Config{}, buggify.For(registry).Config())

		require.NoError(t, buggify.For(registry).Set(alwaysConfig()))
		require.True(t, registry.Armed())
		require.Equal(t, 0, registry.Len())
		require.Equal(t, alwaysConfig(), buggify.For(registry).Config())

		registry.Set(key1, failpoint.NewFailpoint[error](key1))
		require.NoError(t, buggify.For(registry).Set(buggify.Config{}))
		require.True(t, registry.Armed())
		registry.Delete(key1)
		require.False(t, registry.Armed())
	})

	t.Run("keeps the state of an unchanged configuration", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, alwaysConfig())
		src := registrySource{registry: registry}
		require.Error(t, failpoint.Checkpoint(src, key1))

		require.NoError(t, buggify.For(registry).Set(alwaysConfig()))
		require.Error(t, failpoint.Checkpoint(src, key1))
		require.Equal(t, int64(2), events()[2].Execution)

		changed := alwaysConfig()
		changed.Seed = 2
		require.NoError(t, buggify.For(registry).Set(changed))
		require.Error(t, failpoint.Checkpoint(src, key1))
		require.Equal(t, []failpoint.Event{
			{Kind: failpoint.FallbackActivationEvent, Key: key1, Activated: true},
			{Kind: failpoint.FallbackFireEvent, Key: key1, Action: failpoint.Return, Fired: true, Execution: 1},
		}, events()[3:])
	})
}

func TestBuggify_OffPathDoesNotAllocate(t *testing.T) {
	registerBuggifyError()
	suppressed := failpoint.NewRegistry()
	require.NoError(t, buggify.For(suppressed).Set(alwaysConfig()))
	restore := buggify.For(suppressed).Suppress()
	defer restore()

	for name, registry := range map[string]failpoint.Registry{
		"no registry":    nil,
		"empty registry": failpoint.NewRegistry(),
		"suppressed":     suppressed,
	} {
		var src failpoint.Source = registrySource{registry: registry}
		key := key1.ForScope("ns")
		allocs := testing.AllocsPerRun(100, func() {
			_ = buggify.Fires(src, key)
			_ = buggify.FiresWithProb(src, key, 1)
			_ = failpoint.FallbackError(src, key)
			_ = failpoint.Checkpoint(src, key)
		})
		require.Zero(t, allocs, name)
	}
}

func TestBuggify_SetCopiesTheConfiguration(t *testing.T) {
	t.Parallel()
	config := buggify.Config{
		Seed: 1, Activation: 1, Fire: 1,
		Prefixes: []buggify.Prefix{{Prefix: "a", Activation: 1, Fire: 1}},
		Include:  []string{"a*"},
		Exclude:  []string{"ab"},
	}
	registry := failpoint.NewRegistry()
	require.NoError(t, buggify.For(registry).Set(config))

	config.Prefixes[0].Activation = 0
	config.Include[0] = "x"
	got := buggify.For(registry).Config()
	got.Exclude[0] = "a*"
	require.Equal(t, buggify.Config{
		Seed: 1, Activation: 1, Fire: 1,
		Prefixes: []buggify.Prefix{{Prefix: "a", Activation: 1, Fire: 1}},
		Include:  []string{"a*"},
		Exclude:  []string{"ab"},
	}, buggify.For(registry).Config())
	require.True(t, buggify.Fires(registrySource{registry: registry}, failpoint.KeyFromName("ac", "")))
}

func TestFallbackError_Shadows(t *testing.T) {
	t.Parallel()
	registry, _ := newBuggifyRegistry(t, alwaysConfig())
	src := registrySource{registry: registry}
	key := key1.ForScope("ns")
	require.ErrorIs(t, failpoint.FallbackError(src, key, key2.ForScope("ns")), errBuggify)

	// A disabled failpoint of the unscoped shadow key is enough.
	registry.Set(key2, failpoint.NewFailpoint[error](key2))
	require.NoError(t, failpoint.FallbackError(src, key, key2.ForScope("ns")))
	require.ErrorIs(t, failpoint.FallbackError(src, key, key3), errBuggify)
}

func TestBuggify_UnselectedNamesDoNotUseTheBudget(t *testing.T) {
	t.Parallel()
	config := alwaysConfig()
	config.Exclude = []string{"excluded.*"}
	registry, _ := newBuggifyRegistry(t, config)
	src := registrySource{registry: registry}

	for i := range 20000 {
		require.NoError(t, failpoint.FallbackError(src, failpoint.KeyFromName(fmt.Sprintf("excluded.%d", i), "")))
	}
	require.ErrorIs(t, failpoint.Checkpoint(src, key1), errBuggify)
}

func TestBuggify_ActivationPrecedesFires(t *testing.T) {
	t.Parallel()
	registry, events := newBuggifyRegistry(t, alwaysConfig())
	src := registrySource{registry: registry}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 50 {
				_ = failpoint.Checkpoint(src, failpoint.KeyFromName(fmt.Sprintf("key.%d", i), ""))
			}
		})
	}
	wg.Wait()

	activated := map[string]bool{}
	for _, e := range events() {
		switch e.Kind {
		case failpoint.FallbackActivationEvent:
			require.False(t, activated[e.Key.Name()], "activation reported twice for %s", e.Key)
			activated[e.Key.Name()] = true
		case failpoint.FallbackFireEvent:
			require.True(t, activated[e.Key.Name()], "fire before activation for %s", e.Key)
		default:
		}
	}
	require.Len(t, activated, 50)
}

func TestBuggify_ScopesCountExecutionsSeparately(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := buggify.Config{Seed: 3, Activation: 1, Fire: 0.5, Error: buggifyErrName}
	require.Equal(t, math.Float64bits(config.Draw("key", 4)), math.Float64bits(config.DrawScoped("key", "", 4)))
	require.NotEqual(t, config.DrawScoped("key", "a", 4), config.DrawScoped("key", "b", 4))

	// failures returns which of 40 executions in scope "b" fail, with interleaved executions in
	// scope "a" if busy.
	failures := func(busy bool) []bool {
		registry, _ := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}
		var failed []bool
		for range 40 {
			if busy {
				_ = failpoint.Checkpoint(src, key1.ForScope("a"))
				_ = failpoint.Checkpoint(src, key1)
			}
			failed = append(failed, failpoint.Checkpoint(src, key1.ForScope("b")) != nil)
		}
		return failed
	}
	quiet := failures(false)
	require.Contains(t, quiet, true)
	require.Contains(t, quiet, false)
	require.Equal(t, quiet, failures(true))
}

func TestBuggify_FallbackStats(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := buggify.Config{
		Seed: 3, Activation: 1, Fire: 1, Error: buggifyErrName,
		Prefixes: []buggify.Prefix{{Prefix: "2", Activation: 0}},
		Exclude:  []string{"3"},
	}
	registry, _ := newBuggifyRegistry(t, config)
	src := registrySource{registry: registry}
	for _, key := range []failpoint.Key{key1.ForScope("a"), key1.ForScope("b"), key1, key2, key2.ForScope("a"), key3} {
		_ = failpoint.Checkpoint(src, key)
	}
	// A registered failpoint keeps the execution away from BUGGIFY.
	registry.Set(key100, failpoint.NewFailpoint[error](key100))
	_ = failpoint.Checkpoint(src, key100)

	require.Equal(t, []failpoint.FallbackKeyStats{
		{Name: "1", Activated: true, Hits: 3, Fires: 3},
		{Name: "2", Activated: false, Hits: 2, Fires: 0},
	}, buggify.For(registry).FallbackStats())

	require.NoError(t, buggify.For(registry).Set(buggify.Config{}))
	require.Empty(t, buggify.For(registry).FallbackStats())
}

func TestBuggify_ErrorMessageIsValidUTF8(t *testing.T) {
	t.Parallel()
	registry, _ := newBuggifyRegistry(t, alwaysConfig())
	err := failpoint.Checkpoint(registrySource{registry: registry}, failpoint.KeyFromName("http:GET /\xff", "ns"))
	require.ErrorIs(t, err, errBuggify)
	require.True(t, utf8.ValidString(err.Error()), err.Error())
	require.Contains(t, err.Error(), "BUGGIFY: injected by failpoint http:GET /�")
}

func TestBuggify_ExecuteCtxRespectsUnscopedRegistration(t *testing.T) {
	t.Parallel()
	registry, _ := newBuggifyRegistry(t, alwaysConfig())
	ctx := failpoint.InjectRegistry(t.Context(), registry)
	key := key1.ForScope("ns")
	ctxErr, _ := failpoint.ExecuteCtx[error](ctx, key)
	require.ErrorIs(t, ctxErr, errBuggify)

	registry.Set(key1, failpoint.NewFailpoint[error](key1))
	err, set := failpoint.ExecuteCtx[error](ctx, key)
	require.NoError(t, err)
	require.False(t, set)
}

func TestFor(t *testing.T) {
	t.Parallel()

	t.Run("returns the installed BUGGIFY, also inside Configure", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		b := buggify.For(registry)
		require.Same(t, b, registry.Fallback())
		require.False(t, registry.Armed())
		registry.Configure(func() {
			require.Same(t, b, buggify.For(registry))
			require.NoError(t, buggify.For(registry).Set(alwaysConfig()))
		})
		require.True(t, registry.Armed())
	})

	t.Run("panics if the registry has another fallback", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		registry.SetFallback(otherFallback{})
		require.Panics(t, func() { buggify.For(registry) })
		require.Equal(t, otherFallback{}, registry.Fallback())
	})
}

// otherFallback is a fallback that isn't BUGGIFY.
type otherFallback struct{}

func (otherFallback) Error(failpoint.Key) error { return errors.New("other") }

func TestBuggify_StaleHandleDoesNotActivateReplacement(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	b := buggify.For(registry)
	require.NoError(t, b.Set(alwaysConfig()))
	restore := b.Suppress()

	registry.SetFallback(otherFallback{})
	restore()
	require.ErrorIs(t, b.Set(alwaysConfig()), buggify.ErrNotInstalled)
	require.False(t, registry.Armed())
	require.NoError(t, failpoint.Checkpoint(registrySource{registry: registry}, key1))
}

func TestFires_FollowsTheActiveFallback(t *testing.T) {
	t.Parallel()
	registry, _ := newBuggifyRegistry(t, alwaysConfig())
	src := registrySource{registry: registry}
	b := buggify.For(registry)
	require.True(t, buggify.Fires(src, key1))

	// Turned off in the registry, while an unrelated failpoint keeps the registry armed.
	require.True(t, registry.SetFallbackActive(b, false))
	registry.Set(key2, failpoint.NewFailpoint[error](key2))
	require.True(t, registry.Armed())
	require.False(t, buggify.Fires(src, key1))
	require.NoError(t, failpoint.Checkpoint(src, key1))
}

func TestBuggify_ListFailpointsReportsTheCounters(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	require.NoError(t, buggify.For(registry).Set(buggify.Config{
		Seed:     1,
		Prefixes: []buggify.Prefix{{Prefix: "z", Activation: 1}},
	}))
	src := registrySource{registry: registry}
	// More key names than a ListFailpoints response reports.
	padding := strings.Repeat("x", 200)
	for i := range 12000 {
		_ = buggify.Fires(src, failpoint.KeyFromName(fmt.Sprintf("a%s%d", padding, i), ""))
	}
	for i := range 3 {
		require.False(t, buggify.Fires(src, failpoint.KeyFromName(fmt.Sprintf("z%d", i), "")))
	}

	resp, err := failpointgrpc.NewServer(registry).ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.LessOrEqual(t, proto.Size(resp), 2<<20)
	require.Positive(t, resp.FallbackKeysOmitted)
	require.Equal(t, int64(12003), int64(len(resp.FallbackKeys))+resp.FallbackKeysOmitted)
	for i, name := range []string{"z0", "z1", "z2"} {
		require.True(t, proto.Equal(&failpointpb.FallbackKeyInfo{Key: name, Activated: true, Hits: 1}, resp.FallbackKeys[i]), resp.FallbackKeys[i])
	}
}

// switchingSource returns first on the first call of Failpoints, and then other.
type switchingSource struct {
	first, other failpoint.Registry
	calls        *int
}

func (s switchingSource) Failpoints() failpoint.Registry {
	*s.calls++
	if *s.calls == 1 {
		return s.first
	}
	return s.other
}

func TestFires_ReadsTheRegistryOnce(t *testing.T) {
	t.Parallel()
	// The first registry forces the branch with a failpoint; the second has nothing.
	first := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint[error](key1)
	fp.On(failpoint.Always)
	first.Set(key1, fp)
	calls := 0
	require.True(t, buggify.Fires(switchingSource{first: first, other: failpoint.NewRegistry(), calls: &calls}, key1))
	require.Equal(t, 1, calls)
}

func TestBuggify_ErrorCached(t *testing.T) {
	t.Parallel()
	registerBuggifyError()

	t.Run("decides like Error", func(t *testing.T) {
		t.Parallel()
		config := buggify.Config{Seed: 7, Activation: 0.5, Fire: 0.5, Error: buggifyErrName}
		// run executes the keys a few times in two scopes, and returns which executions failed and
		// the events.
		run := func(cached bool) (failed []string, events []failpoint.Event) {
			registry, getEvents := newBuggifyRegistry(t, config)
			src := registrySource{registry: registry}
			for i := range 20 {
				name := fmt.Sprintf("key.%d", i)
				var cache failpoint.FallbackCache
				for n := range 10 {
					key := failpoint.KeyFromName(name, []string{"", "a"}[n%2])
					var err error
					if cached {
						err = failpoint.FallbackErrorCached(src, &cache, key)
					} else {
						err = failpoint.FallbackError(src, key)
					}
					if err != nil {
						require.ErrorIs(t, err, errBuggify)
						failed = append(failed, fmt.Sprintf("%s#%d", key, n))
					}
				}
			}
			return failed, getEvents()
		}
		failed, events := run(false)
		require.NotEmpty(t, failed)
		cachedFailed, cachedEvents := run(true)
		require.Equal(t, failed, cachedFailed)
		require.Equal(t, events, cachedEvents)
	})

	t.Run("follows a new configuration", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, alwaysConfig())
		src := registrySource{registry: registry}
		var cache failpoint.FallbackCache
		require.ErrorIs(t, failpoint.FallbackErrorCached(src, &cache, key1), errBuggify)

		never := alwaysConfig()
		never.Activation = 0
		require.NoError(t, buggify.For(registry).Set(never))
		require.NoError(t, failpoint.FallbackErrorCached(src, &cache, key1))
		require.NoError(t, buggify.For(registry).Set(alwaysConfig()))
		require.ErrorIs(t, failpoint.FallbackErrorCached(src, &cache, key1), errBuggify)

		// Each configuration reported the activation of the name once, before its fire events.
		require.Equal(t, []failpoint.Event{
			{Kind: failpoint.FallbackActivationEvent, Key: key1, Activated: true},
			{Kind: failpoint.FallbackFireEvent, Key: key1, Action: failpoint.Return, Fired: true, Execution: 1},
			{Kind: failpoint.FallbackActivationEvent, Key: key1, Activated: false},
			{Kind: failpoint.FallbackActivationEvent, Key: key1, Activated: true},
			{Kind: failpoint.FallbackFireEvent, Key: key1, Action: failpoint.Return, Fired: true, Execution: 1},
		}, events())
	})

	t.Run("caches unselected names", func(t *testing.T) {
		t.Parallel()
		config := alwaysConfig()
		config.Exclude = []string{key2.Name()}
		registry, _ := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}
		var cache failpoint.FallbackCache
		require.NoError(t, failpoint.FallbackErrorCached(src, &cache, key2))
		require.NoError(t, failpoint.FallbackErrorCached(src, &cache, key2))
	})

	t.Run("doesn't use the state of another name", func(t *testing.T) {
		t.Parallel()
		config := alwaysConfig()
		config.Exclude = []string{key2.Name()}
		registry, _ := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}
		var cache failpoint.FallbackCache
		require.ErrorIs(t, failpoint.FallbackErrorCached(src, &cache, key1), errBuggify)
		require.NoError(t, failpoint.FallbackErrorCached(src, &cache, key2))
		require.ErrorIs(t, failpoint.FallbackErrorCached(src, &cache, key1), errBuggify)
	})

	t.Run("does nothing while suppressed", func(t *testing.T) {
		t.Parallel()
		registry, _ := newBuggifyRegistry(t, alwaysConfig())
		src := registrySource{registry: registry}
		var cache failpoint.FallbackCache
		require.ErrorIs(t, failpoint.FallbackErrorCached(src, &cache, key1), errBuggify)
		restore := buggify.For(registry).Suppress()
		require.NoError(t, failpoint.FallbackErrorCached(src, &cache, key1))
		restore()
		require.ErrorIs(t, failpoint.FallbackErrorCached(src, &cache, key1), errBuggify)
	})
}

func TestBuggify_OnPathDoesNotAllocate(t *testing.T) {
	config := alwaysConfig()
	config.Error = ""
	registry, _ := newBuggifyRegistry(t, config)
	var src failpoint.Source = registrySource{registry: registry}
	key := key1.ForScope("ns")
	var cache failpoint.FallbackCache
	_ = failpoint.FallbackErrorCached(src, &cache, key, key2)
	allocs := testing.AllocsPerRun(100, func() {
		_ = failpoint.FallbackErrorCached(src, &cache, key, key2)
		_ = failpoint.FallbackError(src, key, key2)
		_ = failpoint.Checkpoint(src, key)
	})
	require.Zero(t, allocs)
}

func TestBuggify_FiresOnPathDoesNotAllocate(t *testing.T) {
	registerBuggifyError()
	for name, config := range map[string]buggify.Config{
		"fires":         alwaysConfig(),
		"activated":     {Seed: 1, Activation: 1, Fire: 0, Error: buggifyErrName},
		"not activated": {Seed: 1, Activation: 0, Fire: 1, Error: buggifyErrName},
	} {
		// No subscriber, since the events of the fires would be appended to its slice.
		registry := failpoint.NewRegistry()
		require.NoError(t, buggify.For(registry).Set(config))
		var src failpoint.Source = registrySource{registry: registry}
		key := key1.ForScope("ns")
		_ = buggify.Fires(src, key)
		allocs := testing.AllocsPerRun(100, func() {
			_ = buggify.Fires(src, key)
			_ = buggify.FiresWithProb(src, key, 0.5)
		})
		require.Zero(t, allocs, name)
	}
}
