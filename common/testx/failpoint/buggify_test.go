package failpoint_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
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
func alwaysConfig() failpoint.BuggifyConfig {
	registerBuggifyError()
	return failpoint.BuggifyConfig{Seed: 1, Activation: 1, Fire: 1, Error: buggifyErrName}
}

// newBuggifyRegistry returns a registry with BUGGIFY configured, and the events it reports.
func newBuggifyRegistry(t *testing.T, config failpoint.BuggifyConfig) (failpoint.Registry, func() []failpoint.Event) {
	t.Helper()
	registry := failpoint.NewRegistry()
	require.NoError(t, registry.SetBuggify(config))
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

func TestBuggifyConfig_Validate(t *testing.T) {
	t.Parallel()
	registerBuggifyError()

	tests := []struct {
		name    string
		config  failpoint.BuggifyConfig
		wantErr string
	}{
		{name: "zero", config: failpoint.BuggifyConfig{}},
		{
			name: "valid",
			config: failpoint.BuggifyConfig{
				Seed: 1, Activation: 0.25, Fire: 1, Error: buggifyErrName,
				Prefixes: []failpoint.BuggifyPrefix{{Prefix: "a:", Activation: 0, Fire: 0.5}},
				Include:  []string{"a:*"},
				Exclude:  []string{"a:b"},
			},
		},
		{name: "activation out of range", config: failpoint.BuggifyConfig{Activation: 1.5}, wantErr: "probability 1.5"},
		{name: "fire negative", config: failpoint.BuggifyConfig{Fire: -0.1}, wantErr: "probability -0.1"},
		{name: "unknown error", config: failpoint.BuggifyConfig{Error: "Oops"}, wantErr: `unknown error "Oops"`},
		{name: "empty prefix", config: failpoint.BuggifyConfig{Prefixes: []failpoint.BuggifyPrefix{{}}}, wantErr: "prefix must not be empty"},
		{
			name:    "duplicate prefix",
			config:  failpoint.BuggifyConfig{Prefixes: []failpoint.BuggifyPrefix{{Prefix: "a"}, {Prefix: "a"}}},
			wantErr: `duplicate buggify prefix "a"`,
		},
		{
			name:    "prefix probability",
			config:  failpoint.BuggifyConfig{Prefixes: []failpoint.BuggifyPrefix{{Prefix: "a", Fire: 2}}},
			wantErr: `probability 2 for prefix "a"`,
		},
		{name: "empty pattern", config: failpoint.BuggifyConfig{Exclude: []string{""}}, wantErr: "must not be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.config.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorContains(t, failpoint.NewRegistry().SetBuggify(tc.config), tc.wantErr)
		})
	}
}

func TestBuggifyConfig_Activated(t *testing.T) {
	t.Parallel()

	names := make([]string, 2000)
	for i := range names {
		names[i] = fmt.Sprintf("key.%d", i)
	}
	activated := func(config failpoint.BuggifyConfig) map[string]bool {
		set := map[string]bool{}
		for _, name := range names {
			if config.Activated(name) {
				set[name] = true
			}
		}
		return set
	}
	config := failpoint.BuggifyConfig{Seed: 42, Activation: 0.25}

	t.Run("is a function of the seed", func(t *testing.T) {
		t.Parallel()
		set := activated(config)
		require.Equal(t, set, activated(config))
		require.InDelta(t, 0.25, float64(len(set))/float64(len(names)), 0.03)
		require.NotEqual(t, set, activated(failpoint.BuggifyConfig{Seed: 43, Activation: 0.25}))
	})

	t.Run("is off without a seed", func(t *testing.T) {
		t.Parallel()
		require.Empty(t, activated(failpoint.BuggifyConfig{Activation: 1}))
	})

	t.Run("applies the longest prefix and the patterns", func(t *testing.T) {
		t.Parallel()
		config := failpoint.BuggifyConfig{
			Seed:       7,
			Activation: 1,
			Prefixes: []failpoint.BuggifyPrefix{
				{Prefix: "grpc:", Activation: 0},
				{Prefix: "grpc:/svc.A/", Activation: 1},
			},
			Include: []string{"grpc:*", "queue.*"},
			Exclude: []string{"grpc:/svc.A/Skip*", "*.internal"},
		}
		for name, want := range map[string]bool{
			"grpc:/svc.B/Call":     false, // grpc: prefix
			"grpc:/svc.A/Call":     true,  // longer prefix
			"grpc:/svc.A/SkipThis": false, // excluded
			"queue.forward":        true,  // base probability
			"queue.internal":       false, // excluded by a pattern with a leading *
			"store.update":         false, // not included
		} {
			require.Equal(t, want, config.Activated(name), name)
		}
	})
}

func TestBuggifyConfig_Draw(t *testing.T) {
	t.Parallel()
	config := failpoint.BuggifyConfig{Seed: 1}
	seen := map[float64]bool{}
	for n := range uint64(100) {
		d := config.Draw("key", n)
		require.GreaterOrEqual(t, d, 0.0)
		require.Less(t, d, 1.0)
		require.Equal(t, math.Float64bits(d), math.Float64bits(config.Draw("key", n)))
		seen[d] = true
	}
	require.Len(t, seen, 100)
	require.NotEqual(t, config.Draw("key", 1), config.Draw("other", 1))
	require.NotEqual(t, config.Draw("key", 1), failpoint.BuggifyConfig{Seed: 2}.Draw("key", 1))
}

func TestBuggify_Determinism(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := failpoint.BuggifyConfig{Seed: 99, Activation: 0.5, Fire: 0.5, Error: buggifyErrName}

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
		if e.Kind == failpoint.BuggifyActivationEvent {
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
			{Kind: failpoint.BuggifyActivationEvent, Key: failpoint.KeyFromName("1", ""), Activated: true},
			{Kind: failpoint.BuggifyFireEvent, Key: key, Action: failpoint.Return, Fired: true, Execution: 1},
			{Kind: failpoint.BuggifyFireEvent, Key: key, Action: failpoint.Return, Fired: true, Execution: 2},
			{Kind: failpoint.BuggifyFireEvent, Key: key, Action: failpoint.Return, Fired: true, Execution: 3},
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
		config.Prefixes = []failpoint.BuggifyPrefix{{Prefix: "2", Activation: 1, Fire: 1}}
		registry, _ := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}

		require.NoError(t, failpoint.Checkpoint(src, key2))
		require.ErrorIs(t, failpoint.Checkpoint(src, key1), errBuggify)
		require.True(t, failpoint.Buggify(src, key2))
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
			require.True(t, failpoint.Buggify(src, key))

			// A disabled failpoint still keeps BUGGIFY away from the key.
			fp.Disable()
			require.NoError(t, failpoint.Checkpoint(src, key))
			require.NoError(t, failpoint.BuggifyError(src, key))
			require.False(t, failpoint.Buggify(src, key))

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
		config := failpoint.BuggifyConfig{Seed: 5, Activation: 1, Fire: 0.25}
		registry, events := newBuggifyRegistry(t, config)
		src := registrySource{registry: registry}

		var fired int
		for range 2000 {
			if failpoint.Buggify(src, key1) {
				fired++
			}
		}
		require.InDelta(t, 500, fired, 75)
		require.Len(t, events(), fired+1)
		require.Equal(t, failpoint.Event{Kind: failpoint.BuggifyActivationEvent, Key: key1, Activated: true}, events()[0])
	})

	t.Run("BuggifyWithProb overrides the fire probability", func(t *testing.T) {
		t.Parallel()
		registry, _ := newBuggifyRegistry(t, failpoint.BuggifyConfig{Seed: 5, Activation: 1, Fire: 0})
		src := registrySource{registry: registry}

		require.False(t, failpoint.Buggify(src, key1))
		require.True(t, failpoint.BuggifyWithProb(src, key1, 1))
		require.False(t, failpoint.BuggifyWithProb(src, key1, 0))
	})

	t.Run("never fires for a key that is not activated", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, failpoint.BuggifyConfig{Seed: 5, Activation: 0, Fire: 1})
		src := registrySource{registry: registry}

		require.False(t, failpoint.BuggifyWithProb(src, key1, 1))
		require.False(t, failpoint.Buggify(src, key1))
		require.Equal(t, []failpoint.Event{{Kind: failpoint.BuggifyActivationEvent, Key: key1, Activated: false}}, events())
	})

	t.Run("is off without a registry", func(t *testing.T) {
		t.Parallel()
		require.False(t, failpoint.Buggify(registrySource{}, key1))
		require.NoError(t, failpoint.BuggifyError(registrySource{}, key1))
	})
}

func TestBuggify_Suppress(t *testing.T) {
	t.Parallel()
	registry, _ := newBuggifyRegistry(t, alwaysConfig())
	src := registrySource{registry: registry}
	require.True(t, registry.Armed())

	restore := registry.SuppressBuggify()
	restoreNested := registry.SuppressBuggify()
	require.False(t, registry.Armed())
	require.NoError(t, failpoint.Checkpoint(src, key1))
	require.False(t, failpoint.Buggify(src, key1))
	require.Equal(t, alwaysConfig(), registry.BuggifyConfig())

	restoreNested()
	restoreNested()
	require.False(t, registry.Armed())
	restore()
	require.True(t, registry.Armed())
	require.ErrorIs(t, failpoint.Checkpoint(src, key1), errBuggify)
}

func TestRegistry_SetBuggify(t *testing.T) {
	t.Parallel()

	t.Run("arms the registry while enabled", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		require.False(t, registry.Armed())
		require.Equal(t, failpoint.BuggifyConfig{}, registry.BuggifyConfig())

		require.NoError(t, registry.SetBuggify(alwaysConfig()))
		require.True(t, registry.Armed())
		require.Equal(t, 0, registry.Len())
		require.Equal(t, alwaysConfig(), registry.BuggifyConfig())

		registry.Set(key1, failpoint.NewFailpoint[error](key1))
		require.NoError(t, registry.SetBuggify(failpoint.BuggifyConfig{}))
		require.True(t, registry.Armed())
		registry.Delete(key1)
		require.False(t, registry.Armed())
	})

	t.Run("keeps the state of an unchanged configuration", func(t *testing.T) {
		t.Parallel()
		registry, events := newBuggifyRegistry(t, alwaysConfig())
		src := registrySource{registry: registry}
		require.Error(t, failpoint.Checkpoint(src, key1))

		require.NoError(t, registry.SetBuggify(alwaysConfig()))
		require.Error(t, failpoint.Checkpoint(src, key1))
		require.Equal(t, int64(2), events()[2].Execution)

		changed := alwaysConfig()
		changed.Seed = 2
		require.NoError(t, registry.SetBuggify(changed))
		require.Error(t, failpoint.Checkpoint(src, key1))
		require.Equal(t, []failpoint.Event{
			{Kind: failpoint.BuggifyActivationEvent, Key: key1, Activated: true},
			{Kind: failpoint.BuggifyFireEvent, Key: key1, Action: failpoint.Return, Fired: true, Execution: 1},
		}, events()[3:])
	})
}

func TestBuggify_OffPathDoesNotAllocate(t *testing.T) {
	registerBuggifyError()
	suppressed := failpoint.NewRegistry()
	require.NoError(t, suppressed.SetBuggify(alwaysConfig()))
	restore := suppressed.SuppressBuggify()
	defer restore()

	for name, registry := range map[string]failpoint.Registry{
		"no registry":    nil,
		"empty registry": failpoint.NewRegistry(),
		"suppressed":     suppressed,
	} {
		var src failpoint.Source = registrySource{registry: registry}
		key := key1.ForScope("ns")
		allocs := testing.AllocsPerRun(100, func() {
			_ = failpoint.Buggify(src, key)
			_ = failpoint.BuggifyWithProb(src, key, 1)
			_ = failpoint.BuggifyError(src, key)
			_ = failpoint.Checkpoint(src, key)
		})
		require.Zero(t, allocs, name)
	}
}

func TestRegistry_SetBuggifyCopiesTheConfiguration(t *testing.T) {
	t.Parallel()
	config := failpoint.BuggifyConfig{
		Seed: 1, Activation: 1, Fire: 1,
		Prefixes: []failpoint.BuggifyPrefix{{Prefix: "a", Activation: 1, Fire: 1}},
		Include:  []string{"a*"},
		Exclude:  []string{"ab"},
	}
	registry := failpoint.NewRegistry()
	require.NoError(t, registry.SetBuggify(config))

	config.Prefixes[0].Activation = 0
	config.Include[0] = "x"
	got := registry.BuggifyConfig()
	got.Exclude[0] = "a*"
	require.Equal(t, failpoint.BuggifyConfig{
		Seed: 1, Activation: 1, Fire: 1,
		Prefixes: []failpoint.BuggifyPrefix{{Prefix: "a", Activation: 1, Fire: 1}},
		Include:  []string{"a*"},
		Exclude:  []string{"ab"},
	}, registry.BuggifyConfig())
	require.True(t, failpoint.Buggify(registrySource{registry: registry}, failpoint.KeyFromName("ac", "")))
}

func TestBuggifyError_Shadows(t *testing.T) {
	t.Parallel()
	registry, _ := newBuggifyRegistry(t, alwaysConfig())
	src := registrySource{registry: registry}
	key := key1.ForScope("ns")
	require.ErrorIs(t, failpoint.BuggifyError(src, key, key2.ForScope("ns")), errBuggify)

	// A disabled failpoint of the unscoped shadow key is enough.
	registry.Set(key2, failpoint.NewFailpoint[error](key2))
	require.NoError(t, failpoint.BuggifyError(src, key, key2.ForScope("ns")))
	require.ErrorIs(t, failpoint.BuggifyError(src, key, key3), errBuggify)
}

func TestBuggify_UnselectedNamesDoNotUseTheBudget(t *testing.T) {
	t.Parallel()
	config := alwaysConfig()
	config.Exclude = []string{"excluded.*"}
	registry, _ := newBuggifyRegistry(t, config)
	src := registrySource{registry: registry}

	for i := range 20000 {
		require.NoError(t, failpoint.BuggifyError(src, failpoint.KeyFromName(fmt.Sprintf("excluded.%d", i), "")))
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
		case failpoint.BuggifyActivationEvent:
			require.False(t, activated[e.Key.Name()], "activation reported twice for %s", e.Key)
			activated[e.Key.Name()] = true
		case failpoint.BuggifyFireEvent:
			require.True(t, activated[e.Key.Name()], "fire before activation for %s", e.Key)
		default:
		}
	}
	require.Len(t, activated, 50)
}

func TestBuggify_ScopesCountExecutionsSeparately(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := failpoint.BuggifyConfig{Seed: 3, Activation: 1, Fire: 0.5, Error: buggifyErrName}
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

func TestRegistry_BuggifyStats(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := failpoint.BuggifyConfig{
		Seed: 3, Activation: 1, Fire: 1, Error: buggifyErrName,
		Prefixes: []failpoint.BuggifyPrefix{{Prefix: "2", Activation: 0}},
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

	require.Equal(t, []failpoint.BuggifyKeyStats{
		{Name: "1", Activated: true, Hits: 3, Fires: 3},
		{Name: "2", Activated: false, Hits: 2, Fires: 0},
	}, registry.BuggifyStats())

	require.NoError(t, registry.SetBuggify(failpoint.BuggifyConfig{}))
	require.Empty(t, registry.BuggifyStats())
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
