package buggify_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
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
	return buggify.Config{Seed: 1, Keys: allKeys(1, 1, buggifyErrName)}
}

// newBuggifyRegistry returns a registry with BUGGIFY configured, the Buggify instance, and the events it reports.
func newBuggifyRegistry(t *testing.T, config buggify.Config) (*failpoint.Registry, *buggify.Buggify, func() []buggify.Event) {
	t.Helper()
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	var mu sync.Mutex
	var events []buggify.Event
	b.OnDecision(func(e buggify.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	})
	require.NoError(t, b.Set(config))
	return registry, b, func() []buggify.Event {
		mu.Lock()
		defer mu.Unlock()
		return append([]buggify.Event(nil), events...)
	}
}

// undeclaredPrefixes are the prefixes of the names that the tests build keys of, as the code under
// test does from the method of a request.
var undeclaredPrefixes = map[string]failpoint.KeyPrefix[error]{
	"key.":      failpoint.ReserveKeyPrefix[error]("key."),
	"excluded.": failpoint.ReserveKeyPrefix[error]("excluded."),
	"ac":        failpoint.ReserveKeyPrefix[error]("ac"),
	"http:":     failpoint.ReserveKeyPrefix[error]("http:"),
}

// undeclared returns the key of the failpoint that the code under test builds from name.
func undeclared(name, scope string) failpoint.Key[error] {
	for prefix, keys := range undeclaredPrefixes {
		if suffix, ok := strings.CutPrefix(name, prefix); ok {
			return keys.Key(suffix).ForScope(scope)
		}
	}
	//nolint:forbidigo // a missing prefix is a programming error of the test
	panic("buggify test: reserve a prefix for the key " + name)
}

func TestBuggify_Determinism(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := buggify.Config{Seed: 99, Keys: allKeys(0.5, 0.5, buggifyErrName)}

	// run executes each key a few times and returns which executions failed.
	run := func() (failed []string, events []buggify.Event) {
		registry, _, getEvents := newBuggifyRegistry(t, config)
		src := registry
		for i := range 20 {
			key := undeclared(fmt.Sprintf("key.%d", i), "")
			for n := range 5 {
				if failpoint.ExecuteError(src, key) != nil {
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
		if e.Kind == buggify.EventActivation {
			require.Equal(t, config.Activated(e.Name), e.Activated)
		}
	}
}

func TestBuggify_Execute(t *testing.T) {
	t.Parallel()

	t.Run("returns the configured error for a key with nothing registered", func(t *testing.T) {
		t.Parallel()
		registry, _, events := newBuggifyRegistry(t, alwaysConfig())
		src := registry
		key := key1.ForScope("ns")

		err := failpoint.ExecuteError(src, key)
		require.ErrorIs(t, err, errBuggify)
		require.ErrorContains(t, err, "BUGGIFY: injected by failpoint 1")
		result := failpoint.ExecuteInOrder(src, failpoint.ExecuteOptions{}, key)
		require.ErrorIs(t, result.Value, errBuggify)
		require.True(t, result.Set)
		require.True(t, result.Fired)

		require.Equal(t, []buggify.Event{
			{Kind: buggify.EventActivation, Name: key1.Name(), Activated: true},
			{Kind: buggify.EventFire, Name: key.Name(), Scope: key.Scope(), Action: failpoint.ActionReturn, ExecutionNumber: 1},
			{Kind: buggify.EventFire, Name: key.Name(), Scope: key.Scope(), Action: failpoint.ActionReturn, ExecutionNumber: 2},
		}, events())
	})

	t.Run("Execute only injects into error call sites", func(t *testing.T) {
		t.Parallel()
		registry, _, events := newBuggifyRegistry(t, alwaysConfig())
		src := registry

		_, set := failpoint.Execute(src, anyKey)
		require.False(t, set)
		_, set = failpoint.Execute(src, stringKey)
		require.False(t, set)
		require.Empty(t, events())
	})

	t.Run("does not inject error without an error for the key", func(t *testing.T) {
		t.Parallel()
		config := alwaysConfig()
		// The first matching entry wins, so key "2" has no error and key "1" has one.
		config.Keys = []buggify.Key{{Match: "2", ActivationProbability: 1, FireProbability: 1}, config.Keys[0]}
		registry, _, _ := newBuggifyRegistry(t, config)
		src := registry

		require.NoError(t, failpoint.ExecuteError(src, key2))
		require.ErrorIs(t, failpoint.ExecuteError(src, key1), errBuggify)
		require.True(t, failpoint.ExecuteSwitch(src, key2))
	})

	t.Run("ExplicitOnly ignores BUGGIFY", func(t *testing.T) {
		t.Parallel()
		registry, _, events := newBuggifyRegistry(t, alwaysConfig())
		result := failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{ExplicitOnly: true}, key1)
		require.Equal(t, failpoint.Result[error]{}, result)
		require.Empty(t, events())
	})
}

func TestBuggify_Precedence(t *testing.T) {
	t.Parallel()
	registered := errors.New("registered")

	for _, tc := range []struct {
		name          string
		registeredKey failpoint.Key[error]
	}{
		{name: "same key", registeredKey: key1.ForScope("ns")},
		{name: "unscoped key", registeredKey: key1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry, _, events := newBuggifyRegistry(t, alwaysConfig())
			src := registry
			key := key1.ForScope("ns")
			fp := failpoint.New[error](tc.registeredKey)
			fp.Return(registered)
			registry.Set(fp)

			require.ErrorIs(t, failpoint.ExecuteError(src, key), registered)
			require.True(t, failpoint.ExecuteSwitch(src, key))

			// A disabled failpoint still keeps BUGGIFY away from the key.
			fp.Disable()
			require.NoError(t, failpoint.ExecuteError(src, key))
			require.False(t, failpoint.ExecuteSwitch(src, key))

			require.Empty(t, events())

			registry.Delete(tc.registeredKey)
			require.ErrorIs(t, failpoint.ExecuteError(src, key), errBuggify)
		})
	}

	t.Run("a failpoint of another scope does not", func(t *testing.T) {
		t.Parallel()
		registry, _, _ := newBuggifyRegistry(t, alwaysConfig())
		fp := failpoint.New[error](key1.ForScope("other"))
		fp.Return(registered)
		registry.Set(fp)

		require.ErrorIs(t, failpoint.ExecuteError(registry, key1.ForScope("ns")), errBuggify)
	})
}

func TestBuggify_Branch(t *testing.T) {
	t.Parallel()

	t.Run("fires with the configured probability", func(t *testing.T) {
		t.Parallel()
		config := buggify.Config{Seed: 5, Keys: allKeys(1, 0.25, "")}
		registry, _, events := newBuggifyRegistry(t, config)
		src := registry

		var fired int
		for range 2000 {
			if failpoint.ExecuteSwitch(src, key1) {
				fired++
			}
		}
		require.InDelta(t, 500, fired, 75)
		require.Len(t, events(), fired+1)
		require.Equal(t, buggify.Event{Kind: buggify.EventActivation, Name: key1.Name(), Activated: true}, events()[0])
	})

	t.Run("never fires for a key that is not activated", func(t *testing.T) {
		t.Parallel()
		registry, _, events := newBuggifyRegistry(t, buggify.Config{Seed: 5, Keys: allKeys(0, 1, "")})
		src := registry

		require.False(t, failpoint.ExecuteSwitch(src, key1))
		require.Equal(t, []buggify.Event{{Kind: buggify.EventActivation, Name: key1.Name(), Activated: false}}, events())
	})

	t.Run("is off without a registry", func(t *testing.T) {
		t.Parallel()
		require.False(t, failpoint.ExecuteSwitch((*failpoint.Registry)(nil), key1))
		require.NoError(t, failpoint.ExecuteError((*failpoint.Registry)(nil), key1))
	})
}

func TestBuggify_Set(t *testing.T) {
	t.Parallel()

	t.Run("arms the registry while enabled", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		b := buggify.New(registry)
		require.False(t, registry.MayFire())
		require.Nil(t, registry.Policy())
		require.Equal(t, buggify.Config{}, b.Config())

		require.NoError(t, b.Set(alwaysConfig()))
		require.True(t, registry.MayFire())
		require.Same(t, b, registry.Policy())
		require.Equal(t, alwaysConfig(), b.Config())

		registry.Set(failpoint.New[error](key1))
		require.NoError(t, b.Set(buggify.Config{}))
		require.Nil(t, registry.Policy())
		require.True(t, registry.MayFire())
		registry.Delete(key1)
		require.False(t, registry.MayFire())
	})

	t.Run("keeps the state of an unchanged configuration", func(t *testing.T) {
		t.Parallel()
		registry, b, events := newBuggifyRegistry(t, alwaysConfig())
		src := registry
		require.Error(t, failpoint.ExecuteError(src, key1))

		require.NoError(t, b.Set(alwaysConfig()))
		require.Error(t, failpoint.ExecuteError(src, key1))
		require.Equal(t, int64(2), events()[2].ExecutionNumber)

		changed := alwaysConfig()
		changed.Seed = 2
		require.NoError(t, b.Set(changed))
		require.Error(t, failpoint.ExecuteError(src, key1))
		require.Equal(t, []buggify.Event{
			{Kind: buggify.EventActivation, Name: key1.Name(), Activated: true},
			{Kind: buggify.EventFire, Name: key1.Name(), Scope: key1.Scope(), Action: failpoint.ActionReturn, ExecutionNumber: 1},
		}, events()[3:])
	})
}

func TestBuggify_SetCopiesTheConfiguration(t *testing.T) {
	t.Parallel()
	config := buggify.Config{
		Seed: 1,
		Keys: []buggify.Key{
			{Match: "ab", ActivationProbability: 0, FireProbability: 0},
			{Match: "a*", ActivationProbability: 1, FireProbability: 1},
		},
	}
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	require.NoError(t, b.Set(config))

	config.Keys[1].ActivationProbability = 0
	got := b.Config()
	got.Keys[0].Match = "a*"
	require.Equal(t, buggify.Config{
		Seed: 1,
		Keys: []buggify.Key{
			{Match: "ab", ActivationProbability: 0, FireProbability: 0},
			{Match: "a*", ActivationProbability: 1, FireProbability: 1},
		},
	}, b.Config())
	require.True(t, failpoint.ExecuteSwitch(registry, undeclared("ac", "")))
}

func TestExecuteInOrder_Buggify(t *testing.T) {
	t.Parallel()
	registry, _, _ := newBuggifyRegistry(t, alwaysConfig())
	key := key1.ForScope("ns")
	errorOf := func(result failpoint.Result[error]) error { return result.Value }
	opts := failpoint.ExecuteOptions{PolicyKey: key.Name(), PolicyScope: key.Scope()}
	require.ErrorIs(t, errorOf(failpoint.ExecuteInOrder(registry, opts, key2.ForScope("ns"))), errBuggify)

	// A disabled failpoint of the unscoped key of another key is enough to keep BUGGIFY away.
	registry.Set(failpoint.New[error](key2))
	require.NoError(t, errorOf(failpoint.ExecuteInOrder(registry, opts, key2.ForScope("ns"))))
	require.ErrorIs(t, errorOf(failpoint.ExecuteInOrder(registry, opts, key3)), errBuggify)
}

func TestBuggify_ManyUnselectedNames(t *testing.T) {
	t.Parallel()
	// The names that no entry matches are never activated.
	config := alwaysConfig()
	config.Keys = []buggify.Key{{Match: "1", ActivationProbability: 1, FireProbability: 1, Error: buggifyErrName}}
	registry, _, _ := newBuggifyRegistry(t, config)
	src := registry

	for i := range 20000 {
		require.NoError(t, failpoint.ExecuteError(src, undeclared(fmt.Sprintf("excluded.%d", i), "")))
	}
	require.ErrorIs(t, failpoint.ExecuteError(src, key1), errBuggify)
}

// TestBuggify_ManyNames checks that BUGGIFY keeps deciding for names beyond the number that it
// once bounded, so a run that executes many names doesn't stop injecting into the later ones.
func TestBuggify_ManyNames(t *testing.T) {
	t.Parallel()
	registry, _, _ := newBuggifyRegistry(t, alwaysConfig())

	for i := range 20000 {
		require.ErrorIs(t, failpoint.ExecuteError(registry, undeclared(fmt.Sprintf("key.%d", i), "")), errBuggify, i)
	}
}

func TestBuggify_ActivationPrecedesFires(t *testing.T) {
	t.Parallel()
	registry, _, events := newBuggifyRegistry(t, alwaysConfig())
	src := registry

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 50 {
				_ = failpoint.ExecuteError(src, undeclared(fmt.Sprintf("key.%d", i), ""))
			}
		})
	}
	wg.Wait()

	activated := map[string]bool{}
	for _, e := range events() {
		switch e.Kind {
		case buggify.EventActivation:
			require.False(t, activated[e.Name], "activation reported twice for %s", e.Name)
			activated[e.Name] = true
		case buggify.EventFire:
			require.True(t, activated[e.Name], "fire before activation for %s", e.Name)
		default:
		}
	}
	require.Len(t, activated, 50)
}

func TestBuggify_FiresOfAScopeAreReportedInExecutionOrder(t *testing.T) {
	t.Parallel()
	registry, _, events := newBuggifyRegistry(t, alwaysConfig())

	const goroutines, executions = 8, 500
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range executions {
				_ = failpoint.ExecuteError(registry, key1.ForScope("scope"))
				_ = failpoint.ExecuteError(registry, key1)
			}
		})
	}
	wg.Wait()

	last := map[string]int64{}
	for _, e := range events() {
		if e.Kind != buggify.EventFire {
			continue
		}
		require.Equal(t, last[e.Scope]+1, e.ExecutionNumber, "fires of scope %q are reported out of order", e.Scope)
		last[e.Scope] = e.ExecutionNumber
	}
	require.Equal(t, map[string]int64{"scope": goroutines * executions, "": goroutines * executions}, last)
}

func TestBuggify_ScopesCountExecutionsSeparately(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := buggify.Config{Seed: 3, Keys: allKeys(1, 0.5, buggifyErrName)}
	require.Equal(t, math.Float64bits(config.Draw("key", 4)), math.Float64bits(config.DrawScoped("key", "", 4)))
	require.NotEqual(t, config.DrawScoped("key", "a", 4), config.DrawScoped("key", "b", 4))

	// failures returns which of 40 executions in scope "b" fail, with interleaved executions in
	// scope "a" if busy.
	failures := func(busy bool) []bool {
		registry, _, _ := newBuggifyRegistry(t, config)
		src := registry
		var failed []bool
		for range 40 {
			if busy {
				_ = failpoint.ExecuteError(src, key1.ForScope("a"))
				_ = failpoint.ExecuteError(src, key1)
			}
			failed = append(failed, failpoint.ExecuteError(src, key1.ForScope("b")) != nil)
		}
		return failed
	}
	quiet := failures(false)
	require.Contains(t, quiet, true)
	require.Contains(t, quiet, false)
	require.Equal(t, quiet, failures(true))
}

func TestBuggify_ErrorMessageIsValidUTF8(t *testing.T) {
	t.Parallel()
	registry, _, _ := newBuggifyRegistry(t, alwaysConfig())
	err := failpoint.ExecuteError(registry, undeclared("http:GET /\xff", "ns"))
	require.ErrorIs(t, err, errBuggify)
	require.True(t, utf8.ValidString(err.Error()), err.Error())
	require.Contains(t, err.Error(), "BUGGIFY: injected by failpoint http:GET /�")
}

func TestBuggify_FollowsTheActivePolicy(t *testing.T) {
	t.Parallel()
	registry, _, _ := newBuggifyRegistry(t, alwaysConfig())
	src := registry
	require.True(t, failpoint.ExecuteSwitch(src, key1))

	// Turned off in the registry, while an unrelated failpoint keeps the registry armed.
	registry.SetPolicy(nil)
	registry.Set(failpoint.New[error](key2))
	require.True(t, registry.MayFire())
	require.False(t, failpoint.ExecuteSwitch(src, key1))
	require.NoError(t, failpoint.ExecuteError(src, key1))
}

func TestBuggify_ObserverMayExecuteOtherKeys(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	var inner []error
	b.OnDecision(func(e buggify.Event) {
		// The observer executes the key that was activated first, so the executions nest.
		if e.Kind == buggify.EventActivation && e.Name == key1.Name() {
			inner = append(inner, failpoint.ExecuteError(registry, key2))
		}
	})
	require.NoError(t, b.Set(alwaysConfig()))

	require.ErrorIs(t, executeBefore(t, registry, key1), errBuggify)
	require.Len(t, inner, 1)
	require.ErrorIs(t, inner[0], errBuggify)
}

// executeBefore executes key in a goroutine and fails the test if that doesn't return.
func executeBefore(t *testing.T, registry *failpoint.Registry, key failpoint.Key[error]) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- failpoint.ExecuteError(registry, key) }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("executing %s deadlocked", key)
		return nil
	}
}

// TestBuggify_ObserverMayExecuteTheKeyOfItsEvent covers observers that execute each other's keys: A's
// observer executes B, and B's executes A, so one goroutine nests A, B and A again.
func TestBuggify_ObserverMayExecuteTheKeyOfItsEvent(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	other := map[string]failpoint.Key[error]{key1.Name(): key2, key2.Name(): key1}
	var mu sync.Mutex
	var events []buggify.Event
	b.OnDecision(func(e buggify.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
		if e.Kind == buggify.EventActivation {
			_ = failpoint.ExecuteError(registry, other[e.Name])
		}
	})
	require.NoError(t, b.Set(alwaysConfig()))

	require.ErrorIs(t, executeBefore(t, registry, key1), errBuggify)

	// The events of the nested executions follow those of the execution that delivers them, and
	// the activation of each name precedes its fires.
	mu.Lock()
	defer mu.Unlock()
	type decision struct {
		kind buggify.EventKind
		name string
		n    int64
	}
	var got []decision
	for _, e := range events {
		got = append(got, decision{e.Kind, e.Name, e.ExecutionNumber})
	}
	require.Equal(t, []decision{
		{buggify.EventActivation, key1.Name(), 0},
		{buggify.EventFire, key1.Name(), 1},
		{buggify.EventActivation, key2.Name(), 0},
		{buggify.EventFire, key2.Name(), 1},
		{buggify.EventFire, key1.Name(), 2},
	}, got)
}

// TestBuggify_ConcurrentObserversMayExecuteEachOthersKeys covers goroutines that activate A and B
// at the same time, with observers that execute the other key.
func TestBuggify_ConcurrentObserversMayExecuteEachOthersKeys(t *testing.T) {
	t.Parallel()
	for range 200 {
		registry := failpoint.NewRegistry()
		b := buggify.New(registry)
		other := map[string]failpoint.Key[error]{key1.Name(): key2, key2.Name(): key1}
		b.OnDecision(func(e buggify.Event) {
			if e.Kind == buggify.EventActivation {
				_ = failpoint.ExecuteError(registry, other[e.Name])
			}
		})
		require.NoError(t, b.Set(alwaysConfig()))

		var wg sync.WaitGroup
		for _, key := range []failpoint.Key[error]{key1, key2} {
			wg.Go(func() { _ = failpoint.ExecuteError(registry, key) })
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("observers that execute each other's keys deadlocked")
		}
	}
}

func TestBuggify_DeliversEventsAfterAPanickingObserver(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	var mu sync.Mutex
	var names []string
	b.OnDecision(func(e buggify.Event) {
		mu.Lock()
		names = append(names, e.Name)
		mu.Unlock()
		if len(names) == 1 {
			panic("observer")
		}
	})
	require.NoError(t, b.Set(alwaysConfig()))

	require.PanicsWithValue(t, "observer", func() { _ = failpoint.ExecuteError(registry, key1) })
	// The events that the panic left are delivered by the next execution.
	require.ErrorIs(t, failpoint.ExecuteError(registry, key2), errBuggify)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{key1.Name(), key1.Name(), key2.Name(), key2.Name()}, names)
}

func TestBuggify_ScopesBeyondTheBoundKeepFiring(t *testing.T) {
	t.Parallel()
	registry, _, events := newBuggifyRegistry(t, alwaysConfig())

	const scopes = 1<<16 + 100
	var failed int
	for i := range scopes {
		if failpoint.ExecuteError(registry, key1.ForScope(fmt.Sprintf("ns-%d", i))) != nil {
			failed++
		}
	}
	require.Equal(t, scopes, failed)

	var limits int
	for _, e := range events() {
		if e.Kind == buggify.EventScopeLimit {
			limits++
		}
	}
	require.Equal(t, 1, limits)
}

func TestExecuteSwitchWithPolicyScope_DecidesInTheGivenScope(t *testing.T) {
	t.Parallel()
	registerBuggifyError()
	config := buggify.Config{Seed: 7, Keys: []buggify.Key{{Match: "*", ActivationProbability: 1, FireProbability: 0.5, Error: buggifyErrName}}}

	// decisions returns whether each of 40 executions fires, on a registry of its own.
	decisions := func(key failpoint.Key[error], scope string) []bool {
		registry, _, _ := newBuggifyRegistry(t, config)
		var fired []bool
		for range 40 {
			fired = append(fired, failpoint.ExecuteSwitchWithPolicyScope(registry, key, scope))
		}
		return fired
	}

	// Keys of other scopes, such as a recreated namespace with another ID, decide the same when
	// BUGGIFY's scope is the same, and as ExecuteSwitch does when it is the scope of the key.
	byName := decisions(key1.ForScope("id-1"), "name")
	require.Equal(t, byName, decisions(key1.ForScope("id-2"), "name"))
	require.NotEqual(t, byName, decisions(key1.ForScope("id-1"), "other"))
	require.Equal(t, decisions(key1.ForScope("id-1"), "id-1"), decisions(key1.ForScope("id-1"), key1.ForScope("id-1").Scope()))
	require.Contains(t, byName, true)
	require.Contains(t, byName, false)

	t.Run("the events carry the scope", func(t *testing.T) {
		t.Parallel()
		registry, _, events := newBuggifyRegistry(t, alwaysConfig())
		require.True(t, failpoint.ExecuteSwitchWithPolicyScope(registry, key1.ForScope("id-1"), "name"))
		require.Contains(t, events(), buggify.Event{
			Kind: buggify.EventFire, Name: key1.Name(), Scope: "name", Action: failpoint.ActionOn, ExecutionNumber: 1,
		})
	})

	t.Run("a failpoint matches by the scope of the key", func(t *testing.T) {
		t.Parallel()
		registry, _, _ := newBuggifyRegistry(t, config)
		fp := failpoint.New[error](key1.ForScope("id-1"))
		registry.Set(fp)
		// A failpoint without an action decides, so BUGGIFY doesn't, in any scope.
		for range 20 {
			require.False(t, failpoint.ExecuteSwitchWithPolicyScope(registry, key1.ForScope("id-1"), "name"))
		}
		fp.On()
		require.True(t, failpoint.ExecuteSwitchWithPolicyScope(registry, key1.ForScope("id-1"), "name"))
		// Its scope is the ID, not the scope that BUGGIFY uses.
		scopeNameKey := key1.ForScope("name")
		decided := false
		for range 40 {
			decided = decided || failpoint.ExecuteSwitchWithPolicyScope(registry, scopeNameKey, "name")
		}
		require.True(t, decided, "BUGGIFY decides for a key without a failpoint of its scope")
	})
}

// blockingObserver returns an observer that blocks on its first event until release is closed,
// and the events that it received.
func blockingObserver(b *buggify.Buggify) (release func(), blocked <-chan struct{}, received func() []buggify.Event) {
	var mu sync.Mutex
	var events []buggify.Event
	gate := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	b.OnDecision(func(e buggify.Event) {
		once.Do(func() {
			close(entered)
			<-gate
		})
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	var releaseOnce sync.Once
	return func() { releaseOnce.Do(func() { close(gate) }) }, entered, func() []buggify.Event {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(events)
	}
}

// TestBuggify_StopObservingDeliversTheQueuedEvents covers a shutdown while an execution delivers:
// the events that it leaves queued reach the observer before it is removed.
func TestBuggify_StopObservingDeliversTheQueuedEvents(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	release, blocked, received := blockingObserver(b)
	require.NoError(t, b.Set(alwaysConfig()))

	first := make(chan error, 1)
	go func() { first <- failpoint.ExecuteError(registry, key1) }()
	<-blocked
	// This execution decides while the first delivers, so its events are queued.
	require.ErrorIs(t, executeBefore(t, registry, key2), errBuggify)

	stopped := make(chan error, 1)
	go func() { stopped <- b.StopObserving(context.Background()) }()
	select {
	case err := <-stopped:
		t.Fatalf("StopObserving returned %v before the queued events were delivered", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	require.NoError(t, <-stopped)
	require.ErrorIs(t, <-first, errBuggify)

	var names []string
	for _, e := range received() {
		names = append(names, e.Name)
	}
	require.Equal(t, []string{key1.Name(), key1.Name(), key2.Name(), key2.Name()}, names,
		"the activation and the fire of both keys")

	// The observer is gone and the queue doesn't accept events: a later execution delivers none.
	require.ErrorIs(t, failpoint.ExecuteError(registry, key3), errBuggify)
	require.Len(t, received(), 4)

	// OnDecision sets an observer again, and the queue accepts events again.
	var got atomic.Int64
	b.OnDecision(func(buggify.Event) { got.Add(1) })
	require.ErrorIs(t, failpoint.ExecuteError(registry, key100), errBuggify)
	require.EqualValues(t, 2, got.Load())
}

// TestBuggify_StopObservingIsBoundedByItsContext covers an observer that never returns.
func TestBuggify_StopObservingIsBoundedByItsContext(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	release, blocked, received := blockingObserver(b)
	t.Cleanup(release)
	require.NoError(t, b.Set(alwaysConfig()))

	go func() { _ = failpoint.ExecuteError(registry, key1) }()
	<-blocked
	require.ErrorIs(t, executeBefore(t, registry, key2), errBuggify)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, b.StopObserving(ctx), context.DeadlineExceeded)
	release()
	// The observer that was blocked returns, and the events that it has not received are dropped.
	await.RequireTrue(t, func() bool { return len(received()) == 1 }, 10*time.Second, time.Millisecond)
}
