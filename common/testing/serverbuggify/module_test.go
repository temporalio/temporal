package serverbuggify

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"gopkg.in/yaml.v3"
)

// newApp returns an app with the module around a server hook, as the server graph has it.
func newApp(t *testing.T, registry *failpoint.Registry, client dynamicconfig.Client, server fx.Hook) *fxtest.App {
	return fxtest.New(t,
		fx.Supply(
			registry,
			fx.Annotate(client, fx.As(new(dynamicconfig.Client))),
			fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger))),
		),
		Module,
		fx.Invoke(func(lc fx.Lifecycle) { lc.Append(server) }),
	)
}

// effective returns the keys that the registry's BUGGIFY applies to, by match.
func effective(t *testing.T, registry *failpoint.Registry) []buggify.Key {
	t.Helper()
	b, ok := registry.Policy().(*buggify.Buggify)
	require.True(t, ok)
	return b.Config().Keys
}

func startedWith(t *testing.T, value any) (*failpoint.Registry, *fxtest.App) {
	t.Helper()
	client := dynamicconfig.NewMemoryClient()
	client.OverrideSetting(Setting, value)
	registry := failpoint.NewRegistry()
	app := newApp(t, registry, client, fx.Hook{})
	app.RequireStart()
	t.Cleanup(app.RequireStop)
	return registry, app
}

var defaults = []buggify.Key{
	{Match: "grpc:/grpc.health.v1.Health/*"},
	{Match: "grpc:*", ActivationProbability: 0.1, FireProbability: 0.01, Error: "Unavailable"},
	{Match: "http:*", ActivationProbability: 0.1, FireProbability: 0.01, Error: "Unavailable"},
	{Match: "matching.skipSyncMatch", ActivationProbability: 0.25, FireProbability: 0.25},
}

func TestDefaultKeys(t *testing.T) {
	t.Parallel()
	require.Equal(t, matching.SkipSyncMatchKey.Name(), defaultKeys[len(defaultKeys)-1].Match)
}

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("a disabled registry installs nothing, whatever the value is", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, map[string]any{"seed": "no", "unknown": 1})
		started := false
		app := newApp(t, nil, client, fx.Hook{OnStart: func(context.Context) error {
			started = true
			return nil
		}})
		app.RequireStart()
		app.RequireStop()
		require.True(t, started)
	})

	t.Run("without a seed BUGGIFY stays off", func(t *testing.T) {
		t.Parallel()
		for _, value := range []any{nil, map[string]any{}, map[string]any{"seed": 0, "defaults": false}} {
			registry, _ := startedWith(t, value)
			require.Nil(t, registry.Policy())
		}
	})

	t.Run("a seed enables the defaults", func(t *testing.T) {
		t.Parallel()
		registry, _ := startedWith(t, map[string]any{"seed": 7})
		require.Equal(t, defaults, effective(t, registry))
	})

	t.Run("an entry of a default merges into it, field by field, and keeps its position", func(t *testing.T) {
		t.Parallel()
		registry, _ := startedWith(t, map[string]any{"seed": 7, "keys": []any{
			map[string]any{"match": "matching.skipSyncMatch", "activationProbability": 1},
			map[string]any{"match": "grpc:*", "fireProbability": 1},
		}})
		require.Equal(t, []buggify.Key{
			defaults[0], // The health check stays ahead of the overridden grpc:*.
			{Match: "grpc:*", ActivationProbability: 0.1, FireProbability: 1, Error: "Unavailable"},
			defaults[2],
			{Match: "matching.skipSyncMatch", ActivationProbability: 1, FireProbability: 0.25},
		}, effective(t, registry))
	})

	t.Run("an entry with a new match comes before the defaults, in order", func(t *testing.T) {
		t.Parallel()
		registry, _ := startedWith(t, map[string]any{"seed": 7, "keys": []any{
			map[string]any{"match": "x:*", "fireProbability": 0.5, "error": "Unavailable"},
			map[string]any{"match": "y", "activationProbability": 1},
		}})
		require.Equal(t, append([]buggify.Key{
			{Match: "x:*", FireProbability: 0.5, Error: "Unavailable"},
			{Match: "y", ActivationProbability: 1},
		}, defaults...), effective(t, registry))
	})

	t.Run("a match that an entry repeats is a duplicate", func(t *testing.T) {
		t.Parallel()
		_, _, err := decode(map[string]any{"seed": 7, "keys": []any{map[string]any{"match": "grpc:*"}, map[string]any{"match": "grpc:*"}}})
		require.ErrorContains(t, err, "duplicate")
	})

	t.Run("without the defaults only the entries apply, in order", func(t *testing.T) {
		t.Parallel()
		registry, _ := startedWith(t, map[string]any{"seed": 7, "defaults": false, "keys": []any{
			map[string]any{"match": "grpc:*", "activationProbability": 1},
			map[string]any{"match": "a"},
		}})
		require.Equal(t, []buggify.Key{{Match: "grpc:*", ActivationProbability: 1}, {Match: "a"}}, effective(t, registry))
	})

	t.Run("reads the typed value, nested maps and the field names of YAML", func(t *testing.T) {
		t.Parallel()
		var fromYAML any
		require.NoError(t, yaml.Unmarshal([]byte("seed: 7\ndefaults: false\nkeys:\n  - match: a\n    activationProbability: 1\n    error: Unavailable\n"), &fromYAML))
		want := []buggify.Key{{Match: "a", ActivationProbability: 1, Error: "Unavailable"}}
		for _, value := range []any{
			fromYAML,
			Config{Seed: 7, Defaults: new(false), Keys: []Key{{Match: "a", ActivationProbability: new(1.0), Error: "Unavailable"}}},
			map[string]any{"SEED": 7, "Defaults": false, "keys": []map[any]any{{"MATCH": "a", "activationprobability": 1.0, "error": "Unavailable"}}},
		} {
			registry, _ := startedWith(t, value)
			require.Equal(t, want, effective(t, registry))
		}
	})
}

func TestModule_InvalidValue(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		value any
		err   string
	}{
		"unknown field":         {map[string]any{"seed": 1, "activation": 1}, "invalid keys: activation"},
		"unknown key field":     {map[string]any{"seed": 1, "keys": []any{map[string]any{"match": "a", "prob": 1}}}, "invalid keys: prob"},
		"bad probability":       {map[string]any{"seed": 1, "keys": []any{map[string]any{"match": "a", "fireProbability": 1.5}}}, "not in [0, 1]"},
		"unknown error":         {map[string]any{"seed": 1, "keys": []any{map[string]any{"match": "a", "error": "Nope"}}}, `unknown error "Nope"`},
		"weak types":            {map[string]any{"seed": "1"}, "expected type 'uint64'"},
		"negative seed":         {map[string]any{"seed": -1}, "-1 overflows uint"},
		"not a map":             {[]any{1}, "expected a map"},
		"non-string key":        {map[string]any{"seed": 1, "keys": []any{map[any]any{"match": "x", 42: "typo"}}}, "invalid value"},
		"seed without keys":     {map[string]any{"seed": 1, "defaults": false}, "needs keys"},
		"duplicate match":       {map[string]any{"seed": 1, "keys": []any{map[string]any{"match": "a"}, map[string]any{"match": "a"}}}, `duplicate buggify key match "a"`},
		"invalid without seed":  {map[string]any{"keys": []any{map[string]any{"match": "a", "error": "Nope"}}}, `unknown error "Nope"`},
		"empty match":           {map[string]any{"seed": 1, "keys": []any{map[string]any{"fireProbability": 1}}}, "must not be empty"},
		"trace is not a bool":   {map[string]any{"seed": 1, "trace": "yes"}, "expected type 'bool'"},
		"defaults is not a map": {map[string]any{"seed": 1, "defaults": []any{}}, "expected type 'bool'"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := dynamicconfig.NewMemoryClient()
			client.OverrideSetting(Setting, tc.value)
			registry := failpoint.NewRegistry()
			serverStarted := false
			app := newApp(t, registry, client, fx.Hook{OnStart: func(context.Context) error {
				serverStarted = true
				return nil
			}})

			err := app.Start(t.Context())
			require.ErrorContains(t, err, "invalid dynamic config setting testing.buggify")
			require.ErrorContains(t, err, tc.err)
			require.False(t, serverStarted)
			require.Nil(t, registry.Policy())
		})
	}
}

func TestApplier_Update(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	registry := failpoint.NewRegistry()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	kept := logger.Expect(testlogger.Error, "Keeping the previous BUGGIFY configuration")
	a := &applier{buggify: buggify.New(registry), client: client, logger: logger}
	require.NoError(t, a.start(t.Context()))
	require.Nil(t, registry.Policy())

	// A change at runtime applies.
	client.OverrideSetting(Setting, map[string]any{"seed": 1, "defaults": false, "keys": []any{map[string]any{"match": "a"}}})
	await.RequireTrue(t, func() bool { return registry.Policy() != nil }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(1), a.buggify.Config().Seed)

	// An invalid value keeps the previous configuration.
	client.OverrideSetting(Setting, map[string]any{"seed": 2, "bogus": true})
	await.RequireTrue(t, kept.Matched, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(1), a.buggify.Config().Seed)

	// So does a value that the strict decoding panics for.
	client.OverrideSetting(Setting, map[string]any{"seed": 2, "keys": []any{map[any]any{"match": "x", 42: "typo"}}})
	await.RequireTrue(t, func() bool { return kept.MatchCount() == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(1), a.buggify.Config().Seed)

	// A change after stop does nothing.
	require.NoError(t, a.stop(t.Context()))
	client.OverrideSetting(Setting, map[string]any{"seed": 3, "defaults": false, "keys": []any{map[string]any{"match": "a"}}})
	require.NoError(t, a.apply())
	require.Nil(t, registry.Policy())
}

// Removing the setting, which notifies with no values, turns BUGGIFY off.
func TestApplier_Removal(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	registry := failpoint.NewRegistry()
	a := &applier{buggify: buggify.New(registry), client: client, logger: log.NewNoopLogger()}
	remove := client.OverrideSetting(Setting, map[string]any{"seed": 1, "defaults": false, "keys": []any{map[string]any{"match": "a"}}})
	require.NoError(t, a.start(t.Context()))
	t.Cleanup(func() { _ = a.stop(t.Context()) })
	require.NotNil(t, registry.Policy())

	remove()
	await.RequireTrue(t, func() bool { return registry.Policy() == nil }, 5*time.Second, 10*time.Millisecond)
}

var traceKey = failpoint.NewKey[bool]("serverbuggify.trace")

func traceSetting(trace bool) map[string]any {
	return map[string]any{
		"seed": 3, "trace": trace, "defaults": false,
		"keys": []any{map[string]any{"match": traceKey.Name(), "activationProbability": 1, "fireProbability": 1}},
	}
}

// The observer is the one that a harness installed, until the setting asks for the trace.
func TestApplier_Observer(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	registry := failpoint.NewRegistry()
	a := &applier{buggify: buggify.New(registry), client: client, logger: log.NewNoopLogger()}

	var mu sync.Mutex
	var events []buggify.Event
	a.buggify.OnDecision(func(e buggify.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	})
	harnessEvents := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(events)
	}

	// Updates with the trace off leave the observer of the harness.
	client.OverrideSetting(Setting, traceSetting(false))
	require.NoError(t, a.apply())
	require.False(t, a.tracing)
	traceKey.Trip(registry)
	require.Equal(t, 2, harnessEvents()) // An activation and a fire.
	require.NoError(t, a.apply())
	traceKey.Trip(registry)
	require.Equal(t, 3, harnessEvents()) // A fire.

	// The trace replaces it, and turning the trace off removes the trace.
	client.OverrideSetting(Setting, traceSetting(true))
	require.NoError(t, a.apply())
	require.True(t, a.tracing)
	traceKey.Trip(registry)
	require.Equal(t, 3, harnessEvents())
	client.OverrideSetting(Setting, traceSetting(false))
	require.NoError(t, a.apply())
	require.False(t, a.tracing)
	traceKey.Trip(registry)
	require.Equal(t, 3, harnessEvents())
}

// Not parallel: it replaces the standard output, which the trace writes to. The trace of the
// decisions is written by the time that the shutdown returns, although its writer is blocked
// until the shutdown has started.
func TestModule_TraceIsDrainedAtShutdown(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	stdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = stdout }()

	registry := failpoint.NewRegistry()
	client := dynamicconfig.NewMemoryClient()
	client.OverrideSetting(Setting, traceSetting(true))
	app := newApp(t, registry, client, fx.Hook{})
	app.RequireStart()

	// Fill the pipe, so that the next write blocks until the pipe is read.
	for {
		require.NoError(t, w.SetWriteDeadline(time.Now().Add(20*time.Millisecond)))
		if _, err := w.Write([]byte("x")); err != nil {
			require.ErrorIs(t, err, os.ErrDeadlineExceeded)
			break
		}
	}
	require.NoError(t, w.SetWriteDeadline(time.Time{}))

	// This execution blocks in the write of its activation, and queues its fire.
	executed := make(chan struct{})
	go func() {
		defer close(executed)
		traceKey.Trip(registry)
	}()
	select {
	case <-executed:
		require.Fail(t, "the trace writer should be blocked")
	case <-time.After(200 * time.Millisecond):
	}

	// Only the shutdown can deliver the trace, once the pipe is read.
	var trace []byte
	read := make(chan struct{})
	time.AfterFunc(200*time.Millisecond, func() {
		defer close(read)
		trace, _ = io.ReadAll(r)
	})
	app.RequireStop() // Waits for the lines of the decisions so far.
	require.NoError(t, w.Close())
	<-read

	require.Equal(t, "BUGGIFY event=activation key=\"serverbuggify.trace\" active=true\n"+
		"BUGGIFY event=fire key=\"serverbuggify.trace\" scope=\"\" execution=1 action=On\n", string(bytes.TrimLeft(trace, "x")))
}

// The spelling of the setting is frozen: the lowerCamelCase names of the fields, in any case.
func TestSpelling(t *testing.T) {
	t.Parallel()
	value := map[string]any{
		"seed": 9, "defaults": false, "trace": true,
		"keys": []any{map[string]any{"match": "a", "activationProbability": 0.5, "fireProbability": 0.25, "error": "Unavailable"}},
	}
	c, cfg, err := decode(value)
	require.NoError(t, err)
	require.Equal(t, Config{
		Seed: 9, Defaults: new(false), Trace: true,
		Keys: []Key{{Match: "a", ActivationProbability: new(0.5), FireProbability: new(0.25), Error: "Unavailable"}},
	}, c)
	require.Equal(t, buggify.Config{Seed: 9, Keys: []buggify.Key{{Match: "a", ActivationProbability: 0.5, FireProbability: 0.25, Error: "Unavailable"}}}, cfg)

	for _, field := range []string{"activation_probability", "fire-probability", "Probability"} {
		value["keys"] = []any{map[string]any{"match": "a", field: 1}}
		_, _, err := decode(value)
		require.ErrorContains(t, err, field, field)
	}
}
