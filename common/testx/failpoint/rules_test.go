package failpoint_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
)

// The catalogs are global, so each test registers names of its own, also when it runs again.

func registerTestError(t *testing.T) (name string, base error) {
	name = t.Name() + "_" + rand.Text()
	base = errors.New(name)
	failpoint.RegisterError(name, func(msg string) error { return errors.Join(base, errors.New(msg)) })
	return name, base
}

func registerTestBehavior(t *testing.T, fn func() error) string {
	name := t.Name() + "_" + rand.Text()
	failpoint.RegisterBehavior(name, fn)
	return name
}

// infos returns the state of every failpoint in registry.
func infos(registry failpoint.Registry) map[failpoint.Key]failpoint.Info {
	result := map[failpoint.Key]failpoint.Info{}
	for _, key := range registry.Keys() {
		result[key] = registry.Get(key).(interface{ Info() failpoint.Info }).Info()
	}
	return result
}

func TestRuleValidate(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)
	behavior := registerTestBehavior(t, func() error { return nil })

	tests := []struct {
		name    string
		rule    failpoint.Rule
		wantErr string
	}{
		{name: "return", rule: failpoint.Rule{Key: "k", Action: failpoint.Return, Error: errName, Message: "m"}},
		{name: "panic", rule: failpoint.Rule{Key: "k", Action: failpoint.Panic, Error: errName}},
		{name: "block once", rule: failpoint.Rule{Key: "k", Action: failpoint.Block, Recurrence: failpoint.Once}},
		{name: "pause", rule: failpoint.Rule{Key: "k", Action: failpoint.Pause, Delay: time.Second}},
		{name: "call", rule: failpoint.Rule{Key: "k", Action: failpoint.Call, Behavior: behavior}},
		{name: "on with trigger", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Trigger: failpoint.Trigger{Probability: 1, Seed: 1, After: 2, Max: 3}}},
		{name: "disabled", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Disabled: true}},
		{name: "missing key", rule: failpoint.Rule{Action: failpoint.On}, wantErr: "key must be set"},
		{name: "unspecified action", rule: failpoint.Rule{Key: "k"}, wantErr: "unsupported action Unspecified"},
		{name: "off action", rule: failpoint.Rule{Key: "k", Action: failpoint.Off}, wantErr: "unsupported action Off"},
		{name: "unknown action", rule: failpoint.Rule{Key: "k", Action: 99}, wantErr: "unsupported action Action(99)"},
		{name: "return without error", rule: failpoint.Rule{Key: "k", Action: failpoint.Return}, wantErr: "error must be set for action Return"},
		{name: "panic without error", rule: failpoint.Rule{Key: "k", Action: failpoint.Panic}, wantErr: "error must be set for action Panic"},
		{name: "unknown error", rule: failpoint.Rule{Key: "k", Action: failpoint.Return, Error: errName + "_Unknown"}, wantErr: "unknown error"},
		{name: "call without behavior", rule: failpoint.Rule{Key: "k", Action: failpoint.Call}, wantErr: "behavior must be set"},
		{name: "unknown behavior", rule: failpoint.Rule{Key: "k", Action: failpoint.Call, Behavior: behavior + "_Unknown"}, wantErr: "unknown behavior"},
		{name: "pause without delay", rule: failpoint.Rule{Key: "k", Action: failpoint.Pause}, wantErr: "delay must be positive"},
		{name: "negative delay", rule: failpoint.Rule{Key: "k", Action: failpoint.Pause, Delay: -time.Second}, wantErr: "delay must be positive"},
		{name: "delay for block", rule: failpoint.Rule{Key: "k", Action: failpoint.Block, Delay: time.Second}, wantErr: "delay is not allowed for action Block"},
		{name: "error for on", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Error: errName}, wantErr: "error is not allowed for action On"},
		{name: "message without error", rule: failpoint.Rule{Key: "k", Action: failpoint.Block, Message: "m"}, wantErr: "message is only allowed with an error"},
		{name: "behavior for return", rule: failpoint.Rule{Key: "k", Action: failpoint.Return, Error: errName, Behavior: behavior}, wantErr: "behavior is not allowed for action Return"},
		{name: "unknown recurrence", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Recurrence: 9}, wantErr: "unsupported recurrence 9"},
		{name: "negative probability", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Trigger: failpoint.Trigger{Probability: -0.1}}, wantErr: "probability -0.1 is not in [0, 1]"},
		{name: "probability above one", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Trigger: failpoint.Trigger{Probability: 1.5}}, wantErr: "probability 1.5 is not in [0, 1]"},
		{name: "NaN probability", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Trigger: failpoint.Trigger{Probability: math.NaN()}}, wantErr: "probability NaN"},
		{name: "negative after", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Trigger: failpoint.Trigger{After: -1}}, wantErr: "trigger after -1 is negative"},
		{name: "negative max", rule: failpoint.Rule{Key: "k", Action: failpoint.On, Trigger: failpoint.Trigger{Max: -1}}, wantErr: "trigger max -1 is negative"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.rule.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestRuleSet_Apply(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)

	ret := failpoint.Rule{Key: "a", Action: failpoint.Return, Error: errName}
	block := failpoint.Rule{Key: "a", Action: failpoint.Block, Recurrence: failpoint.Once}
	on := failpoint.Rule{Key: "b", Action: failpoint.On}
	scoped := failpoint.Rule{Key: "a", Scope: "s1", Action: failpoint.On}
	keyA, keyB, keyAS1 := failpoint.KeyFromName("a", ""), failpoint.KeyFromName("b", ""), failpoint.KeyFromName("a", "s1")

	tests := []struct {
		name  string
		steps [][]failpoint.Rule
		want  map[failpoint.Key]failpoint.Info
	}{
		{
			name:  "creates failpoints",
			steps: [][]failpoint.Rule{{ret, on}},
			want: map[failpoint.Key]failpoint.Info{
				keyA: {Key: keyA, Enabled: true, Action: failpoint.Return, Recurrence: failpoint.Always},
				keyB: {Key: keyB, Enabled: true, Action: failpoint.On, Recurrence: failpoint.Always},
			},
		},
		{
			name:  "reconfigures a changed rule",
			steps: [][]failpoint.Rule{{ret}, {block}},
			want: map[failpoint.Key]failpoint.Info{
				keyA: {Key: keyA, Enabled: true, Action: failpoint.Block, Recurrence: failpoint.Once},
			},
		},
		{
			name:  "removes an unlisted rule",
			steps: [][]failpoint.Rule{{ret, on}, {on}},
			want: map[failpoint.Key]failpoint.Info{
				keyB: {Key: keyB, Enabled: true, Action: failpoint.On, Recurrence: failpoint.Always},
			},
		},
		{
			name:  "removes all",
			steps: [][]failpoint.Rule{{ret, on}, nil},
			want:  map[failpoint.Key]failpoint.Info{},
		},
		{
			name:  "keeps scopes apart",
			steps: [][]failpoint.Rule{{ret, scoped}},
			want: map[failpoint.Key]failpoint.Info{
				keyA:   {Key: keyA, Enabled: true, Action: failpoint.Return, Recurrence: failpoint.Always},
				keyAS1: {Key: keyAS1, Enabled: true, Action: failpoint.On, Recurrence: failpoint.Always},
			},
		},
		{
			name:  "suspends a disabled rule",
			steps: [][]failpoint.Rule{{ret}, {{Key: "a", Action: failpoint.Return, Error: errName, Disabled: true}}},
			want: map[failpoint.Key]failpoint.Info{
				keyA: {Key: keyA, Enabled: false, Action: failpoint.Return, Recurrence: failpoint.Always},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := failpoint.NewRegistry()
			set := failpoint.NewRuleSet(registry, "test rules")
			for _, rules := range tc.steps {
				require.NoError(t, set.Apply(rules))
			}
			require.Equal(t, tc.want, infos(registry))
		})
	}
}

func TestRuleSet_RulesFire(t *testing.T) {
	t.Parallel()
	errName, base := registerTestError(t)
	var calls int
	behaviorErr := errors.New("behavior")
	behavior := registerTestBehavior(t, func() error {
		calls++
		return behaviorErr
	})
	registry := failpoint.NewRegistry()
	src := registrySource{registry: registry}
	set := failpoint.NewRuleSet(registry, "test rules")

	require.NoError(t, set.Apply([]failpoint.Rule{
		{Key: "return", Action: failpoint.Return, Error: errName, Message: "detail"},
		{Key: "call", Action: failpoint.Call, Behavior: behavior},
		{Key: "on", Scope: "s1", Action: failpoint.On, Trigger: failpoint.Trigger{After: 1}},
		{Key: "panic", Action: failpoint.Panic, Error: errName},
	}))

	err := failpoint.Checkpoint(src, failpoint.KeyFromName("return", ""))
	require.ErrorIs(t, err, base)
	require.ErrorContains(t, err, "detail")

	require.ErrorIs(t, failpoint.Checkpoint(src, failpoint.KeyFromName("call", "")), behaviorErr)
	require.Equal(t, 1, calls)

	onKey := failpoint.KeyFromName("on", "s1")
	require.False(t, failpoint.Active(src, onKey), "skipped by the trigger")
	require.True(t, failpoint.Active(src, onKey))
	require.False(t, failpoint.Active(src, failpoint.KeyFromName("on", "s2")))

	panicValue := func() (v any) {
		defer func() { v = recover() }()
		_ = failpoint.Checkpoint(src, failpoint.KeyFromName("panic", ""))
		return nil
	}()
	require.ErrorIs(t, panicValue.(error), base)
}

// boundedContext returns a context for a wait that fails the test instead of hanging on a
// regression.
func boundedContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// holdExecution starts an execution of the block failpoint for key and waits until it is held. The
// returned channel receives the result of the execution when it is released.
func holdExecution(t *testing.T, registry failpoint.Registry, key failpoint.Key) <-chan error {
	fp := registry.Get(key).(*failpoint.Failpoint[error])
	released := make(chan error, 1)
	go func() { released <- failpoint.Checkpoint(registrySource{registry: registry}, key) }()
	require.NoError(t, fp.AwaitBlock(boundedContext(t)))
	return released
}

func TestRuleSet_UnchangedRuleKeepsState(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)
	once := failpoint.Rule{Key: "once", Action: failpoint.Return, Error: errName, Recurrence: failpoint.Once}
	block := failpoint.Rule{Key: "block", Action: failpoint.Block}
	other := failpoint.Rule{Key: "other", Action: failpoint.On}
	registry := failpoint.NewRegistry()
	src := registrySource{registry: registry}
	set := failpoint.NewRuleSet(registry, "test rules")
	t.Cleanup(set.Close)
	require.NoError(t, set.Apply([]failpoint.Rule{once, block}))

	onceKey, blockKey := failpoint.KeyFromName("once", ""), failpoint.KeyFromName("block", "")
	require.Error(t, failpoint.Checkpoint(src, onceKey))
	blockFP := registry.Get(blockKey).(*failpoint.Failpoint[error])
	released := holdExecution(t, registry, blockKey)

	// Applying the same rules again, together with a new one, keeps the fired Once and the held
	// block.
	require.NoError(t, set.Apply([]failpoint.Rule{once, block, other}))
	require.NoError(t, failpoint.Checkpoint(src, onceKey))
	require.True(t, blockFP.CurrentlyBlocked())
	require.Same(t, blockFP, registry.Get(blockKey))

	// Disabling the block rule releases the execution but keeps the counts.
	block.Disabled = true
	require.NoError(t, set.Apply([]failpoint.Rule{once, block, other}))
	require.NoError(t, await.Rcv(t, released))
	executions, fires := blockFP.Stats()
	require.Equal(t, [2]int64{1, 1}, [2]int64{executions, fires})
	require.NoError(t, failpoint.Checkpoint(src, blockKey), "a disabled rule doesn't block")

	// Enabling it again resumes it with its counts.
	block.Disabled = false
	require.NoError(t, set.Apply([]failpoint.Rule{once, block, other}))
	require.Same(t, blockFP, registry.Get(blockKey))
	released = holdExecution(t, registry, blockKey)
	executions, fires = blockFP.Stats()
	require.Equal(t, [2]int64{2, 2}, [2]int64{executions, fires})

	set.Close()
	require.NoError(t, await.Rcv(t, released))
}

func TestRuleSet_CollisionChangesNothing(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)
	registry := failpoint.NewRegistry()
	foreignKey := failpoint.KeyFromName("foreign", "")
	foreign := failpoint.NewFailpoint[error](foreignKey)
	foreign.On(failpoint.Always)
	registry.Set(foreignKey, foreign)
	set := failpoint.NewRuleSet(registry, "test rules")
	t.Cleanup(set.Close)

	block := failpoint.Rule{Key: "block", Action: failpoint.Block}
	once := failpoint.Rule{Key: "once", Action: failpoint.Return, Error: errName, Recurrence: failpoint.Once, Trigger: failpoint.Trigger{After: 1}}
	require.NoError(t, set.Apply([]failpoint.Rule{block, once}))
	blockKey, onceKey := failpoint.KeyFromName("block", ""), failpoint.KeyFromName("once", "")
	blockFP := registry.Get(blockKey).(*failpoint.Failpoint[error])
	onceFP := registry.Get(onceKey).(*failpoint.Failpoint[error])
	released := holdExecution(t, registry, blockKey)
	require.NoError(t, failpoint.Checkpoint(registrySource{registry: registry}, onceKey), "skipped by the trigger")
	before := infos(registry)

	// The new rules change the block, drop once, add new, and collide with foreign.
	err := set.Apply([]failpoint.Rule{
		{Key: "block", Action: failpoint.On},
		{Key: "new", Action: failpoint.On},
		{Key: "foreign", Action: failpoint.Block},
	})
	require.ErrorContains(t, err, "foreign")
	require.Equal(t, before, infos(registry))
	require.Same(t, foreign, registry.Get(foreignKey))
	require.Same(t, onceFP, registry.Get(onceKey))
	require.True(t, blockFP.CurrentlyBlocked())
	executions, fires := onceFP.Stats()
	require.Equal(t, [2]int64{1, 0}, [2]int64{executions, fires})

	// Once the key is free, the rules apply.
	registry.Delete(foreignKey)
	require.NoError(t, set.Apply([]failpoint.Rule{{Key: "foreign", Action: failpoint.On}}))
	require.NotSame(t, foreign, registry.Get(foreignKey))
	require.NoError(t, await.Rcv(t, released))
}

func TestRuleSet_ReleasesReplacedFailpoint(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry, "test rules")
	t.Cleanup(set.Close)
	rules := []failpoint.Rule{{Key: "block", Action: failpoint.Block}}
	require.NoError(t, set.Apply(rules))
	blockKey := failpoint.KeyFromName("block", "")
	released := holdExecution(t, registry, blockKey)

	replacement := failpoint.NewFailpoint[error](blockKey)
	registry.Set(blockKey, replacement)
	require.ErrorContains(t, set.Apply(rules), "block")
	require.NoError(t, await.Rcv(t, released), "the replaced failpoint releases its execution")
	require.Same(t, replacement, registry.Get(blockKey))

	set.Close()
	require.Same(t, replacement, registry.Get(blockKey))
}

func TestNewRuleSet_RequiresSource(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { failpoint.NewRuleSet(failpoint.NewRegistry(), "") })
}

func TestRuleSet_ErrorConstructorCallsRegistry(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	name := t.Name() + "_" + rand.Text()
	failpoint.RegisterError(name, func(msg string) error {
		return fmt.Errorf("%s: %d failpoints", msg, len(registry.Keys()))
	})
	set := failpoint.NewRuleSet(registry, "test rules")
	t.Cleanup(set.Close)

	applied := make(chan error, 1)
	go func() {
		applied <- set.Apply([]failpoint.Rule{{Key: "r", Action: failpoint.Return, Error: name, Message: "injected"}})
	}()
	require.NoError(t, await.Rcv(t, applied))
	require.EqualError(t, failpoint.Checkpoint(registrySource{registry: registry}, failpoint.KeyFromName("r", "")), "injected: 0 failpoints")
}

func TestRuleSet_Owner(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry, "test rules")
	t.Cleanup(set.Close)
	require.NoError(t, set.Apply([]failpoint.Rule{{Key: "on", Action: failpoint.On}}))

	require.Equal(t, "test rules", registry.Get(failpoint.KeyFromName("on", "")).(*failpoint.Failpoint[error]).Owner())
	require.Empty(t, failpoint.NewFailpoint[error](key1).Owner())
}

func TestRuleSet_RecreatesRemovedFailpoint(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry, "test rules")
	rules := []failpoint.Rule{{Key: "a", Action: failpoint.On}}
	require.NoError(t, set.Apply(rules))

	registry.DeleteAll()
	require.NoError(t, set.Apply(rules))
	require.NotNil(t, registry.Get(failpoint.KeyFromName("a", "")))
}

func TestRuleSet_InvalidRulesChangeNothing(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry, "test rules")
	require.NoError(t, set.Apply([]failpoint.Rule{{Key: "a", Action: failpoint.On}}))
	keyA := failpoint.KeyFromName("a", "")
	before := registry.Get(keyA)

	tests := []struct {
		name    string
		rules   []failpoint.Rule
		wantErr string
	}{
		{
			name:    "invalid rule",
			rules:   []failpoint.Rule{{Key: "b", Action: failpoint.On}, {Key: "c", Action: failpoint.Return}},
			wantErr: "error must be set",
		},
		{
			name:    "duplicate key",
			rules:   []failpoint.Rule{{Key: "b", Action: failpoint.On}, {Key: "b", Action: failpoint.Block}},
			wantErr: "duplicate failpoint rule for b",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, set.Apply(tc.rules), tc.wantErr)
			require.Equal(t, []failpoint.Key{keyA}, registry.Keys())
			require.Same(t, before, registry.Get(keyA))
		})
	}
}

func TestRuleSet_Close(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	foreignKey := failpoint.KeyFromName("foreign", "")
	registry.Set(foreignKey, failpoint.NewFailpoint[error](foreignKey))
	set := failpoint.NewRuleSet(registry, "test rules")
	require.NoError(t, set.Apply([]failpoint.Rule{{Key: "a", Action: failpoint.On}}))
	own := registry.Get(failpoint.KeyFromName("a", "")).(*failpoint.Failpoint[error])

	set.Close()
	require.Equal(t, []failpoint.Key{foreignKey}, registry.Keys())
	require.False(t, own.Info().Enabled)
	require.ErrorContains(t, set.Apply(nil), "closed")
	set.Close()
}

func TestRuleSet_ConcurrentApply(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	src := registrySource{registry: registry}
	set := failpoint.NewRuleSet(registry, "test rules")
	t.Cleanup(set.Close)
	ruleSets := [][]failpoint.Rule{
		{{Key: "a", Action: failpoint.On}, {Key: "b", Action: failpoint.Block}},
		{{Key: "b", Action: failpoint.On}, {Key: "c", Action: failpoint.On, Disabled: true}},
		nil,
	}

	const workers = 8
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			for j := range 50 {
				if err := set.Apply(ruleSets[(i+j)%len(ruleSets)]); err != nil {
					errs <- err
					return
				}
				failpoint.Active(src, failpoint.KeyFromName("a", ""))
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.NoError(t, set.Apply(ruleSets[1]))
	keyB, keyC := failpoint.KeyFromName("b", ""), failpoint.KeyFromName("c", "")
	require.Equal(t, map[failpoint.Key]failpoint.Info{
		keyB: {Key: keyB, Enabled: true, Action: failpoint.On, Recurrence: failpoint.Always},
		keyC: {Key: keyC, Enabled: false, Action: failpoint.On, Recurrence: failpoint.Always},
	}, infos(registry))
}
