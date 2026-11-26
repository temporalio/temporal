package failpoint_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
)

// The catalogs are global, so each test registers names of its own, also when it runs again.

var (
	ruleKeys    = failpoint.ReserveKeyPrefix[error]("rule:")
	ruleIntKeys = failpoint.ReserveKeyPrefix[int]("rule.int:")
)

// applyRules applies rules and returns only the error.
func applyRules(set *failpoint.RuleSet, rules []failpoint.Rule) error {
	_, err := set.Apply(rules)
	return err
}

func registerTestError(t *testing.T) (name string, base error) {
	name = t.Name() + "_" + rand.Text()
	base = errors.New(name)
	failpoint.RegisterError(name, func(msg string) error { return errors.Join(base, errors.New(msg)) })
	return name, base
}

func TestRuleValidate(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)

	tests := []struct {
		name    string
		rule    failpoint.Rule
		wantErr string
	}{
		{name: "return", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionReturn, Error: errName, Message: "m"}},
		{name: "panic", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionPanic, Error: errName}},
		{name: "block once", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionBlock, Trigger: failpoint.Trigger{MaxFires: 1}}},
		{name: "pause", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionPause, Delay: time.Second}},
		{name: "on with trigger", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{Probability: 1, Seed: 1, SkipFirst: 2, MaxFires: 3}}},
		{name: "declared key", rule: failpoint.Rule{Key: ruleErrorKey.Name(), Action: failpoint.ActionOn}},
		{name: "unknown key", rule: failpoint.Rule{Key: "typo", Action: failpoint.ActionOn}, wantErr: `unknown key "typo", declared keys are `},
		{name: "missing key", rule: failpoint.Rule{Action: failpoint.ActionOn}, wantErr: "key must be set"},
		{name: "missing action", rule: failpoint.Rule{Key: "rule:k"}, wantErr: "unsupported action Off"},
		{name: "call action", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionCall}, wantErr: "unsupported action Call, valid actions are Return, Panic, Block, Pause and On"},
		{name: "off action", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOff}, wantErr: "unsupported action Off"},
		{name: "unknown action", rule: failpoint.Rule{Key: "rule:k", Action: 99}, wantErr: "unsupported action Action(99)"},
		{name: "return without error", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionReturn}, wantErr: "error must be set for action Return"},
		{name: "panic without error", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionPanic}, wantErr: "error must be set for action Panic"},
		{name: "unknown error", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionReturn, Error: errName + "_Unknown"}, wantErr: "valid errors are "},
		{name: "pause without delay", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionPause}, wantErr: "delay must be positive"},
		{name: "negative delay", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionPause, Delay: -time.Second}, wantErr: "delay must be positive"},
		{name: "delay for block", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionBlock, Delay: time.Second}, wantErr: "delay is not allowed for action Block"},
		{name: "error for on", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOn, Error: errName}, wantErr: "error is not allowed for action On"},
		{name: "message without error", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionBlock, Message: "m"}, wantErr: "message is only allowed with an error"},
		{name: "negative probability", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{Probability: -0.1}}, wantErr: "probability -0.1 is not in [0, 1]"},
		{name: "probability above one", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{Probability: 1.5}}, wantErr: "probability 1.5 is not in [0, 1]"},
		{name: "NaN probability", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{Probability: math.NaN()}}, wantErr: "probability NaN"},
		{name: "negative skip first", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{SkipFirst: -1}}, wantErr: "trigger skip first -1 is negative"},
		{name: "negative max fires", rule: failpoint.Rule{Key: "rule:k", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{MaxFires: -1}}, wantErr: "trigger max fires -1 is negative"},
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

	ret := failpoint.Rule{Key: "rule:a", Action: failpoint.ActionReturn, Error: errName}
	block := failpoint.Rule{Key: "rule:a", Action: failpoint.ActionBlock, Trigger: failpoint.Trigger{MaxFires: 1}}
	on := failpoint.Rule{Key: "rule:b", Action: failpoint.ActionOn}
	scoped := failpoint.Rule{Key: "rule:a", Scope: "s1", Action: failpoint.ActionOn}
	keyA, keyB, keyAS1 := ruleKeys.Key("a"), ruleKeys.Key("b"), ruleKeys.Key("a").ForScope("s1")

	tests := []struct {
		name  string
		steps [][]failpoint.Rule
		want  map[string]failpoint.State
	}{
		{
			name:  "creates failpoints",
			steps: [][]failpoint.Rule{{ret, on}},
			want: map[string]failpoint.State{
				keyA.String(): {Key: keyA, Action: failpoint.ActionReturn},
				keyB.String(): {Key: keyB, Action: failpoint.ActionOn},
			},
		},
		{
			name:  "reconfigures a changed rule",
			steps: [][]failpoint.Rule{{ret}, {block}},
			want: map[string]failpoint.State{
				keyA.String(): {Key: keyA, Action: failpoint.ActionBlock, Trigger: failpoint.Trigger{MaxFires: 1}},
			},
		},
		{
			name:  "removes an unlisted rule",
			steps: [][]failpoint.Rule{{ret, on}, {on}},
			want: map[string]failpoint.State{
				keyB.String(): {Key: keyB, Action: failpoint.ActionOn},
			},
		},
		{
			name:  "removes all",
			steps: [][]failpoint.Rule{{ret, on}, nil},
			want:  map[string]failpoint.State{},
		},
		{
			name:  "keeps scopes apart",
			steps: [][]failpoint.Rule{{ret, scoped}},
			want: map[string]failpoint.State{
				keyA.String():   {Key: keyA, Action: failpoint.ActionReturn},
				keyAS1.String(): {Key: keyAS1, Action: failpoint.ActionOn},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := failpoint.NewRegistry()
			set := failpoint.NewRuleSet(registry)
			for _, rules := range tc.steps {
				require.NoError(t, applyRules(set, rules))
			}
			require.Equal(t, tc.want, registry.States())
		})
	}
}

func TestRuleSet_RulesFire(t *testing.T) {
	t.Parallel()
	errName, base := registerTestError(t)
	registry := failpoint.NewRegistry()
	src := registry
	set := failpoint.NewRuleSet(registry)

	require.NoError(t, applyRules(set, []failpoint.Rule{
		{Key: "rule:return", Action: failpoint.ActionReturn, Error: errName, Message: "detail"},
		{Key: "rule:on", Scope: "s1", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{SkipFirst: 1}},
		{Key: "rule:panic", Action: failpoint.ActionPanic, Error: errName},
	}))

	err := failpoint.CheckError(src, ruleKeys.Key("return"))
	require.ErrorIs(t, err, base)
	require.ErrorContains(t, err, "detail")

	onKey := ruleKeys.Key("on").ForScope("s1")
	require.False(t, failpoint.ExecuteSwitch(src, onKey), "skipped by the trigger")
	require.True(t, failpoint.ExecuteSwitch(src, onKey))
	require.False(t, failpoint.ExecuteSwitch(src, ruleKeys.Key("on").ForScope("s2")))

	panicValue := func() (v any) {
		defer func() { v = recover() }()
		_ = failpoint.CheckError(src, ruleKeys.Key("panic"))
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
func holdExecution(t *testing.T, registry *failpoint.Registry, key failpoint.Key[error]) <-chan error {
	fp := registry.Get(key).(*failpoint.Failpoint[error])
	released := make(chan error, 1)
	go func() { released <- failpoint.CheckError(registry, key) }()
	require.NoError(t, fp.Wait(boundedContext(t)))
	return released
}

func TestRuleSet_UnchangedRuleKeepsState(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)
	once := failpoint.Rule{Key: "rule:once", Action: failpoint.ActionReturn, Error: errName, Trigger: failpoint.Trigger{MaxFires: 1}}
	block := failpoint.Rule{Key: "rule:block", Action: failpoint.ActionBlock}
	other := failpoint.Rule{Key: "rule:other", Action: failpoint.ActionOn}
	registry := failpoint.NewRegistry()
	src := registry
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)
	require.NoError(t, applyRules(set, []failpoint.Rule{once, block}))

	onceKey, blockKey := ruleKeys.Key("once"), ruleKeys.Key("block")
	require.Error(t, failpoint.CheckError(src, onceKey))
	blockFP := registry.Get(blockKey).(*failpoint.Failpoint[error])
	released := holdExecution(t, registry, blockKey)

	// Applying the same rules again, together with a new one, keeps the fired Once and the held
	// block.
	require.NoError(t, applyRules(set, []failpoint.Rule{once, block, other}))
	require.NoError(t, failpoint.CheckError(src, onceKey))
	select {
	case <-released:
		t.Fatal("applying unchanged rules released the held block")
	default:
	}
	require.Same(t, blockFP, registry.Get(blockKey))

	// Removing the block rule releases the execution, and the rule is gone.
	require.NoError(t, applyRules(set, []failpoint.Rule{once, other}))
	require.NoError(t, await.Rcv(t, released))
	require.Nil(t, registry.Get(blockKey))
	require.NoError(t, failpoint.CheckError(src, blockKey), "a removed rule doesn't block")

	// Listing it again creates a new failpoint with fresh counts.
	require.NoError(t, applyRules(set, []failpoint.Rule{once, block, other}))
	require.NotSame(t, blockFP, registry.Get(blockKey))
	released = holdExecution(t, registry, blockKey)
	executions, fires := registry.Get(blockKey).(*failpoint.Failpoint[error]).Stats()
	require.Equal(t, [2]int64{1, 1}, [2]int64{executions, fires})

	set.Close()
	require.NoError(t, await.Rcv(t, released))
}

func TestRuleSet_CollisionSkipsOnlyTheCollidingRule(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)
	registry := failpoint.NewRegistry()
	foreignKey := ruleKeys.Key("foreign")
	foreign := failpoint.NewFailpoint[error](foreignKey)
	foreign.On()
	registry.Set(foreign)
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)

	block := failpoint.Rule{Key: "rule:block", Action: failpoint.ActionBlock}
	once := failpoint.Rule{Key: "rule:once", Action: failpoint.ActionReturn, Error: errName, Trigger: failpoint.Trigger{MaxFires: 1, SkipFirst: 1}}
	require.NoError(t, applyRules(set, []failpoint.Rule{block, once}))
	blockKey, onceKey := ruleKeys.Key("block"), ruleKeys.Key("once")
	blockFP := registry.Get(blockKey).(*failpoint.Failpoint[error])
	onceFP := registry.Get(onceKey).(*failpoint.Failpoint[error])
	released := holdExecution(t, registry, blockKey)
	require.NoError(t, failpoint.CheckError(registry, onceKey), "skipped by the trigger")
	executions, fires := onceFP.Stats()
	require.Equal(t, [2]int64{1, 0}, [2]int64{executions, fires})

	// The new rules change the block, drop once, add new, and collide with foreign. The rule of
	// foreign is skipped and the others apply.
	rules := []failpoint.Rule{
		{Key: "rule:block", Action: failpoint.ActionOn},
		{Key: "rule:new", Action: failpoint.ActionOn},
		{Key: "rule:foreign", Action: failpoint.ActionBlock},
	}
	err := applyRules(set, rules)
	require.ErrorContains(t, err, "foreign")
	require.NotContains(t, err.Error(), "new")
	require.Same(t, foreign, registry.Get(foreignKey))
	require.Nil(t, registry.Get(onceKey), "the removed rule no longer applies")
	require.NotNil(t, registry.Get(ruleKeys.Key("new")))
	require.Same(t, blockFP, registry.Get(blockKey))
	require.Equal(t, failpoint.ActionOn, blockFP.State().Action, "the changed rule replaced the block")
	require.NoError(t, await.Rcv(t, released))

	// Once the key is free, the next Apply creates the skipped rule.
	registry.Delete(foreignKey)
	require.NoError(t, applyRules(set, rules))
	require.NotSame(t, foreign, registry.Get(foreignKey))
}

func TestRuleSet_ReleasesReplacedFailpoint(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)
	rules := []failpoint.Rule{{Key: "rule:block", Action: failpoint.ActionBlock}}
	require.NoError(t, applyRules(set, rules))
	blockKey := ruleKeys.Key("block")
	released := holdExecution(t, registry, blockKey)

	replacement := failpoint.NewFailpoint[error](blockKey)
	registry.Set(replacement)
	require.ErrorContains(t, applyRules(set, rules), "block")
	require.NoError(t, await.Rcv(t, released), "the replaced failpoint releases its execution")
	require.Same(t, replacement, registry.Get(blockKey))

	set.Close()
	require.Same(t, replacement, registry.Get(blockKey))
}

func TestRuleSet_ErrorConstructorCallsRegistry(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	name := t.Name() + "_" + rand.Text()
	failpoint.RegisterError(name, func(msg string) error {
		return fmt.Errorf("%s: mayFire=%t", msg, registry.MayFire())
	})
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)

	applied := make(chan error, 1)
	go func() {
		applied <- applyRules(set, []failpoint.Rule{{Key: "rule:r", Action: failpoint.ActionReturn, Error: name, Message: "injected"}})
	}()
	require.NoError(t, await.Rcv(t, applied))
	require.EqualError(t, failpoint.CheckError(registry, ruleKeys.Key("r")), "injected: mayFire=false")
}

func TestRuleSet_RecreatesRemovedFailpoint(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry)
	rules := []failpoint.Rule{{Key: "rule:a", Action: failpoint.ActionOn}}
	require.NoError(t, applyRules(set, rules))

	registry.Delete(ruleKeys.Key("a"))
	require.NoError(t, applyRules(set, rules))
	require.NotNil(t, registry.Get(ruleKeys.Key("a")))
}

func TestRuleSet_InvalidRulesChangeNothing(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry)
	require.NoError(t, applyRules(set, []failpoint.Rule{{Key: "rule:a", Action: failpoint.ActionOn}}))
	keyA := ruleKeys.Key("a")
	before := registry.Get(keyA)

	tests := []struct {
		name    string
		rules   []failpoint.Rule
		wantErr string
	}{
		{
			name:    "invalid rule",
			rules:   []failpoint.Rule{{Key: "rule:b", Action: failpoint.ActionOn}, {Key: "rule:c", Action: failpoint.ActionReturn}},
			wantErr: "error must be set",
		},
		{
			name:    "duplicate key",
			rules:   []failpoint.Rule{{Key: "rule:b", Action: failpoint.ActionOn}, {Key: "rule:b", Action: failpoint.ActionBlock}},
			wantErr: "duplicate failpoint rule for rule:b",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, applyRules(set, tc.rules), tc.wantErr)
			require.Equal(t, []string{keyA.String()}, registry.Keys())
			require.Same(t, before, registry.Get(keyA))
		})
	}
}

func TestRuleSet_ManagedFailpointsAreReported(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	own := failpoint.NewFailpoint(ruleKeys.Key("own"))
	registry.Set(own)
	set := failpoint.NewRuleSet(registry)
	require.NoError(t, applyRules(set, []failpoint.Rule{{Key: "rule:a", Action: failpoint.ActionOn}}))

	require.False(t, registry.Get(ruleKeys.Key("own")).Info().Managed)
	require.True(t, registry.Get(ruleKeys.Key("a")).Info().Managed)
}

func TestRuleSet_Close(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	foreignKey := ruleKeys.Key("foreign")
	registry.Set(failpoint.NewFailpoint[error](foreignKey))
	set := failpoint.NewRuleSet(registry)
	require.NoError(t, applyRules(set, []failpoint.Rule{{Key: "rule:a", Action: failpoint.ActionOn}}))
	own := registry.Get(ruleKeys.Key("a")).(*failpoint.Failpoint[error])

	set.Close()
	require.Equal(t, []string{foreignKey.String()}, registry.Keys())
	require.Equal(t, failpoint.ActionOff, own.State().Action)
	require.ErrorContains(t, applyRules(set, nil), "closed")
	set.Close()
}

func TestRuleSet_ConcurrentApply(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	src := registry
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)
	ruleSets := [][]failpoint.Rule{
		{{Key: "rule:a", Action: failpoint.ActionOn}, {Key: "rule:b", Action: failpoint.ActionBlock}},
		{{Key: "rule:b", Action: failpoint.ActionOn}, {Key: "rule:c", Action: failpoint.ActionOn}},
		nil,
	}

	const workers = 8
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			for j := range 50 {
				if err := applyRules(set, ruleSets[(i+j)%len(ruleSets)]); err != nil {
					errs <- err
					return
				}
				failpoint.ExecuteSwitch(src, ruleKeys.Key("a"))
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.NoError(t, applyRules(set, ruleSets[1]))
	keyB, keyC := ruleKeys.Key("b"), ruleKeys.Key("c")
	require.Equal(t, map[string]failpoint.State{
		keyB.String(): {Key: keyB, Action: failpoint.ActionOn},
		keyC.String(): {Key: keyC, Action: failpoint.ActionOn},
	}, registry.States())
}

var (
	ruleIntKey   = failpoint.NewKey[int]("test.rules.int")
	ruleErrorKey = failpoint.NewKey[error]("test.rules.error")
	rulePathKey  = failpoint.NewKey[*fs.PathError]("test.rules.patherror")
)

func TestRuleSet_CreatesFailpointsOfTheTypeOfTheKey(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)

	require.NoError(t, applyRules(set, []failpoint.Rule{
		{Key: ruleIntKey.Name(), Action: failpoint.ActionOn},
		{Key: ruleErrorKey.Name(), Action: failpoint.ActionReturn, Error: errName},
	}))
	require.True(t, failpoint.ExecuteSwitch(registry, ruleIntKey))
	require.Error(t, failpoint.CheckError(registry, ruleErrorKey))

	// A key whose value is not an error can't return one.
	err := applyRules(set, []failpoint.Rule{{Key: ruleIntKey.Name(), Action: failpoint.ActionReturn, Error: errName}})
	require.ErrorContains(t, err, "not of the type")
}

func TestRuleSet_CreatesFailpointsOfTheTypeOfTheKeyPrefix(t *testing.T) {
	t.Parallel()
	errName, _ := registerTestError(t)
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)

	on := ruleIntKeys.Key("on")
	require.NoError(t, applyRules(set, []failpoint.Rule{{Key: on.Name(), Action: failpoint.ActionOn}}))
	require.True(t, failpoint.ExecuteSwitch(registry, on))

	// The prefix has the value type int, which can't return an error.
	err := applyRules(set, []failpoint.Rule{{Key: ruleIntKeys.Key("return").Name(), Action: failpoint.ActionReturn, Error: errName}})
	require.ErrorContains(t, err, "not of the type")
}

func TestRuleSet_ReturnsTheErrorOfTheTypeOfTheKey(t *testing.T) {
	t.Parallel()
	pathErrName := t.Name() + "_" + rand.Text()
	failpoint.RegisterError(pathErrName, func(msg string) error { return &fs.PathError{Op: "open", Path: msg, Err: fs.ErrNotExist} })
	plainErrName, _ := registerTestError(t)
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)

	// The error that the constructor returns decides, not the interface of the key's type.
	require.NoError(t, applyRules(set, []failpoint.Rule{{Key: rulePathKey.Name(), Action: failpoint.ActionReturn, Error: pathErrName, Message: "/x"}}))
	got, ok := failpoint.Execute(registry, rulePathKey)
	require.True(t, ok)
	require.Equal(t, "/x", got.Path)

	// A constructor that returns another type is rejected before Apply changes anything.
	err := applyRules(set, []failpoint.Rule{
		{Key: ruleKeys.Key("new").Name(), Action: failpoint.ActionOn},
		{Key: rulePathKey.Name(), Action: failpoint.ActionReturn, Error: plainErrName},
	})
	require.ErrorContains(t, err, "not of the type of")
	require.Nil(t, registry.Get(ruleKeys.Key("new")))
	_, ok = failpoint.Execute(registry, rulePathKey)
	require.True(t, ok)
}

func TestRuleSet_ApplyReportsTheSeeds(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)

	unseeded := failpoint.Rule{Key: "rule:unseeded", Scope: "s1", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{Probability: 0.5}}
	seeded := failpoint.Rule{Key: "rule:seeded", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{Probability: 0.5, Seed: 42}}
	plain := failpoint.Rule{Key: "rule:plain", Action: failpoint.ActionOn}

	applied, err := set.Apply([]failpoint.Rule{unseeded, seeded, plain})
	require.NoError(t, err)
	require.Len(t, applied, 3)
	picked := applied[0].Seed
	require.Equal(t, failpoint.AppliedRule{Key: "rule:unseeded", Scope: "s1", Seed: picked}, applied[0])
	require.NotZero(t, picked)
	require.Equal(t, failpoint.AppliedRule{Key: "rule:seeded", Seed: 42}, applied[1])
	require.Equal(t, failpoint.AppliedRule{Key: "rule:plain"}, applied[2])
	require.Equal(t, picked, registry.Get(ruleKeys.Key("unseeded").ForScope("s1")).(*failpoint.Failpoint[error]).Seed())

	// Only the rules that changed are reported, not an unchanged rule.
	changed := seeded
	changed.Trigger.Seed = 43
	applied, err = set.Apply([]failpoint.Rule{unseeded, changed, plain})
	require.NoError(t, err)
	require.Equal(t, []failpoint.AppliedRule{{Key: "rule:seeded", Seed: 43}}, applied)

	// Setting the reported seed repeats the sequence of fires.
	repeat := unseeded
	repeat.Trigger.Seed = picked
	applied, err = set.Apply([]failpoint.Rule{repeat, changed, plain})
	require.NoError(t, err)
	require.Equal(t, []failpoint.AppliedRule{{Key: "rule:unseeded", Scope: "s1", Seed: picked}}, applied)

	// A rejected Apply reports nothing.
	applied, err = set.Apply([]failpoint.Rule{{Key: "typo", Action: failpoint.ActionOn}})
	require.Error(t, err)
	require.Empty(t, applied)
}
