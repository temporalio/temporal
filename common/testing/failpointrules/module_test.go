package failpointrules

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/testx/failpoint"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

// newApp returns an app with the modules around a server hook, as the server graph has them.
func newApp(t *testing.T, registry *failpoint.Registry, client dynamicconfig.Client, server fx.Hook) *fxtest.App {
	return fxtest.New(t,
		fx.Supply(
			registry,
			fx.Annotate(client, fx.As(new(dynamicconfig.Client))),
			fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger))),
		),
		Module,
		fx.Invoke(func(lc fx.Lifecycle) { lc.Append(server) }),
		ReleaseModule,
	)
}

var onRule = map[string]any{"key": "on", "action": "On"}

func TestModule(t *testing.T) {
	t.Parallel()
	t.Run("applies the initial value before the server starts and removes it before the server stops", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, []any{onRule})
		registry := failpoint.NewRegistry()
		var activeAtStart, activeAtStop bool
		app := newApp(t, registry, client, fx.Hook{
			OnStart: func(context.Context) error {
				activeAtStart = failpoint.ExecuteSwitch(registry, onKey)
				return nil
			},
			OnStop: func(context.Context) error {
				activeAtStop = failpoint.ExecuteSwitch(registry, onKey)
				return nil
			},
		})

		app.RequireStart()
		app.RequireStop()
		require.True(t, activeAtStart)
		require.False(t, activeAtStop)
		require.False(t, registry.MayFire())
	})

	t.Run("removes the rules when the server fails to start", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, []any{onRule})
		registry := failpoint.NewRegistry()
		app := newApp(t, registry, client, fx.Hook{
			OnStart: func(context.Context) error { return errors.New("server failed") },
		})

		require.ErrorContains(t, app.Start(t.Context()), "server failed")
		require.False(t, registry.MayFire())
	})

	t.Run("fails to start with an invalid initial value", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, []any{map[string]any{"key": "on", "action": "Of"}})
		registry := failpoint.NewRegistry()
		serverStarted := false
		app := newApp(t, registry, client, fx.Hook{
			OnStart: func(context.Context) error {
				serverStarted = true
				return nil
			},
		})

		err := app.Start(t.Context())
		require.ErrorContains(t, err, `invalid dynamic config setting testing.failpointrules: rule 0: unknown action "Of"`)
		require.False(t, serverStarted)
		require.False(t, registry.MayFire())
	})
}

func TestModule_Disabled(t *testing.T) {
	t.Parallel()
	// Even an invalid value doesn't stop the server: the module doesn't read the setting.
	client := dynamicconfig.NewMemoryClient()
	client.OverrideSetting(Setting, []any{map[string]any{"key": "on", "action": "Of"}})
	started := false
	app := newApp(t, nil, client, fx.Hook{OnStart: func(context.Context) error {
		started = true
		return nil
	}})

	app.RequireStart()
	app.RequireStop()
	require.True(t, started)
}

func TestApplier_Update(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	registry := failpoint.NewRegistry()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	keptPrevious := logger.Expect(testlogger.Error, "Keeping the previous failpoint rules")
	s := newApplier(registry, client, logger)
	require.NoError(t, s.start(t.Context()))
	require.False(t, registry.MayFire())

	cleanup := client.OverrideSetting(Setting, []any{onRule})
	await.RequireTrue(t, func() bool { return registry.Get(onKey) != nil }, 5*time.Second, 10*time.Millisecond)

	// An invalid value keeps the previous rules.
	client.OverrideSetting(Setting, []any{onRule, map[string]any{"key": "r", "action": "Return"}})
	await.RequireTrue(t, keptPrevious.Matched, 5*time.Second, 10*time.Millisecond)
	require.NotNil(t, registry.Get(onKey))
	require.Nil(t, registry.Get(rKey))

	// A valid value applies again.
	client.OverrideSetting(Setting, []any{onRule, map[string]any{"key": "r", "action": "Return", "error": "InvalidArgument"}})
	await.RequireTrue(t, func() bool { return registry.Get(rKey) != nil }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, s.stop(t.Context()))
	require.False(t, registry.MayFire())
	cleanup()
}

// An invalid update keeps the previous rules, and deleting the invalid value removes them: a Block
// rule that held a request must not outlive the setting that an operator removed.
func TestApplier_RemovesRulesWhenAnInvalidValueIsDeleted(t *testing.T) {
	t.Parallel()
	const rulesFile = `
testing.failpointRules:
  - value:
      - key: block
        action: %s
`
	file := newFakeFile(fmt.Sprintf(rulesFile, "Block"))
	client, err := newFileClient(t, file)
	require.NoError(t, err)
	registry := failpoint.NewRegistry()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	keptPrevious := logger.Expect(testlogger.Error, "Keeping the previous failpoint rules")
	s := newApplier(registry, client, logger)
	require.NoError(t, s.start(t.Context()))
	t.Cleanup(func() { _ = s.stop(context.Background()) })
	held := make(chan struct{})
	go func() {
		defer close(held)
		_ = failpoint.ExecuteError(registry, blockKey)
	}()
	blockFP, ok := registry.Get(blockKey).(*failpoint.Failpoint[error])
	require.True(t, ok)
	require.NoError(t, blockFP.Wait(t.Context()))

	file.set(fmt.Sprintf(rulesFile, "Of"))
	require.NoError(t, client.Update())
	await.RequireTrue(t, keptPrevious.Matched, 5*time.Second, 10*time.Millisecond)
	require.NotNil(t, registry.Get(blockKey))
	select {
	case <-held:
		require.Fail(t, "the invalid value released the block")
	default:
	}

	file.set("{}\n")
	require.NoError(t, client.Update())
	await.RequireTrue(t, func() bool { return registry.Get(blockKey) == nil }, 5*time.Second, 10*time.Millisecond)
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		require.Fail(t, "deleting the setting did not release the block")
	}
}

func TestSetting_Validate(t *testing.T) {
	t.Parallel()
	require.NoError(t, Setting.Validate([]any{onRule}))
	require.ErrorContains(t, Setting.Validate([]any{map[string]any{"key": "on", "action": "Of"}}), `unknown action "Of"`)
	require.ErrorContains(t, Setting.Validate([]any{map[string]any{"key": "on", "action": "On", "actoin": "On"}}), "invalid keys: actoin")
	require.ErrorContains(t, Setting.Validate([]any{map[string]any{"key": "grpc:temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution", "action": "On"}}),
		"is not the full name of a unary method")
	require.ErrorContains(t, Setting.Validate([]any{map[string]any{"key": "grpc:" + historyservice.HistoryService_StreamWorkflowReplicationMessages_FullMethodName, "action": "On"}}),
		"is not the full name of a unary method")
	// A custom client can return a typed slice with a key that isn't a string: an error, not a panic.
	require.NotPanics(t, func() {
		require.NoError(t, Setting.Validate([]map[any]any{{"key": "on", "action": "On"}}))
		require.ErrorContains(t, Setting.Validate([]map[any]any{{"key": "on", "action": "On", 1: 2}}), "key 1 is not a string")
	})
}

// A rule for the key of a failpoint that a test registered is skipped and logged, and the other
// rules apply.
func TestApplier_SkipsRulesOfOtherFailpoints(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	testFP := failpoint.New(onKey)
	registry.Set(testFP)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logs := logger.StartCapture()
	s := newApplier(registry, dynamicconfig.NewMemoryClient(), logger)

	s.apply([]failpoint.Rule{{Key: "on", Action: failpoint.ActionOn}, {Key: "block", Action: failpoint.ActionBlock}})

	require.Same(t, testFP, registry.Get(onKey))
	require.NotNil(t, registry.Get(blockKey))
	records := logs.Snapshot()
	require.Len(t, records, 2)
	require.Equal(t, "Applied failpoint rule", records[0].Message)
	require.Equal(t, testlogger.Warn, records[1].Level)
	require.Equal(t, "Skipped failpoint rules for keys of other failpoints", records[1].Message)
	require.Contains(t, records[1].Tags, tag.Error(errors.New("failpoint rules for keys of failpoints that the rule set didn't create, skipped: [on]")))
}

// The log shows the rules that a change applied, with the seed that a probability without one
// picked, so that a run can be repeated, and the rules that it removed.
func TestApplier_LogsAppliedAndRemovedRules(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logs := logger.StartCapture()
	registry := failpoint.NewRegistry()
	s := newApplier(registry, dynamicconfig.NewMemoryClient(), logger)
	probable := failpoint.Rule{Key: "on", Scope: "ns", Action: failpoint.ActionOn, Trigger: failpoint.Trigger{Probability: 0.5}}
	seeded := failpoint.Rule{Key: "block", Action: failpoint.ActionBlock, Trigger: failpoint.Trigger{Probability: 0.5, Seed: 7}}
	plain := failpoint.Rule{Key: "p", Action: failpoint.ActionPause, Delay: time.Second}

	s.apply([]failpoint.Rule{probable, seeded, plain})
	s.apply([]failpoint.Rule{probable, seeded})

	records := logs.Snapshot()
	require.Len(t, records, 4)
	for _, record := range records[:3] {
		require.Equal(t, "Applied failpoint rule", record.Message)
	}
	seedOf := func(record testlogger.CapturedLog) tag.Tag {
		for _, t := range record.Tags {
			if t.Key() == "failpoint-seed" {
				return t
			}
		}
		return nil
	}
	require.Equal(t, tag.NewStringTag("failpoint-key", "on"), records[0].Tags[0])
	require.Equal(t, tag.NewStringTag("failpoint-scope", "ns"), records[0].Tags[1])
	require.NotNil(t, seedOf(records[0]), "the seed of a probability that picked one is logged")
	require.Equal(t, tag.NewInt64("failpoint-seed", 7), seedOf(records[1]))
	require.Nil(t, seedOf(records[2]), "a rule without a probability has no seed")
	require.Equal(t, "Removed failpoint rule", records[3].Message)
	require.Equal(t, []tag.Tag{tag.NewStringTag("failpoint-key", "p"), tag.NewStringTag("failpoint-scope", "")}, records[3].Tags)
}

// A rule that the rule set skipped because other code holds its failpoint never was installed, so
// removing it logs no removal.
func TestApplier_DoesNotLogRemovalOfSkippedRules(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	registry.Set(failpoint.New(onKey))
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logs := logger.StartCapture()
	s := newApplier(registry, dynamicconfig.NewMemoryClient(), logger)
	skipped := failpoint.Rule{Key: "on", Action: failpoint.ActionOn}
	installed := failpoint.Rule{Key: "block", Action: failpoint.ActionBlock}

	s.apply([]failpoint.Rule{skipped, installed})
	s.apply([]failpoint.Rule{skipped, installed})
	s.apply(nil)

	var messages []string
	for _, record := range logs.Snapshot() {
		messages = append(messages, record.Message)
	}
	require.Equal(t, []string{
		"Applied failpoint rule",
		"Skipped failpoint rules for keys of other failpoints",
		"Skipped failpoint rules for keys of other failpoints",
		"Removed failpoint rule",
	}, messages)
	require.Equal(t, tag.NewStringTag("failpoint-key", "block"), logs.Snapshot()[3].Tags[0])
}

// A callback can arrive after a later change replaced its value, so update takes no value: it
// applies the current value of the setting.
func TestApplier_UpdateAppliesCurrentValue(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	registry := failpoint.NewRegistry()
	s := newApplier(registry, client, log.NewNoopLogger())
	require.NoError(t, s.start(t.Context()))
	t.Cleanup(func() { _ = s.stop(context.Background()) })
	client.OverrideSetting(Setting, []any{onRule})
	await.RequireTrue(t, func() bool { return registry.Get(onKey) != nil }, 5*time.Second, 10*time.Millisecond)

	s.update()

	require.NotNil(t, registry.Get(onKey))
}

// Only the entry without constraints is the value of the setting, so an invalid entry for a
// constraint that the global setting never selects doesn't stop the server or the rules.
func TestApplier_IgnoresInvalidConstrainedEntries(t *testing.T) {
	t.Parallel()
	file := newFakeFile(`
testing.failpointRules:
  - value:
      - key: on
        action: On
  - constraints: {namespace: other}
    value:
      - key: on
        action: Of
`)
	client, err := newFileClient(t, file)
	require.NoError(t, err)
	registry := failpoint.NewRegistry()
	s := newApplier(registry, client, log.NewNoopLogger())

	require.NoError(t, s.start(t.Context()))
	t.Cleanup(func() { _ = s.stop(context.Background()) })
	require.NotNil(t, registry.Get(onKey))
}

// fakeFile is a dynamic config file whose contents a test changes.
type fakeFile struct {
	mu       sync.Mutex
	contents string
	modTime  time.Time
}

func newFakeFile(contents string) *fakeFile {
	return &fakeFile{contents: contents, modTime: time.Unix(1, 0)}
}

func (f *fakeFile) set(contents string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contents = contents
	f.modTime = f.modTime.Add(time.Second)
}

func (f *fakeFile) GetModTime() (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.modTime, nil
}

func (f *fakeFile) ReadFile() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []byte(f.contents), nil
}

func newFileClient(t *testing.T, file *fakeFile) (*dynamicconfig.FileBasedClient, error) {
	done := make(chan any)
	t.Cleanup(func() { close(done) })
	return dynamicconfig.NewFileBasedClientWithReader(file,
		&dynamicconfig.FileBasedClientConfig{PollInterval: time.Hour},
		log.NewNoopLogger(), done, nil)
}

const validFile = `
testing.failpointRules:
  - value:
      - key: on
        action: On
`

func TestApplier_File(t *testing.T) {
	t.Parallel()

	invalidFiles := []struct {
		name     string
		contents string
		wantErr  string
	}{
		{
			name: "non-string trigger key",
			contents: `
testing.failpointRules:
  - value:
      - key: on
        action: On
        trigger: {1: 2}
`,
			wantErr: "key 1 is not a string",
		},
		{
			name: "fractional count",
			contents: `
testing.failpointRules:
  - value:
      - key: on
        action: On
        trigger: {maxFires: 0.5}
`,
			wantErr: "0.5 is not an integer",
		},
	}
	for _, tc := range invalidFiles {
		t.Run("fails to start with "+tc.name, func(t *testing.T) {
			t.Parallel()
			client, err := newFileClient(t, newFakeFile(tc.contents))
			require.NoError(t, err)
			s := newApplier(failpoint.NewRegistry(), client, log.NewNoopLogger())
			require.ErrorContains(t, s.start(t.Context()), tc.wantErr)
		})

		t.Run("keeps the previous rules after an update to "+tc.name, func(t *testing.T) {
			t.Parallel()
			file := newFakeFile(validFile)
			client, err := newFileClient(t, file)
			require.NoError(t, err)
			registry := failpoint.NewRegistry()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			keptPrevious := logger.Expect(testlogger.Error, "Keeping the previous failpoint rules")
			s := newApplier(registry, client, logger)
			require.NoError(t, s.start(t.Context()))
			t.Cleanup(func() { _ = s.stop(context.Background()) })
			require.NotNil(t, registry.Get(onKey))

			file.set(tc.contents)
			require.NoError(t, client.Update())
			await.RequireTrue(t, keptPrevious.Matched, 5*time.Second, 10*time.Millisecond)
			require.NotNil(t, registry.Get(onKey))
		})
	}

	// The loader rejects a file with a non-string key in a rule as a whole: the server fails to
	// start, and an update keeps the previous dynamic config and logs an error.
	const nonStringRuleKey = `
testing.failpointRules:
  - value:
      - key: on
        action: On
        1: 2
`
	t.Run("loader rejects a non-string rule key at start", func(t *testing.T) {
		t.Parallel()
		_, err := newFileClient(t, newFakeFile(nonStringRuleKey))
		require.ErrorContains(t, err, "unable to read dynamic config")
	})

	t.Run("loader rejects a non-string rule key in an update", func(t *testing.T) {
		t.Parallel()
		file := newFakeFile(validFile)
		client, err := newFileClient(t, file)
		require.NoError(t, err)
		file.set(nonStringRuleKey)
		require.ErrorContains(t, client.Update(), "loading dynamic config failed")
	})
}

// With failpoints not enabled, an invalid value in a dynamic config file has no effect: the file
// loads without error and the module doesn't read it.
func TestModule_DisabledIgnoresInvalidFile(t *testing.T) {
	t.Parallel()
	client, err := newFileClient(t, newFakeFile(`
testing.failpointRules:
  - value:
      - key: on
        action: On
        trigger: {1: 2}
`))
	require.NoError(t, err)
	collection := dynamicconfig.NewCollection(client, log.NewNoopLogger())
	collection.Start()
	t.Cleanup(collection.Stop)
	require.NotPanics(t, func() { _ = Setting.Get(collection)() })

	var registry *failpoint.Registry
	app := fxtest.New(t,
		fx.Supply(
			registry,
			fx.Annotate(client, fx.As(new(dynamicconfig.Client))),
			fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger))),
		),
		Module,
		ReleaseModule,
	)
	app.RequireStart()
	app.RequireStop()
}
