package failpointrules

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/failpoints"
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

// newApp returns an app with the modules around a server hook, as the server graph has them.
func newApp(t *testing.T, source *failpoints.Source, client dynamicconfig.Client, server fx.Hook) *fxtest.App {
	return fxtest.New(t,
		fx.Supply(
			source,
			newCollection(t, client),
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
	onKey := failpoint.KeyFromName("on", "")

	t.Run("applies the initial value before the server starts and removes it before the server stops", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, []any{onRule})
		source := failpoints.NewEnabledSource()
		var activeAtStart, activeAtStop bool
		app := newApp(t, source, client, fx.Hook{
			OnStart: func(context.Context) error {
				activeAtStart = failpoint.Active(source, onKey)
				return nil
			},
			OnStop: func(context.Context) error {
				activeAtStop = failpoint.Active(source, onKey)
				return nil
			},
		})

		app.RequireStart()
		app.RequireStop()
		require.True(t, activeAtStart)
		require.False(t, activeAtStop)
		require.Empty(t, source.Failpoints().Keys())
	})

	t.Run("removes the rules when the server fails to start", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, []any{onRule})
		source := failpoints.NewEnabledSource()
		app := newApp(t, source, client, fx.Hook{
			OnStart: func(context.Context) error { return errors.New("server failed") },
		})

		require.ErrorContains(t, app.Start(t.Context()), "server failed")
		require.Empty(t, source.Failpoints().Keys())
	})

	t.Run("fails to start with an invalid initial value", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(Setting, []any{map[string]any{"key": "on", "action": "Of"}})
		source := failpoints.NewEnabledSource()
		serverStarted := false
		app := newApp(t, source, client, fx.Hook{
			OnStart: func(context.Context) error {
				serverStarted = true
				return nil
			},
		})

		err := app.Start(t.Context())
		require.ErrorContains(t, err, `invalid dynamic config setting testing.failpointrules: rule 0: unknown action "Of"`)
		require.False(t, serverStarted)
		require.Empty(t, source.Failpoints().Keys())
	})
}

func TestModule_Disabled(t *testing.T) {
	t.Parallel()
	for name, source := range map[string]*failpoints.Source{
		"disabled": failpoints.NewSource(failpoints.Decision{}),
		"nil":      nil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Even an invalid value doesn't stop the server: the module doesn't read the setting.
			client := dynamicconfig.NewMemoryClient()
			client.OverrideSetting(Setting, []any{map[string]any{"key": "on", "action": "Of"}})
			started := false
			app := newApp(t, source, client, fx.Hook{OnStart: func(context.Context) error {
				started = true
				return nil
			}})

			app.RequireStart()
			app.RequireStop()
			require.True(t, started)
			require.Nil(t, source.Failpoints())
		})
	}
}

func TestSource_Update(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	registry := failpoint.NewRegistry()
	fatalErrs := make(chan error, 1)
	s := newSource(registry, newCollection(t, client), log.NewNoopLogger(), func(err error) { fatalErrs <- err })
	require.NoError(t, s.start(t.Context()))
	require.Empty(t, registry.Keys())

	onKey := failpoint.KeyFromName("on", "")
	cleanup := client.OverrideSetting(Setting, []any{onRule})
	await.RequireTrue(t, func() bool { return registry.Get(onKey) != nil }, 5*time.Second, 10*time.Millisecond)

	// An invalid value is fatal and keeps the previous rules.
	client.OverrideSetting(Setting, []any{onRule, map[string]any{"key": "r", "action": "Return"}})
	require.ErrorContains(t, await.Rcv(t, fatalErrs), "error must be set for action Return")
	require.NotNil(t, registry.Get(onKey))

	require.NoError(t, s.stop(t.Context()))
	require.Empty(t, registry.Keys())
	cleanup()
}

func TestSource_InvalidValueReplacedBeforeItsCallback(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	fatalErrs := make(chan error, 1)
	s := newSource(failpoint.NewRegistry(), newCollection(t, client), log.NewNoopLogger(), func(err error) { fatalErrs <- err })
	require.NoError(t, s.start(t.Context()))
	t.Cleanup(func() { _ = s.stop(context.Background()) })

	client.OverrideSetting(Setting, []any{map[string]any{"key": "r", "action": "Return"}})
	client.OverrideSetting(Setting, []any{onRule})
	require.ErrorContains(t, await.Rcv(t, fatalErrs), "error must be set for action Return")
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

func TestSource_File(t *testing.T) {
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
        trigger: {max: 0.5}
`,
			wantErr: "0.5 is not an integer",
		},
	}
	for _, tc := range invalidFiles {
		t.Run("fails to start with "+tc.name, func(t *testing.T) {
			t.Parallel()
			client, err := newFileClient(t, newFakeFile(tc.contents))
			require.NoError(t, err)
			s := newSource(failpoint.NewRegistry(), newCollection(t, client), log.NewNoopLogger(), func(err error) {
				t.Errorf("unexpected fatal error: %v", err)
			})
			require.ErrorContains(t, s.start(t.Context()), tc.wantErr)
		})

		t.Run("is fatal after an update to "+tc.name, func(t *testing.T) {
			t.Parallel()
			file := newFakeFile(validFile)
			client, err := newFileClient(t, file)
			require.NoError(t, err)
			registry := failpoint.NewRegistry()
			fatalErrs := make(chan error, 1)
			s := newSource(registry, newCollection(t, client), log.NewNoopLogger(), func(err error) { fatalErrs <- err })
			require.NoError(t, s.start(t.Context()))
			t.Cleanup(func() { _ = s.stop(context.Background()) })
			require.NotNil(t, registry.Get(failpoint.KeyFromName("on", "")))

			file.set(tc.contents)
			require.NoError(t, client.Update())
			require.ErrorContains(t, await.Rcv(t, fatalErrs), tc.wantErr)
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
	collection := newCollection(t, client)
	require.NotPanics(t, func() { _ = Setting.Get(collection)() })

	source := failpoints.NewSource(failpoints.Decision{})
	app := fxtest.New(t,
		fx.Supply(source, collection, fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
		Module,
		ReleaseModule,
	)
	app.RequireStart()
	app.RequireStop()
	require.Nil(t, source.Failpoints())
}
