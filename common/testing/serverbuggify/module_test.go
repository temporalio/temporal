package serverbuggify

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

// staticSeed returns the static BUGGIFY configuration of a block with the seed and the key.
func staticSeed(t *testing.T, seed uint64) StaticConfig {
	t.Helper()
	one := 1.0
	static, err := ReadStatic(config.FailpointsBuggify{Seed: seed, Keys: []config.FailpointsBuggifyKey{
		{Match: "*", ActivationProbability: &one, FireProbability: &one},
	}})
	require.NoError(t, err)
	return static
}

func newCollection(t *testing.T, client dynamicconfig.Client) *dynamicconfig.Collection {
	collection := dynamicconfig.NewCollection(client, log.NewNoopLogger())
	collection.Start()
	t.Cleanup(collection.Stop)
	return collection
}

// lockedBuffer collects trace lines that executions on several goroutines write.
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

// alwaysStatic buggifies every execution of the key "always" with an Internal error.
func alwaysStatic(t *testing.T) StaticConfig {
	t.Helper()
	one := 1.0
	static, err := ReadStatic(config.FailpointsBuggify{Seed: 5, Keys: []config.FailpointsBuggifyKey{
		{Match: "always", ActivationProbability: &one, FireProbability: &one, Error: "Internal"},
	}})
	require.NoError(t, err)
	return static
}

var (
	alwaysKey = failpoint.NewKey[error]("always")
	// otherKey is not matched by alwaysStatic.
	otherKey = failpoint.NewKey[error]("other")
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("configures BUGGIFY before the server starts and disables it when it stops", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		var errAtStart error
		app := fxtest.New(t,
			fx.Supply(registry, alwaysStatic(t), fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
			Module,
			fx.Invoke(func(lc fx.Lifecycle) {
				lc.Append(fx.StartHook(func() { errAtStart = failpoint.CheckError(registry, alwaysKey) }))
			}),
		)

		app.RequireStart()
		require.ErrorContains(t, errAtStart, "BUGGIFY: injected by failpoint always")
		b, ok := registry.Policy().(*buggify.Buggify)
		require.True(t, ok)
		require.Equal(t, uint64(5), b.Config().Seed)
		app.RequireStop()
		require.False(t, b.Config().Enabled())
		require.Nil(t, registry.Policy())
	})

	t.Run("leaves BUGGIFY off without a seed", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		app := fxtest.New(t,
			fx.Supply(registry, StaticConfig{}, fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
			Module,
		)

		app.RequireStart()
		require.Nil(t, registry.Policy())
		require.NoError(t, failpoint.CheckError(registry, alwaysKey))
		app.RequireStop()
	})
}

func TestApplier(t *testing.T) {
	t.Parallel()

	t.Run("configures BUGGIFY from the static config", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		static := staticSeed(t, 9)
		s := newApplier(registry, log.NewNoopLogger(), static, nil)
		require.NoError(t, s.start(t.Context()))
		t.Cleanup(func() { _ = s.stop(context.Background()) })
		require.Equal(t, static.Config, s.buggify.Config())

		require.NoError(t, s.stop(context.Background()))
		require.False(t, s.buggify.Config().Enabled())
	})

	t.Run("logs the seed in a tag", func(t *testing.T) {
		t.Parallel()
		logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
		logs := logger.StartCapture()
		s := newApplier(failpoint.NewRegistry(), logger, staticSeed(t, 9), nil)
		require.NoError(t, s.start(t.Context()))
		t.Cleanup(func() { _ = s.stop(context.Background()) })

		records := logs.Snapshot()
		require.Len(t, records, 1)
		require.Equal(t, "BUGGIFY enabled", records[0].Message)
		require.ElementsMatch(t, []tag.Tag{tag.NewUInt64("buggify-seed", 9)}, records[0].Tags)
	})

	t.Run("prints trace lines", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		var trace lockedBuffer
		s := newApplier(registry, log.NewNoopLogger(), alwaysStatic(t), &trace)
		require.NoError(t, s.start(t.Context()))
		t.Cleanup(func() { _ = s.stop(context.Background()) })

		require.Error(t, failpoint.CheckError(registry, alwaysKey.ForScope("ns")))
		require.Error(t, failpoint.CheckError(registry, alwaysKey))
		require.NoError(t, failpoint.CheckError(registry, otherKey))
		// BUGGIFY delivers its events to the observer, possibly after the executions returned.
		want := strings.Join([]string{
			`BUGGIFY event=activation key="always" active=true`,
			`BUGGIFY event=fire key="always" scope="ns" execution=1 action=Return`,
			`BUGGIFY event=fire key="always" scope="" execution=1 action=Return`,
			``,
		}, "\n")
		await.RequireTrue(t, func() bool { return trace.String() == want }, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("writes the queued trace lines when it stops", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		trace := newBlockingBuffer()
		s := newApplier(registry, log.NewNoopLogger(), alwaysStatic(t), trace)
		require.NoError(t, s.start(t.Context()))
		t.Cleanup(trace.release)

		// The first execution delivers its events and blocks in the writer. The second decides
		// meanwhile, so its event stays queued.
		go func() { _ = failpoint.CheckError(registry, alwaysKey.ForScope("a")) }()
		<-trace.blocked
		require.Error(t, failpoint.CheckError(registry, alwaysKey.ForScope("b")))

		stopped := make(chan error, 1)
		go func() { stopped <- s.stop(context.Background()) }()
		select {
		case <-stopped:
			t.Fatal("stop returned before the queued trace lines were written")
		case <-time.After(50 * time.Millisecond):
		}
		trace.release()
		require.NoError(t, <-stopped)
		require.Equal(t, strings.Join([]string{
			`BUGGIFY event=activation key="always" active=true`,
			`BUGGIFY event=fire key="always" scope="a" execution=1 action=Return`,
			`BUGGIFY event=fire key="always" scope="b" execution=1 action=Return`,
			``,
		}, "\n"), trace.String())
	})

	t.Run("stops when the writer blocks until the context ends", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		trace := newBlockingBuffer()
		s := newApplier(registry, log.NewNoopLogger(), alwaysStatic(t), trace)
		require.NoError(t, s.start(t.Context()))
		t.Cleanup(trace.release)

		go func() { _ = failpoint.CheckError(registry, alwaysKey.ForScope("a")) }()
		<-trace.blocked
		require.Error(t, failpoint.CheckError(registry, alwaysKey.ForScope("b")))

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		require.NoError(t, s.stop(ctx))
		require.Equal(t, buggify.Config{}, s.buggify.Config())
	})
}

// blockingBuffer is a trace writer that blocks in its first write until it is released.
type blockingBuffer struct {
	lockedBuffer
	blocked chan struct{}
	gate    chan struct{}
	once    sync.Once
	open    sync.Once
}

func newBlockingBuffer() *blockingBuffer {
	return &blockingBuffer{blocked: make(chan struct{}), gate: make(chan struct{})}
}

func (b *blockingBuffer) Write(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.blocked)
		<-b.gate
	})
	return b.lockedBuffer.Write(p)
}

func (b *blockingBuffer) release() { b.open.Do(func() { close(b.gate) }) }

// Without failpoints enabled, BUGGIFY never randomizes settings, even with a seed in the static config.
func TestRandomizeSettings_Disabled(t *testing.T) {
	t.Parallel()
	client := dynamicconfig.NewMemoryClient()
	wrapped, static, err := RandomizeSettings(client, log.NewNoopLogger(), false, config.FailpointsBuggify{Seed: 1, Trace: true})
	require.NoError(t, err)
	require.Same(t, client, wrapped)
	require.Equal(t, StaticConfig{}, static)
}

// The static config is the block that the server passes, never the environment of the process.
func TestRandomizeSettings_ReadsTheGivenStaticConfig(t *testing.T) {
	t.Setenv("FAILPOINT_BUGGIFY_SEED", "7")
	t.Setenv("FAILPOINT_BUGGIFY_TRACE", "1")
	one := 1.0
	keys := []config.FailpointsBuggifyKey{{Match: "*", ActivationProbability: &one, FireProbability: &one}}

	t.Run("an empty block", func(t *testing.T) {
		wrapped, static, err := RandomizeSettings(dynamicconfig.NewMemoryClient(), log.NewNoopLogger(), true, config.FailpointsBuggify{})
		require.NoError(t, err)
		require.NotNil(t, wrapped)
		require.Equal(t, StaticConfig{}, static)
	})

	t.Run("a block with a seed", func(t *testing.T) {
		_, static, err := RandomizeSettings(dynamicconfig.NewMemoryClient(), log.NewNoopLogger(), true,
			config.FailpointsBuggify{Seed: 3, Keys: keys, Trace: true})
		require.NoError(t, err)
		require.Equal(t, uint64(3), static.Config.Seed)
		require.True(t, static.Trace)
	})

	t.Run("an invalid block", func(t *testing.T) {
		_, _, err := RandomizeSettings(dynamicconfig.NewMemoryClient(), log.NewNoopLogger(), true, config.FailpointsBuggify{Seed: 3})
		require.ErrorContains(t, err, "a seed needs keys")
	})
}

func TestModule_Disabled(t *testing.T) {
	t.Parallel()
	var registry *failpoint.Registry
	app := fxtest.New(t,
		fx.Supply(registry, fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
		Module,
	)

	app.RequireStart()
	app.RequireStop()
}

func TestRandomizeSettings(t *testing.T) {
	t.Parallel()
	zero, one := 0.0, 1.0

	// Randomizes every marked setting.
	allSettings := config.FailpointsBuggify{Seed: 3, Keys: []config.FailpointsBuggifyKey{
		{Match: SettingPrefix + "*", ActivationProbability: &one, FireProbability: &zero},
	}}

	t.Run("returns the client without BUGGIFY", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		wrapped, static, err := randomizeSettings(client, log.NewNoopLogger(), config.FailpointsBuggify{})
		require.NoError(t, err)
		require.Same(t, client, wrapped)
		require.Equal(t, StaticConfig{}, static)
	})

	t.Run("randomizes marked settings unless they are set", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		client.OverrideSetting(dynamicconfig.TimerTaskBatchSize, 77)
		wrapped, _, err := randomizeSettings(client, log.NewNoopLogger(), allSettings)
		require.NoError(t, err)
		collection := newCollection(t, wrapped)

		require.Contains(t, []int{1, 2, 10}, dynamicconfig.TransferTaskBatchSize.Get(collection)())
		require.Contains(t, []int{1, 2, 10}, dynamicconfig.MatchingMaxTaskBatchSize.Get(collection)("ns", "tq", 1))
		require.Equal(t, 77, dynamicconfig.TimerTaskBatchSize.Get(collection)())
	})

	t.Run("randomizes only the settings that a key matches", func(t *testing.T) {
		t.Parallel()
		client := dynamicconfig.NewMemoryClient()
		block := config.FailpointsBuggify{Seed: 3, Keys: []config.FailpointsBuggifyKey{
			{Match: "grpc:*", ActivationProbability: &one, FireProbability: &one},
		}}
		wrapped, _, err := randomizeSettings(client, log.NewNoopLogger(), block)
		require.NoError(t, err)
		collection := newCollection(t, wrapped)
		require.NotContains(t, []int{1, 2, 10}, dynamicconfig.TransferTaskBatchSize.Get(collection)())
	})

	t.Run("fails with an invalid block", func(t *testing.T) {
		t.Parallel()
		_, _, err := randomizeSettings(dynamicconfig.NewMemoryClient(), log.NewNoopLogger(),
			config.FailpointsBuggify{Seed: 1, Keys: []config.FailpointsBuggifyKey{{Match: "a", ActivationProbability: &one, FireProbability: &one, Error: "Oops"}}})
		require.ErrorContains(t, err, `unknown error "Oops"`)
	})
}
