// Tests for the test engine.
package chasmtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/service/history/tasks"
)

// TestTasksArePhysicallyGenerated: a task a component adds must reach the backend as a physical task, in
// the category its TaskAttributes imply, whether it was added while starting the execution or while
// updating it.
func TestTasksArePhysicallyGenerated(t *testing.T) {
	const ttl = time.Hour

	t.Run("added while starting", func(t *testing.T) {
		e, ref := startStore(t, ttl)
		require.Equal(t, 1, countTasks(t, e, ref, tasks.CategoryTimer))
	})

	t.Run("added while updating", func(t *testing.T) {
		e, ref := startStore(t, 0)
		require.Equal(t, 0, countTasks(t, e, ref, tasks.CategoryTimer))
		_, _, err := chasm.UpdateComponent(engineContext(e), ref,
			func(s *tests.PayloadStore, mc chasm.MutableContext, _ any) (any, error) {
				return nil, addPayload(s, mc, "second", ttl)
			}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, countTasks(t, e, ref, tasks.CategoryTimer))
	})
}

func startStore(t *testing.T, ttl time.Duration) (*chasmtest.Engine, chasm.ComponentRef) {
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(tests.Library))

	ts := clock.NewEventTimeSource()
	ts.Update(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	e := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))

	key := chasm.ExecutionKey{NamespaceID: "test-ns", BusinessID: "store"}
	result, err := chasm.StartExecution(engineContext(e), key,
		func(mc chasm.MutableContext, _ any) (*tests.PayloadStore, error) {
			store, err := tests.NewPayloadStore(mc)
			if err != nil {
				return nil, err
			}
			return store, addPayload(store, mc, "first", ttl)
		}, nil)
	require.NoError(t, err)

	key.RunID = result.ExecutionKey.RunID
	return e, chasm.NewComponentRef[*tests.PayloadStore](key)
}

// addPayload stores one payload, which schedules a TTL task when ttl is non-zero.
func addPayload(store *tests.PayloadStore, mc chasm.MutableContext, key string, ttl time.Duration) error {
	_, err := store.AddPayload(mc, tests.AddPayloadRequest{
		PayloadKey: key,
		Payload:    payload.EncodeString("payload"),
		TTL:        ttl,
	})
	return err
}

// countTasks is how many physical tasks the execution has accumulated in the given category.
func countTasks(t *testing.T, e *chasmtest.Engine, ref chasm.ComponentRef, category tasks.Category) int {
	byCategory, err := e.Tasks(ref)
	require.NoError(t, err)
	return len(byCategory[category])
}

func engineContext(e *chasmtest.Engine) context.Context {
	return chasm.NewEngineContext(context.Background(), e)
}
