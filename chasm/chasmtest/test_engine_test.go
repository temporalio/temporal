// Tests for the test engine.
package chasmtest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
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

func TestInvariantCheckRunsAfterSuccessfulTransitions(t *testing.T) {
	var calls []string
	check := func(name string) chasmtest.InvariantCheck {
		return func(checkT *testing.T, node *chasm.Node, root chasm.RootComponent) {
			checkT.Helper()
			require.Same(checkT, t, checkT)
			require.IsType(checkT, &tests.PayloadStore{}, root)
			require.NotPanics(checkT, func() { node.Snapshot(nil) })
			calls = append(calls, name)
		}
	}

	e, ref := startStoreWithEngineOptions(
		t,
		time.Hour,
		[]chasmtest.EngineOption{
			chasmtest.WithInvariantCheck(check("first")),
			chasmtest.WithInvariantCheck(check("second")),
		},
	)
	require.Equal(t, []string{"first", "second"}, calls)

	_, _, err := chasm.UpdateComponent(engineContext(e), ref,
		func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
			return nil, nil
		}, nil, chasm.WithRequestID("invariant-check"))
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second", "first", "second"}, calls)

	_, _, err = chasm.UpdateComponent(engineContext(e), ref,
		func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
			return nil, nil
		}, nil, chasm.WithRequestID("invariant-check"))
	require.ErrorIs(t, err, chasm.ErrRequestIDAlreadyUsed)
	require.Equal(t, []string{"first", "second", "first", "second"}, calls)

	transitionErr := errors.New("transition failed")
	_, _, err = chasm.UpdateComponent(engineContext(e), ref,
		func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
			return nil, transitionErr
		}, nil)
	require.ErrorIs(t, err, transitionErr)
	require.Equal(t, []string{"first", "second", "first", "second"}, calls)

	executed, err := e.FirePureTasks(ref, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, executed)
	require.Equal(t, []string{"first", "second", "first", "second", "first", "second"}, calls)

	key := chasm.ExecutionKey{NamespaceID: "test-ns", BusinessID: "update-with-start"}
	result, err := chasm.UpdateWithStartExecution(
		engineContext(e),
		key,
		func(mc chasm.MutableContext, _ any) (*tests.PayloadStore, error) {
			return tests.NewPayloadStore(mc)
		},
		func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
			return nil, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.True(t, result.Created)
	require.Equal(t, []string{
		"first", "second",
		"first", "second",
		"first", "second",
		"first", "second",
	}, calls)
}

func TestUpdateComponentDeduplicatesRequestID(t *testing.T) {
	e, ref := startStore(t, 0)
	updateCount := 0
	update := func() ([]byte, error) {
		_, updatedRef, err := chasm.UpdateComponent(engineContext(e), ref,
			func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
				updateCount++
				return nil, nil
			}, nil, chasm.WithRequestID("request-id"))
		return updatedRef, err
	}

	updatedRef, err := update()
	require.NoError(t, err)
	require.NotEmpty(t, updatedRef)

	updatedRef, err = update()
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.ErrorIs(t, err, chasm.ErrRequestIDAlreadyUsed)
	require.Nil(t, updatedRef)
	require.Equal(t, 1, updateCount)
}

func TestUpdateComponentDoesNotRecordFailedRequestID(t *testing.T) {
	e, ref := startStore(t, 0)
	updateErr := errors.New("update failed")
	updateCount := 0
	update := func(errToReturn error) error {
		_, _, err := chasm.UpdateComponent(engineContext(e), ref,
			func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
				updateCount++
				return nil, errToReturn
			}, nil, chasm.WithRequestID("request-id"))
		return err
	}

	require.ErrorIs(t, update(updateErr), updateErr)
	require.NoError(t, update(nil))
	require.Equal(t, 2, updateCount)
}

func TestUpdateComponentDeduplicatesCreationRequestID(t *testing.T) {
	e, ref := startStore(t, 0, chasm.WithRequestID("request-id"))
	updateCount := 0
	_, _, err := chasm.UpdateComponent(engineContext(e), ref,
		func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
			updateCount++
			return nil, nil
		}, nil, chasm.WithRequestID("request-id"))
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.Equal(t, 0, updateCount)
}

func TestStartExecutionDeduplicatesUpdateRequestID(t *testing.T) {
	e, ref := startStore(t, 0)
	ctx := engineContext(e)
	_, _, err := chasm.UpdateComponent(ctx, ref,
		func(*tests.PayloadStore, chasm.MutableContext, any) (any, error) {
			return nil, nil
		}, nil, chasm.WithRequestID("request-id"))
	require.NoError(t, err)

	startCount := 0
	startKey := ref.ExecutionKey
	startKey.RunID = ""
	result, err := chasm.StartExecution(ctx, startKey,
		func(chasm.MutableContext, any) (*tests.PayloadStore, error) {
			startCount++
			return nil, nil
		}, nil, chasm.WithRequestID("request-id"))
	require.NoError(t, err)
	require.False(t, result.Created)
	require.Equal(t, ref.ExecutionKey, result.ExecutionKey)
	require.Equal(t, 0, startCount)
}

func startStore(
	t *testing.T,
	ttl time.Duration,
	opts ...chasm.TransitionOption,
) (*chasmtest.Engine, chasm.ComponentRef) {
	return startStoreWithEngineOptions(t, ttl, nil, opts...)
}

func startStoreWithEngineOptions(
	t *testing.T,
	ttl time.Duration,
	engineOpts []chasmtest.EngineOption,
	opts ...chasm.TransitionOption,
) (*chasmtest.Engine, chasm.ComponentRef) {
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(tests.Library))

	ts := clock.NewEventTimeSource()
	ts.Update(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	engineOpts = append([]chasmtest.EngineOption{chasmtest.WithTimeSource(ts)}, engineOpts...)
	e := chasmtest.NewEngine(t, registry, engineOpts...)

	key := chasm.ExecutionKey{NamespaceID: "test-ns", BusinessID: "store"}
	result, err := chasm.StartExecution(engineContext(e), key,
		func(mc chasm.MutableContext, _ any) (*tests.PayloadStore, error) {
			store, err := tests.NewPayloadStore(mc)
			if err != nil {
				return nil, err
			}
			return store, addPayload(store, mc, "first", ttl)
		}, nil, opts...)
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
