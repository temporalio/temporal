package chasmtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type (
	driverComponent struct {
		chasm.UnimplementedComponent

		Data      *wrapperspb.Int64Value
		PureValue chasm.Field[*wrapperspb.Int64Value]
		SideValue chasm.Field[*wrapperspb.Int64Value]
		Revision  chasm.Field[*wrapperspb.Int64Value]
	}

	pureDriverTask struct {
		Value *wrapperspb.Int64Value
	}

	sideEffectDriverTask struct {
		Value *wrapperspb.Int64Value
	}

	pureDriverTaskHandler struct {
		chasm.PureTaskHandlerBase

		err        error
		reschedule time.Duration
	}

	sideEffectDriverTaskHandler struct {
		chasm.SideEffectTaskHandlerBase[*sideEffectDriverTask]

		err        error
		reschedule bool
	}

	driverLibrary struct {
		chasm.UnimplementedLibrary

		pureHandler       *pureDriverTaskHandler
		sideEffectHandler *sideEffectDriverTaskHandler
	}

	driverTestEnv struct {
		engine            *chasmtest.Engine
		engineCtx         context.Context
		ref               chasm.ComponentRef
		timeSource        *clock.EventTimeSource
		backend           *chasm.MockNodeBackend
		pureHandler       *pureDriverTaskHandler
		sideEffectHandler *sideEffectDriverTaskHandler
	}
)

func (c *driverComponent) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (c *driverComponent) Terminate(
	chasm.MutableContext,
	chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

func (c *driverComponent) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

func (h *pureDriverTaskHandler) Validate(
	ctx chasm.Context,
	component *driverComponent,
	_ chasm.TaskInvocation,
	task *pureDriverTask,
) (bool, error) {
	return component.PureValue.Get(ctx).Value < task.Value.Value, nil
}

func (h *pureDriverTaskHandler) Execute(
	ctx chasm.MutableContext,
	component *driverComponent,
	attrs chasm.TaskAttributes,
	task *pureDriverTask,
) error {
	if h.err != nil {
		return h.err
	}
	component.PureValue = chasm.NewDataField(ctx, wrapperspb.Int64(task.Value.Value))
	if h.reschedule > 0 {
		ctx.AddTask(
			component,
			chasm.TaskAttributes{ScheduledTime: attrs.ScheduledTime.Add(h.reschedule)},
			&pureDriverTask{Value: wrapperspb.Int64(task.Value.Value + 1)},
		)
	}
	return nil
}

func (h *sideEffectDriverTaskHandler) Validate(
	ctx chasm.Context,
	component *driverComponent,
	_ chasm.TaskInvocation,
	task *sideEffectDriverTask,
) (bool, error) {
	return component.SideValue.Get(ctx).Value < task.Value.Value, nil
}

func (h *sideEffectDriverTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *sideEffectDriverTask,
) error {
	if h.err != nil {
		return h.err
	}
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(component *driverComponent, mutableCtx chasm.MutableContext, task *sideEffectDriverTask) (struct{}, error) {
			component.SideValue = chasm.NewDataField(mutableCtx, wrapperspb.Int64(task.Value.Value))
			if h.reschedule {
				mutableCtx.AddTask(
					component,
					chasm.TaskAttributes{},
					&sideEffectDriverTask{Value: wrapperspb.Int64(task.Value.Value + 1)},
				)
			}
			return struct{}{}, nil
		},
		task,
	)
	return err
}

func (l *driverLibrary) Name() string {
	return "TaskDriver"
}

func (l *driverLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*driverComponent]("Component"),
	}
}

func (l *driverLibrary) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("Pure", l.pureHandler),
		chasm.NewRegistrableSideEffectTask("SideEffect", l.sideEffectHandler),
	}
}

func TestRunnableTasksFiltersAndOrders(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "ordering")

	env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 2)
	env.addSideEffectTask(t, chasm.TaskAttributes{Destination: "example.test"}, 3)
	env.addSideEffectTask(t, chasm.TaskAttributes{ScheduledTime: now.Add(-time.Minute)}, 4)
	env.addSideEffectTask(t, chasm.TaskAttributes{ScheduledTime: now.Add(time.Minute)}, 5)

	allTasks, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	visibilityTask := allTasks[tasks.CategoryTransfer][0]
	env.backend.TasksByCategory[tasks.CategoryVisibility] = []tasks.Task{visibilityTask}

	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Equal(t, []tasks.Task{
		allTasks[tasks.CategoryTransfer][0],
		allTasks[tasks.CategoryTransfer][1],
		allTasks[tasks.CategoryOutbound][0],
		allTasks[tasks.CategoryTimer][0],
	}, runnable)
}

func TestExecutePureTaskBatchesAndSupportsRedelivery(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "pure-batch")
	env.addPureTasks(t,
		pureTaskSpec{scheduledTime: now.Add(-time.Minute), value: 1},
		pureTaskSpec{scheduledTime: now, value: 2},
		pureTaskSpec{scheduledTime: now.Add(time.Minute), value: 3},
	)

	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	savedTask := runnable[0]

	result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, savedTask)
	require.NoError(t, err)
	require.Equal(t, chasmtest.TaskExecutionResult{Executed: 2}, result)
	require.Equal(t, int64(2), env.values(t).pure)

	runnable, err = env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Empty(t, runnable)

	result, err = env.engine.ExecuteTask(env.engineCtx, env.ref, savedTask)
	require.NoError(t, err)
	require.Equal(t, chasmtest.TaskExecutionResult{Dropped: true}, result)
	require.Equal(t, int64(2), env.values(t).pure)

	env.timeSource.Update(now.Add(time.Minute))
	runnable, err = env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	result, err = env.engine.ExecuteTask(env.engineCtx, env.ref, runnable[0])
	require.NoError(t, err)
	require.Equal(t, chasmtest.TaskExecutionResult{Executed: 1}, result)
	require.Equal(t, int64(3), env.values(t).pure)
}

func TestExecuteSideEffectTaskAcknowledgesSuccessAndStaleTasks(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)

	t.Run("success and redelivery", func(t *testing.T) {
		env := newDriverTestEnv(t, now, "side-effect-success")
		env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Len(t, runnable, 1)
		savedTask := runnable[0]

		result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, savedTask)
		require.NoError(t, err)
		require.Equal(t, chasmtest.TaskExecutionResult{Executed: 1}, result)
		require.Equal(t, int64(1), env.values(t).sideEffect)

		result, err = env.engine.ExecuteTask(env.engineCtx, env.ref, savedTask)
		require.NoError(t, err)
		require.Equal(t, chasmtest.TaskExecutionResult{Dropped: true}, result)
	})

	t.Run("stale", func(t *testing.T) {
		env := newDriverTestEnv(t, now, "side-effect-stale")
		env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Len(t, runnable, 1)

		env.setSideEffectValue(t, 1)
		result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, runnable[0])
		require.NoError(t, err)
		require.Equal(t, chasmtest.TaskExecutionResult{Dropped: true}, result)

		runnable, err = env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Empty(t, runnable)
	})
}

func TestExecutePureTaskReplacesPhysicalTimer(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "pure-reschedule")
	env.pureHandler.reschedule = time.Minute
	env.addPureTasks(t, pureTaskSpec{scheduledTime: now, value: 1})

	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	_, err = env.engine.ExecuteTask(env.engineCtx, env.ref, runnable[0])
	require.NoError(t, err)

	runnable, err = env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Empty(t, runnable)
	queued, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	require.Len(t, queued[tasks.CategoryTimer], 1)
	require.Equal(t, now.Add(time.Minute), queued[tasks.CategoryTimer][0].GetVisibilityTime())

	env.pureHandler.reschedule = 0
	env.timeSource.Update(now.Add(time.Minute))
	runnable, err = env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	_, err = env.engine.ExecuteTask(env.engineCtx, env.ref, runnable[0])
	require.NoError(t, err)
	require.Equal(t, int64(2), env.values(t).pure)
}

func TestExecuteTaskRetriesHandlerErrors(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	retryErr := errors.New("retry")

	t.Run("side effect", func(t *testing.T) {
		env := newDriverTestEnv(t, now, "side-effect-retry")
		env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Len(t, runnable, 1)

		env.sideEffectHandler.err = retryErr
		_, err = env.engine.ExecuteTask(env.engineCtx, env.ref, runnable[0])
		require.ErrorIs(t, err, retryErr)

		stillRunnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Equal(t, runnable, stillRunnable)
		require.Equal(t, int64(0), env.values(t).sideEffect)

		env.sideEffectHandler.err = nil
		result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, stillRunnable[0])
		require.NoError(t, err)
		require.Equal(t, chasmtest.TaskExecutionResult{Executed: 1}, result)
		require.Equal(t, int64(1), env.values(t).sideEffect)
	})

	t.Run("pure", func(t *testing.T) {
		env := newDriverTestEnv(t, now, "pure-retry")
		env.addPureTasks(t, pureTaskSpec{scheduledTime: now, value: 1})
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Len(t, runnable, 1)

		env.pureHandler.err = retryErr
		_, err = env.engine.ExecuteTask(env.engineCtx, env.ref, runnable[0])
		require.ErrorIs(t, err, retryErr)
		require.Equal(t, int64(0), env.values(t).pure)
		stillRunnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Equal(t, runnable, stillRunnable)

		env.pureHandler.err = nil
		result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, stillRunnable[0])
		require.NoError(t, err)
		require.Equal(t, chasmtest.TaskExecutionResult{Executed: 1}, result)
		require.Equal(t, int64(1), env.values(t).pure)
	})
}

func TestUpdateComponentRollsBackErrors(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "update-rollback")
	beforeTasks, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	updateErr := errors.New("reject update")

	_, err = env.engine.UpdateComponent(
		env.engineCtx,
		env.ref,
		func(ctx chasm.MutableContext, component chasm.Component) error {
			driver := component.(*driverComponent)
			driver.PureValue = chasm.NewDataField(ctx, wrapperspb.Int64(10))
			ctx.AddTask(driver, chasm.TaskAttributes{}, &sideEffectDriverTask{Value: wrapperspb.Int64(10)})
			return updateErr
		},
	)
	require.ErrorIs(t, err, updateErr)
	require.Equal(t, driverValues{}, env.values(t))

	afterTasks, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	require.Equal(t, beforeTasks, afterTasks)
}

func TestExecuteTaskKeepsExecutionsIsolated(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "execution-a")
	otherRef := env.startExecution(t, "execution-b")
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)

	_, err = env.engine.ExecuteTask(env.engineCtx, otherRef, runnable[0])
	require.ErrorContains(t, err, "does not match ref execution key")
	require.Equal(t, int64(0), env.values(t).sideEffect)

	otherValues, err := chasm.ReadComponent(
		env.engineCtx,
		otherRef,
		func(component *driverComponent, ctx chasm.Context, _ struct{}) (driverValues, error) {
			return readDriverValues(component, ctx), nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	require.Equal(t, driverValues{}, otherValues)
}

func TestReloadExecutionReconstructsTreeAndPreservesTasks(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "reload")
	env.addPureTasks(t, pureTaskSpec{scheduledTime: now.Add(time.Minute), value: 1})
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 2)

	var before *driverComponent
	require.NoError(t, env.engine.ReadComponent(
		env.engineCtx,
		env.ref,
		func(_ chasm.Context, component chasm.Component) error {
			before = component.(*driverComponent)
			return nil
		},
	))
	queuedBefore, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	savedTask := runnable[0]

	require.NoError(t, env.engine.ReloadExecution(env.engineCtx, env.ref))

	var after *driverComponent
	require.NoError(t, env.engine.ReadComponent(
		env.engineCtx,
		env.ref,
		func(_ chasm.Context, component chasm.Component) error {
			after = component.(*driverComponent)
			return nil
		},
	))
	require.NotSame(t, before, after)
	queuedAfter, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	require.Equal(t, queuedBefore, queuedAfter)
	require.Equal(t, driverValues{}, env.values(t))

	result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, savedTask)
	require.NoError(t, err)
	require.Equal(t, chasmtest.TaskExecutionResult{Executed: 1}, result)
	env.timeSource.Update(now.Add(time.Minute))
	runnable, err = env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	_, err = env.engine.ExecuteTask(env.engineCtx, env.ref, runnable[0])
	require.NoError(t, err)
	require.Equal(t, driverValues{pure: 1, sideEffect: 2}, env.values(t))

	require.NoError(t, env.engine.ReloadExecution(env.engineCtx, env.ref))
	result, err = env.engine.ExecuteTask(env.engineCtx, env.ref, savedTask)
	require.NoError(t, err)
	require.Equal(t, chasmtest.TaskExecutionResult{Dropped: true}, result)
}

func TestReloadExecutionRejectsInvalidReference(t *testing.T) {
	env := newDriverTestEnv(t, time.Now(), "reload-invalid")
	invalid := chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
		NamespaceID: "namespace-id",
		BusinessID:  "missing",
	}}

	err := env.engine.ReloadExecution(env.engineCtx, invalid)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
}

func TestDrainTasksReportsLivelock(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "livelock")
	env.sideEffectHandler.reschedule = true
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)

	drained, err := env.engine.DrainTasks(env.engineCtx, env.ref, 3)
	require.Equal(t, 3, drained)
	require.ErrorContains(t, err, "task drain limit 3 reached")
	require.Equal(t, int64(3), env.values(t).sideEffect)

	runnable, runnableErr := env.engine.RunnableTasks(env.ref)
	require.NoError(t, runnableErr)
	require.NotEmpty(t, runnable)
}

func TestReplayableDeliveriesRespectCategoryHeadsAndRetainRedelivery(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "replayable-deliveries")
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 2)
	env.addSideEffectTask(t, chasm.TaskAttributes{Destination: "example.test"}, 3)

	deliveries, err := env.engine.RunnableDeliveries(env.ref)
	require.NoError(t, err)
	require.Len(t, deliveries, 2)

	byCategory := make(map[int]chasmtest.DeliveryRef, len(deliveries))
	for _, delivery := range deliveries {
		byCategory[delivery.CategoryID] = delivery
	}
	transfer := byCategory[tasks.CategoryTransfer.ID()]
	outbound := byCategory[tasks.CategoryOutbound.ID()]
	require.NotEmpty(t, transfer.ID)
	require.NotEmpty(t, outbound.ID)

	// Categories are independent: outbound can run while the transfer head is delayed.
	receipt, err := env.engine.Deliver(env.engineCtx, env.ref, outbound)
	require.NoError(t, err)
	require.Equal(t, chasmtest.TaskExecutionResult{Executed: 1}, receipt.Result)
	require.Equal(t, int64(3), env.values(t).sideEffect)

	// The second transfer task remains blocked by its category head.
	deliveries, err = env.engine.RunnableDeliveries(env.ref)
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	require.Equal(t, transfer.ID, deliveries[0].ID)

	receipt, err = env.engine.Deliver(env.engineCtx, env.ref, transfer)
	require.NoError(t, err)
	require.Equal(t, chasmtest.TaskExecutionResult{Dropped: true}, receipt.Result)

	// A delivered task survives reload for an explicit duplicate redelivery.
	var trace chasmtest.DeliveryTrace
	trace.RecordDelivery(receipt)
	require.NoError(t, env.engine.Reload(env.engineCtx, env.ref, &trace))
	receipt, err = env.engine.Redeliver(env.engineCtx, env.ref, outbound)
	require.NoError(t, err)
	require.True(t, receipt.Redelivery)
	require.Equal(t, chasmtest.TaskExecutionResult{Dropped: true}, receipt.Result)
	trace.RecordDelivery(receipt)

	encoded, err := json.Marshal(trace)
	require.NoError(t, err)
	var replayed chasmtest.DeliveryTrace
	require.NoError(t, json.Unmarshal(encoded, &replayed))
	require.Equal(t, 1, replayed.Version)
	require.Len(t, replayed.Events, 3)
}

func TestDeliverRejectsUnknownAndNonHeadReferences(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "invalid-delivery")
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 1)
	env.addSideEffectTask(t, chasm.TaskAttributes{}, 2)

	_, err := env.engine.Deliver(env.engineCtx, env.ref, chasmtest.DeliveryRef{ID: "unknown"})
	require.ErrorContains(t, err, "unknown delivery")

	deliveries, err := env.engine.RunnableDeliveries(env.ref)
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	// A made-up ref cannot select the second queued task or bypass the head.
	deliveries[0].ID = "delivery-2"
	_, err = env.engine.Deliver(env.engineCtx, env.ref, deliveries[0])
	require.ErrorContains(t, err, "unknown delivery")
}

type pureTaskSpec struct {
	scheduledTime time.Time
	value         int64
}

type driverValues struct {
	pure       int64
	sideEffect int64
}

func newDriverTestEnv(t *testing.T, now time.Time, businessID string) *driverTestEnv {
	t.Helper()

	pureHandler := &pureDriverTaskHandler{}
	sideEffectHandler := &sideEffectDriverTaskHandler{}
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&driverLibrary{
		pureHandler:       pureHandler,
		sideEffectHandler: sideEffectHandler,
	}))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	var backend *chasm.MockNodeBackend
	engine := chasmtest.NewEngine(
		t,
		registry,
		chasmtest.WithTimeSource(timeSource),
		chasmtest.WithNodeBackendDecorator(func(nodeBackend *chasm.MockNodeBackend) {
			backend = nodeBackend
		}),
	)
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	env := &driverTestEnv{
		engine:            engine,
		engineCtx:         engineCtx,
		timeSource:        timeSource,
		pureHandler:       pureHandler,
		sideEffectHandler: sideEffectHandler,
	}
	env.ref = env.startExecution(t, businessID)
	env.backend = backend
	return env
}

func (e *driverTestEnv) startExecution(t *testing.T, businessID string) chasm.ComponentRef {
	t.Helper()

	result, err := e.engine.StartExecution(
		e.engineCtx,
		chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
			NamespaceID: "namespace-id",
			BusinessID:  businessID,
			RunID:       businessID + "-run",
		}},
		func(ctx chasm.MutableContext) (chasm.RootComponent, error) {
			return &driverComponent{
				Data:      wrapperspb.Int64(0),
				PureValue: chasm.NewDataField(ctx, wrapperspb.Int64(0)),
				SideValue: chasm.NewDataField(ctx, wrapperspb.Int64(0)),
				Revision:  chasm.NewDataField(ctx, wrapperspb.Int64(0)),
			}, nil
		},
	)
	require.NoError(t, err)
	ref, err := chasm.DeserializeComponentRef(result.ExecutionRef)
	require.NoError(t, err)
	return ref
}

func (e *driverTestEnv) addPureTasks(t *testing.T, specs ...pureTaskSpec) {
	t.Helper()
	e.update(t, func(ctx chasm.MutableContext, component *driverComponent) {
		component.Revision = chasm.NewDataField(ctx, wrapperspb.Int64(component.Revision.Get(ctx).Value+1))
		for _, spec := range specs {
			ctx.AddTask(
				component,
				chasm.TaskAttributes{ScheduledTime: spec.scheduledTime},
				&pureDriverTask{Value: wrapperspb.Int64(spec.value)},
			)
		}
	})
}

func (e *driverTestEnv) addSideEffectTask(t *testing.T, attrs chasm.TaskAttributes, value int64) {
	t.Helper()
	e.update(t, func(ctx chasm.MutableContext, component *driverComponent) {
		component.Revision = chasm.NewDataField(ctx, wrapperspb.Int64(component.Revision.Get(ctx).Value+1))
		ctx.AddTask(component, attrs, &sideEffectDriverTask{Value: wrapperspb.Int64(value)})
	})
}

func (e *driverTestEnv) setSideEffectValue(t *testing.T, value int64) {
	t.Helper()
	e.update(t, func(ctx chasm.MutableContext, component *driverComponent) {
		component.SideValue = chasm.NewDataField(ctx, wrapperspb.Int64(value))
	})
}

func (e *driverTestEnv) update(
	t *testing.T,
	updateFn func(chasm.MutableContext, *driverComponent),
) {
	t.Helper()
	_, err := e.engine.UpdateComponent(
		e.engineCtx,
		e.ref,
		func(ctx chasm.MutableContext, component chasm.Component) error {
			updateFn(ctx, component.(*driverComponent))
			return nil
		},
	)
	require.NoError(t, err)
}

func (e *driverTestEnv) values(t *testing.T) driverValues {
	t.Helper()
	values, err := chasm.ReadComponent(
		e.engineCtx,
		e.ref,
		func(component *driverComponent, ctx chasm.Context, _ struct{}) (driverValues, error) {
			return readDriverValues(component, ctx), nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	return values
}

func readDriverValues(component *driverComponent, ctx chasm.Context) driverValues {
	return driverValues{
		pure:       component.PureValue.Get(ctx).Value,
		sideEffect: component.SideValue.Get(ctx).Value,
	}
}
