package workflow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/workflow"
)

type (
	mockObserver struct {
		workflow.Observer
		ConnectFunc func(context.Context, workflow.Context) error
	}

	mockContext struct {
		workflow.Context
		GetWorkflowKeyFunc func() definition.WorkflowKey
	}
)

func (mo *mockObserver) Connect(ctx context.Context, wfctx workflow.Context) error {
	return mo.ConnectFunc(ctx, wfctx)
}

func (mc *mockContext) GetWorkflowKey() definition.WorkflowKey {
	return mc.GetWorkflowKeyFunc()
}

func KeyForTest(t *testing.T, suffix string) workflow.ObserverKey {
	return workflow.ObserverKey{
		ObservableID: fmt.Sprintf("%v-observable_id.%v", t.Name(), suffix),
		WFKey: definition.WorkflowKey{
			NamespaceID: fmt.Sprintf("%v-namespace_id.%v", t.Name(), suffix),
			WorkflowID:  fmt.Sprintf("%v-workflow_id.%v", t.Name(), suffix),
			RunID:       fmt.Sprintf("%v-run_id.%v", t.Name(), suffix),
		},
	}
}

func TestRefCountedObserver(t *testing.T) {
	key := KeyForTest(t, "0")
	observers := workflow.NewObserverSet()

	connectCalls := 0
	mockObs := &mockObserver{
		ConnectFunc: func(context.Context, workflow.Context) error {
			connectCalls++
			return nil
		},
	}
	mockWfCtx := &mockContext{
		GetWorkflowKeyFunc: func() definition.WorkflowKey {
			return key.WFKey
		},
	}

	mockObserverCtor := func() (workflow.Observer, error) {
		return mockObs, nil
	}
	mockCtxLookup := func() (workflow.Context, func(error), error) {
		return mockWfCtx, func(error) {}, nil
	}

	observer1, release1, err := observers.FindOrCreate(
		context.TODO(),
		key,
		mockCtxLookup,
		mockObserverCtor,
	)
	require.NoError(t, err)
	require.Same(t, observer1, mockObs)
	require.EqualValues(t, 1, connectCalls,
		"Connect should have been called once immediately after construction")

	observer2, release2, err := observers.FindOrCreate(
		context.TODO(),
		key,
		mockCtxLookup,
		func() (workflow.Observer, error) {
			require.FailNow(t, "ctor called during 2nd FindOrCreate")
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Same(t, observer1, observer2,
		"second lookup with same key should find the same observer")

	observers.ConnectAll(context.TODO(), mockWfCtx)
	require.EqualValues(t, 2, connectCalls,
		"manual call to ConnectAll should have called through to registered Observer.Connect")

	release2()
	observers.ConnectAll(context.TODO(), mockWfCtx)
	require.EqualValues(t, 3, connectCalls,
		"Releasing one Observer reference should not have removed it from ObserverSet")

	release1()
	observers.ConnectAll(context.TODO(), mockWfCtx)
	require.EqualValues(t, 3, connectCalls,
		"Releasing the last Observer reference should have unregistered it")
}
