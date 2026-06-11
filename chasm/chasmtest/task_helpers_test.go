package chasmtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/log"
)

type noOpPureTTLTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *noOpPureTTLTaskHandler) Execute(
	_ chasm.MutableContext,
	_ *chasmtests.PayloadStore,
	_ chasm.TaskAttributes,
	_ *testspb.TestPayloadTTLPureTask,
) error {
	return nil
}

func (h *noOpPureTTLTaskHandler) Validate(
	_ chasm.Context,
	store *chasmtests.PayloadStore,
	_ chasm.TaskAttributes,
	task *testspb.TestPayloadTTLPureTask,
) (bool, error) {
	_, ok := store.State.ExpirationTimes[task.PayloadKey]
	return ok, nil
}

func TestExecutePureTaskRequiresPostExecutionInvalidation(t *testing.T) {
	engine, store, attrs := newPayloadStoreWithExpiringPayload(t, "pure-task-payload")

	task := &testspb.TestPayloadTTLPureTask{PayloadKey: "pure-task-payload"}
	taskDropped, err := ExecutePureTask(
		context.Background(),
		engine,
		store,
		&chasmtests.PayloadTTLPureTaskHandler{},
		attrs,
		task,
	)
	require.NoError(t, err)
	require.False(t, taskDropped)

	engine, store, attrs = newPayloadStoreWithExpiringPayload(t, "still-valid-pure-task-payload")
	task = &testspb.TestPayloadTTLPureTask{PayloadKey: "still-valid-pure-task-payload"}
	taskDropped, err = ExecutePureTask(
		context.Background(),
		engine,
		store,
		&noOpPureTTLTaskHandler{},
		attrs,
		task,
	)
	require.ErrorContains(t, err, "CHASM pure task remained valid after successful execution")
	var taskNotInvalidatedErr *chasm.TaskNotInvalidatedError
	require.ErrorAs(t, err, &taskNotInvalidatedErr)
	require.True(t, taskNotInvalidatedErr.IsTerminalTaskError())
	require.Equal(t, "pure", taskNotInvalidatedErr.TaskKind)
	require.Contains(t, taskNotInvalidatedErr.TaskType, "TestPayloadTTLPureTask")
	require.Equal(t, attrs, taskNotInvalidatedErr.TaskAttributes)
	require.False(t, taskNotInvalidatedErr.TaskAttributes.IsImmediate())
	require.False(t, taskDropped)
}

func newPayloadStoreWithExpiringPayload(
	t *testing.T,
	payloadKey string,
) (*Engine, *chasmtests.PayloadStore, chasm.TaskAttributes) {
	t.Helper()

	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(chasmtests.Library))

	engine := NewEngine(t, registry)
	ctx := chasm.NewEngineContext(context.Background(), engine)
	executionKey := chasm.ExecutionKey{
		NamespaceID: "namespace-id",
		BusinessID:  payloadKey,
	}

	var store *chasmtests.PayloadStore
	_, err := chasm.StartExecution(
		ctx,
		executionKey,
		func(mutableContext chasm.MutableContext, _ any) (*chasmtests.PayloadStore, error) {
			var err error
			store, err = chasmtests.NewPayloadStore(mutableContext)
			return store, err
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, store)

	_, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*chasmtests.PayloadStore](executionKey),
		(*chasmtests.PayloadStore).AddPayload,
		chasmtests.AddPayloadRequest{
			PayloadKey: payloadKey,
			Payload: &commonpb.Payload{
				Data: []byte("payload"),
			},
			TTL: time.Hour,
		},
	)
	require.NoError(t, err)

	return engine, store, chasm.TaskAttributes{
		ScheduledTime: time.Now().Add(2 * time.Hour),
	}
}
