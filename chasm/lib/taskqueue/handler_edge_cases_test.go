package taskqueue

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	chasmtaskqueuepb "go.temporal.io/server/chasm/lib/taskqueue/gen/taskqueuepb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
)

func requireTestTaskQueueAbsent(ctx context.Context, t *testing.T, req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) {
	t.Helper()
	_, err := chasm.ReadComponent(ctx,
		chasm.NewComponentRef[*TaskQueue](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(*TaskQueue, chasm.Context, chasm.NoValue) (chasm.NoValue, error) {
			return nil, nil
		}, nil)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestTaskQueueUserDataEmptyChild(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		data      *persistencespb.TaskQueueTypeUserData
		keepClock bool
	}{
		{name: "nil type"},
		{name: "empty type", data: &persistencespb.TaskQueueTypeUserData{}},
		{name: "clock only", keepClock: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, req := newTestHandler(t)
			populated := common.CloneProto(req)
			if !testCase.keepClock {
				req.Clock = nil
			}
			req.PerType = map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): testCase.data,
			}
			created, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 1}, created)
			key := readTestTaskQueueExecutionKey(ctx, t, req)
			expected := &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
				Version: 1,
				Clock:   req.Clock,
			}
			protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))

			_, err = h.UpsertTaskQueueUserData(ctx, populated)
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))
			populated.ExpectedVersion = 1
			updated, err := h.UpsertTaskQueueUserData(ctx, populated)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 2}, updated)
			protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
				Version: 2,
				Clock:   populated.Clock,
				PerType: populated.PerType,
			}, readTestTaskQueueUserData(ctx, t, h, req))
			require.Equal(t, key, readTestTaskQueueExecutionKey(ctx, t, req))
		})
	}
}

func TestTaskQueueUserDataRejectedWritePreservesParentWithoutChild(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		data map[int32]*persistencespb.TaskQueueTypeUserData
	}{
		{name: "nil map"},
		{name: "empty map", data: map[int32]*persistencespb.TaskQueueTypeUserData{}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, req := newTestHandler(t)
			started, err := chasm.StartExecution(ctx,
				chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue},
				func(chasm.MutableContext, chasm.NoValue) (*TaskQueue, error) {
					return NewTaskQueue(), nil
				}, nil)
			require.NoError(t, err)
			req.PerType = testCase.data
			_, err = h.UpsertTaskQueueUserData(ctx, req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			_, err = chasm.ReadComponent(ctx,
				chasm.NewComponentRef[*TaskQueue](started.ExecutionKey),
				func(taskQueue *TaskQueue, chasmContext chasm.Context, _ chasm.NoValue) (chasm.NoValue, error) {
					require.Equal(t, started.ExecutionKey, chasmContext.ExecutionKey())
					require.Equal(t, chasm.LifecycleStateRunning, taskQueue.LifecycleState(chasmContext))
					_, present := taskQueue.UserData.TryGet(chasmContext)
					require.False(t, present)
					return nil, nil
				}, nil)
			require.NoError(t, err)
			require.Equal(t, started.ExecutionKey, readTestTaskQueueExecutionKey(ctx, t, req))
		})
	}
}

// Serialize the in-memory engine's transactions as the production execution lease does.
// Gates allow competing requests to arrive before either transaction executes.
type gatedTaskQueueEngine struct {
	chasm.Engine
	mu         sync.Mutex
	startGate  chan chan struct{}
	updateGate chan chan struct{}
}

func waitForTaskQueueWrite(ctx context.Context, gate chan chan struct{}) error {
	if gate == nil {
		return nil
	}
	release := make(chan struct{})
	select {
	case gate <- release:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *gatedTaskQueueEngine) StartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.RootComponent, error),
	opts ...chasm.TransitionOption,
) (chasm.StartExecutionResult, error) {
	if err := waitForTaskQueueWrite(ctx, e.startGate); err != nil {
		return chasm.StartExecutionResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Engine.StartExecution(ctx, ref, startFn, opts...)
}

func (e *gatedTaskQueueEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) ([]byte, error) {
	if err := waitForTaskQueueWrite(ctx, e.updateGate); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Engine.UpdateComponent(ctx, ref, updateFn, opts...)
}

func TestTaskQueueUserDataCompetingWrites(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name            string
		parentExists    bool
		childExists     bool
		expectedVersion int64
	}{
		{name: "create without parent"},
		{name: "create under existing parent", parentExists: true},
		{name: "compare and swap", parentExists: true, childExists: true, expectedVersion: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var engine *chasmtest.Engine
			h, ctx, req := newTestHandler(t, func(e *chasmtest.Engine) { engine = e })
			var originalKey chasm.ExecutionKey
			if testCase.childExists {
				_, err := h.UpsertTaskQueueUserData(ctx, req)
				require.NoError(t, err)
			} else if testCase.parentExists {
				_, err := chasm.StartExecution(ctx,
					chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue},
					func(chasm.MutableContext, chasm.NoValue) (*TaskQueue, error) {
						return NewTaskQueue(), nil
					}, nil)
				require.NoError(t, err)
			}
			if testCase.parentExists {
				originalKey = readTestTaskQueueExecutionKey(ctx, t, req)
			}
			gate := make(chan chan struct{})
			gatedEngine := &gatedTaskQueueEngine{Engine: engine}
			if testCase.parentExists {
				gatedEngine.updateGate = gate
			} else {
				gatedEngine.startGate = gate
			}
			writeCtx, cancel := context.WithCancel(chasm.NewEngineContext(ctx, gatedEngine))
			var workers sync.WaitGroup
			t.Cleanup(func() {
				cancel()
				workers.Wait()
			})
			type writeResult struct {
				request  *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest
				response *chasmtaskqueuepb.UpsertTaskQueueUserDataResponse
				err      error
			}
			results := make(chan writeResult, 2)
			for index := range 2 {
				request := common.CloneProto(req)
				request.ExpectedVersion = testCase.expectedVersion
				request.Clock.WallClock = int64(100 + index)
				workers.Go(func() {
					response, err := h.UpsertTaskQueueUserData(writeCtx, request)
					results <- writeResult{request: request, response: response, err: err}
				})
			}
			first := await.Rcv(t, gate)
			second := await.Rcv(t, gate)
			close(first)
			winner := await.Rcv(t, results)
			require.NoError(t, winner.err)
			version := testCase.expectedVersion + 1
			protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: version}, winner.response)
			close(second)
			loser := await.Rcv(t, results)
			require.ErrorAs(t, loser.err, new(*serviceerror.FailedPrecondition))
			require.Nil(t, loser.response)
			protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
				Version: version,
				Clock:   winner.request.Clock,
				PerType: winner.request.PerType,
			}, readTestTaskQueueUserData(ctx, t, h, req))
			if testCase.parentExists {
				require.Equal(t, originalKey, readTestTaskQueueExecutionKey(ctx, t, req))
			}
		})
	}
}
