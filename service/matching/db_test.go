package matching

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
)

// hookedTaskManager runs afterGet once, right after the first GetTaskQueue call returns.
type hookedTaskManager struct {
	persistence.TaskManager
	afterGet func()
}

func (h *hookedTaskManager) GetTaskQueue(
	ctx context.Context,
	request *persistence.GetTaskQueueRequest,
) (*persistence.GetTaskQueueResponse, error) {
	resp, err := h.TaskManager.GetTaskQueue(ctx, request)
	if f := h.afterGet; f != nil {
		h.afterGet = nil
		f()
	}
	return resp, err
}

func newTestTaskQueueDB(t *testing.T, store persistence.TaskManager, logger log.Logger) *taskQueueDB {
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	f, err := tqid.NewTaskQueueFamily("", "test-queue")
	require.NoError(t, err)
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")
	return newTaskQueueDB(tlCfg, store, UnversionedQueueKey(prtn), logger, metrics.NoopMetricsHandler, false)
}

// The previous owner may write metadata (without changing the range id) between the new
// owner's read and conditional update. The new owner must not overwrite that write.
func TestTakeoverDoesNotClobberConcurrentWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	tm := newTestTaskManager(logger)

	dbA := newTestTaskQueueDB(t, tm, logger)
	_, err := dbA.RenewLease(ctx)
	require.NoError(t, err)

	hooked := &hookedTaskManager{TaskManager: tm}
	dbB := newTestTaskQueueDB(t, hooked, logger)
	hooked.afterGet = func() {
		// the old owner allocates a new subqueue in between
		_, err := dbA.AllocateSubqueue(ctx, &persistencespb.SubqueueKey{Priority: 2})
		require.NoError(t, err)
	}

	state, err := dbB.RenewLease(ctx)
	require.NoError(t, err)
	require.Len(t, state.subqueues, 2, "new owner should see subqueue allocated by old owner")

	// the old owner is fenced out
	_, err = dbA.AllocateSubqueue(ctx, &persistencespb.SubqueueKey{Priority: 3})
	require.ErrorAs(t, err, new(*persistence.ConditionFailedError))
}

// If someone else takes over while we're retrying, give up instead of fighting.
func TestTakeoverGivesUpIfRangeChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	tm := newTestTaskManager(logger)

	dbA := newTestTaskQueueDB(t, tm, logger)
	_, err := dbA.RenewLease(ctx)
	require.NoError(t, err)

	hooked := &hookedTaskManager{TaskManager: tm}
	dbB := newTestTaskQueueDB(t, hooked, logger)
	hooked.afterGet = func() {
		// a third owner takes over in between
		_, err := newTestTaskQueueDB(t, tm, logger).RenewLease(ctx)
		require.NoError(t, err)
	}

	_, err = dbB.RenewLease(ctx)
	require.ErrorAs(t, err, new(*persistence.ConditionFailedError))
}
