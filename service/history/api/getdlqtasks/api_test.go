package getdlqtasks_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api/getdlqtasks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

// failingHistoryTaskQueueManager is a [persistence.HistoryTaskQueueManager] that always fails.
type failingHistoryTaskQueueManager struct {
	persistence.HistoryTaskQueueManager
}

func TestInvoke_InvalidTaskCategory(t *testing.T) {
	t.Parallel()

	_, err := getdlqtasks.Invoke(context.Background(),
		nil,
		tasks.NewDefaultTaskCategoryRegistry(),
		&historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory: -1,
			},
		})

	var invalidArgErr *serviceerror.InvalidArgument

	require.ErrorAs(t, err, &invalidArgErr)
	assert.ErrorContains(t, invalidArgErr, "Invalid task category")
	assert.ErrorContains(t, invalidArgErr, "-1")
}

func TestInvoke_ZeroPageSize(t *testing.T) {
	t.Parallel()

	_, err := getdlqtasks.Invoke(
		context.Background(),
		new(persistence.HistoryTaskQueueManagerImpl),
		tasks.NewDefaultTaskCategoryRegistry(),
		&historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory: int32(tasks.CategoryTransfer.ID()),
			},
			PageSize: 0,
		},
	)

	assert.ErrorIs(t, err, consts.ErrInvalidPageSize)
}

func TestInvoke_UnavailableError(t *testing.T) {
	t.Parallel()

	_, err := getdlqtasks.Invoke(
		context.Background(),
		failingHistoryTaskQueueManager{},
		tasks.NewDefaultTaskCategoryRegistry(),
		&historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory: int32(tasks.CategoryTransfer.ID()),
			},
			PageSize: 0,
		},
	)

	var unavailableErr *serviceerror.Unavailable

	require.ErrorAs(t, err, &unavailableErr)
	assert.ErrorContains(t, unavailableErr, "some random error")
}

func (m failingHistoryTaskQueueManager) ReadRawTasks(
	context.Context,
	*persistence.ReadRawTasksRequest,
) (*persistence.ReadRawTasksResponse, error) {
	return nil, errors.New("some random error")
}
