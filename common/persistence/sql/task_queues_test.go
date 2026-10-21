package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type testTaskQueueDB struct {
	sqlplugin.DB
	err error
}

func (db *testTaskQueueDB) SelectFromTaskQueues(
	context.Context,
	sqlplugin.TaskQueuesFilter,
	sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	return nil, db.err
}

func TestGetTaskQueuePreservesContextErrors(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(err.Error(), func(t *testing.T) {
			store := &taskQueueStore{
				SqlStore: SqlStore{DB: &testTaskQueueDB{err: err}},
				version:  sqlplugin.MatchingTaskVersion1,
			}

			_, actualErr := store.GetTaskQueue(context.Background(), &persistence.InternalGetTaskQueueRequest{
				NamespaceID: "00000000-0000-0000-0000-000000000001",
				TaskQueue:   "test-task-queue",
				TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			})

			require.ErrorIs(t, actualErr, err)
			var unavailable *serviceerror.Unavailable
			require.NotErrorAs(t, actualErr, &unavailable)
		})
	}
}
