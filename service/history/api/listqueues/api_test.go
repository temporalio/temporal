package listqueues_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api/historydlq"
	"go.temporal.io/server/service/history/api/listqueues"
	"go.temporal.io/server/service/history/configs"
)

// failingHistoryTaskQueueManager is a [persistence.HistoryTaskQueueManager] that always fails.
type failingHistoryTaskQueueManager struct {
	persistence.HistoryTaskQueueManager
}

func TestInvoke_UnavailableError(t *testing.T) {
	t.Parallel()

	_, err := listqueues.Invoke(
		context.Background(),
		historydlq.NewStore(failingHistoryTaskQueueManager{}, nil, nil, nil, &configs.Config{TaskDLQReadCHASM: dynamicconfig.GetBoolPropertyFn(false)}, nil),
		&historyservice.ListQueuesRequest{
			QueueType: int32(persistence.QueueTypeHistoryDLQ),
			PageSize:  0,
		},
	)
	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
	assert.ErrorContains(t, unavailableErr, "some random error")
}

func (m failingHistoryTaskQueueManager) ListQueues(
	context.Context,
	*persistence.ListQueuesRequest,
) (*persistence.ListQueuesResponse, error) {
	return nil, errors.New("some random error")
}
