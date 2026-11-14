package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestClearMutableStateHoldsWorkflowLock(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	wfContext := historyi.NewMockWorkflowContext(controller)
	c := &cacheImpl{Cache: cache.New(10, nil)}
	key := Key{WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")}
	_, err := c.PutIfNotExist(key, &cacheItem{wfContext: wfContext})
	require.NoError(t, err)

	gomock.InOrder(
		wfContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil),
		wfContext.EXPECT().Clear(),
		wfContext.EXPECT().Unlock(),
	)

	require.NoError(t, ClearMutableState(c, key))
}
