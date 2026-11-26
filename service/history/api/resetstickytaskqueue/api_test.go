package resetstickytaskqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestResetStickyTaskQueue(t *testing.T) {
	t.Run("workflow completed", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(false)

		_, err := resetStickyTaskQueue(ms)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("not bound clears stickiness", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
		ms.EXPECT().SessionWorkerQueue().Return("")
		ms.EXPECT().ClearStickyTaskQueue(historyi.StickyClearForResetStickyTaskQueue)

		action, err := resetStickyTaskQueue(ms)
		require.NoError(t, err)
		require.True(t, action.Noop)
	})

	t.Run("non-exclusive binding is unbound and its tasks rescheduled", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
		ms.EXPECT().SessionWorkerQueue().Return("worker-queue")
		ms.EXPECT().UpdateNonExclusiveWorkerBinding(nil, "", "worker-queue").Return(true, nil)

		action, err := resetStickyTaskQueue(ms)
		require.NoError(t, err)
		require.False(t, action.Noop)
	})

	t.Run("exclusive binding is kept", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
		ms.EXPECT().SessionWorkerQueue().Return("stable-identity")
		ms.EXPECT().UpdateNonExclusiveWorkerBinding(nil, "", "stable-identity").Return(false, nil)

		action, err := resetStickyTaskQueue(ms)
		require.NoError(t, err)
		require.True(t, action.Noop)
	})
}
