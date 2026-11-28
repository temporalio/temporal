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

	for _, workerQueue := range []string{"multi-session-worker", "single-session-worker"} {
		t.Run("session binding is refused: "+workerQueue, func(t *testing.T) {
			ms := historyi.NewMockMutableState(gomock.NewController(t))
			ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
			ms.EXPECT().SessionWorkerQueue().Return(workerQueue)

			_, err := resetStickyTaskQueue(ms)
			var failedPrecondition *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecondition)
		})
	}
}
