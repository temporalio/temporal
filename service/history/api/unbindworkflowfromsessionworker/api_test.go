package unbindworkflowfromsessionworker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestUnbindWorkflowFromSessionWorker(t *testing.T) {
	const drainedQueue = "drained-worker-queue"

	t.Run("workflow completed", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(false)

		_, err := unbindWorkflowFromSessionWorker(ms, drainedQueue)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
	})

	for _, tc := range []struct {
		name       string
		boundQueue string
	}{
		{name: "bound to another worker", boundQueue: "other-worker-queue"},
		{name: "not bound", boundQueue: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := historyi.NewMockMutableState(gomock.NewController(t))
			ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
			ms.EXPECT().SessionWorkerQueue().Return(tc.boundQueue)

			action, err := unbindWorkflowFromSessionWorker(ms, drainedQueue)
			require.NoError(t, err)
			require.True(t, action.Noop)
		})
	}

	for _, tc := range []struct {
		name                   string
		pendingSessionActivity bool
	}{
		{name: "bound to the drained worker"},
		{name: "bound to the drained worker with session activities waiting", pendingSessionActivity: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := historyi.NewMockMutableState(gomock.NewController(t))
			ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
			ms.EXPECT().SessionWorkerQueue().Return(drainedQueue)
			ms.EXPECT().HasExclusiveBinding().Return(false)
			ms.EXPECT().UpdateNonExclusiveWorkerBinding(nil, "", drainedQueue).Return(true, nil)
			ms.EXPECT().HasPendingSessionActivities().Return(tc.pendingSessionActivity)

			action, err := unbindWorkflowFromSessionWorker(ms, drainedQueue)
			require.NoError(t, err)
			require.False(t, action.Noop)
			// A workflow task lets a session worker bind the workflow and pick up its waiting activities.
			require.Equal(t, tc.pendingSessionActivity, action.CreateWorkflowTask)
		})
	}

	t.Run("bound to a single-session worker", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
		ms.EXPECT().SessionWorkerQueue().Return(drainedQueue)
		ms.EXPECT().HasExclusiveBinding().Return(true)

		_, err := unbindWorkflowFromSessionWorker(ms, drainedQueue)
		var failedPrecondition *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecondition)
	})

	t.Run("unbind fails", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
		ms.EXPECT().SessionWorkerQueue().Return(drainedQueue)
		ms.EXPECT().HasExclusiveBinding().Return(false)
		ms.EXPECT().UpdateNonExclusiveWorkerBinding(nil, "", drainedQueue).Return(false, errors.New("reschedule failed"))

		_, err := unbindWorkflowFromSessionWorker(ms, drainedQueue)
		require.EqualError(t, err, "reschedule failed")
	})
}
