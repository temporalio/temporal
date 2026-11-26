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
		name     string
		unbound  bool
		wantNoop bool
	}{
		{name: "bound to the drained worker", unbound: true, wantNoop: false},
		{name: "bound exclusively to the drained worker", unbound: false, wantNoop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := historyi.NewMockMutableState(gomock.NewController(t))
			ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
			ms.EXPECT().SessionWorkerQueue().Return(drainedQueue)
			ms.EXPECT().UpdateNonExclusiveWorkerBinding(nil, "", drainedQueue).Return(tc.unbound, nil)

			action, err := unbindWorkflowFromSessionWorker(ms, drainedQueue)
			require.NoError(t, err)
			require.Equal(t, tc.wantNoop, action.Noop)
		})
	}

	t.Run("unbind fails", func(t *testing.T) {
		ms := historyi.NewMockMutableState(gomock.NewController(t))
		ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
		ms.EXPECT().SessionWorkerQueue().Return(drainedQueue)
		ms.EXPECT().UpdateNonExclusiveWorkerBinding(nil, "", drainedQueue).Return(false, errors.New("reschedule failed"))

		_, err := unbindWorkflowFromSessionWorker(ms, drainedQueue)
		require.EqualError(t, err, "reschedule failed")
	})
}
