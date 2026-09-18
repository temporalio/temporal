package queues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/mock/gomock"
)

func apsTestExecutable(ctrl *gomock.Controller, taskType enumsspb.TaskType) *MockExecutable {
	e := NewMockExecutable(ctrl)
	e.EXPECT().GetType().Return(taskType).AnyTimes()
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	return e
}

type passthroughExecutor struct{ calls int }

func (p *passthroughExecutor) Execute(context.Context, Executable) ExecuteResponse {
	p.calls++
	return ExecuteResponse{}
}

func apsWrapped(rps int, activityOnly bool) (Executor, *passthroughExecutor) {
	inner := &passthroughExecutor{}
	w := NewAPSExecutorWrapper(APSLimiterOptions{
		Enabled:           dynamicconfig.GetBoolPropertyFn(true),
		RPS:               dynamicconfig.GetIntPropertyFn(rps),
		ActivityTasksOnly: dynamicconfig.GetBoolPropertyFn(activityOnly),
	})
	return w.Wrap(inner), inner
}

// The refusal has to look exactly like the SaaS limiter's, because that shape is what the
// controller keys on: an APS cause at namespace scope.
func TestAPSLimiter_RefusesWithNamespaceScopedAPSError(t *testing.T) {
	ctrl := gomock.NewController(t)
	executor, inner := apsWrapped(1, true)
	task := apsTestExecutable(ctrl, enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK)

	require.Nil(t, executor.Execute(context.Background(), task).ExecutionErr, "the first is admitted")
	err := executor.Execute(context.Background(), task).ExecutionErr
	require.Error(t, err)

	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, exhausted.Cause)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, exhausted.Scope)
	require.Equal(t, 1, inner.calls, "a refused dispatch must not reach the executor")
}

// A throttled workflow task regenerates forever, which ties the backlog size to the run's
// duration. Metering activity dispatches alone is what makes the backlog a real queue depth.
func TestAPSLimiter_ActivityTasksOnlySkipsWorkflowTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	executor, inner := apsWrapped(1, true)
	workflowTask := apsTestExecutable(ctrl, enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK)

	for range 50 {
		require.Nil(t, executor.Execute(context.Background(), workflowTask).ExecutionErr)
	}
	require.Equal(t, 50, inner.calls, "workflow tasks bypass the limiter entirely")
}

func TestAPSLimiter_DisabledIsAPassthrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := &passthroughExecutor{}
	w := NewAPSExecutorWrapper(APSLimiterOptions{
		Enabled:           dynamicconfig.GetBoolPropertyFn(false),
		RPS:               dynamicconfig.GetIntPropertyFn(1),
		ActivityTasksOnly: dynamicconfig.GetBoolPropertyFn(true),
	})
	executor := w.Wrap(inner)
	task := apsTestExecutable(ctrl, enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK)

	for range 50 {
		require.Nil(t, executor.Execute(context.Background(), task).ExecutionErr)
	}
	require.Equal(t, 50, inner.calls)
}
