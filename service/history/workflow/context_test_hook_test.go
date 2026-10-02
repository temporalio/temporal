//go:build test_dep

package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	historyi "go.temporal.io/server/service/history/interfaces"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type workflowContextPassiveReplicationTestHook struct {
	err error
}

func (h workflowContextPassiveReplicationTestHook) InterceptUpdate(
	context.Context,
	any,
	func() error,
) error {
	return h.err
}

func (workflowContextPassiveReplicationTestHook) UseTransientWorkflowContextForReplication(context.Context) bool {
	return false
}

func (workflowContextPassiveReplicationTestHook) ShouldExecuteTaskAsPassive(historytasks.Task) bool {
	return false
}

func TestContextImpl_PassiveReplicationHookRecordsMetric(t *testing.T) {
	testHooks := testhooks.NewTestHooks()
	hookErr := errors.New("hook called")
	t.Cleanup(testhooks.Set[testhooks.HistoryPassiveReplicationTestHook](
		testHooks,
		testhooks.HistoryPassiveReplicationTest,
		workflowContextPassiveReplicationTestHook{err: hookErr},
		tests.NamespaceID,
	))
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	workflowContext := NewContext(
		tests.NewDynamicConfig(),
		tests.WorkflowKey,
		chasm.WorkflowArchetypeID,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
		metricsHandler,
		nil,
		testHooks,
	)

	err := workflowContext.UpdateWorkflowExecutionWithNew(
		context.Background(),
		nil,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		historyi.TransactionPolicyActive,
		nil,
	)
	require.ErrorIs(t, err, hookErr)
	recordings := capture.Snapshot()[metrics.HistoryPassiveReplicationTestHookCounter.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, "WorkflowContext", recordings[0].Tags[metrics.OperationTagName])
}
