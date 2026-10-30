//go:build test_dep

package ndc

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	historytasks "go.temporal.io/server/service/history/tasks"
)

type workflowStateReplicatorPassiveReplicationTestHook struct{}

func (workflowStateReplicatorPassiveReplicationTestHook) InterceptUpdate(
	_ context.Context,
	_ any,
	next func() error,
) error {
	return next()
}

func (workflowStateReplicatorPassiveReplicationTestHook) UseTransientWorkflowContextForReplication(context.Context) bool {
	return true
}

func (workflowStateReplicatorPassiveReplicationTestHook) ShouldExecuteTaskAsPassive(historytasks.Task) bool {
	return false
}

func (s *workflowReplicatorSuite) Test_GetWorkflowContext_PassiveReplicationHookRecordsMetric() {
	namespaceID := namespace.ID("namespace-id")
	testHooks := s.workflowStateReplicator.testHooks
	s.T().Cleanup(testhooks.Set[testhooks.HistoryPassiveReplicationTestHook](
		testHooks,
		testhooks.HistoryPassiveReplicationTest,
		workflowStateReplicatorPassiveReplicationTestHook{},
		namespaceID,
	))
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	s.T().Cleanup(func() { metricsHandler.StopCapture(capture) })
	s.mockShard.SetMetricsHandler(metricsHandler)

	wfCtx, releaseFn, err := s.workflowStateReplicator.getWorkflowContext(
		context.Background(),
		namespaceID,
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		chasm.WorkflowArchetypeID,
	)
	s.Require().NoError(err)
	s.Require().NotNil(wfCtx)
	releaseFn(nil)
	recordings := capture.Snapshot()[metrics.HistoryPassiveReplicationTestHookCounter.Name()]
	s.Require().Len(recordings, 1)
	s.Require().Equal("ReplicateVersionedTransition", recordings[0].Tags[metrics.OperationTagName])
}
