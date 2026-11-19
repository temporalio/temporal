package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestForceTerminateWorkflowRecordsMetric(t *testing.T) {
	ctrl := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		"force terminate",
		nil,
		consts.IdentityHistoryService,
		false,
		nil,
	).Return(&historypb.HistoryEvent{}, nil)
	mutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		"active",
	))

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	err := ForceTerminateWorkflow(
		mutableState,
		"force terminate",
		nil,
		consts.IdentityHistoryService,
		false,
		nil,
		metricsHandler,
		chasm.ExecutionForceTerminationReasonEventBatchSizeExceedsLimit,
	)
	require.NoError(t, err)

	recordings := capture.Snapshot()[metrics.ExecutionForceTerminations.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, "test-namespace", recordings[0].Tags["namespace"])
	require.Equal(t, string(chasm.WorkflowArchetype), recordings[0].Tags["archetype"])
	require.Equal(t, string(chasm.ExecutionForceTerminationReasonEventBatchSizeExceedsLimit), recordings[0].Tags["reason"])
}

// TestTaskSessionDirective names the bound worker (queue and instance key) only on pushes to that worker's sticky queue,
// for exclusive and non-exclusive bindings alike.
func TestTaskSessionDirective(t *testing.T) {
	t.Parallel()
	worker := &taskqueuespb.BoundWorkerInfo{WorkerQueue: "worker", InstanceKey: "key"}
	sticky := &taskqueuepb.TaskQueue{Name: "worker", Kind: enumspb.TASK_QUEUE_KIND_STICKY}
	for _, tc := range []struct {
		name         string
		sessionState *persistencespb.SessionState
		queue        *taskqueuepb.TaskQueue
		expected     *taskqueuespb.TaskSessionDirective
	}{
		{name: "exclusive binding", sessionState: &persistencespb.SessionState{ExclusiveWorker: true, BoundWorkerInfo: worker}, queue: sticky,
			expected: &taskqueuespb.TaskSessionDirective{BoundWorkerInfo: worker, IsExclusiveWorkerAssigned: true}},
		{name: "non-exclusive binding", sessionState: &persistencespb.SessionState{BoundWorkerInfo: worker}, queue: sticky,
			expected: &taskqueuespb.TaskSessionDirective{BoundWorkerInfo: worker}},
		{name: "unbound sticky", queue: sticky},
		{name: "another worker's sticky queue", sessionState: &persistencespb.SessionState{BoundWorkerInfo: worker},
			queue: &taskqueuepb.TaskQueue{Name: "other", Kind: enumspb.TASK_QUEUE_KIND_STICKY}},
		{name: "normal fallback", sessionState: &persistencespb.SessionState{BoundWorkerInfo: worker},
			queue: &taskqueuepb.TaskQueue{Name: "normal", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}},
		{name: "missing destination", sessionState: &persistencespb.SessionState{BoundWorkerInfo: worker}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			protorequire.ProtoEqual(t, tc.expected, TaskSessionDirective(tc.sessionState, tc.queue))
		})
	}
}
