package queues

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestNamespaceStateGateScheduler(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		state       enumspb.NamespaceState
		notFound    bool
		task        tasks.Task
		wantDiscard bool
	}{
		{name: "registered", state: enumspb.NAMESPACE_STATE_REGISTERED, task: &tasks.UserTimerTask{}},
		{name: "namespace not found", notFound: true, task: &tasks.UserTimerTask{}},
		{name: "soft-deleted", state: enumspb.NAMESPACE_STATE_SOFT_DELETED, task: &tasks.UserTimerTask{}, wantDiscard: true},
		{name: "soft-deleted close", state: enumspb.NAMESPACE_STATE_SOFT_DELETED, task: &tasks.CloseExecutionTask{}, wantDiscard: true},
		{name: "soft-deleted retention", state: enumspb.NAMESPACE_STATE_SOFT_DELETED, task: &tasks.DeleteHistoryEventTask{}},
		{name: "soft-deleted delete execution", state: enumspb.NAMESPACE_STATE_SOFT_DELETED, task: &tasks.DeleteExecutionTask{}},
		{name: "soft-deleted close and delete", state: enumspb.NAMESPACE_STATE_SOFT_DELETED, task: &tasks.CloseExecutionTask{DeleteAfterClose: true}},
		{name: "deleted", state: enumspb.NAMESPACE_STATE_DELETED, task: &tasks.UserTimerTask{}, wantDiscard: true},
		{name: "deleted retention", state: enumspb.NAMESPACE_STATE_DELETED, task: &tasks.DeleteHistoryEventTask{}},
	}
	for _, tc := range testCases {
		for _, method := range []string{"Submit", "TrySubmit", "HandleBusyWorkflow"} {
			t.Run(fmt.Sprintf("%s/%s", tc.name, method), func(t *testing.T) {
				t.Parallel()
				ctrl := gomock.NewController(t)
				registry := namespace.NewMockRegistry(ctrl)
				base := NewMockScheduler(ctrl)
				executable := NewMockExecutable(ctrl)
				metricsHandler := metricstest.NewCaptureHandler()
				capture := metricsHandler.StartCapture()
				t.Cleanup(func() { metricsHandler.StopCapture(capture) })

				if tc.notFound {
					registry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(nil, serviceerror.NewNamespaceNotFound("ns-id"))
				} else {
					registry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(namespace.NewLocalNamespaceForTest(
						&persistencespb.NamespaceInfo{Id: "ns-id", Name: "ns", State: tc.state},
						nil,
						cluster.TestCurrentClusterName,
					), nil)
				}
				executable.EXPECT().GetNamespaceID().Return("ns-id").AnyTimes()
				executable.EXPECT().GetTask().Return(tc.task).AnyTimes()
				executable.EXPECT().GetType().Return(tc.task.GetType()).AnyTimes()
				if tc.wantDiscard {
					executable.EXPECT().Ack()
				} else if method == "TrySubmit" {
					base.EXPECT().TrySubmit(executable).Return(true)
				} else if method == "Submit" {
					base.EXPECT().Submit(executable)
				}

				gate := NewNamespaceStateGateScheduler(
					base,
					registry,
					IsExemptFromNamespaceStateGate,
					metricsHandler,
				)
				switch method {
				case "Submit":
					gate.Submit(executable)
				case "TrySubmit":
					require.True(t, gate.TrySubmit(executable))
				case "HandleBusyWorkflow":
					// The mock base scheduler is not a BusyWorkflowHandler, so a task that passes the
					// gate is reported as unhandled.
					handled := gate.(BusyWorkflowHandler).HandleBusyWorkflow(executable)
					require.Equal(t, tc.wantDiscard, handled)
				default:
					t.Fatalf("unknown method %q", method)
				}

				recordings := capture.SnapshotMetric(metrics.TaskDiscardedByNamespaceState.Name())
				if tc.wantDiscard {
					require.Len(t, recordings, 1)
					require.Equal(t, "ns", recordings[0].Tags["namespace"])
				} else {
					require.Empty(t, recordings)
				}
			})
		}
	}
}
