package queues

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
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
		gateDeleted bool
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
		{name: "deleted, flag off", state: enumspb.NAMESPACE_STATE_DELETED, task: &tasks.UserTimerTask{}},
		{name: "deleted, flag on", state: enumspb.NAMESPACE_STATE_DELETED, gateDeleted: true, task: &tasks.UserTimerTask{}, wantDiscard: true},
	}
	for _, tc := range testCases {
		for _, trySubmit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/trySubmit=%v", tc.name, trySubmit), func(t *testing.T) {
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
				} else if trySubmit {
					base.EXPECT().TrySubmit(executable).Return(true)
				} else {
					base.EXPECT().Submit(executable)
				}

				gate := NewNamespaceStateGateScheduler(
					base,
					registry,
					dynamicconfig.GetBoolPropertyFn(tc.gateDeleted),
					IsExemptFromNamespaceStateGate,
					metricsHandler,
				)
				if trySubmit {
					require.True(t, gate.TrySubmit(executable))
				} else {
					gate.Submit(executable)
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
