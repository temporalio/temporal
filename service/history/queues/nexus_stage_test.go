package queues_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackpb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexuspb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestNexusStageQueueFailures(t *testing.T) {
	t.Parallel()
	registry := chasm.NewRegistry(nil)
	require.NoError(t, registry.Register(&nexusoperation.Library{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	invocationID, ok := registry.TaskIDFor(&nexuspb.InvocationTask{})
	require.True(t, ok)
	callbackID, ok := registry.TaskIDFor(&callbackpb.InvocationTask{})
	require.True(t, ok)
	key := definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)
	for _, tc := range []struct {
		name  string
		task  tasks.Task
		stage string
	}{
		{"HSM invocation", &tasks.StateMachineOutboundTask{StateMachineTask: tasks.StateMachineTask{WorkflowKey: key, Info: &persistencespb.StateMachineTaskInfo{Type: "nexusoperations.Invocation"}}}, "caller-outbound"},
		{"HSM cancellation", &tasks.StateMachineOutboundTask{StateMachineTask: tasks.StateMachineTask{WorkflowKey: key, Info: &persistencespb.StateMachineTaskInfo{Type: "nexusoperations.Cancelation"}}}, "caller-outbound"},
		{"HSM callback", &tasks.StateMachineOutboundTask{StateMachineTask: tasks.StateMachineTask{WorkflowKey: key, Info: &persistencespb.StateMachineTaskInfo{Type: "callbacks.Invocation"}}}, ""},
		{"CHASM invocation", &tasks.ChasmTask{WorkflowKey: key, Category: tasks.CategoryOutbound, Info: &persistencespb.ChasmTaskInfo{TypeId: invocationID}}, "caller-outbound"},
		{"CHASM callback", &tasks.ChasmTask{WorkflowKey: key, Category: tasks.CategoryOutbound, Info: &persistencespb.ChasmTaskInfo{TypeId: callbackID}}, ""},
		{"unregistered task", &tasks.ChasmTask{WorkflowKey: key, Category: tasks.CategoryOutbound, Info: &persistencespb.ChasmTaskInfo{TypeId: 123}}, ""},
		{"mixed timer", &tasks.StateMachineTimerTask{WorkflowKey: key}, ""},
		{"unrelated HSM", &tasks.StateMachineOutboundTask{StateMachineTask: tasks.StateMachineTask{WorkflowKey: key, Info: &persistencespb.StateMachineTaskInfo{Type: "other.Invocation"}}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ns := namespace.NewMockRegistry(ctrl)
			ns.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
			metadata := cluster.NewMockMetadata(ctrl)
			metadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Warn, "Fail to process task")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			executable := queues.NewExecutable(queues.DefaultReaderId, tc.task, nil, nil, nil, queues.NewNoopPriorityAssigner(), clock.NewEventTimeSource().Update(time.Now()), ns, metadata, registry, queues.GetTaskTypeTagValue, logger, metrics.NoopMetricsHandler, telemetry.NoopTracer)
			require.Error(t, executable.HandleErr(errors.New("unexpected task failure")))
			records := capture.Snapshot()
			require.NotEmpty(t, records)
			for _, record := range records {
				tags := map[string]any{}
				for _, tag := range record.Tags {
					tags[tag.Key()] = tag.Value()
				}
				if tc.stage == "" {
					require.NotContains(t, tags, "nexus-stage")
				} else {
					require.Equal(t, tc.stage, tags["nexus-stage"])
				}
			}
		})
	}
}
