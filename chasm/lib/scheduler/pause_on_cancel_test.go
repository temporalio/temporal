package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestKnownIssue_CanceledWorkflowDoesNotPauseByDefault(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	sched.Schedule.Policies.PauseOnFailure = true
	invoker := sched.Invoker.Get(ctx)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:  "request",
		WorkflowId: "workflow",
		RunId:      "run",
		Attempt:    1,
	}}

	err := sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
		RequestId: "request",
		Outcome: &persistencespb.ChasmNexusCompletion_Failure{
			Failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
				},
			},
		},
		CloseTime: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.False(t, sched.Schedule.State.Paused,
		"CanceledTerminatedCountAsFailures defaults to false, so cancellation must not pause")
}
