package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
)

type InflightActivityPauseProbeSuite struct {
	parallelsuite.Suite[*InflightActivityPauseProbeSuite]
}

func TestInflightActivityPauseProbeSuite(t *testing.T) {
	parallelsuite.Run(t, &InflightActivityPauseProbeSuite{})
}

// TestInflightActivityFailsAfterPause: an activity attempt is running on a worker
// when the workflow is paused; the attempt then fails retryably with retries left.
func (s *InflightActivityPauseProbeSuite) TestInflightActivityFailsAfterPause() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true))

	var starts atomic.Int32
	release := make(chan struct{})

	activityFn := func(ctx context.Context) error {
		starts.Add(1)
		<-release
		return errors.New("boom")
	}
	workflowFn := func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "probe-activity",
			StartToCloseTimeout:    60 * time.Second,
			ScheduleToCloseTimeout: 120 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 1,
			},
		})
		return workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil)
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFn)

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	// Wait until attempt 1 is running on the worker.
	s.Await(func(s *InflightActivityPauseProbeSuite) {
		s.Equal(int32(1), starts.Load())
	}, 15*time.Second, 100*time.Millisecond)

	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
		Identity:   "probe",
		Reason:     "probe",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *InflightActivityPauseProbeSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 10*time.Second, 100*time.Millisecond)

	// Now let the in-flight attempt fail retryably.
	close(release)

	s.NoError(util.InterruptibleSleep(s.Context(), 8*time.Second))
	desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
	s.NoError(err)
	s.T().Logf("PROBE: activity worker invocations=%d", starts.Load())
	s.T().Logf("PROBE: status=%v pendingActivities=%d", desc.GetWorkflowExecutionInfo().GetStatus(), len(desc.PendingActivities))
	for _, pa := range desc.PendingActivities {
		s.T().Logf("PROBE: activity state=%v attempt=%d paused=%v lastFailure=%v scheduled=%v",
			pa.State, pa.Attempt, pa.Paused, pa.GetLastFailure().GetMessage(), pa.GetScheduledTime().AsTime())
	}

	// The claim under test: pause defers activity retries.
	s.Equal(int32(1), starts.Load(), "no new activity attempt should be dispatched while the workflow is paused")
}
