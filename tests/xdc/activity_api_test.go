package xdc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type (
	ActivityFunctions func() (string, error)
	WorkflowFunction  func(context2 workflow.Context) error

	ActivityApiStateReplicationSuite struct {
		xdcBaseSuite
	}
)

func TestActivityApiStateReplicationSuite(t *testing.T) {
	s := new(ActivityApiStateReplicationSuite)
	suite.Run(t, s)
}

func (s *ActivityApiStateReplicationSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}
	s.setupSuite()
}

func (s *ActivityApiStateReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *ActivityApiStateReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *ActivityApiStateReplicationSuite) TestPauseActivityFailover() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityPausedCn := make(chan struct{})
	var activityWasPaused atomic.Bool
	var startedActivityCount atomic.Int32

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if activityWasPaused.Load() == false {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}
		s.waitForChannel(ctx, activityPausedCn)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	ns := s.createGlobalNamespace()
	activeSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr("tq")
	worker1 := sdkworker.New(activeSDKClient, taskQueue, sdkworker.Options{})

	worker1.RegisterWorkflow(workflowFn)
	worker1.RegisterActivity(activityFunction)

	s.NoError(worker1.Start())
	defer worker1.Stop()

	// start a workflow
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start/fail few times
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Greater(t, startedActivityCount.Load(), int32(2))
	}, 5*time.Second, 200*time.Millisecond)

	// pause the activity in cluster0
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}
	pauseResp, err := s.clusters[0].Host().FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// verify activity is paused is cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, 1, len(description.PendingActivities))
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	// update the activity properties in cluster0
	updateRequest := &workflowservice.UpdateActivityOptionsRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
		ActivityOptions: &activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(2 * time.Second),
				MaximumAttempts: 10,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval", "retry_policy.maximum_attempts"}},
	}
	respUpdate, err := s.clusters[0].Host().FrontendClient().UpdateActivityOptions(ctx, updateRequest)
	s.NoError(err)
	s.NotNil(respUpdate)

	// verify activity is updated in cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.GetPendingActivities())
		if description.GetPendingActivities() != nil {
			require.Equal(t, 1, len(description.PendingActivities))
			require.True(t, description.PendingActivities[0].Paused)
			require.Equal(t, int64(2), description.PendingActivities[0].CurrentRetryInterval.GetSeconds())
		}
	}, 5*time.Second, 200*time.Millisecond)

	// reset the activity in cluster0, while keeping it paused
	resetRequest := &workflowservice.ResetActivityRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity:   &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
		KeepPaused: true,
	}
	resetResp, err := s.clusters[0].Host().FrontendClient().ResetActivity(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resetResp)

	// verify activity is reset, updated and paused in cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.GetPendingActivities())
		if description.GetPendingActivities() != nil {
			require.Equal(t, 1, len(description.PendingActivities))
			require.True(t, description.PendingActivities[0].Paused)
			require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
			require.Equal(t, int64(2), description.PendingActivities[0].CurrentRetryInterval.GetSeconds())
		}
	}, 5*time.Second, 200*time.Millisecond)

	// stop worker1 so cluster0 won't make any progress on the activity (just in case)
	worker1.Stop()

	// failover to standby cluster
	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)

	// get standby client
	standbyClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[1].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	s.NotNil(standbyClient)

	// verify activity is still paused in cluster1
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := standbyClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.GetPendingActivities())
		if description.GetPendingActivities() != nil {
			require.Equal(t, 1, len(description.PendingActivities))
			require.True(t, description.PendingActivities[0].Paused)
			require.Equal(t, int64(2), description.PendingActivities[0].CurrentRetryInterval.GetSeconds())
			require.Equal(t, int32(10), description.PendingActivities[0].MaximumAttempts)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// start worker2
	worker2 := sdkworker.New(standbyClient, taskQueue, sdkworker.Options{})
	worker2.RegisterWorkflow(workflowFn)
	worker2.RegisterActivity(activityFunction)
	s.NoError(worker2.Start())
	defer worker2.Stop()

	// let the activity make progress once unpaused
	activityWasPaused.Store(true)

	// unpause the activity in cluster1
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := s.clusters[1].Host().FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// unblock the activity
	activityPausedCn <- struct{}{}

	// wait for activity to finish
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiStateReplicationSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	initialRetryInterval := 1 * time.Second
	scheduleToCloseTimeout := 30 * time.Minute
	startToCloseTimeout := 15 * time.Minute

	activityRetryPolicy := &temporal.RetryPolicy{
		InitialInterval:    initialRetryInterval,
		BackoffCoefficient: 1,
	}

	return func(ctx workflow.Context) error {

		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			RetryPolicy:            activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return err
	}
}

func (s *ActivityApiStateReplicationSuite) waitForChannel(ctx context.Context, ch chan struct{}) {
	s.T().Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		s.FailNow("context timeout while waiting for channel")
	}
}
