package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type AdminTestSuite struct {
	parallelsuite.Suite[*AdminTestSuite]
}

func TestAdminRebuildMutableState_ChasmDisabled(t *testing.T) {
	parallelsuite.Run(t, &AdminTestSuite{}, false)
}

func TestAdminRebuildMutableState_ChasmEnabled(t *testing.T) {
	parallelsuite.Run(t, &AdminTestSuite{}, true)
}

func (s *AdminTestSuite) TestAdminRebuildMutableState(testWithChasm bool) {
	env := s.newRebuildEnv(testWithChasm)

	tv := testvars.New(s.T())
	workflowFn := func(ctx workflow.Context) error {
		var randomUUID string
		err := workflow.SideEffect(
			ctx,
			func(workflow.Context) any { return uuid.New().String() },
		).Get(&randomUUID)
		s.NoError(err)

		_ = workflow.Sleep(ctx, 10*time.Minute)
		return nil
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowID := tv.Any().String()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	runID := workflowRun.GetRunID()

	// there are total 6 events, 3 state transitions
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//
	//  3. WorkflowTaskStarted
	//
	//  4. WorkflowTaskCompleted
	//  5. MarkerRecord
	//  6. TimerStarted

	var response1 *adminservice.DescribeMutableStateResponse
	for {
		response1, err = env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		if response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount == 3 {
			// Note: ChasmNodes may be empty even with CHASM enabled, so we only check if the rebuild can be performed,
			// and not checking whether it is rebuildable because ChasmNodes are present.
			if !testWithChasm {
				s.Empty(response1.DatabaseMutableState.ChasmNodes, "CHASM-disabled workflows should not have ChasmNodes")
			}
			break
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
	}

	_, err = env.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err)

	response2, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Equal(response1.DatabaseMutableState.ExecutionInfo.VersionHistories, response2.DatabaseMutableState.ExecutionInfo.VersionHistories)
	s.Equal(response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount, response2.DatabaseMutableState.ExecutionInfo.StateTransitionCount)

	s.Equal(response1.DatabaseMutableState.ExecutionState.CreateRequestId, response2.DatabaseMutableState.ExecutionState.CreateRequestId)
	s.Equal(response1.DatabaseMutableState.ExecutionState.RunId, response2.DatabaseMutableState.ExecutionState.RunId)
	s.Equal(response1.DatabaseMutableState.ExecutionState.State, response2.DatabaseMutableState.ExecutionState.State)
	s.Equal(response1.DatabaseMutableState.ExecutionState.Status, response2.DatabaseMutableState.ExecutionState.Status)

	// From transition history perspective, Rebuild is considered as an update to the workflow and updates
	// all sub state machines in the workflow, which includes the workflow ExecutionState.
	s.Equal(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: response1.DatabaseMutableState.ExecutionState.LastUpdateVersionedTransition.NamespaceFailoverVersion,
		TransitionCount:          response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount + 1,
	}, response2.DatabaseMutableState.ExecutionState.LastUpdateVersionedTransition)

	// Rebuild recreates mutable state for the same run, so the recorded start time must survive it.
	s.NotNil(response1.DatabaseMutableState.ExecutionState.StartTime)
	s.NotNil(response2.DatabaseMutableState.ExecutionState.StartTime)

	timeBefore := timestamp.TimeValue(response1.DatabaseMutableState.ExecutionState.StartTime)
	timeAfter := timestamp.TimeValue(response2.DatabaseMutableState.ExecutionState.StartTime)
	s.Equal(timeBefore, timeAfter)

	s.Equal(
		timestamp.TimeValue(response1.DatabaseMutableState.ExecutionInfo.ExecutionTime),
		timestamp.TimeValue(response2.DatabaseMutableState.ExecutionInfo.ExecutionTime),
	)
	s.Nil(response1.DatabaseMutableState.ExecutionInfo.MutableStateRebuildTime)
	s.NotNil(response2.DatabaseMutableState.ExecutionInfo.MutableStateRebuildTime)
}

// TestAdminRebuildMutableStateRunTimeout checks that the rebuild re-anchors the run timeout
// deadline at the rebuild time, the way reset does, and that the run still times out there.
func (s *AdminTestSuite) TestAdminRebuildMutableStateRunTimeout(testWithChasm bool) {
	const runTimeout = 10 * time.Second

	env := s.newRebuildEnv(testWithChasm)
	ctx, cancel := context.WithTimeout(s.Context(), 90*time.Second)
	defer cancel()

	workflowFn := func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, time.Hour)
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowID := testvars.New(s.T()).Any().String()
	_, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: env.WorkerTaskQueue(),
		// No execution timeout, so the run deadline is purely start time plus run timeout.
		WorkflowRunTimeout: runTimeout,
	}, workflowFn)
	s.NoError(err)

	before := s.awaitRebuildableMutableState(ctx, env, workflowID)
	originalRunExpiration := timestamp.TimeValue(before.ExecutionInfo.WorkflowRunExpirationTime)
	s.False(originalRunExpiration.IsZero())
	s.Zero(timestamp.TimeValue(before.ExecutionInfo.WorkflowExecutionExpirationTime))

	s.rebuildMutableState(ctx, env, workflowID)

	after := s.describeMutableState(ctx, env, workflowID)
	runExpiration := timestamp.TimeValue(after.ExecutionInfo.WorkflowRunExpirationTime)
	s.True(runExpiration.After(originalRunExpiration))
	s.NotNil(after.ExecutionInfo.MutableStateRebuildTime)
	s.Zero(
		timestamp.TimeValue(after.ExecutionInfo.WorkflowExecutionExpirationTime),
	)

	s.assertTimesOutAt(ctx, env, workflowID, runExpiration)
}

// TestAdminRebuildMutableStateExecutionTimeout checks that the rebuild re-anchors the execution
// timeout deadline at the rebuild time and that the workflow still times out there, the same thing
// TestAdminRebuildMutableStateRunTimeout checks for the run timeout deadline.
//
// The continue-as-new is what puts the execution timeout timer under test. The server skips the
// WorkflowExecutionTimeoutTask on a chain's first run and lets the run
// timeout timer enforce the deadline there, so a first run would exercise the same path as
// TestAdminRebuildMutableStateRunTimeout. On the second run the opposite holds: the run timeout
// defaults to the execution timeout, which makes both deadlines the same instant, and
// GenerateWorkflowStartTasks then skips the run timeout task, leaving the execution timeout timer
// as the only thing that can end the workflow.
func (s *AdminTestSuite) TestAdminRebuildMutableStateExecutionTimeout(testWithChasm bool) {
	const executionTimeout = 15 * time.Second

	env := s.newRebuildEnv(testWithChasm)
	ctx, cancel := context.WithTimeout(s.Context(), 90*time.Second)
	defer cancel()

	var workflowFn func(ctx workflow.Context) error
	workflowFn = func(ctx workflow.Context) error {
		if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
			return workflow.NewContinueAsNewError(ctx, workflowFn)
		}
		return workflow.Sleep(ctx, time.Hour)
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowID := testvars.New(s.T()).Any().String()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                env.WorkerTaskQueue(),
		WorkflowExecutionTimeout: executionTimeout,
	}, workflowFn)
	s.NoError(err)
	firstRunID := run.GetRunID()

	s.Await(func(s *AdminTestSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, "")
		s.NoError(err)
		s.NotEqual(firstRunID, desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
			"workflow should continue as new")
	}, 20*time.Second, 100*time.Millisecond)

	before := s.awaitRebuildableMutableState(ctx, env, workflowID)
	originalExecutionExpiration := timestamp.TimeValue(before.ExecutionInfo.WorkflowExecutionExpirationTime)
	originalRunExpiration := timestamp.TimeValue(before.ExecutionInfo.WorkflowRunExpirationTime)
	s.False(originalExecutionExpiration.IsZero(), "execution timeout deadline should be set before the rebuild")
	s.NotZero(before.ExecutionInfo.WorkflowExecutionTimerTaskStatus,
		"the second run should carry an execution timeout timer task")

	s.rebuildMutableState(ctx, env, workflowID)

	after := s.describeMutableState(ctx, env, workflowID)
	executionExpiration := timestamp.TimeValue(after.ExecutionInfo.WorkflowExecutionExpirationTime)
	runExpiration := timestamp.TimeValue(after.ExecutionInfo.WorkflowRunExpirationTime)
	s.True(executionExpiration.After(originalExecutionExpiration),
		"rebuild should re-anchor the execution deadline")
	s.True(runExpiration.After(originalRunExpiration), "rebuild should re-anchor the run deadline")
	s.NotZero(after.ExecutionInfo.WorkflowExecutionTimerTaskStatus,
		"the execution timeout timer task should survive the rebuild")

	s.assertTimesOutAt(ctx, env, workflowID, executionExpiration)
}

func (s *AdminTestSuite) newRebuildEnv(testWithChasm bool) *testcore.TestEnv {
	var opts []testcore.TestOption
	if testWithChasm {
		opts = append(opts, testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true))
	}
	env := testcore.NewEnv(s.T(), opts...)

	if testWithChasm {
		configValues := env.GetTestCluster().Host().DcClient().GetValue(dynamicconfig.EnableChasm.Key())
		s.NotEmpty(configValues, "EnableChasm config should be set")
		configValue, _ := configValues[0].Value.(bool)
		s.True(configValue, "EnableChasm config should be true")
	}
	return env
}

func (s *AdminTestSuite) describeMutableState(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
) *persistencespb.WorkflowMutableState {
	resp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	return resp.DatabaseMutableState
}

// awaitRebuildableMutableState waits until the current run has completed its first workflow task,
// so that the rebuild replays more than just the start event.
func (s *AdminTestSuite) awaitRebuildableMutableState(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
) *persistencespb.WorkflowMutableState {
	var mutableState *persistencespb.WorkflowMutableState
	s.Await(func(s *AdminTestSuite) {
		mutableState = s.describeMutableState(ctx, env, workflowID)
		s.Positive(mutableState.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId,
			"workflow should complete its first workflow task")
	}, 20*time.Second, 50*time.Millisecond)
	return mutableState
}

func (s *AdminTestSuite) rebuildMutableState(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
) {
	_, err := env.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	s.NoError(err)
}

// assertTimesOutAt asserts that the workflow survives the rebuild and then times out at deadline.
func (s *AdminTestSuite) assertTimesOutAt(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
	deadline time.Time,
) {
	desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, "")
	s.NoError(err)
	s.NotEqual(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, desc.GetWorkflowExecutionInfo().GetStatus(),
		"workflow should not time out immediately after the rebuild")

	s.Await(func(s *AdminTestSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, "")
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, desc.GetWorkflowExecutionInfo().GetStatus(),
			"workflow did not time out at its deadline")
	}, time.Until(deadline)+5*time.Second, 250*time.Millisecond)
}
