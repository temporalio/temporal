package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type AdminTestSuite struct {
	testcore.FunctionalTestBase
}

func TestAdminTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AdminTestSuite))
}

func (s *AdminTestSuite) TestAdminRebuildMutableState() {

	workflowFn := func(ctx workflow.Context) error {
		var randomUUID string
		err := workflow.SideEffect(
			ctx,
			func(workflow.Context) interface{} { return uuid.New().String() },
		).Get(&randomUUID)
		s.NoError(err)

		_ = workflow.Sleep(ctx, 10*time.Minute)
		return nil
	}

	s.Worker().RegisterWorkflow(workflowFn)

	workflowID := "functional-admin-rebuild-mutable-state-test"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
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
		response1, err = s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		if response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
	}

	_, err = s.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err)

	response2, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
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

	// Rebuild explicitly sets start time, thus start time will change after rebuild.
	s.NotNil(response1.DatabaseMutableState.ExecutionState.StartTime)
	s.NotNil(response2.DatabaseMutableState.ExecutionState.StartTime)

	timeBefore := timestamp.TimeValue(response1.DatabaseMutableState.ExecutionState.StartTime)
	timeAfter := timestamp.TimeValue(response2.DatabaseMutableState.ExecutionState.StartTime)
	s.False(timeAfter.Before(timeBefore))
}

func (s *AdminTestSuite) TestAdminRebuildMutableState_BlocksNonWorkflow() {
	// This test verifies that RebuildMutableState correctly rejects non-workflow executions
	// such as schedulers (CHASM archetype)

	workflowFn := func(ctx workflow.Context) error {
		return nil
	}
	s.Worker().RegisterWorkflow(workflowFn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a CHASM scheduler execution
	scheduleID := "test-rebuild-blocks-scheduler-" + uuid.New().String()

	// Create context with CHASM scheduler experiment header
	chasmCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		headers.ExperimentHeaderName, "chasm-scheduler",
	))

	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: scheduleID,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   "scheduled-workflow-" + uuid.New().String(),
						WorkflowType: &commonpb.WorkflowType{Name: "workflowFn"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.TaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		Identity: "test",
	}

	_, err := s.FrontendClient().CreateSchedule(chasmCtx, req)
	s.NoError(err)

	// Wait for scheduler to be created and get its runID
	time.Sleep(500 * time.Millisecond) //nolint:forbidigo

	// Get the scheduler's execution info
	descResp, err := s.FrontendClient().DescribeSchedule(chasmCtx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: scheduleID,
	})
	s.NoError(err)
	s.NotNil(descResp.Info)

	runID := descResp.Info.RunningWorkflows[0].RunId

	// Attempt to rebuild the scheduler's mutable state - this should fail
	_, err = s.AdminClient().RebuildMutableState(chasmCtx, &adminservice.RebuildMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: scheduleID,
			RunId:      runID,
		},
	})

	// Verify that the request was rejected with InvalidArgument error
	s.Error(err)
	st, ok := status.FromError(err)
	s.True(ok, "error should be a gRPC status error")
	s.Equal(codes.InvalidArgument, st.Code())
	s.Contains(st.Message(), "RebuildMutableState only supports workflow executions")
}
