package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type AdminTestSuite struct {
	testcore.FunctionalTestBase
	testContext context.Context
}

func TestAdminTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AdminTestSuite))
}

func (s *AdminTestSuite) SetupSuite() {
	// Call parent setup to initialize the test cluster
	s.FunctionalTestBase.SetupSuite()
	s.testContext = context.Background()
}

func (s *AdminTestSuite) TestAdminRebuildMutableState_ChasmDisabled() {
	rebuildMutableStateWorkflowHelper(s, false)
}

func (s *AdminTestSuite) TestAdminRebuildMutableState_ChasmEnabled() {
	cleanup := s.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	defer cleanup()

	configValues := s.GetTestCluster().Host().DcClient().GetValue(dynamicconfig.EnableChasm.Key())
	s.NotEmpty(configValues, "EnableChasm config should be set")
	configValue, _ := configValues[0].Value.(bool)
	s.True(configValue, "EnableChasm config should be true")
	rebuildMutableStateWorkflowHelper(s, true)
}

// common test helper
func rebuildMutableStateWorkflowHelper(s *AdminTestSuite, testWithChasm bool) {
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

	s.Worker().RegisterWorkflow(workflowFn)

	workflowID := tv.Any().String()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(s.testContext, 30*time.Second)
	defer cancel()

	workflowRun, err := s.SdkClient().ExecuteWorkflow(s.testContext, workflowOptions, workflowFn)
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
		response1, err = s.AdminClient().DescribeMutableState(ctx, adminservice.DescribeMutableStateRequest_builder{
			Namespace: s.Namespace().String(),
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: workflowID,
				RunId:      runID,
			}.Build(),
			Archetype: chasm.WorkflowArchetype,
		}.Build())
		s.NoError(err)
		if response1.GetDatabaseMutableState().GetExecutionInfo().GetStateTransitionCount() == 3 {
			// Note: ChasmNodes may be empty even with CHASM enabled, so we only check if the rebuild can be performed,
			// and not checking whether it is rebuildable because ChasmNodes are present.
			if !testWithChasm {
				s.Empty(response1.GetDatabaseMutableState().GetChasmNodes(), "CHASM-disabled workflows should not have ChasmNodes")
			}
			break
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
	}

	_, err = s.AdminClient().RebuildMutableState(ctx, adminservice.RebuildMutableStateRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
	}.Build())
	s.NoError(err)

	response2, err := s.AdminClient().DescribeMutableState(ctx, adminservice.DescribeMutableStateRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
		Archetype: chasm.WorkflowArchetype,
	}.Build())
	s.NoError(err)
	s.Equal(response1.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories(), response2.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories())
	s.Equal(response1.GetDatabaseMutableState().GetExecutionInfo().GetStateTransitionCount(), response2.GetDatabaseMutableState().GetExecutionInfo().GetStateTransitionCount())

	s.Equal(response1.GetDatabaseMutableState().GetExecutionState().GetCreateRequestId(), response2.GetDatabaseMutableState().GetExecutionState().GetCreateRequestId())
	s.Equal(response1.GetDatabaseMutableState().GetExecutionState().GetRunId(), response2.GetDatabaseMutableState().GetExecutionState().GetRunId())
	s.Equal(response1.GetDatabaseMutableState().GetExecutionState().GetState(), response2.GetDatabaseMutableState().GetExecutionState().GetState())
	s.Equal(response1.GetDatabaseMutableState().GetExecutionState().GetStatus(), response2.GetDatabaseMutableState().GetExecutionState().GetStatus())

	// From transition history perspective, Rebuild is considered as an update to the workflow and updates
	// all sub state machines in the workflow, which includes the workflow ExecutionState.
	s.Equal(persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: response1.GetDatabaseMutableState().GetExecutionState().GetLastUpdateVersionedTransition().GetNamespaceFailoverVersion(),
		TransitionCount:          response1.GetDatabaseMutableState().GetExecutionInfo().GetStateTransitionCount() + 1,
	}.Build(), response2.GetDatabaseMutableState().GetExecutionState().GetLastUpdateVersionedTransition())

	// Rebuild explicitly sets start time, thus start time will change after rebuild.
	s.NotNil(response1.GetDatabaseMutableState().GetExecutionState().GetStartTime())
	s.NotNil(response2.GetDatabaseMutableState().GetExecutionState().GetStartTime())

	timeBefore := timestamp.TimeValue(response1.GetDatabaseMutableState().GetExecutionState().GetStartTime())
	timeAfter := timestamp.TimeValue(response2.GetDatabaseMutableState().GetExecutionState().GetStartTime())
	s.False(timeAfter.Before(timeBefore))
}
