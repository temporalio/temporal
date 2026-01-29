package xdc

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/tests/testcore"
)

type (
	WorkflowTaskReportedProblemsReplicationSuite struct {
		xdcBaseSuite
		shouldFail       atomic.Bool
		activeSDKClient  sdkclient.Client
		standbySDKClient sdkclient.Client
	}
)

func TestWorkflowTaskReportedProblemsReplicationSuite(t *testing.T) {
	s := new(WorkflowTaskReportedProblemsReplicationSuite)
	suite.Run(t, s)
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	}
	s.dynamicConfigOverrides[dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute.Key()] = 2
	s.enableTransitionHistory = true
	s.setupSuite()
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) SetupTest() {
	s.setupTest()
	s.shouldFail.Store(true)
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) simpleWorkflow(ctx workflow.Context) (string, error) {
	if s.shouldFail.Load() {
		panic("forced-panic-to-fail-wft")
	}
	return "done!", nil
}

// checkReportedProblemsSearchAttribute is a helper function to verify reported problems search attributes
func (s *WorkflowTaskReportedProblemsReplicationSuite) checkReportedProblemsSearchAttribute(
	admin adminservice.AdminServiceClient,
	client sdkclient.Client,
	workflowID, runID string,
	namespace string,
	expectedCategory, expectedCause string,
	shouldExist bool,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := client.DescribeWorkflow(context.Background(), workflowID, runID)
		require.NoError(t, err)

		if shouldExist {
			saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.True(t, ok)
			require.NotEmpty(t, saValues)
			require.Len(t, saValues, 2)
			require.Contains(t, saValues, "category="+expectedCategory)
			require.Contains(t, saValues, "cause="+expectedCause)

			category, cause, err := s.getWFTFailure(admin, namespace, workflowID, runID)
			require.NoError(t, err)
			require.Equal(t, expectedCategory, category)
			require.Equal(t, expectedCause, cause)
		} else {
			_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
			require.False(t, ok)
		}
	}, 20*time.Second, 500*time.Millisecond)
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) getWFTFailure(admin adminservice.AdminServiceClient, ns, wfid, runid string) (lastWFTCause string, lastWFTCategory string, err error) {
	resp, err := admin.DescribeMutableState(context.Background(), &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wfid,
			RunId:      runid,
		},
		Archetype: chasm.WorkflowArchetype,
	})
	require.NoError(s.T(), err)
	require.NotNil(s.T(), resp)
	require.NotNil(s.T(), resp.DatabaseMutableState)
	require.NotNil(s.T(), resp.DatabaseMutableState.ExecutionInfo)
	require.NotNil(s.T(), resp.DatabaseMutableState.ExecutionInfo.LastWorkflowTaskFailure)
	switch i := resp.DatabaseMutableState.ExecutionInfo.GetLastWorkflowTaskFailure().(type) {
	case *persistencespb.WorkflowExecutionInfo_LastWorkflowTaskFailureCause:
		return "WorkflowTaskFailed", fmt.Sprintf("WorkflowTaskFailedCause%s", i.LastWorkflowTaskFailureCause.String()), nil
	case *persistencespb.WorkflowExecutionInfo_LastWorkflowTaskTimedOutType:
		return "WorkflowTaskTimedOut", fmt.Sprintf("WorkflowTaskTimedOutCause%s", i.LastWorkflowTaskTimedOutType.String()), nil
	default:
		return "", "", nil
	}
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) TestWFTFailureReportedProblemsReplication() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns := s.createGlobalNamespace()
	activeSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr("tq")
	worker1 := sdkworker.New(activeSDKClient, taskQueue, sdkworker.Options{})

	worker1.RegisterWorkflow(s.simpleWorkflow)

	s.NoError(worker1.Start())
	defer worker1.Stop()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflow)
	s.NoError(err)

	// verify search attributes are set in cluster0 using the helper function
	s.checkReportedProblemsSearchAttribute(
		s.clusters[0].Host().AdminClient(),
		activeSDKClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"WorkflowTaskFailed",
		"WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure",
		true,
	)

	// get standby client
	standbyClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[1].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	s.NotNil(standbyClient)

	// verify search attributes are replicated to cluster1 using the helper function
	s.checkReportedProblemsSearchAttribute(
		s.clusters[1].Host().AdminClient(),
		standbyClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"WorkflowTaskFailed",
		"WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure",
		true,
	)

	// allow the workflow to succeed
	s.shouldFail.Store(false)

	// wait for workflow to complete
	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	// verify search attributes are cleared in cluster1 using the helper function
	s.checkReportedProblemsSearchAttribute(
		s.clusters[1].Host().AdminClient(),
		standbyClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"",
		"",
		false,
	)
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) TestWFTFailureReportedProblems_DynamicConfigChanges() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns := s.createGlobalNamespace()
	var err error
	s.activeSDKClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr("tq")
	worker1 := sdkworker.New(s.activeSDKClient, taskQueue, sdkworker.Options{})

	worker1.RegisterWorkflow(s.simpleWorkflow)

	s.NoError(worker1.Start())
	defer worker1.Stop()

	// Override dynamic config to disable search attribute setting initially
	cleanup1 := s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 0)
	defer cleanup1()
	s.shouldFail.Store(true)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := s.activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflow)
	s.NoError(err)

	// Verify search attributes are NOT set in cluster0 when config is 0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.activeSDKClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)
	}, 5*time.Second, 500*time.Millisecond)

	// Verify workflow task attempts are accumulating
	s.EventuallyWithT(func(t *assert.CollectT) {
		exec, err := s.activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, exec.PendingWorkflowTask.Attempt, int32(2))
	}, 5*time.Second, 500*time.Millisecond)

	// Change dynamic config to enable search attribute setting
	cleanup2 := s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)
	defer cleanup2()

	// Verify search attributes are now set in cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.activeSDKClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 15*time.Second, 500*time.Millisecond)

	// get standby client
	s.standbySDKClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[1].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	s.NotNil(s.standbySDKClient)

	// verify search attributes are replicated to cluster1 using the helper function
	s.checkReportedProblemsSearchAttribute(
		s.clusters[1].Host().AdminClient(),
		s.standbySDKClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"WorkflowTaskFailed",
		"WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure",
		true,
	)

	// allow the workflow to succeed
	s.shouldFail.Store(false)

	// wait for workflow to complete
	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	// verify search attributes are cleared in cluster1 using the helper function
	s.checkReportedProblemsSearchAttribute(
		s.clusters[1].Host().AdminClient(),
		s.standbySDKClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"",
		"",
		false,
	)
}

// workflowWithSignalsThatFails creates a workflow that listens for signals and fails on each workflow task.
func (s *WorkflowTaskReportedProblemsReplicationSuite) workflowWithSignalsThatFails(ctx workflow.Context) (string, error) { // If we should fail, signal ourselves (creating a side effect) and immediately panic.
	// This will create buffered.
	if s.shouldFail.Load() {
		// Signal ourselves to create buffered events
		err := s.activeSDKClient.SignalWorkflow(context.Background(), workflow.GetInfo(ctx).WorkflowExecution.ID, "", "test-signal", "self-signal")
		if err != nil {
			return "", err
		}
		panic("forced-panic-after-self-signal")
	}

	// If we reach here, shouldFail is false, so we can complete
	return "done!", nil
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) TestWFTFailureReportedProblems_NotClearedBySignals() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ns := s.createGlobalNamespace()
	var err error
	s.activeSDKClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)

	s.shouldFail.Store(true)

	taskQueue := testcore.RandomizeStr("tq")
	worker1 := sdkworker.New(s.activeSDKClient, taskQueue, sdkworker.Options{})
	worker1.RegisterWorkflow(s.workflowWithSignalsThatFails)
	s.NoError(worker1.Start())
	defer worker1.Stop()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := s.activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, s.workflowWithSignalsThatFails)
	s.NoError(err)

	// The workflow will signal itself and panic on each WFT, creating buffered events naturally.
	// Wait for the search attribute to be set due to consecutive failures
	s.checkReportedProblemsSearchAttribute(
		s.clusters[0].Host().AdminClient(),
		s.activeSDKClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"WorkflowTaskFailed",
		"WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure",
		true,
	)

	// Verify the search attribute persists even as the workflow continues to fail and create buffered events
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.activeSDKClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		s.True(ok, "Search attribute should still be present during continued failures")
		s.NotEmpty(saVal, "Search attribute should not be empty during continued failures")
	}, 5*time.Second, 500*time.Millisecond)

	// get standby client
	s.standbySDKClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[1].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	s.NotNil(s.standbySDKClient)

	// verify search attributes are replicated to cluster1
	s.checkReportedProblemsSearchAttribute(
		s.clusters[1].Host().AdminClient(),
		s.standbySDKClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"WorkflowTaskFailed",
		"WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure",
		true,
	)

	// Terminate the workflow for cleanup
	err = s.activeSDKClient.TerminateWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID(), "test cleanup")
	s.NoError(err)
}
