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
		shouldFail   atomic.Bool
		failureCount atomic.Int32
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

	// Override dynamic config to disable search attribute setting initially
	cleanup1 := s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 0)
	defer cleanup1()
	s.shouldFail.Store(true)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflow)
	s.NoError(err)

	// Verify search attributes are NOT set in cluster0 when config is 0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)
	}, 5*time.Second, 500*time.Millisecond)

	// Verify workflow task attempts are accumulating
	s.EventuallyWithT(func(t *assert.CollectT) {
		exec, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, exec.PendingWorkflowTask.Attempt, int32(2))
	}, 5*time.Second, 500*time.Millisecond)

	// Change dynamic config to enable search attribute setting
	cleanup2 := s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)
	defer cleanup2()

	// Verify search attributes are now set in cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 15*time.Second, 500*time.Millisecond)

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
