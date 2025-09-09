package xdc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	adminservicespb "go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
)

type (
	WorkflowTaskReportedProblemsReplicationSuite struct {
		xdcBaseSuite
		shouldFail atomic.Bool
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
	admin adminservicespb.AdminServiceClient,
	client sdkclient.Client,
	workflowID, runID string,
	namespace string,
	expectedCategory, expectedCause string,
	shouldExist bool,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := client.DescribeWorkflowExecution(context.Background(), workflowID, runID)
		require.NoError(t, err)

		if shouldExist {
			require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes)
			require.NotEmpty(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields)
			require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])

			// Decode the search attribute in keyword list format
			searchValBytes := description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems]
			searchVal, err := searchattribute.DecodeValue(searchValBytes, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
			require.NoError(t, err)
			require.NotEmpty(t, searchVal)

			searchValList := searchVal.([]string)
			require.Len(t, searchValList, 2)
			require.Equal(t, "category="+expectedCategory, searchValList[0])
			require.Equal(t, "cause="+expectedCause, searchValList[1])

			// Validate attempt number after verifying search attribute values
			require.GreaterOrEqual(t, description.GetPendingWorkflowTask().Attempt, int32(2))

			category, cause, err := s.getWFTFailure(admin, namespace, workflowID, runID)
			require.NoError(t, err)
			require.Equal(t, expectedCategory, category)
			require.Equal(t, expectedCause, cause)
		} else {
			// Check that the search attribute is not present or is nil
			if description.WorkflowExecutionInfo.SearchAttributes != nil &&
				description.WorkflowExecutionInfo.SearchAttributes.IndexedFields != nil {
				require.Nil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])
			}
		}
	}, 20*time.Second, 500*time.Millisecond)
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) getWFTFailure(admin adminservicespb.AdminServiceClient, ns, wfid, runid string) (string, string, error) {
	resp, err := admin.DescribeMutableState(context.Background(), &adminservicespb.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wfid,
			RunId:      runid,
		},
	})
	require.NoError(s.T(), err)
	require.NotNil(s.T(), resp)
	require.NotNil(s.T(), resp.DatabaseMutableState)
	require.NotNil(s.T(), resp.DatabaseMutableState.ExecutionInfo)
	require.NotNil(s.T(), resp.DatabaseMutableState.ExecutionInfo.LastWorkflowTaskFailure)
	switch i := resp.DatabaseMutableState.ExecutionInfo.GetLastWorkflowTaskFailure().(type) {
	case *persistencespb.WorkflowExecutionInfo_LastWorkflowTaskFailureCause:
		return "WorkflowTaskFailed", i.LastWorkflowTaskFailureCause.String(), nil
	case *persistencespb.WorkflowExecutionInfo_LastWorkflowTaskTimedOutType:
		return "WorkflowTaskTimedOut", i.LastWorkflowTaskTimedOutType.String(), nil
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

	// start a workflow
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflow)
	s.NoError(err)

	// wait for workflow to start and fail
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.WorkflowExecutionInfo.Status)
	}, 5*time.Second, 500*time.Millisecond)

	// verify search attributes are set in cluster0 using the helper function
	s.checkReportedProblemsSearchAttribute(
		s.clusters[0].Host().AdminClient(),
		activeSDKClient,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		ns,
		"WorkflowTaskFailed",
		"WorkflowWorkerUnhandledFailure",
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
		"WorkflowWorkerUnhandledFailure",
		true,
	)

	// start worker2 in cluster1
	worker2 := sdkworker.New(standbyClient, taskQueue, sdkworker.Options{})
	worker2.RegisterWorkflow(s.simpleWorkflow)
	s.NoError(worker2.Start())
	defer worker2.Stop()

	// allow the workflow to succeed
	s.shouldFail.Store(false)

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)

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
