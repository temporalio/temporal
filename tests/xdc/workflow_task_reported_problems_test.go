package xdc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
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
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}
	s.setupSuite()
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) SetupTest() {
	s.setupTest()
	s.shouldFail.Store(false)
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *WorkflowTaskReportedProblemsReplicationSuite) TestWFTFailureReportedProblemsReplication() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityFunction := func() (string, error) {
		return "done!", nil
	}

	workflowFn := func(ctx workflow.Context) error {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    15 * time.Minute,
			ScheduleToCloseTimeout: 30 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				BackoffCoefficient: 1,
			},
		}), activityFunction).Get(ctx, &ret)

		if !s.shouldFail.Load() {
			panic("forced-panic-to-fail-wft")
		}
		return err
	}

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

	// wait for workflow to start and fail
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.WorkflowExecutionInfo.Status)
	}, 5*time.Second, 500*time.Millisecond)

	// verify search attributes are set in cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes)
		require.NotEmpty(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])

		// Decode the search attribute in keyword list format
		searchValBytes := description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems]
		searchVal, err := searchattribute.DecodeValue(searchValBytes, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
		require.NoError(t, err)
		require.NotEmpty(t, searchVal)
		require.Equal(t, "category=WorkflowTaskFailed", searchVal.([]string)[0])
		require.Equal(t, "cause=WorkflowWorkerUnhandledFailure", searchVal.([]string)[1])
	}, 5*time.Second, 500*time.Millisecond)

	// stop worker1 so cluster0 won't make any progress
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

	// verify search attributes are replicated to cluster1
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := standbyClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes)
		require.NotEmpty(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])

		// Decode the search attribute in keyword list format
		searchValBytes := description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems]
		searchVal, err := searchattribute.DecodeValue(searchValBytes, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
		require.NoError(t, err)
		require.NotEmpty(t, searchVal)
		require.Equal(t, "category=WorkflowTaskFailed", searchVal.([]string)[0])
		require.Equal(t, "cause=WorkflowWorkerUnhandledFailure", searchVal.([]string)[1])
	}, 5*time.Second, 500*time.Millisecond)

	// start worker2 in cluster1
	worker2 := sdkworker.New(standbyClient, taskQueue, sdkworker.Options{})
	worker2.RegisterWorkflow(workflowFn)
	worker2.RegisterActivity(activityFunction)
	s.NoError(worker2.Start())
	defer worker2.Stop()

	// allow the workflow to succeed
	s.shouldFail.Store(true)

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)

	// verify search attributes are cleared in cluster1
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := standbyClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, description.WorkflowExecutionInfo.Status)
		require.Nil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])
	}, 5*time.Second, 500*time.Millisecond)
}
