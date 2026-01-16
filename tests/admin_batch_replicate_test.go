package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type AdminBatchReplicateTestSuite struct {
	testcore.FunctionalTestBase
}

func TestAdminBatchReplicateTestSuite(t *testing.T) {
	s := new(AdminBatchReplicateTestSuite)
	suite.Run(t, s)
}

func (s *AdminBatchReplicateTestSuite) SetupSuite() {
	s.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
		dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace.Key(): 10,
	}))
}

func (s *AdminBatchReplicateTestSuite) simpleWorkflow(ctx workflow.Context) (string, error) {
	return "done", nil
}

func (s *AdminBatchReplicateTestSuite) createWorkflow(ctx context.Context, workflowFn interface{}) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)
	return workflowRun
}

func (s *AdminBatchReplicateTestSuite) TestStartAdminBatchOperation_Replicate_InvalidArgument_NoClusters() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Request without clusters should fail
	_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace: s.Namespace().String(),
		JobId:     uuid.NewString(),
		Reason:    "test",
		Identity:  "test-identity",
		Executions: []*commonpb.WorkflowExecution{
			{WorkflowId: "test-wf-id", RunId: "test-run-id"},
		},
		Operation: &adminservice.StartAdminBatchOperationRequest_ReplicateOperation{
			ReplicateOperation: &adminservice.BatchOperationReplicate{},
		},
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchReplicateTestSuite) TestStartAdminBatchOperation_Replicate_InvalidArgument_NoNamespace() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Request without namespace should fail
	_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		JobId:    uuid.NewString(),
		Reason:   "test",
		Identity: "test-identity",
		Executions: []*commonpb.WorkflowExecution{
			{WorkflowId: "test-wf-id", RunId: "test-run-id"},
		},
		Operation: &adminservice.StartAdminBatchOperationRequest_ReplicateOperation{
			ReplicateOperation: &adminservice.BatchOperationReplicate{},
		},
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchReplicateTestSuite) TestStartAdminBatchOperation_Replicate_InvalidArgument_NoJobId() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Request without job_id should fail
	_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Reason:    "test",
		Identity:  "test-identity",
		Executions: []*commonpb.WorkflowExecution{
			{WorkflowId: "test-wf-id", RunId: "test-run-id"},
		},
		Operation: &adminservice.StartAdminBatchOperationRequest_ReplicateOperation{
			ReplicateOperation: &adminservice.BatchOperationReplicate{
				TargetClusters: []string{"cluster1", "cluster2"},
			},
		},
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchReplicateTestSuite) TestStartAdminBatchOperation_Replicate_InvalidArgument_ExecutionsOrQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Request without executions or visibility_query should fail
	_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace: s.Namespace().String(),
		JobId:     uuid.NewString(),
		Reason:    "test",
		Identity:  "test-identity",
		Operation: &adminservice.StartAdminBatchOperationRequest_ReplicateOperation{
			ReplicateOperation: &adminservice.BatchOperationReplicate{
				TargetClusters: []string{"cluster1", "cluster2"},
			},
		},
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	// requests with both executions and visibility query should fail
	_, err = s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace: s.Namespace().String(),
		JobId:     uuid.NewString(),
		Reason:    "test",
		Identity:  "test-identity",
		Executions: []*commonpb.WorkflowExecution{
			{WorkflowId: "test-wf-id", RunId: "test-run-id"},
		},
		VisibilityQuery: "WorkflowType='simpleWorkflow'",
		Operation: &adminservice.StartAdminBatchOperationRequest_ReplicateOperation{
			ReplicateOperation: &adminservice.BatchOperationReplicate{
				TargetClusters: []string{"cluster1", "cluster2"},
			},
		},
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchReplicateTestSuite) TestStartAdminBatchOperation_Replicate_Success_By_VisibilityQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.simpleWorkflow)

	// Create workflows
	workflowRun1 := s.createWorkflow(ctx, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(ctx, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(ctx, &out)
	s.NoError(err)
	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)

	// Wait for workflows to be visible
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "WorkflowType='simpleWorkflow'",
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, resp.GetCount(), int64(2))
	}, 10*time.Second, 500*time.Millisecond)

	// Start admin batch operation using visibility query
	resp, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace:       s.Namespace().String(),
		VisibilityQuery: "WorkflowType='simpleWorkflow'",
		JobId:           uuid.NewString(),
		Reason:          "test replicate workflows with query",
		Identity:        "test-identity",
		Operation: &adminservice.StartAdminBatchOperationRequest_ReplicateOperation{
			ReplicateOperation: &adminservice.BatchOperationReplicate{
				TargetClusters: []string{"cluster1", "cluster2"},
			},
		},
	})
	s.NoError(err)
	s.NotNil(resp)
	// todo, maki: need to find a way to create clusters in testing env and check if wfs are replicated
	// because it seems single replication is not tested e2e as well?
}

func (s *AdminBatchReplicateTestSuite) TestStartAdminBatchOperation_Replicate_Success_By_Executions() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.Worker().RegisterWorkflow(s.simpleWorkflow)

	// Register cluster1 and cluster2 as remote clusters
	// Create workflows
	workflowRun1 := s.createWorkflow(ctx, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(ctx, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(ctx, &out)
	s.NoError(err)
	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)

	// Start admin batch operation to replicate to cluster1 and cluster2
	resp, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace: s.Namespace().String(),
		JobId:     uuid.NewString(),
		Reason:    "test replicate to specific clusters",
		Identity:  "test-identity",
		Executions: []*commonpb.WorkflowExecution{
			{WorkflowId: workflowRun1.GetID(), RunId: workflowRun1.GetRunID()},
			{WorkflowId: workflowRun2.GetID(), RunId: workflowRun2.GetRunID()},
		},
		Operation: &adminservice.StartAdminBatchOperationRequest_ReplicateOperation{
			ReplicateOperation: &adminservice.BatchOperationReplicate{
				TargetClusters: []string{"cluster1", "cluster2"},
			},
		},
	})
	s.NoError(err)
	s.NotNil(resp)
	// todo, maki: need to find a way to create clusters in testing env and check if wfs are replicated

}
