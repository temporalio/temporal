package tdbg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

type (
	batchTestAdminClient struct {
		adminservice.AdminServiceClient
		currentCluster string
		lastRequest    *adminservice.StartAdminBatchOperationRequest
		requests       []*adminservice.StartAdminBatchOperationRequest
		startErrors    map[string]error
	}

	batchTestWorkflowClient struct {
		workflowservice.WorkflowServiceClient
		isGlobalNamespace bool
		activeCluster     string
	}

	batchTestClient struct {
		admin    *batchTestAdminClient
		workflow *batchTestWorkflowClient
	}

	batchCommandTestSuite struct {
		*require.Assertions
		suite.Suite
		app    *cli.App
		client *batchTestClient
		output bytes.Buffer
	}
)

func (t *batchTestClient) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return t.admin
}

func (t *batchTestClient) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return t.workflow
}

func (t *batchTestWorkflowClient) DescribeNamespace(
	context.Context,
	*workflowservice.DescribeNamespaceRequest,
	...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{
		IsGlobalNamespace: t.isGlobalNamespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: t.activeCluster,
		},
	}, nil
}

func (t *batchTestAdminClient) DescribeCluster(
	context.Context,
	*adminservice.DescribeClusterRequest,
	...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	return &adminservice.DescribeClusterResponse{ClusterName: t.currentCluster}, nil
}

func (t *batchTestWorkflowClient) CountWorkflowExecutions(
	context.Context,
	*workflowservice.CountWorkflowExecutionsRequest,
	...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 3}, nil
}

func (t *batchTestWorkflowClient) CountActivityExecutions(
	context.Context,
	*workflowservice.CountActivityExecutionsRequest,
	...grpc.CallOption,
) (*workflowservice.CountActivityExecutionsResponse, error) {
	return &workflowservice.CountActivityExecutionsResponse{Count: 5}, nil
}

func (t *batchTestAdminClient) StartAdminBatchOperation(
	_ context.Context,
	request *adminservice.StartAdminBatchOperationRequest,
	_ ...grpc.CallOption,
) (*adminservice.StartAdminBatchOperationResponse, error) {
	t.lastRequest = request
	t.requests = append(t.requests, request)
	if err := t.startErrors[request.GetNamespace()]; err != nil {
		return nil, err
	}
	return &adminservice.StartAdminBatchOperationResponse{}, nil
}

const testCurrentCluster = "active-cluster"

func TestBatchCommandSuite(t *testing.T) {
	suite.Run(t, new(batchCommandTestSuite))
}

func (s *batchCommandTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.client = &batchTestClient{
		admin:    &batchTestAdminClient{currentCluster: testCurrentCluster},
		workflow: &batchTestWorkflowClient{activeCluster: testCurrentCluster},
	}
	s.app = NewCliApp(func(params *Params) {
		params.ClientFactory = s.client
		params.Writer = &s.output
		params.ErrWriter = &s.output
	})
	s.app.ExitErrHandler = func(*cli.Context, error) {}
}

func (s *batchCommandTestSuite) run(args ...string) error {
	s.output.Reset()
	return s.app.Run(append([]string{"tdbg", "--namespace", "target-ns", "--yes", "delegated-batch", "start"}, args...))
}

func (s *batchCommandTestSuite) TestAdminBatchStart() {
	s.Run("Terminate populates the admin envelope", func() {
		s.NoError(s.run(
			"--batch-type", batchTypeTerminateWorkflows,
			"--query", "WorkflowType='MyWorkflow'",
			"--reason", "cleanup",
			"--job-id", "my-job",
		))

		request := s.client.admin.lastRequest
		s.NotNil(request)
		if request == nil {
			return
		}
		s.Equal("target-ns", request.GetNamespace())
		s.Equal("WorkflowType='MyWorkflow'", request.GetVisibilityQuery())
		s.Equal("cleanup", request.GetReason())
		s.Equal("my-job:target-ns", request.GetJobId())
		s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW, request.GetDelegationOperation().GetBatchType())
		s.Contains(s.output.String(), "DANGER: destructive delegated batch operation")
		s.Contains(s.output.String(), "User namespace: \"target-ns\"")
		s.Contains(s.output.String(), "Batch workflow namespace: \"temporal-system\"")
		s.Contains(s.output.String(), "Operation: terminate-workflows")
		s.Contains(s.output.String(), "Currently matching: 3 workflows")
	})

	s.Run("Terminate activities delegates the activity batch type", func() {
		s.NoError(s.run(
			"--batch-type", batchTypeTerminateActivities,
			"--query", "A=B",
			"--reason", "stuck activities",
		))

		request := s.client.admin.lastRequest
		s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, request.GetDelegationOperation().GetBatchType())
		// The operation itself needs no payload: identity and reason travel on the envelope.
		s.Equal("stuck activities", request.GetReason())
		s.NotEmpty(request.GetIdentity())
		s.Contains(s.output.String(), "Operation: terminate-activities")
		s.Contains(s.output.String(), "Currently matching: 5 activities")
	})

	s.Run("Unknown batch type is rejected", func() {
		s.ErrorContains(s.run("--batch-type", "nonsense", "--query", "A=B", "--reason", "r"), "unknown batch type")
	})

	s.Run("Query is required", func() {
		s.ErrorContains(s.run("--batch-type", batchTypeTerminateWorkflows, "--reason", "r"), FlagVisibilityQuery)
	})

	s.Run("Reason is required", func() {
		s.ErrorContains(s.run("--batch-type", batchTypeTerminateWorkflows, "--query", "A=B"), FlagReason)
	})

	s.Run("Global namespace active in this cluster is allowed", func() {
		s.client.workflow.isGlobalNamespace = true
		s.client.workflow.activeCluster = testCurrentCluster
		s.NoError(s.run("--batch-type", batchTypeTerminateWorkflows, "--query", "A=B", "--reason", "r"))
	})

	s.Run("Global namespace active in another cluster is rejected", func() {
		s.client.workflow.isGlobalNamespace = true
		s.client.workflow.activeCluster = "other-cluster"
		s.client.admin.lastRequest = nil
		err := s.run("--batch-type", batchTypeTerminateWorkflows, "--query", "A=B", "--reason", "r")
		s.ErrorContains(err, "must be started in the active cluster")
		s.Nil(s.client.admin.lastRequest, "the job must not be started")
	})
}

func (s *batchCommandTestSuite) TestAdminBatchStart_MultipleNamespaces() {
	err := s.run(
		"--"+FlagNamespaces, "target-ns",
		"--"+FlagNamespaces, "other-ns",
		"--batch-type", batchTypeTerminateWorkflows,
		"--query", "WorkflowType='MyWorkflow'",
		"--reason", "cleanup",
		"--job-id", "my-job",
	)
	s.NoError(err)
	s.Len(s.client.admin.requests, 2)
	s.Equal("target-ns", s.client.admin.requests[0].GetNamespace())
	s.Equal("my-job:target-ns", s.client.admin.requests[0].GetJobId())
	s.Equal("other-ns", s.client.admin.requests[1].GetNamespace())
	s.Equal("my-job:other-ns", s.client.admin.requests[1].GetJobId())
	s.Contains(s.output.String(), `"target-ns": 3 workflows`)
	s.Contains(s.output.String(), `"other-ns": 3 workflows`)
	s.Contains(s.output.String(), "Currently matching: 6 workflows across 2 namespaces")
	s.Contains(s.output.String(), "Submission summary: 2 started, 0 failed.")
}

func (s *batchCommandTestSuite) TestAdminBatchStart_MultipleNamespacesPartialFailure() {
	s.client.admin.startErrors = map[string]error{"other-ns": errors.New("start failed")}
	err := s.run(
		"--"+FlagNamespaces, "target-ns",
		"--"+FlagNamespaces, "other-ns",
		"--batch-type", batchTypeTerminateWorkflows,
		"--query", "WorkflowType='MyWorkflow'",
		"--reason", "cleanup",
		"--job-id", "my-job",
	)
	s.ErrorContains(err, `namespace "other-ns": start failed`)
	s.Len(s.client.admin.requests, 2)
	s.Contains(s.output.String(), `Batch operation started successfully for namespace "target-ns"`)
	s.Contains(s.output.String(), `Failed to start batch operation for namespace "other-ns"`)
	s.Contains(s.output.String(), "Submission summary: 1 started, 1 failed.")
}

func (s *batchCommandTestSuite) TestAdminBatchRefreshWorkflowTasks_MultipleNamespaces() {
	s.output.Reset()
	err := s.app.Run([]string{
		"tdbg", "--namespace", "target-ns", "--yes",
		"execution", "refresh-tasks",
		"--" + FlagNamespaces, "target-ns",
		"--" + FlagNamespaces, "other-ns",
		"--query", "WorkflowType='MyWorkflow'",
		"--reason", "refresh",
		"--job-id", "my-job",
	})
	s.NoError(err)
	s.Len(s.client.admin.requests, 2)
	s.NotNil(s.client.admin.requests[0].GetRefreshTasksOperation())
	s.NotNil(s.client.admin.requests[1].GetRefreshTasksOperation())
	s.Equal("my-job:target-ns", s.client.admin.requests[0].GetJobId())
	s.Equal("my-job:other-ns", s.client.admin.requests[1].GetJobId())
	s.Contains(s.output.String(), `"target-ns": 3 execution(s)`)
	s.Contains(s.output.String(), `"other-ns": 3 execution(s)`)
	s.Contains(s.output.String(), "Submission summary: 2 started, 0 failed.")
}

func (s *batchCommandTestSuite) TestAdminBatchStart_RejectsMoreThanMaximumNamespaces() {
	args := []string{
		"--batch-type", batchTypeTerminateWorkflows,
		"--query", "WorkflowType='MyWorkflow'",
		"--reason", "cleanup",
	}
	for i := 0; i <= maxBatchNamespaces; i++ {
		args = append(args, "--"+FlagNamespaces, fmt.Sprintf("namespace-%d", i))
	}

	err := s.run(args...)
	s.ErrorContains(err, "at most 100 namespaces are supported, got 101")
	s.Empty(s.client.admin.requests)
}
