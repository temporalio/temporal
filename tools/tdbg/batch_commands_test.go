package tdbg

import (
	"bytes"
	"context"
	"flag"
	"strings"
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
		workflowID     string
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

func (t *batchTestAdminClient) CountExecutions(
	_ context.Context,
	request *adminservice.CountExecutionsRequest,
	_ ...grpc.CallOption,
) (*adminservice.CountExecutionsResponse, error) {
	return &adminservice.CountExecutionsResponse{Count: 2}, nil
}

func (t *batchTestAdminClient) StartAdminBatchOperation(
	_ context.Context,
	request *adminservice.StartAdminBatchOperationRequest,
	_ ...grpc.CallOption,
) (*adminservice.StartAdminBatchOperationResponse, error) {
	t.lastRequest = request
	return &adminservice.StartAdminBatchOperationResponse{WorkflowId: t.workflowID}, nil
}

const testCurrentCluster = "active-cluster"

func TestBatchCommandSuite(t *testing.T) {
	suite.Run(t, new(batchCommandTestSuite))
}

func (s *batchCommandTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.client = &batchTestClient{
		admin:    &batchTestAdminClient{currentCluster: testCurrentCluster, workflowID: "target-ns:my-job"},
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
		s.Equal("my-job", request.GetJobId())
		s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW, request.GetDelegationOperation())
		s.Contains(s.output.String(), "DANGER: destructive delegated batch operation")
		s.Contains(s.output.String(), "User namespace: \"target-ns\"")
		s.Contains(s.output.String(), "Batch workflow namespace: \"temporal-system\"")
		s.Contains(s.output.String(), "Operation: terminate-workflows")
		s.Contains(s.output.String(), "Currently matching: 3 workflows")
	})

	s.Run("Uses the server workflow ID", func() {
		s.client.admin.workflowID = "server-returned-id"
		defer func() { s.client.admin.workflowID = "target-ns:my-job" }()
		s.NoError(s.run(
			"--batch-type", batchTypeTerminateWorkflows,
			"--query", "A=B",
			"--reason", "cleanup",
			"--job-id", "another-job",
		))
		s.Equal("another-job", s.client.admin.lastRequest.GetJobId())
		s.Contains(s.output.String(), "with Job ID: server-returned-id")
	})

	s.Run("Colon in job ID is rejected", func() {
		s.client.admin.lastRequest = nil
		err := s.run(
			"--batch-type", batchTypeTerminateWorkflows,
			"--query", "A=B",
			"--reason", "cleanup",
			"--job-id", "target-ns:my-job",
		)
		s.ErrorContains(err, "cannot contain ':'")
		s.ErrorContains(err, "use '-' or '_' instead")
		s.Nil(s.client.admin.lastRequest)
	})

	s.Run("Terminate activities delegates the activity batch type", func() {
		s.NoError(s.run(
			"--batch-type", batchTypeTerminateActivities,
			"--query", "A=B",
			"--reason", "stuck activities",
		))

		request := s.client.admin.lastRequest
		s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, request.GetDelegationOperation())
		// The operation itself needs no payload: identity and reason travel on the envelope.
		s.Equal("stuck activities", request.GetReason())
		s.NotEmpty(request.GetIdentity())
		s.Contains(s.output.String(), "Operation: terminate-activities")
		s.Contains(s.output.String(), "Currently matching: 5 activities")
	})

	s.Run("Delete workflows delegates the workflow delete batch type", func() {
		s.NoError(s.run(
			"--batch-type", batchTypeDeleteWorkflows,
			"--query", "WorkflowType='ExpiredWorkflow'",
			"--reason", "retention cleanup",
		))

		request := s.client.admin.lastRequest
		s.Equal(enumspb.BATCH_OPERATION_TYPE_DELETE_WORKFLOW, request.GetDelegationOperation())
		s.Contains(s.output.String(), "Operation: delete-workflows")
		s.Contains(s.output.String(), "Currently matching: 3 workflows")
	})

	s.Run("Delete activities delegates the activity delete batch type", func() {
		s.NoError(s.run(
			"--batch-type", batchTypeDeleteActivities,
			"--query", "ActivityType='ExpiredActivity'",
			"--reason", "retention cleanup",
		))

		request := s.client.admin.lastRequest
		s.Equal(enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY, request.GetDelegationOperation())
		s.Contains(s.output.String(), "Operation: delete-activities")
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

func (s *batchCommandTestSuite) TestAdminBatchStartConfirmationWarnsOnlyForTermination() {
	for _, tc := range []struct {
		batchType   string
		wantWarning bool
	}{
		{batchTypeTerminateWorkflows, true},
		{batchTypeTerminateActivities, true},
		{batchTypeDeleteWorkflows, false},
		{batchTypeDeleteActivities, false},
	} {
		s.Run(tc.batchType, func() {
			s.output.Reset()
			flags := flag.NewFlagSet("delegated-batch", flag.ContinueOnError)
			flags.String(FlagNamespace, "target-ns", "")
			flags.String(FlagVisibilityQuery, "A=B", "")
			flags.String(FlagReason, "cleanup", "")
			flags.String(FlagBatchType, tc.batchType, "")
			ctx := cli.NewContext(s.app, flags, nil)
			ctx.Context = context.Background()
			prompter := NewPrompter(ctx, func(params *PrompterParams) {
				params.Writer = &s.output
				params.Reader = strings.NewReader("y\n")
				params.Exiter = func(int) { s.T().FailNow() }
			})

			s.Require().NoError(AdminBatchStart(ctx, s.client, prompter))
			warning := "Termination applies only to Running or Paused executions"
			if tc.wantWarning {
				s.Contains(s.output.String(), warning)
			} else {
				s.NotContains(s.output.String(), warning)
			}
			s.Contains(s.output.String(), "Proceed with "+tc.batchType)
		})
	}
}

func (s *batchCommandTestSuite) TestAdminBatchRefreshTasksSendsRawJobID() {
	err := s.app.Run([]string{
		"tdbg", "--namespace", "target-ns", "--yes", "execution", "refresh-tasks",
		"--query", "WorkflowType='MyWorkflow'", "--reason", "refresh", "--job-id", "my-job",
	})
	s.NoError(err)
	s.Equal("my-job", s.client.admin.lastRequest.GetJobId())
	s.Contains(s.output.String(), "target-ns:my-job")

	s.output.Reset()
	s.client.admin.workflowID = "server-returned-id"
	err = s.app.Run([]string{
		"tdbg", "--namespace", "target-ns", "--yes", "execution", "refresh-tasks",
		"--query", "WorkflowType='MyWorkflow'", "--reason", "refresh", "--job-id", "another-job",
	})
	s.NoError(err)
	s.Equal("another-job", s.client.admin.lastRequest.GetJobId())
	s.Contains(s.output.String(), "Job ID: server-returned-id")

	s.client.admin.lastRequest = nil
	err = s.app.Run([]string{
		"tdbg", "--namespace", "target-ns", "--yes", "execution", "refresh-tasks",
		"--query", "WorkflowType='MyWorkflow'", "--reason", "refresh", "--job-id", "target-ns:my-job",
	})
	s.ErrorContains(err, "cannot contain ':'")
	s.ErrorContains(err, "use '-' or '_' instead")
	s.Nil(s.client.admin.lastRequest)
}

func (s *batchCommandTestSuite) TestAdminBatchRefreshTasksConfirmationShowsClusterRole() {
	tests := []struct {
		name              string
		isGlobalNamespace bool
		activeCluster     string
		wantRole          string
	}{
		{name: "local namespace", wantRole: "active"},
		{name: "global namespace active here", isGlobalNamespace: true, activeCluster: testCurrentCluster, wantRole: "active"},
		{name: "global namespace passive here", isGlobalNamespace: true, activeCluster: "other-cluster", wantRole: "passive"},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.output.Reset()
			s.client.workflow.isGlobalNamespace = tc.isGlobalNamespace
			s.client.workflow.activeCluster = tc.activeCluster
			flags := flag.NewFlagSet("refresh-tasks", flag.ContinueOnError)
			flags.String(FlagNamespace, "target-ns", "")
			flags.String(FlagVisibilityQuery, "A=B", "")
			flags.String(FlagReason, "refresh", "")
			flags.String(FlagJobID, "my-job", "")
			ctx := cli.NewContext(s.app, flags, nil)
			ctx.Context = context.Background()
			prompter := NewPrompter(ctx, func(params *PrompterParams) {
				params.Writer = &s.output
				params.Reader = strings.NewReader("y\n")
				params.Exiter = func(int) { s.T().FailNow() }
			})

			s.Require().NoError(AdminBatchRefreshWorkflowTasks(ctx, s.client, prompter))
			s.Contains(s.output.String(), "This cluster is "+tc.wantRole+" for namespace \"target-ns\"")
			s.Contains(s.output.String(), "A batch workflow will be started in \"temporal-system\"")
			s.Contains(s.output.String(), "Continue? [y/N]:")
			s.Equal("my-job", s.client.admin.lastRequest.GetJobId())
		})
	}
}

func (s *batchCommandTestSuite) SetupSubTest() {
	previousAssertions := s.Assertions
	s.Assertions = require.New(s.T())
	s.T().Cleanup(func() {
		s.Assertions = previousAssertions
	})
}

func (s *batchCommandTestSuite) TestAdminAllNamespacesBatchRefreshTasks() {
	s.Run("starts one batch across all namespaces", func() {
		s.output.Reset()
		s.client.admin.lastRequest = nil
		s.NoError(s.app.Run([]string{
			"tdbg", "--yes", "execution", "refresh-tasks", "--all-namespaces",
			"--query", "WorkflowType='MyWorkflow' AND TemporalScheduledById='s' AND ScheduleId='s' AND ExecutionStatus='Running' AND TemporalNamespaceDivision IS NULL",
			"--reason", "refresh", "--job-id", "my-job",
		}))
		s.True(s.client.admin.lastRequest.GetRefreshTasksOperation().GetAllNamespaces())
		s.Empty(s.client.admin.lastRequest.GetNamespace())
	})

	s.Run("confirmation shows the count across all namespaces", func() {
		s.output.Reset()
		flags := flag.NewFlagSet("refresh-tasks", flag.ContinueOnError)
		flags.Bool(FlagAllNamespaces, true, "")
		flags.String(FlagVisibilityQuery, "A=B", "")
		flags.String(FlagReason, "refresh", "")
		ctx := cli.NewContext(s.app, flags, nil)
		ctx.Context = context.Background()
		prompter := NewPrompter(ctx, func(params *PrompterParams) {
			params.Writer = &s.output
			params.Reader = strings.NewReader("y\n")
			params.Exiter = func(int) { s.T().FailNow() }
		})

		s.Require().NoError(AdminAllNamespacesBatchRefreshWorkflowTasks(ctx, s.client, prompter))
		s.Contains(s.output.String(), "refresh tasks for 2 execution(s)")
		s.Contains(s.output.String(), `in all namespaces, including "temporal-system"`)
		s.Contains(s.output.String(), "Reminder: use system or predefined search attributes to narrow the query down")
	})

	s.Run("calls out that custom search attributes are not supported", func() {
		s.client.admin.lastRequest = nil
		err := s.app.Run([]string{
			"tdbg", "--yes", "execution", "refresh-tasks", "--all-namespaces",
			"--query", "WorkflowType='MyWorkflow' AND (CustomerId='abc' OR Keyword01='x')", "--reason", "refresh",
		})
		s.ErrorContains(err, "invalid --query for --all-namespaces: a query across all namespaces cannot filter on custom search attributes; only system and predefined search attributes are supported, got: CustomerId, Keyword01")
		s.Nil(s.client.admin.lastRequest)
	})

	s.Run("ignores --namespace with --all-namespaces", func() {
		s.client.admin.lastRequest = nil
		s.NoError(s.app.Run([]string{
			"tdbg", "--namespace", "ns-1", "--yes", "execution", "refresh-tasks", "--all-namespaces",
			"--query", "WorkflowType='MyWorkflow'", "--reason", "refresh",
		}))
		s.True(s.client.admin.lastRequest.GetRefreshTasksOperation().GetAllNamespaces())
		s.Empty(s.client.admin.lastRequest.GetNamespace())
	})

	s.Run("rejects a missing query with --all-namespaces", func() {
		s.client.admin.lastRequest = nil
		err := s.app.Run([]string{
			"tdbg", "--yes", "execution", "refresh-tasks", "--all-namespaces", "--reason", "refresh",
		})
		s.ErrorContains(err, "--all-namespaces requires --query")
		s.Nil(s.client.admin.lastRequest)
	})

	s.Run("rejects --workflow-id with --all-namespaces", func() {
		s.client.admin.lastRequest = nil
		err := s.app.Run([]string{
			"tdbg", "--yes", "execution", "refresh-tasks", "--all-namespaces",
			"--workflow-id", "wf", "--query", "WorkflowType='MyWorkflow'", "--reason", "refresh",
		})
		s.ErrorContains(err, "cannot be used with --workflow-id")
		s.Nil(s.client.admin.lastRequest)
	})
}
