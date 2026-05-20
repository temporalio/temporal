package migration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type ForceReplicationWorkflowV3TestSuite struct {
	suite.Suite
}

func TestForceReplicationWorkflowV3TestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ForceReplicationWorkflowV3TestSuite{})
}

// TestHappyPath drives V3 through three pages of ListWorkflows. Each batch
// runs InjectBatch then VerifyBatch (both succeed, no pending), so no
// retries fire and the workflow completes cleanly.
func (s *ForceReplicationWorkflowV3TestSuite) TestHappyPath() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 3}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	pageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		pageCount++
		if pageCount < 3 {
			return &listWorkflowsResponse{
				Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
				NextPageToken: []byte("next"),
			}, nil
		}
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
			NextPageToken: nil,
		}, nil
	}).Times(3)

	// One inject + one verify per batch. Each verify returns clean (no
	// pending) so the controller doesn't even need to retry.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(3)
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).
		Return(&adaptiveVerifyBatchResponse{Verified: 1, Pending: nil}, nil).
		Times(3)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   10,
		EnableVerification:      true,
		TargetClusterEndpoint:   "test-target",
		HistoryShardCount:       4,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	s.Equal(int64(3), status.ReplicatedWorkflowCount)
	s.Equal(0, status.QuarantinedWFIDCount)
	s.Equal(0, status.QuarantinedShardCount)
}

// TestInjectOnlyMode runs V3 with EnableVerification=false. VerifyBatch
// must not be invoked; the workflow completes once all pages have been
// injected, and the status query reports the injected count as
// ReplicatedWorkflowCount (since inject-success is the only progress
// signal available in this mode).
func (s *ForceReplicationWorkflowV3TestSuite) TestInjectOnlyMode() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 3}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	pageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		pageCount++
		if pageCount < 3 {
			return &listWorkflowsResponse{
				Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
				NextPageToken: []byte("next"),
			}, nil
		}
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{{BusinessID: "wf-1"}},
			NextPageToken: nil,
		}, nil
	}).Times(3)

	// InjectBatch runs once per page; VerifyBatch must not be called.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil).Times(3)
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(nil, nil).Times(0)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   10,
		EnableVerification:      false,
		HistoryShardCount:       4,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	// Inject-only counts toward ReplicatedWorkflowCount via inject success.
	s.Equal(int64(3), status.ReplicatedWorkflowCount)
	s.Equal(0, status.QuarantinedWFIDCount)
	s.Equal(0, status.QuarantinedShardCount)
}

// TestQuarantineAfterRepeatedPending drives V3 with a hot WF ID that keeps
// returning as pending. After WFIDQuarantineThreshold pendings, that WF ID
// is quarantined; subsequent retries call VerifyBatch alone (no inject)
// until the hot exec finally verifies.
func (s *ForceReplicationWorkflowV3TestSuite) TestQuarantineAfterRepeatedPending() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 1}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	hotExec := &ExecutionInfo{BusinessID: "hot-wf", RunID: "run-1"}
	listed := false
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		if listed {
			return &listWorkflowsResponse{Executions: nil, NextPageToken: nil}, nil
		}
		listed = true
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{hotExec},
			NextPageToken: nil,
		}, nil
	})

	// InjectBatch always succeeds; the verify side is where the hot exec
	// keeps coming back as pending until it eventually clears. Three
	// pending observations (calls 1-3) trip the
	// WFIDQuarantineThreshold of 3; the fourth call verifies cleanly.
	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil)
	verifyCalls := 0
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		verifyCalls++
		if verifyCalls < 4 {
			return &adaptiveVerifyBatchResponse{Verified: 0, Pending: []*ExecutionInfo{hotExec}}, nil
		}
		return &adaptiveVerifyBatchResponse{Verified: 1, Pending: nil}, nil
	})

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		Query:                    "",
		ConcurrentActivityCount:  2,
		OverallRps:               10,
		ListWorkflowsPageSize:    1,
		PageCountPerExecution:    10,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		HistoryShardCount:        4,
		WFIDQuarantineThreshold:  3,
		NoProgressTimeoutSeconds: 3600,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	s.Equal(1, status.QuarantinedWFIDCount)
}

// TestShardQuarantineRecovers drives V3 against a one-shard topology with
// two distinct WF IDs both struggling, then both clearing. After two WF IDs
// each generate two pendings, shardPending == 4 → shard goes into
// quarantine. Then both WF IDs verify cleanly across a couple more rounds,
// decaying shardPending back below the release threshold → quarantine
// releases. WFIDQuarantineThreshold is set high so WF-ID quarantine doesn't
// fire and steal the shard counter increments.
func (s *ForceReplicationWorkflowV3TestSuite) TestShardQuarantineRecovers() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})

	namespaceID := uuid.NewString()
	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 2}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 1, NamespaceID: namespaceID}, nil)

	execA := &ExecutionInfo{BusinessID: "wf-a", RunID: "run-1"}
	execB := &ExecutionInfo{BusinessID: "wf-b", RunID: "run-1"}
	listed := false
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		if listed {
			return &listWorkflowsResponse{Executions: nil, NextPageToken: nil}, nil
		}
		listed = true
		return &listWorkflowsResponse{
			Executions:    []*ExecutionInfo{execA, execB},
			NextPageToken: nil,
		}, nil
	})

	env.OnActivity(a.InjectBatch, mock.Anything, mock.Anything).Return(nil)
	// Verify outcomes by call:
	//   1: both pending (initial batch) → shardPending = 2
	//   2: both pending (retry) → shardPending = 4 → shard quarantined
	//   3+: both clean → shardPending decays → quarantine releases
	verifyCalls := 0
	env.OnActivity(a.VerifyBatch, mock.Anything, mock.Anything).Return(func(_ context.Context, req *adaptiveVerifyBatchRequest) (*adaptiveVerifyBatchResponse, error) {
		verifyCalls++
		if verifyCalls <= 2 {
			return &adaptiveVerifyBatchResponse{Verified: 0, Pending: req.Executions}, nil
		}
		return &adaptiveVerifyBatchResponse{Verified: int64(len(req.Executions)), Pending: nil}, nil
	})

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.ExecuteWorkflow(ForceReplicationWorkflowV3, AdaptiveForceReplicationParams{
		Namespace:                "test-ns",
		Query:                    "",
		ConcurrentActivityCount:  2,
		OverallRps:               10,
		ListWorkflowsPageSize:    2,
		PageCountPerExecution:    10,
		EnableVerification:       true,
		TargetClusterEndpoint:    "test-target",
		HistoryShardCount:        1,
		ShardQuarantineThreshold: 4,
		WFIDQuarantineThreshold:  999,
		NoProgressTimeoutSeconds: 3600,
	})

	s.True(env.IsWorkflowCompleted())
	s.Require().NoError(env.GetWorkflowError())

	envValue, err := env.QueryWorkflow(adaptiveForceReplicationStatusQueryType)
	s.NoError(err)
	var status AdaptiveForceReplicationStatus
	s.NoError(envValue.Get(&status))
	s.Equal(0, status.QuarantinedShardCount)
	s.Equal(0, status.QuarantinedWFIDCount)
}
