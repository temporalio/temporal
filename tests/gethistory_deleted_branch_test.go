package tests

import (
	"testing"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type GetHistoryDeletedBranchSuite struct {
	parallelsuite.Suite[*GetHistoryDeletedBranchSuite]
}

func TestGetHistoryDeletedBranchSuite(t *testing.T) {
	parallelsuite.Run(t, &GetHistoryDeletedBranchSuite{})
}

// TestPaginationOverDeletedBranch pins down what a client sees when the history branch its page
// token points at is deleted mid-pagination. The page token carries the branch token captured on
// the first page (tokenspb.HistoryContinuation.branch_token) and is never re-derived from mutable
// state, so the resumed read targets a branch that no longer exists.
//
// Today that read is indistinguishable from reaching the end of history: no error, no events, no
// next page token. The NotFound assertion at the end is the contrast — reading the same deleted
// branch without a page token does report the loss, because the guard in
// executionManagerImpl.readRawHistoryBranchAndFilter only fires on the first page.
func (s *GetHistoryDeletedBranchSuite) TestPaginationOverDeletedBranch() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	ctx := s.Context()

	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		tv.WorkflowID(),
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	execMgr := env.GetTestCluster().TestBase().ExecutionManager
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	})
	s.NoError(err)
	runID := startResp.RunId
	execution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID}

	completeWorkflowWithActivities(env, tv, poller)

	totalEvents := s.countEvents(env, execution)
	s.Greater(totalEvents, 2, "workflow should have more events than the first page returns")

	firstPage, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       env.Namespace().String(),
		Execution:       execution,
		MaximumPageSize: 1,
	})
	s.NoError(err)
	s.NotEmpty(firstPage.NextPageToken, "expected the client to be left mid-pagination")
	s.Less(len(firstPage.History.GetEvents()), totalEvents)

	branchToken := captureCurrentBranchToken(ctx, env, tv.WorkflowID(), runID)
	s.NoError(execMgr.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
	}))

	nextPage, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       env.Namespace().String(),
		Execution:       execution,
		MaximumPageSize: 1,
		NextPageToken:   firstPage.NextPageToken,
	})
	s.NoError(err, "resuming pagination over a deleted branch currently succeeds")
	s.Empty(nextPage.History.GetEvents(), "no events remain to be read")
	s.Empty(nextPage.NextPageToken, "pagination ends, so the client sees a truncated history as complete")

	_, err = env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       env.Namespace().String(),
		Execution:       execution,
		MaximumPageSize: 1,
	})
	s.ErrorAs(err, new(*serviceerror.NotFound), "a first-page read of the same branch does report the loss")
}

func (s *GetHistoryDeletedBranchSuite) countEvents(
	env *testcore.TestEnv,
	execution *commonpb.WorkflowExecution,
) int {
	ctx := s.Context()
	total := 0
	var token []byte
	for {
		resp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       env.Namespace().String(),
			Execution:       execution,
			MaximumPageSize: 100,
			NextPageToken:   token,
		})
		s.NoError(err)
		total += len(resp.History.GetEvents())
		token = resp.NextPageToken
		if len(token) == 0 {
			return total
		}
	}
}
