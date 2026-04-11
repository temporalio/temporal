package tests

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type PrematureEosTestSuite struct {
	parallelsuite.Suite[*PrematureEosTestSuite]
}

func TestPrematureEosTestSuite(t *testing.T) {
	parallelsuite.Run(t, &PrematureEosTestSuite{})
}

// Test_SpeculativeWFTEventsLostAfterSignalMidHistoryPagination demonstrates the
// "premature end of stream" bug in a scenario mimicking SDK workflow cache eviction:
// the SDK uses GetWorkflowExecutionHistory (not PollWorkflowTaskQueue) to replay
// history, fetching page 1 while a speculative WFT is active, then a signal arrives
// before page 2 is fetched.
//
// Root cause (same underlying bug as the shard-reload variant):
//
//	GetWorkflowExecutionHistory page 1 sets continuationToken.NextEventId=8 when a
//	speculative WFT (event 8 in memory) exists. The signal triggers
//	convertSpeculativeWorkflowTaskToNormal, committing event 8 (WFT_SCHEDULED) and
//	event 9 (WorkflowExecutionSignaled) to persistence. When page 2 is fetched with
//	the stale token (NextEventId=8), the DB range [6, 8) returns only events 6–7.
//
//	Without the gap-detection fix in GetWorkflowExecutionHistory:
//	  appendTransientTasks finds no transient events (speculative was committed),
//	  assembled history = events 1..7 (N-2, missing events 8 and 9), causing premature EOS.
//
//	With the gap-detection fix:
//	  freshNextEventId (10) > continuationToken.NextEventId (8) → gap fetch [8, 10)
//	  returns events 8 and 9; assembled history has 9 events (no premature EOS).
//
// This test asserts the FIXED behavior.
func (s *PrematureEosTestSuite) Test_SpeculativeWFTEventsLostAfterSignalMidHistoryPagination() {
	// MaximumPageSize controls the number of DB event batches per page, not individual
	// events. The 7 persisted events are stored in 5 batches:
	//   [1,2] StartWorkflow, [3] WFTStarted, [4,5] WFTCompleted+WFTScheduled,
	//   [6] WFTStarted, [7] WFTCompleted
	// A page size of 3 batches returns events 1..5 on page 1 (batches [1,2]+[3]+[4,5]),
	// leaving batches [6] and [7] for the second page.
	const maxBatchesPerPage = 3

	env := testcore.NewEnv(s.T(), testcore.WithDedicatedCluster())
	tv := env.Tv()
	runID := mustStartWorkflow(env, tv)
	wfExecution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID}

	// Build 7 persisted events:
	//   1: WorkflowExecutionStarted
	//   2: WorkflowTaskScheduled
	//   3: WorkflowTaskStarted
	//   4: WorkflowTaskCompleted  (ForceCreateNewWorkflowTask=true)
	//   5: WorkflowTaskScheduled  (force-created)
	//   6: WorkflowTaskStarted
	//   7: WorkflowTaskCompleted
	_, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				ForceCreateNewWorkflowTask: true,
			}, nil
		})
	s.NoError(err)

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		})
	s.NoError(err)

	// Send an update to create a speculative WFT (event 8 in memory, scheduled but not polled).
	ctx, cancel := context.WithCancel(testcore.NewContext())
	defer cancel()
	updateCh := sendUpdate(ctx, env, tv)
	defer func() { go func() { <-updateCh }() }()

	// Wait until the speculative WFT is scheduled before fetching page 1.
	// This ensures the signal (sent later) arrives while the speculative WFT exists in
	// mutable state, so convertSpeculativeWorkflowTaskToNormal commits both event 8
	// (WFT_SCHEDULED) and event 9 (WorkflowExecutionSignaled), giving freshNextEventId=10.
	// Without this wait there is a race: if the update hasn't been processed yet, the signal
	// would only add event 8 (SignalReceived) with freshNextEventId=9, producing 8 events
	// instead of the expected 9 and causing a false test failure.
	s.Eventually(func() bool {
		desc, descErr := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: wfExecution,
			})
		return descErr == nil && desc.GetPendingWorkflowTask() != nil
	}, 5*time.Second, 250*time.Millisecond, "speculative WFT should be scheduled after sending update")

	// Fetch page 1 via GetWorkflowExecutionHistory — mimicking what the SDK does when a
	// workflow is evicted from its sticky cache and must replay history from scratch.
	// queryMutableState returns nextEventId=8 (speculative WFT scheduled; speculative events
	// do NOT advance hBuilder.NextEventID). With maxBatchesPerPage=3, only the first 3 DB
	// batches are returned ([1,2]+[3]+[4,5] = events 1..5), leaving batches [6] and [7] for
	// the next page. The continuation token encodes NextEventId=8 and PersistenceToken
	// pointing to the next DB batch — this is the "stale token" that exercises the bug.
	histPage1, err := env.FrontendClient().GetWorkflowExecutionHistory(
		testcore.NewContext(),
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       env.Namespace().String(),
			Execution:       wfExecution,
			MaximumPageSize: maxBatchesPerPage,
		},
	)
	s.NoError(err)
	s.NotNil(histPage1.NextPageToken,
		"NextPageToken must be set: with maxBatchesPerPage=3 and 5 total batches, page 1 must not be the last page")
	s.T().Logf("NEXTPAGETOKEN: %s", histPage1.NextPageToken)

	firstPageEvents := histPage1.History.Events
	staleNextPageToken := histPage1.NextPageToken

	// Send the signal. Since the speculative WFT was cleared by the shard reload, signal
	// processing finds the pending update in the registry and schedules a normal WFT:
	//   8: WorkflowTaskScheduled  (normal WFT scheduled to handle the pending update)
	//   9: WorkflowExecutionSignaled  (flushed immediately: HasStartedWorkflowTask=false)
	// After this transaction, freshNextEventId=10.
	_, signalErr := env.FrontendClient().SignalWorkflowExecution(testcore.NewContext(),
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: wfExecution,
			SignalName:        tv.Any().String(),
		})
	s.NoError(signalErr)

	// Fetch remaining history pages using the stale token obtained before the signal.
	allEvents := make([]*historypb.HistoryEvent, len(firstPageEvents))
	copy(allEvents, firstPageEvents)
	for nextPageToken := staleNextPageToken; nextPageToken != nil; {
		histResp, histErr := env.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       env.Namespace().String(),
				Execution:       wfExecution,
				NextPageToken:   nextPageToken,
				MaximumPageSize: maxBatchesPerPage,
			})
		s.NoError(histErr)
		allEvents = append(allEvents, histResp.History.Events...)
		nextPageToken = histResp.NextPageToken
	}

	// With the gap-detection fix: freshNextEventId (10) > stale NextEventId (8), so the
	// gap [8..10) is fetched from DB (events 8 and 9 are now persisted after the signal).
	// 9 events are assembled correctly — no premature EOS.
	//
	// Without the fix: DB range [6, 8) returns only events 6–7; appendTransientTasks finds
	// no transient events (speculative was committed); assembled history = 7 events (N-2),
	// causing premature EOS.
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowExecutionSignaled`, allEvents)
}
