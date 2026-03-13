package tests

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
)

// Test_SpeculativeWFTEventsLostAfterSignalMidHistoryPagination demonstrates the
// "premature end of stream" bug in a scenario mimicking SDK workflow cache eviction:
// the SDK uses GetWorkflowExecutionHistory (not PollWorkflowTaskQueue) to replay
// history, fetching page 1 while a speculative WFT is active, then a signal arrives
// before page 2 is fetched.
//
// Root cause:
//
//	GetWorkflowExecutionHistory page 1 sets continuationToken.NextEventId=8 when a
//	speculative WFT (event 8 in memory) exists. The signal triggers
//	convertSpeculativeWorkflowTaskToNormal, committing event 8 (WFT_SCHEDULED) and
//	event 9 (WorkflowExecutionSignaled) to persistence. When page 2 is fetched with
//	the stale token (NextEventId=8), the DB range [6, 8) returns only events 6–7.
//
//	Without the fix:
//	  appendTransientTasks finds no transient events (speculative was committed),
//	  assembled history = events 1..7 (N-2, missing event 8), causing premature EOS.
//
//	With the token-based fix:
//	  The speculative WFT events are stored in the continuation token on page 1.
//	  On the last page, cachedTransientTasks is loaded from the token and validated
//	  against NextEventId=8. WFT_SCHEDULED(8) passes validation and is appended,
//	  giving 8 events total (no premature EOS). Signal(9) was committed after the
//	  original NextEventId=8 boundary and is not included — the SDK will receive it
//	  as part of the pending WFT response.
//
// This test asserts the FIXED behavior.
func Test_SpeculativeWFTEventsLostAfterSignalMidHistoryPagination(t *testing.T) {
	// MaximumPageSize controls the number of DB event batches per page, not individual
	// events. The 7 persisted events are stored in 5 batches:
	//   [1,2] StartWorkflow, [3] WFTStarted, [4,5] WFTCompleted+WFTScheduled,
	//   [6] WFTStarted, [7] WFTCompleted
	// A page size of 3 batches returns events 1..5 on page 1 (batches [1,2]+[3]+[4,5]),
	// leaving batches [6] and [7] for the second page.
	const maxBatchesPerPage = 3

	s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	tv := s.Tv()
	runID := mustStartWorkflow(s, tv)
	wfExecution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID}

	// Build 7 persisted events:
	//   1: WorkflowExecutionStarted
	//   2: WorkflowTaskScheduled
	//   3: WorkflowTaskStarted
	//   4: WorkflowTaskCompleted  (ForceCreateNewWorkflowTask=true)
	//   5: WorkflowTaskScheduled  (force-created)
	//   6: WorkflowTaskStarted
	//   7: WorkflowTaskCompleted
	_, err := s.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				ForceCreateNewWorkflowTask: true,
			}, nil
		})
	s.NoError(err)

	_, err = s.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		})
	s.NoError(err)

	// Send an update to create a speculative WFT (event 8 in memory, scheduled but not polled).
	ctx, cancel := context.WithCancel(testcore.NewContext())
	defer cancel()
	updateCh := sendUpdate(ctx, s, tv)
	defer func() { go func() { <-updateCh }() }()

	// Wait until the speculative WFT is scheduled before fetching page 1.
	// The token-based fix stores the speculative events (WFT_SCHEDULED=8) in the continuation
	// token. We must ensure the speculative WFT is visible in mutable state before page 1 is
	// fetched, so that cachedTransientTasks is populated correctly.
	s.Eventually(func() bool {
		desc, descErr := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: wfExecution,
			})
		return descErr == nil && desc.GetPendingWorkflowTask() != nil
	}, 5*time.Second, 250*time.Millisecond, "speculative WFT should be scheduled after sending update")

	// Fetch page 1 via GetWorkflowExecutionHistory — mimicking what the SDK does when a
	// workflow is evicted from its sticky cache and must replay history from scratch.
	// queryMutableState returns nextEventId=8 (speculative WFT scheduled; speculative events
	// do NOT advance hBuilder.NextEventID). With maxBatchesPerPage=3, only the first 3 DB
	// batches are returned ([1,2]+[3]+[4,5] = events 1..5), leaving batches [6] and [7] for
	// the next page. The continuation token encodes NextEventId=8, PersistenceToken pointing
	// to the next DB batch, AND TransientWorkflowTask with the speculative WFT_SCHEDULED(8).
	histPage1, err := s.FrontendClient().GetWorkflowExecutionHistory(
		testcore.NewContext(),
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.Namespace().String(),
			Execution:       wfExecution,
			MaximumPageSize: maxBatchesPerPage,
		},
	)
	s.NoError(err)
	s.NotNil(histPage1.NextPageToken,
		"NextPageToken must be set: with maxBatchesPerPage=3 and 5 total batches, page 1 must not be the last page")
	t.Logf("NEXTPAGETOKEN: %s", histPage1.NextPageToken)

	firstPageEvents := histPage1.History.Events
	staleNextPageToken := histPage1.NextPageToken

	// Send the signal. Since the speculative WFT exists, signal processing triggers
	// convertSpeculativeWorkflowTaskToNormal, committing both the speculative WFT to DB:
	//   8: WorkflowTaskScheduled  (formerly speculative, now committed)
	//   9: WorkflowExecutionSignaled
	// The token-based fix doesn't need to detect this gap — it returns the pre-stored
	// speculative WFT_SCHEDULED(8) from the token, ending history at event 8.
	_, signalErr := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(),
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: wfExecution,
			SignalName:        tv.Any().String(),
		})
	s.NoError(signalErr)

	// Fetch remaining history pages using the stale token obtained before the signal.
	allEvents := make([]*historypb.HistoryEvent, len(firstPageEvents))
	copy(allEvents, firstPageEvents)
	for nextPageToken := staleNextPageToken; nextPageToken != nil; {
		histResp, histErr := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       s.Namespace().String(),
				Execution:       wfExecution,
				NextPageToken:   nextPageToken,
				MaximumPageSize: maxBatchesPerPage,
			})
		s.NoError(histErr)
		allEvents = append(allEvents, histResp.History.Events...)
		nextPageToken = histResp.NextPageToken
	}

	// With the token-based fix: speculative WFT_SCHEDULED(8) was stored in the continuation
	// token on page 1. On the last page, cachedTransientTasks is loaded from the token and
	// appended after the DB events — 8 events total, no premature EOS.
	//
	// Without the fix: DB range [6, 8) returns only events 6–7; appendTransientTasks finds
	// no transient events (speculative was committed); assembled history = 7 events (N-2),
	// causing premature EOS.
	//
	// Signal(9) is not included — it was committed after the original NextEventId=8 boundary.
	// The SDK will receive it as part of the pending WFT response from PollWorkflowTaskQueue.
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowTaskScheduled`, allEvents)
}
