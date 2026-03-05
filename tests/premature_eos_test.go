package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestPrematureEndOfStream reproduces the "premature end of stream" bug (ACT-536).
//
// Mechanism: speculative WFT metadata is never persisted (RecordWorkflowTaskStarted
// sets updateAction.Noop=true for speculative WFTs). Closing the shard drops the
// in-memory speculative events. When the SDK has a sticky cache miss and calls
// GetWorkflowExecutionHistory, the reopened shard's mutable state has no speculative
// events to append, producing a 2-event gap.
func TestPrematureEndOfStream(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())

	logCap := &logCapture{}
	armed := atomic.Bool{}
	shardClosed := atomic.Bool{}
	wfID := fmt.Sprintf("eos-%s", uuid.NewString()[:8])

	interceptor := grpc.WithUnaryInterceptor(func(
		ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if armed.Load() &&
			strings.HasSuffix(method, "GetWorkflowExecutionHistory") &&
			shardClosed.CompareAndSwap(false, true) {
			env.CloseShard(env.NamespaceID().String(), wfID)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})

	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:          env.GetTestCluster().Host().FrontendGRPCAddress(),
		Namespace:         env.Namespace().String(),
		Logger:            logCap,
		ConnectionOptions: sdkclient.ConnectionOptions{DialOptions: []grpc.DialOption{interceptor}},
	})
	require.NoError(t, err)
	defer client.Close()

	tq := fmt.Sprintf("eos-%s", uuid.NewString()[:8])
	w := sdkworker.New(client, tq, sdkworker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			_ = workflow.SetUpdateHandler(ctx, "u", func(ctx workflow.Context) error { return nil })
			workflow.GetSignalChannel(ctx, "done").Receive(ctx, nil)
			return nil
		},
		workflow.RegisterOptions{Name: "wf"},
	)
	require.NoError(t, w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: wfID, TaskQueue: tq, WorkflowRunTimeout: 60 * time.Second,
	}, "wf")
	require.NoError(t, err)

	// Wait for first WFT to complete (establishes sticky binding).
	require.Eventually(t, func() bool {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx,
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run.GetRunID()},
			})
		return err == nil && resp.GetWorkflowExecutionInfo().GetHistoryLength() >= 4
	}, 5*time.Second, 50*time.Millisecond)

	// Purge SDK sticky cache → next WFT on sticky queue will be a cache miss.
	sdkworker.PurgeStickyWorkflowCache()
	armed.Store(true)

	// Send Update → speculative WFT on sticky queue → SDK cache miss →
	// GetWorkflowExecutionHistory. The interceptor closes the shard before the
	// RPC, so speculative events are lost.
	handle, err := client.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID: wfID, RunID: run.GetRunID(),
		UpdateName: "u", WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		_ = handle.Get(ctx, nil)
	}

	t.Log(strings.Join(logCap.messages, "\n"))

	require.Eventually(t, func() bool {
		return logCap.contains("CONFIRMING THAT SEANS CHANGES ARE ACTUALLY RUNNING")
	}, 10*time.Second, 100*time.Millisecond,
		"expected SDK to log 'premature end of stream'")

	// require.Eventually(t, func() bool {
	// 	return logCap.contains("premature end of stream")
	// }, 10*time.Second, 100*time.Millisecond,
	// 	"expected SDK to log 'premature end of stream'")

	_ = client.TerminateWorkflow(ctx, wfID, run.GetRunID(), "cleanup")
}

// TestTransientWFT_ContextClear_CausesHistoryGap reproduces the transient-WFT variant of the
// premature end-of-stream bug.
//
// Mechanism: a transient WFT (attempt > 1) is created when a WFT fails. Its WFT_SCHEDULED and
// WFT_STARTED events are not persisted — they live only in mutable state. If the shard is closed
// while the transient WFT is in Started state, workflowContext.Clear() drops that in-memory state.
// On shard reload, execution info records attempt=2 (persisted), so GetWorkflowExecutionHistory
// can reconstruct the WFT_SCHEDULED transient event — but WFT_STARTED is permanently lost.
// A worker holding a token with NextEventId=7 then receives history ending at event 6: premature EOS.
//
// This test fails today (only 5 events returned after CloseShard) and passes after the fix (6 events).
func TestTransientWFT_ContextClear_CausesHistoryGap(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wfID := fmt.Sprintf("transient-eos-%s", uuid.NewString()[:8])
	tq := fmt.Sprintf("tq-%s", uuid.NewString()[:8])
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	identity := "test-worker"

	// Step 1: Start a plain workflow.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          env.Namespace().String(),
		RequestId:          uuid.NewString(),
		WorkflowId:         wfID,
		WorkflowType:       &commonpb.WorkflowType{Name: "transient-eos-wf"},
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: durationpb.New(120 * time.Second),
		Identity:           identity,
	})
	require.NoError(t, err)
	runID := startResp.RunId
	we := &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID}

	// Step 2: Poll initial WFT (attempt=1).
	poll1, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	require.NoError(t, err)
	require.NotEmpty(t, poll1.TaskToken, "expected to poll the initial WFT")

	// Step 3: Fail the initial WFT → server schedules a transient WFT (attempt=2).
	_, err = env.FrontendClient().RespondWorkflowTaskFailed(ctx, &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: poll1.TaskToken,
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		Identity:  identity,
	})
	require.NoError(t, err)

	// Step 4: Poll the transient WFT (attempt=2) — leaves it in Started state.
	// Mutable state now has 2 transient events: WFT_SCHEDULED(2) + WFT_STARTED(2).
	var poll2 *workflowservice.PollWorkflowTaskQueueResponse
	require.Eventually(t, func() bool {
		poll2, err = env.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  identity,
		})
		return err == nil && len(poll2.TaskToken) > 0
	}, 10*time.Second, 100*time.Millisecond, "expected to poll transient WFT (attempt=2)")

	// Step 5: GetHistory before CloseShard — expect 6 events:
	//   4 persisted: WorkflowExecutionStarted, WFT_SCHEDULED, WFT_STARTED, WFT_FAILED
	//   2 transient:  WFT_SCHEDULED(attempt=2), WFT_STARTED(attempt=2)
	histBefore, err := env.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       env.Namespace().String(),
		Execution:       we,
		MaximumPageSize: 1000,
	})
	require.NoError(t, err)
	eventsBefore := histBefore.History.Events
	require.Equal(t, 6, len(eventsBefore),
		"expected 6 events (4 persisted + 2 transient) before CloseShard")
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, eventsBefore[len(eventsBefore)-1].EventType,
		"last event before CloseShard should be transient WFT_STARTED")

	// Step 6: Close the shard — workflowContext.Clear() drops in-memory transient state.
	env.CloseShard(env.NamespaceID().String(), wfID)

	// Step 7: GetHistory after CloseShard — should still return 6 events.
	// Bug: WFT_STARTED(attempt=2) is lost after shard reload; only 5 events returned
	// (WFT_SCHEDULED is reconstructed from exec info, WFT_STARTED is permanently lost).
	// This assertion fails today and passes after the fix.
	histAfter, err := env.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       env.Namespace().String(),
		Execution:       we,
		MaximumPageSize: 1000,
	})
	require.NoError(t, err)
	eventsAfter := histAfter.History.Events
	require.Equal(t, 6, len(eventsAfter),
		"expected 6 events after CloseShard (4 persisted + 2 transient: WFT_SCHEDULED + WFT_STARTED); "+
			"got %d — WFT_STARTED(attempt=2) is lost after shard reload (premature EOS bug)",
		len(eventsAfter))
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, eventsAfter[len(eventsAfter)-1].EventType,
		"last event after CloseShard should be transient WFT_STARTED (attempt=2)")
}

// TestConsecutiveWorkflowTasksViaForceCreate demonstrates the history pattern
//
//	WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskCompleted,
//	WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskCompleted
//
// which occurs when RespondWorkflowTaskCompleted is called with
// ForceCreateNewWorkflowTask=true. The server immediately schedules a new WFT
// without any intervening events (no activities, signals, or timers).
//
// In practice the Go SDK does this when executing local activities: it
// "heartbeats" the current WFT to extend its timeout while local activities
// run, producing consecutive WFT cycles.
func TestConsecutiveWorkflowTasksViaForceCreate(t *testing.T) {
	env := testcore.NewEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := fmt.Sprintf("consec-wft-%s", uuid.NewString()[:8])
	tq := fmt.Sprintf("tq-%s", uuid.NewString()[:8])
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start a workflow.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:          env.Namespace().String(),
		RequestId:          uuid.NewString(),
		WorkflowId:         wfID,
		WorkflowType:       &commonpb.WorkflowType{Name: "test-workflow"},
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: durationpb.New(60 * time.Second),
	})
	require.NoError(t, err)

	we := &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: startResp.RunId}

	// Poll and start the first WFT.
	poll1, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
	})
	require.NoError(t, err)
	require.NotEmpty(t, poll1.TaskToken)

	// Complete the first WFT with ForceCreateNewWorkflowTask=true.
	// The server immediately schedules a second WFT with no intervening events,
	// and ReturnNewWorkflowTask=true delivers its token in-line.
	resp, err := env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:                  env.Namespace().String(),
		TaskToken:                  poll1.TaskToken,
		ForceCreateNewWorkflowTask: true,
		ReturnNewWorkflowTask:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.WorkflowTask)

	// Complete the second WFT, finishing the workflow.
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: resp.WorkflowTask.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	require.NoError(t, err)

	// Events 2-7 are the two consecutive WFT cycles with nothing in between.
	env.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionCompleted`, env.GetHistory(env.Namespace().String(), we))
}

type logCapture struct {
	mu       sync.Mutex
	messages []string
}

var _ sdklog.Logger = (*logCapture)(nil)

func (l *logCapture) Debug(string, ...interface{}) {}
func (l *logCapture) Info(string, ...interface{})  {}
func (l *logCapture) Warn(msg string, kv ...interface{}) {
	l.mu.Lock()
	l.messages = append(l.messages, fmt.Sprintf("%s %v", msg, kv))
	l.mu.Unlock()
}
func (l *logCapture) Error(msg string, kv ...interface{}) {
	l.mu.Lock()
	l.messages = append(l.messages, fmt.Sprintf("%s %v", msg, kv))
	l.mu.Unlock()
}
func (l *logCapture) With(...interface{}) sdklog.Logger { return l }
func (l *logCapture) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.messages {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}

func Test_PrathyushRepro(t *testing.T) {
	t.Run("SpeculativeWFTEventsLostAfterShardReloadMidHistoryPagination", func(t *testing.T) {
		// This test reproduces a race condition that causes SDK workers to fail with
		// "premature end of stream" after server restart or shard rebalancing,
		// when a workflow update creates a speculative WFT.
		//
		// Root cause:
		//   When a speculative WFT is active, RecordWorkflowTaskStarted creates a
		//   continuation token with NextEventId = speculative_scheduled_event_id (e.g., 8).
		//   The speculative WFT events (WFT_SCHEDULED, WFT_STARTED) exist only in memory.
		//   If the shard reloads between page fetches, the in-memory update/WFT state is lost.
		//   appendTransientTasks then finds no transient events and returns nothing.
		//   The worker assembles incomplete history and fails with "premature end of stream".
		//
		// Scenario:
		//   1. Workflow has 7 persisted events (requires 2 pages with HistoryMaxPageSize=5)
		//   2. An update creates a speculative WFT at event 8 (in memory only)
		//   3. Worker polls the WFT → first page (events 1..5) returned with NextPageToken
		//      (NextEventId=8 in the token, pointing to in-memory speculative events)
		//   4. Shard reloads → update and speculative WFT state lost
		//   5. Worker fetches next page → gets events 6..7, no speculative events appended
		//   6. Assembled history: events 1..7, but poll said StartedEventId=9 → "premature end of stream"
		s := testcore.NewEnv(t,
			testcore.WithDedicatedCluster(),
			testcore.WithDynamicConfig(dynamicconfig.HistoryMaxPageSize, 5))
		tv := s.Tv()
		mustStartWorkflow(s, tv)

		// Build 7 events in persistence so the speculative WFT poll response requires pagination
		// (HistoryMaxPageSize=5 → first page has events 1..5, second page has 6..7):
		//   1: WorkflowExecutionStarted
		//   2: WorkflowTaskScheduled
		//   3: WorkflowTaskStarted
		//   4: WorkflowTaskCompleted  (ForceCreateNewWorkflowTask=true)
		//   5: WorkflowTaskScheduled  (force-created)
		//   6: WorkflowTaskStarted
		//   7: WorkflowTaskCompleted

		// WFT1: complete with ForceCreateNewWorkflowTask to schedule a second WFT.
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					ForceCreateNewWorkflowTask: true,
				}, nil
			})
		s.NoError(err)

		// WFT2: complete normally.
		_, err = s.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
			})
		s.NoError(err)

		// Send an update to create a speculative WFT (events 8, 9 in memory only).
		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()
		updateCh := sendUpdate(ctx, s, tv)
		defer func() { go func() { <-updateCh }() }()

		// Poll the speculative WFT directly to get the raw NextPageToken before shard reload.
		// RecordWorkflowTaskStarted adds WFT_STARTED(9) in memory.
		// The poll response first page contains events 1..5; NextPageToken points to NextEventId=8.
		pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(),
			&workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: s.Namespace().String(),
				TaskQueue: tv.TaskQueue(),
			})
		s.NoError(err)
		s.NotEmpty(pollResp.GetTaskToken(), "expected a speculative workflow task to be available")

		// With 7 persisted events and HistoryMaxPageSize=5, the first page has events 1..5
		// and NextPageToken must be set to continue fetching events 6..7 and speculative 8..9.
		s.NotNil(pollResp.NextPageToken,
			"expected pagination: 7 persisted events with HistoryMaxPageSize=5 requires 2 pages")

		// Speculative WFT_SCHEDULED=8, WFT_STARTED=9.
		s.EqualValues(9, pollResp.StartedEventId)

		wfExecution := pollResp.WorkflowExecution
		firstPageEvents := pollResp.History.Events
		staleNextPageToken := pollResp.NextPageToken

		// Simulate shard reload between history page fetches (e.g., server restart or rebalancing).
		// ShardFinalizerTimeout=0 prevents update registry persistence so the update and its
		// in-memory speculative WFT are truly lost after reload.
		loseUpdateRegistryAndAbandonPendingUpdates(s, tv)

		// Simulate what a restarting SDK worker does: fetch remaining history pages using the
		// stale NextPageToken obtained before the shard reload.
		allEvents := make([]*historypb.HistoryEvent, len(firstPageEvents))
		copy(allEvents, firstPageEvents)
		for nextPageToken := staleNextPageToken; nextPageToken != nil; {
			histResp, histErr := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(),
				&workflowservice.GetWorkflowExecutionHistoryRequest{
					Namespace:     s.Namespace().String(),
					Execution:     wfExecution,
					NextPageToken: nextPageToken,
				})
			s.NoError(histErr)
			allEvents = append(allEvents, histResp.History.Events...)
			nextPageToken = histResp.NextPageToken
		}

		// BUG: The assembled history is missing speculative events 8 (WFT_SCHEDULED) and
		// 9 (WFT_STARTED) because appendTransientTasks found no transient events after the
		// shard reload. A real SDK worker would fail with:
		// "premature end of stream: expectedLastEventID=9 but no more events after eventID=7"
		s.Len(allEvents, 7,
			"assembled history should have 7 events; speculative WFT events 8 and 9 are missing after shard reload")
		lastEventID := allEvents[len(allEvents)-1].GetEventId()
		s.Greater(pollResp.StartedEventId, lastEventID,
			"poll StartedEventId > last assembled eventID confirms the premature-end-of-stream bug")
	},
	)
}
