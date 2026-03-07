package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TransientWFTHistorySuite struct {
	testcore.FunctionalTestBase
}

func TestTransientWFTHistorySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TransientWFTHistorySuite))
}

// newCLIContext creates a context with client-name set to temporal-cli.
// Must NOT be based on testcore.NewContext() because AppendToOutgoingContext would produce
// two client-name values and the first (temporal-server) would take precedence.
func (s *TransientWFTHistorySuite) newCLIContext() context.Context {
	ctx, cancel := context.WithTimeout(
		metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
			headers.ClientNameHeaderName, headers.ClientNameCLI,
			headers.ClientVersionHeaderName, "1.0.0",
			headers.SupportedServerVersionsHeaderName, headers.SupportedServerVersions,
		)),
		90*time.Second*debug.TimeoutMultiplier,
	)
	// Cancel is intentionally leaked here for test simplicity; the timeout will clean up.
	_ = cancel
	return ctx
}

// newUIContext creates a context with client-name set to temporal-ui.
func (s *TransientWFTHistorySuite) newUIContext() context.Context {
	ctx, cancel := context.WithTimeout(
		metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
			headers.ClientNameHeaderName, headers.ClientNameUI,
			headers.ClientVersionHeaderName, "2.0.0",
			headers.SupportedServerVersionsHeaderName, headers.SupportedServerVersions,
		)),
		90*time.Second*debug.TimeoutMultiplier,
	)
	_ = cancel
	return ctx
}

// createWorkflowAndFailFirstWFT starts a workflow, polls and fails the first WFT.
// After this call, a transient WFT is Scheduled (attempt=2) but not yet polled.
// Returns the workflowID and runID.
func (s *TransientWFTHistorySuite) createWorkflowAndFailFirstWFT(identity, taskQueueName string) (string, string) {
	workflowID := "functional-transient-wft-history-" + uuid.NewString()

	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "transient-wft-history-test-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)
	runID := startResp.RunId

	// Poll the initial WFT
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	// Fail the initial WFT — this creates a transient WFT (attempt=2) in Scheduled state
	_, err = s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		Identity:  identity,
	})
	s.NoError(err)

	return workflowID, runID
}

// TestGetHistory_TransientWFTStarted_IncludesScheduledAndStartedEvents is the core premature-EOS scenario.
// After the first WFT fails, we poll the transient WFT (putting it in Started state) but don't respond.
// GetWorkflowExecutionHistory should append both WFT_SCHEDULED and WFT_STARTED transient events.
func (s *TransientWFTHistorySuite) TestGetHistory_TransientWFTStarted_IncludesScheduledAndStartedEvents() {
	identity := "worker1"
	taskQueueName := "functional-transient-wft-history-tq-" + uuid.NewString()

	workflowID, runID := s.createWorkflowAndFailFirstWFT(identity, taskQueueName)

	// Poll the transient WFT but do NOT respond — WFT is now in Started state.
	// Mutable state will have 2 transient events: WFT_SCHEDULED + WFT_STARTED.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken, "expected to poll a transient WFT")

	// Get history — default context sets client-name=temporal-server, which receives transient events
	resp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	s.Nil(resp.NextPageToken, "expected single page")

	events := resp.History.Events
	s.GreaterOrEqual(len(events), 2)

	lastEvent := events[len(events)-1]
	secondLastEvent := events[len(events)-2]

	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, lastEvent.EventType,
		"last event should be WFT_STARTED (transient)")
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, secondLastEvent.EventType,
		"second-to-last event should be WFT_SCHEDULED (transient)")
	s.Equal(secondLastEvent.EventId+1, lastEvent.EventId,
		"transient events must have consecutive IDs")
}

// TestGetHistory_TransientWFTScheduled_IncludesScheduledEventOnly tests that when a transient
// WFT is Scheduled but not yet polled, GetHistory returns exactly 1 transient event (WFT_SCHEDULED).
func (s *TransientWFTHistorySuite) TestGetHistory_TransientWFTScheduled_IncludesScheduledEventOnly() {
	identity := "worker1"
	taskQueueName := "functional-transient-wft-history-tq-" + uuid.NewString()

	workflowID, runID := s.createWorkflowAndFailFirstWFT(identity, taskQueueName)
	// Transient WFT is Scheduled but NOT polled — only 1 transient event in mutable state.

	resp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	s.Nil(resp.NextPageToken, "expected single page")

	events := resp.History.Events
	s.GreaterOrEqual(len(events), 1)

	lastEvent := events[len(events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, lastEvent.EventType,
		"last event should be the transient WFT_SCHEDULED")

	// The second-to-last event must not be WFT_STARTED (only 1 transient event when not polled)
	if len(events) >= 2 {
		secondLast := events[len(events)-2]
		s.NotEqual(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, secondLast.EventType,
			"second-to-last event should NOT be WFT_STARTED when WFT is only Scheduled")
	}
}

// TestGetHistory_CLIClient_NoTransientEventsAppended verifies backward compat:
// CLI clients (temporal-cli) must not receive transient events appended to history.
func (s *TransientWFTHistorySuite) TestGetHistory_CLIClient_NoTransientEventsAppended() {
	identity := "worker1"
	taskQueueName := "functional-transient-wft-history-tq-" + uuid.NewString()

	workflowID, runID := s.createWorkflowAndFailFirstWFT(identity, taskQueueName)

	// Poll transient WFT (Started state) — 2 transient events in mutable state
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	// Get history with CLI context — should NOT include transient events
	cliResp, err := s.FrontendClient().GetWorkflowExecutionHistory(s.newCLIContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	cliEvents := cliResp.History.Events

	// Get history with server context — should include transient events
	serverResp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	serverEvents := serverResp.History.Events

	// Server response ends with transient WFT_STARTED
	s.GreaterOrEqual(len(serverEvents), 1)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, serverEvents[len(serverEvents)-1].EventType,
		"temporal-server client should receive transient WFT_STARTED event")

	// CLI response must not end with WFT_STARTED
	if len(cliEvents) > 0 {
		s.NotEqual(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, cliEvents[len(cliEvents)-1].EventType,
			"CLI client should not receive transient WFT_STARTED event")
	}

	// CLI should have fewer events than server (missing the 2 transient events)
	s.Less(len(cliEvents), len(serverEvents),
		"CLI response should have fewer events than server response (no transient events)")
}

// TestGetHistory_UIClient_NoTransientEventsAppended verifies backward compat:
// UI clients (temporal-ui) must not receive transient events appended to history.
func (s *TransientWFTHistorySuite) TestGetHistory_UIClient_NoTransientEventsAppended() {
	identity := "worker1"
	taskQueueName := "functional-transient-wft-history-tq-" + uuid.NewString()

	workflowID, runID := s.createWorkflowAndFailFirstWFT(identity, taskQueueName)

	// Poll transient WFT (Started state) — 2 transient events in mutable state
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	// Get history with UI context — should NOT include transient events
	uiResp, err := s.FrontendClient().GetWorkflowExecutionHistory(s.newUIContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	uiEvents := uiResp.History.Events

	// Get history with server context — should include transient events
	serverResp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	serverEvents := serverResp.History.Events

	// Server response ends with transient WFT_STARTED
	s.GreaterOrEqual(len(serverEvents), 1)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, serverEvents[len(serverEvents)-1].EventType,
		"temporal-server client should receive transient WFT_STARTED event")

	// UI response must not end with WFT_STARTED
	if len(uiEvents) > 0 {
		s.NotEqual(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, uiEvents[len(uiEvents)-1].EventType,
			"UI client should not receive transient WFT_STARTED event")
	}

	// UI should have fewer events than server (missing the 2 transient events)
	s.Less(len(uiEvents), len(serverEvents),
		"UI response should have fewer events than server response (no transient events)")
}

// TestGetHistory_PaginatedRequest_TransientEventsOnlyOnLastPage verifies that transient
// events appear only on the final page of paginated history responses.
func (s *TransientWFTHistorySuite) TestGetHistory_PaginatedRequest_TransientEventsOnlyOnLastPage() {
	identity := "worker1"
	taskQueueName := "functional-transient-wft-history-tq-" + uuid.NewString()

	workflowID, runID := s.createWorkflowAndFailFirstWFT(identity, taskQueueName)

	// Poll transient WFT (Started state) — 2 transient events in mutable state
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	// Paginate with very small page size to force multiple pages
	var allPages []*workflowservice.GetWorkflowExecutionHistoryResponse
	var nextPageToken []byte
	for {
		resp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			MaximumPageSize: 2, // force multiple pages
			NextPageToken:   nextPageToken,
		})
		s.NoError(err)
		allPages = append(allPages, resp)
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
	}

	// We must have gotten multiple pages
	s.GreaterOrEqual(len(allPages), 2, "expected at least 2 pages with MaximumPageSize=2")

	// Concatenate all events across pages
	var combined []*historypb.HistoryEvent
	for _, pg := range allPages {
		combined = append(combined, pg.History.Events...)
	}
	s.GreaterOrEqual(len(combined), 2)

	// Non-final pages must have a continuation token (transient events are not in intermediate pages'
	// persistence token, so pagination won't end prematurely).
	for i, pg := range allPages[:len(allPages)-1] {
		s.NotEmpty(pg.NextPageToken, "non-final page %d must have a next page token", i)
	}

	// Final page has no continuation token
	finalPage := allPages[len(allPages)-1]
	s.Empty(finalPage.NextPageToken, "final page must have no next page token")

	// The last 2 events across ALL pages should be the transient WFT_SCHEDULED + WFT_STARTED.
	// Transient events are only appended on the final page (when PersistenceToken is empty).
	lastEv := combined[len(combined)-1]
	secondLastEv := combined[len(combined)-2]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, lastEv.EventType,
		"last event across all pages should be transient WFT_STARTED")
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, secondLastEv.EventType,
		"second-to-last event across all pages should be transient WFT_SCHEDULED")
	s.Equal(secondLastEv.EventId+1, lastEv.EventId,
		"transient events must have consecutive IDs")
}

// TestGetHistory_CompletedWFT_NoExtraTransientEvents verifies that when no transient WFT exists
// (workflow has completed normally), GetHistory returns no spurious transient events.
func (s *TransientWFTHistorySuite) TestGetHistory_CompletedWFT_NoExtraTransientEvents() {
	identity := "worker1"
	taskQueueName := "functional-transient-wft-history-tq-" + uuid.NewString()
	workflowID := "functional-transient-wft-history-completed-" + uuid.NewString()

	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "transient-wft-history-completed-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)
	runID := startResp.RunId

	// Poll and complete the initial WFT normally (complete workflow execution)
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
		Identity: identity,
	})
	s.NoError(err)

	// Get history — should end with WFT_COMPLETED and WF_EXECUTION_COMPLETED, no transient events
	resp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	events := resp.History.Events
	s.NotEmpty(events)

	lastEvent := events[len(events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.EventType,
		"last event should be WF_EXECUTION_COMPLETED, not a stray transient event")

	// Second-to-last event should be WFT_COMPLETED (not a transient WFT_SCHEDULED)
	s.GreaterOrEqual(len(events), 4, "expected at minimum: WFT_SCHED, WFT_START, WFT_COMP, WF_COMP")
	secondLast := events[len(events)-2]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, secondLast.EventType,
		"second-to-last event should be WFT_COMPLETED, not a spurious transient event")
}

// TestGetHistory_SpeculativeWFTFromUpdate_IncludesEvents mirrors the real production scenario from
// the premature-EOS bug report:
//
//	2157: Update Accepted
//	2158: Local Activity
//	2159: Update Completed
//	2160: WorkflowTaskScheduled  ← speculative, not yet persisted
//	2161: WorkflowTaskStarted    ← speculative, not yet persisted
//
// When an SDK worker evicts its cache and calls GetWorkflowExecutionHistory to rebuild state, it
// must receive the speculative WFT events (2160–2161) or it will report a premature EOS error.
func (s *TransientWFTHistorySuite) TestGetHistory_SpeculativeWFTFromUpdate_IncludesEvents() {
	identity := "worker1"
	taskQueueName := "functional-transient-wft-history-tq-" + uuid.NewString()
	workflowID := "functional-speculative-wft-history-" + uuid.NewString()
	updateID := uuid.NewString()

	// Start workflow
	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "speculative-wft-history-test-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)
	runID := startResp.RunId

	// Complete initial WFT with a timer command so the workflow stays alive and can receive updates
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{
				StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer1",
					StartToFireTimeout: durationpb.New(60 * time.Second),
				},
			},
		}},
		Identity: identity,
	})
	s.NoError(err)

	// Send an update in a goroutine — the server will create a speculative WFT to deliver the update.
	// We use a goroutine because the UpdateWorkflowExecution call blocks until the update is processed
	// (or the context times out), and we want to poll the WFT without processing it.
	go func() {
		_, _ = s.FrontendClient().UpdateWorkflowExecution(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
			},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: updateID},
				Input: &updatepb.Input{Name: "my-update"},
			},
		})
	}()

	// Wait until the update is admitted (speculative WFT has been created in mutable state)
	s.EventuallyWithT(func(c *assert.CollectT) {
		pollUpdateResp, pollUpdateErr := s.FrontendClient().PollWorkflowExecutionUpdate(testcore.NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace: s.Namespace().String(),
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				UpdateId: updateID,
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
			},
		})
		assert.NoError(c, pollUpdateErr)
		assert.GreaterOrEqual(c,
			int(pollUpdateResp.GetStage()),
			int(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED),
			"update must reach admitted stage before polling WFT")
	}, 10*time.Second, 50*time.Millisecond)

	// Poll the speculative WFT but do NOT respond — WFT is in Started state.
	// Mutable state has 2 speculative events: WFT_SCHEDULED + WFT_STARTED.
	wftPollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(wftPollResp.TaskToken, "expected to poll a speculative WFT")

	// GetWorkflowExecutionHistory should include the 2 speculative WFT events at the end.
	// This is the exact code path that was broken before the fix in appendTransientTasks.
	resp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		MaximumPageSize: 1000,
	})
	s.NoError(err)
	s.Nil(resp.NextPageToken, "expected single page")

	events := resp.History.Events
	s.GreaterOrEqual(len(events), 2)

	lastEvent := events[len(events)-1]
	secondLastEvent := events[len(events)-2]

	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, lastEvent.EventType,
		"last event should be speculative WFT_STARTED")
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, secondLastEvent.EventType,
		"second-to-last event should be speculative WFT_SCHEDULED")
	s.Equal(secondLastEvent.EventId+1, lastEvent.EventId,
		"speculative events must have consecutive IDs")
}

// TestGetHistory_CacheMiss_LargeHistory_UpdateCompletesWithoutWFTFailed is a regression test for
// the premature-EOS race condition on multi-page histories.
//
// Scenario (mirrors the production failures observed after PR #9325):
//  1. A workflow accumulates >256 history events (requires multi-page GetHistory).
//  2. Worker 1 processes the workflow (sticky queue, in-process cache populated).
//  3. Worker 1 stops — in-process cache is cleared.
//  4. An update is sent → server creates a speculative WFT (SCHED+STARTED in mutable state only).
//  5. Worker 2 starts fresh (no cached state). On receiving the WFT it calls
//     GetWorkflowExecutionHistory which now spans multiple pages.
//  6. If the race condition fix is absent, the speculative WFT events can be missing from the
//     last page (due to events committed between pages), and worker 2 reports premature EOS
//     by calling RespondWorkflowTaskFailed — leaving a WFT_FAILED event in history.
//
// The test verifies that no WFT_FAILED event appears in the final history.
func (s *TransientWFTHistorySuite) TestGetHistory_CacheMiss_LargeHistory_UpdateCompletesWithoutWFTFailed() {
	ctx := testcore.NewContext()
	tv := testvars.New(s.T()).WithNamespaceName(s.Namespace())

	// Workflow: registers an update handler, processes 300 signals to build a large history,
	// then waits for a "done" signal to complete cleanly.
	wfFn := func(ctx workflow.Context) error {
		if err := workflow.SetUpdateHandler(ctx, tv.HandlerName(),
			func(ctx workflow.Context, arg string) (string, error) {
				return "updated-" + arg, nil
			}); err != nil {
			return err
		}
		sigCh := workflow.GetSignalChannel(ctx, "signal")
		for i := 0; i < 300; i++ {
			sigCh.Receive(ctx, nil)
		}
		doneCh := workflow.GetSignalChannel(ctx, "done")
		doneCh.Receive(ctx, nil)
		return nil
	}

	// Start the workflow.
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                  tv.WorkflowID(),
		TaskQueue:           tv.TaskQueue().GetName(),
		WorkflowTaskTimeout: 10 * time.Second,
	}, wfFn)
	s.NoError(err)
	tv = tv.WithRunID(run.GetRunID())

	// Start worker 1. Use a very short sticky-schedule-to-start timeout so that once w1 stops,
	// the pending sticky WFT times out almost immediately and is rescheduled on the normal queue.
	w1 := worker.New(s.SdkClient(), tv.TaskQueue().GetName(), worker.Options{
		StickyScheduleToStartTimeout: time.Millisecond,
	})
	w1.RegisterWorkflow(wfFn)
	s.NoError(w1.Start())

	// Send 300 signals to build a history large enough to span multiple GetHistory pages
	// (>256 events at the default page size of 256).
	for i := 0; i < 300; i++ {
		s.NoError(s.SdkClient().SignalWorkflow(ctx, tv.WorkflowID(), run.GetRunID(), "signal", nil))
	}

	// Wait until all 300 signals appear in history before stopping worker 1.
	s.EventuallyWithT(func(c *assert.CollectT) {
		hist := s.GetHistory(s.Namespace().String(), tv.WorkflowExecution())
		count := 0
		for _, ev := range hist {
			if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				count++
			}
		}
		assert.Equal(c, 300, count, "all 300 signals must be in history")
	}, 60*time.Second, 200*time.Millisecond)

	// Stop worker 1. Its in-process workflow cache is now cleared.
	w1.Stop()

	// Send an update. The server creates a speculative WFT (events in mutable state only).
	// sendUpdateNoError blocks until the update reaches ADMITTED state, then returns a channel
	// that receives the final response when the update completes.
	updateCh := sendUpdateNoError(s, tv)

	// Start worker 2 with no prior knowledge of this workflow execution. When it picks up
	// the pending WFT (after the ~1ms sticky timeout), it reads the full history via
	// GetWorkflowExecutionHistory — exercising the multi-page path with a speculative WFT.
	w2 := worker.New(s.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
	w2.RegisterWorkflow(wfFn)
	s.NoError(w2.Start())
	defer w2.Stop()

	// Wait for the update to complete. If GetWorkflowExecutionHistory returns a premature EOS
	// (missing the speculative WFT events on the last page), the SDK worker calls
	// RespondWorkflowTaskFailed and the update will not complete within the context timeout.
	updateResp := <-updateCh
	s.NotNil(updateResp, "update must complete — a nil response indicates premature EOS")

	// Signal the workflow to complete cleanly.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, tv.WorkflowID(), run.GetRunID(), "done", nil))

	// Verify no WFT_FAILED events in the final history. Any WFT_FAILED with cause
	// WORKFLOW_WORKER_UNHANDLED_FAILURE would indicate the SDK worker encountered premature EOS.
	hist := s.GetHistory(s.Namespace().String(), tv.WorkflowExecution())
	for _, event := range hist {
		s.NotEqual(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, event.GetEventType(),
			"WFT_FAILED at event %d — indicates premature EOS in GetWorkflowExecutionHistory",
			event.GetEventId())
	}
}
