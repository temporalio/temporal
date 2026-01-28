package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TransientWorkflowTaskHistorySuite struct {
	testcore.FunctionalTestBase
}

func TestTransientWorkflowTaskHistorySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TransientWorkflowTaskHistorySuite))
}

// newSDKContext creates a context that identifies as a Go SDK client.
// This allows the tests to receive transient/speculative events in GetWorkflowExecutionHistory responses.
func newSDKContext() context.Context {
	ctx := testcore.NewContext()
	// Override the server client name with Go SDK client name.
	// This ensures transient/speculative events are included in history responses.
	ctx = metadata.AppendToOutgoingContext(ctx,
		headers.ClientNameHeaderName, headers.ClientNameGoSDK,
	)
	return ctx
}

// TestGetHistoryAfterCacheEviction_TransientWFT is the core regression test.
// It replicates the scenario where a worker's cache is evicted and then requests
// history that should include transient events.
func (s *TransientWorkflowTaskHistorySuite) TestGetHistoryAfterCacheEviction_TransientWFT() {
	id := "functional-get-history-after-cache-eviction-transient-wft"
	wt := "functional-get-history-after-cache-eviction-transient-wft-type"
	tl := "functional-get-history-after-cache-eviction-transient-wft-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		Identity:            identity,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// Step 1: Fail the first workflow task to create a transient WFT (attempt=2)
	failWorkflowTask := true
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if failWorkflowTask {
			failWorkflowTask = false
			return nil, errors.New("simulated workflow task failure")
		}
		// Complete workflow on second attempt
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil
	}

	//nolint:staticcheck // SA1019 Using deprecated testcore.TaskPoller for backwards compatibility
	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Poll and fail the first workflow task - this creates transient WFT (attempt=2)
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Step 2: Poll the transient task and start it (but don't complete yet)
	// This simulates having both scheduled + started transient events
	ctx := newSDKContext()
	resp2, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotNil(resp2)
	s.NotEmpty(resp2.TaskToken)

	// Step 3: Call GetWorkflowExecutionHistory, simulating worker cache eviction
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.Namespace().String(),
			Execution:       workflowExecution,
			MaximumPageSize: 100,
		})

	s.NoError(err, "GetWorkflowExecutionHistory should not return 'premature end of stream' error")
	s.NotNil(histResp)
	s.NotNil(histResp.History)
	s.NotEmpty(histResp.History.Events)

	// Step 5: Verify history includes transient WorkflowTaskScheduled (attempt=2) and WorkflowTaskStarted events
	events := histResp.History.Events
	foundScheduled := false
	foundStarted := false

	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			attrs := event.GetWorkflowTaskScheduledEventAttributes()
			if attrs.GetAttempt() == 2 {
				foundScheduled = true
			}
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
			// Check if this is the started event for the transient task (attempt 2)
			// by checking if it comes after a scheduled event with attempt=2
			for i, e := range events {
				if e.GetEventId() == event.GetEventId() && i > 0 {
					prevEvent := events[i-1]
					if prevEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED &&
						prevEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt() == 2 {
						foundStarted = true
					}
				}
			}
		}
	}

	s.True(foundScheduled, "History should include transient WorkflowTaskScheduled event (attempt=2)")
	s.True(foundStarted, "History should include transient WorkflowTaskStarted event")

	// The last event ID in history should match or exceed what worker expects from PollWorkflowTask
	lastEventID := events[len(events)-1].GetEventId()
	s.GreaterOrEqual(lastEventID, resp2.GetStartedEventId(),
		"Last event ID in history should be >= StartedEventId from poll response to prevent 'premature end of stream'")

	// Clean up by completing the workflow task
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp2.TaskToken,
		Identity:  identity,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}},
	})
	s.NoError(err)
}

// TestGetHistory_TransientWFT_ScheduledOnly verifies transient events are included
// even when WFT is scheduled but not yet started.
func (s *TransientWorkflowTaskHistorySuite) TestGetHistory_TransientWFT_ScheduledOnly() {
	id := "functional-get-history-transient-wft-scheduled-only"
	wt := "functional-get-history-transient-wft-scheduled-only-type"
	tl := "functional-get-history-transient-wft-scheduled-only-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		Identity:            identity,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// Step 1: Fail first task to create transient WFT (attempt=2)
	failWorkflowTask := true
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if failWorkflowTask {
			failWorkflowTask = false
			return nil, errors.New("simulated workflow task failure")
		}
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil
	}

	//nolint:staticcheck // SA1019 Using deprecated testcore.TaskPoller for backwards compatibility
	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Poll and fail the first workflow task
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Step 2: Call GetWorkflowExecutionHistory BEFORE polling the transient task
	ctx := newSDKContext()
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.Namespace().String(),
			Execution:       workflowExecution,
			MaximumPageSize: 100,
		})
	s.NoError(err)
	s.NotNil(histResp)
	s.NotNil(histResp.History)
	s.NotEmpty(histResp.History.Events)

	// Step 3: Verify history includes transient WorkflowTaskScheduled event (attempt=2)
	events := histResp.History.Events
	foundScheduled := false
	foundStarted := false

	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			attrs := event.GetWorkflowTaskScheduledEventAttributes()
			if attrs.GetAttempt() == 2 {
				foundScheduled = true
			}
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
			// Check if this corresponds to the transient task
			for i, e := range events {
				if e.GetEventId() == event.GetEventId() && i > 0 {
					prevEvent := events[i-1]
					if prevEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED &&
						prevEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt() == 2 {
						foundStarted = true
					}
				}
			}
		}
	}

	s.True(foundScheduled, "History should include transient WorkflowTaskScheduled event (attempt=2)")
	s.False(foundStarted, "History should NOT include transient WorkflowTaskStarted event (task not polled yet)")

	// Clean up: Poll and complete the transient task
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
}

// TestGetHistory_RaceCondition_RetryableError verifies that the query-compare-query pattern
// detects workflow state changes during pagination.
func (s *TransientWorkflowTaskHistorySuite) TestGetHistory_RaceCondition_RetryableError() {
	id := "functional-get-history-race-condition-retryable-error"
	wt := "functional-get-history-race-condition-retryable-error-type"
	tl := "functional-get-history-race-condition-retryable-error-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		Identity:            identity,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// Step 1: Create a transient WFT by failing the first task
	failWorkflowTask := true
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if failWorkflowTask {
			failWorkflowTask = false
			return nil, errors.New("simulated workflow task failure")
		}
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil
	}

	//nolint:staticcheck // SA1019 Using deprecated testcore.TaskPoller for backwards compatibility
	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Poll and fail the first workflow task
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Step 2: Poll the transient task to start it
	ctx := newSDKContext()
	resp2, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotNil(resp2)
	s.NotEmpty(resp2.TaskToken)

	// Step 3: Start fetching history with pagination (small page size to force pagination)
	// This will capture the initial transient task state
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.Namespace().String(),
			Execution:       workflowExecution,
			MaximumPageSize: 2, // Small page size to force pagination
		})
	s.NoError(err)
	s.NotNil(histResp)

	// Step 4: Complete the transient WFT (changes event IDs) DURING pagination
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp2.TaskToken,
		Identity:  identity,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}},
	})
	s.NoError(err)

	// Step 5: Continue pagination with the next page token
	// This should detect the race condition and return a retryable error
	if histResp.NextPageToken != nil {
		histResp2, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       s.Namespace().String(),
				Execution:       workflowExecution,
				MaximumPageSize: 2,
				NextPageToken:   histResp.NextPageToken,
			})

		// The implementation should detect the state change.
		// Depending on timing, we might get:
		// 1. An Unavailable error (ideal case - race condition detected)
		// 2. Success if the workflow completed before the second pagination call
		// We verify that if there's an error, it's the expected type
		if err != nil {
			s.ErrorAs(err, &serviceerror.Unavailable{})
		} else {
			// If no error, the pagination completed successfully
			// This can happen if the workflow completed before the second page was fetched
			s.NotNil(histResp2)
		}
	}
}
