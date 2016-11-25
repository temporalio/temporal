package flow

import (
	"context"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
)

type (
	// WorkflowTaskHandler represents workflow task handlers.
	WorkflowTaskHandler interface {
		// Process the workflow task
		ProcessWorkflowTask(workflowTask *WorkflowTask) (*m.RespondDecisionTaskCompletedRequest, error)
	}

	// ActivityTaskHandler represents activity task handlers.
	ActivityTaskHandler interface {
		// Execute the activity task
		// The return interface{} can have three requests, use switch to find the type of it.
		// - RespondActivityTaskCompletedRequest
		// - RespondActivityTaskFailedRequest
		// - RespondActivityTaskCancelRequest
		Execute(context context.Context, activityTask *ActivityTask) (interface{}, error)
	}

	// WorkflowExecutionEventHandler process a single event.
	WorkflowExecutionEventHandler interface {
		// Process a single event and return the assosciated decisions.
		ProcessEvent(event *m.HistoryEvent) ([]*m.Decision, error)

		// Close for cleaning up resources on this event handler
		Close()
	}

	// WorkflowTask wraps a decision task.
	WorkflowTask struct {
		task *m.PollForDecisionTaskResponse
	}

	// ActivityTask wraps a activity task.
	ActivityTask struct {
		task *m.PollForActivityTaskResponse
	}
)
