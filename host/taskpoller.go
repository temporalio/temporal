// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package host

import (
	"context"
	"testing"
	"time"

	"github.com/uber/cadence/common/client"

	"github.com/stretchr/testify/require"
	"go.uber.org/yarpc"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
)

type (
	decisionTaskHandler func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error)
	activityTaskHandler func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, takeToken []byte) ([]byte, bool, error)

	queryHandler func(task *workflow.PollForDecisionTaskResponse) ([]byte, error)

	// TaskPoller is used in integration tests to poll decision or activity tasks
	TaskPoller struct {
		Engine                              FrontendClient
		Domain                              string
		TaskList                            *workflow.TaskList
		StickyTaskList                      *workflow.TaskList
		StickyScheduleToStartTimeoutSeconds *int32
		Identity                            string
		DecisionHandler                     decisionTaskHandler
		ActivityHandler                     activityTaskHandler
		QueryHandler                        queryHandler
		Logger                              log.Logger
		T                                   *testing.T
	}
)

// PollAndProcessDecisionTask for decision tasks
func (p *TaskPoller) PollAndProcessDecisionTask(dumpHistory bool, dropTask bool) (isQueryTask bool, err error) {
	return p.PollAndProcessDecisionTaskWithAttempt(dumpHistory, dropTask, false, false, int64(0))
}

// PollAndProcessDecisionTaskWithSticky for decision tasks
func (p *TaskPoller) PollAndProcessDecisionTaskWithSticky(dumpHistory bool, dropTask bool) (isQueryTask bool, err error) {
	return p.PollAndProcessDecisionTaskWithAttempt(dumpHistory, dropTask, true, true, int64(0))
}

// PollAndProcessDecisionTaskWithoutRetry for decision tasks
func (p *TaskPoller) PollAndProcessDecisionTaskWithoutRetry(dumpHistory bool, dropTask bool) (isQueryTask bool, err error) {
	return p.PollAndProcessDecisionTaskWithAttemptAndRetry(dumpHistory, dropTask, false, false, int64(0), 1)
}

// PollAndProcessDecisionTaskWithAttempt for decision tasks
func (p *TaskPoller) PollAndProcessDecisionTaskWithAttempt(
	dumpHistory bool,
	dropTask bool,
	pollStickyTaskList bool,
	respondStickyTaskList bool,
	decisionAttempt int64,
) (isQueryTask bool, err error) {

	return p.PollAndProcessDecisionTaskWithAttemptAndRetry(
		dumpHistory,
		dropTask,
		pollStickyTaskList,
		respondStickyTaskList,
		decisionAttempt,
		5)
}

// PollAndProcessDecisionTaskWithAttemptAndRetry for decision tasks
func (p *TaskPoller) PollAndProcessDecisionTaskWithAttemptAndRetry(
	dumpHistory bool,
	dropTask bool,
	pollStickyTaskList bool,
	respondStickyTaskList bool,
	decisionAttempt int64,
	retryCount int,
) (isQueryTask bool, err error) {

	isQueryTask, _, err = p.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
		dumpHistory,
		dropTask,
		pollStickyTaskList,
		respondStickyTaskList,
		decisionAttempt,
		retryCount,
		false,
		nil)
	return isQueryTask, err
}

// PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision for decision tasks
func (p *TaskPoller) PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
	dumpHistory bool,
	dropTask bool,
	pollStickyTaskList bool,
	respondStickyTaskList bool,
	decisionAttempt int64,
	retryCount int,
	forceCreateNewDecision bool,
	queryResult *workflow.WorkflowQueryResult,
) (isQueryTask bool, newTask *workflow.RespondDecisionTaskCompletedResponse, err error) {
Loop:
	for attempt := 0; attempt < retryCount; attempt++ {

		taskList := p.TaskList
		if pollStickyTaskList {
			taskList = p.StickyTaskList
		}
		response, err1 := p.Engine.PollForDecisionTask(createContext(), &workflow.PollForDecisionTaskRequest{
			Domain:   common.StringPtr(p.Domain),
			TaskList: taskList,
			Identity: common.StringPtr(p.Identity),
		})

		if err1 == history.ErrDuplicate {
			p.Logger.Info("Duplicate Decision task: Polling again.")
			continue Loop
		}

		if err1 != nil {
			return false, nil, err1
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.Logger.Info("Empty Decision task: Polling again.")
			continue Loop
		}

		var events []*workflow.HistoryEvent
		if response.Query == nil || !pollStickyTaskList {
			// if not query task, should have some history events
			// for non sticky query, there should be events returned
			history := response.History
			if history == nil {
				p.Logger.Fatal("History is nil")
			}

			events = history.Events
			if events == nil || len(events) == 0 {
				p.Logger.Fatal("History Events are empty")
			}

			nextPageToken := response.NextPageToken
			for nextPageToken != nil {
				resp, err2 := p.Engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
					Domain:        common.StringPtr(p.Domain),
					Execution:     response.WorkflowExecution,
					NextPageToken: nextPageToken,
				})

				if err2 != nil {
					return false, nil, err2
				}

				events = append(events, resp.History.Events...)
				nextPageToken = resp.NextPageToken
			}
		} else {
			// for sticky query, there should be NO events returned
			// since worker side already has the state machine and we do not intend to update that.
			history := response.History
			nextPageToken := response.NextPageToken
			if !(history == nil || (len(history.Events) == 0 && nextPageToken == nil)) {
				// if history is not nil, and contains events or next token
				p.Logger.Fatal("History is not empty for sticky query")
			}
		}

		if dropTask {
			p.Logger.Info("Dropping Decision task: ")
			return false, nil, nil
		}

		if dumpHistory {
			common.PrettyPrintHistory(response.History, p.Logger)
		}

		// handle query task response
		if response.Query != nil {
			blob, err := p.QueryHandler(response)

			completeRequest := &workflow.RespondQueryTaskCompletedRequest{TaskToken: response.TaskToken}
			if err != nil {
				completeType := workflow.QueryTaskCompletedTypeFailed
				completeRequest.CompletedType = &completeType
				completeRequest.ErrorMessage = common.StringPtr(err.Error())
			} else {
				completeType := workflow.QueryTaskCompletedTypeCompleted
				completeRequest.CompletedType = &completeType
				completeRequest.QueryResult = blob
			}

			return true, nil, p.Engine.RespondQueryTaskCompleted(createContext(), completeRequest)
		}

		// handle normal decision task / non query task response
		var lastDecisionScheduleEvent *workflow.HistoryEvent
		for _, e := range events {
			if e.GetEventType() == workflow.EventTypeDecisionTaskScheduled {
				lastDecisionScheduleEvent = e
			}
		}
		if lastDecisionScheduleEvent != nil && decisionAttempt > 0 {
			require.Equal(p.T, decisionAttempt, lastDecisionScheduleEvent.DecisionTaskScheduledEventAttributes.GetAttempt())
		}

		executionCtx, decisions, err := p.DecisionHandler(response.WorkflowExecution, response.WorkflowType,
			common.Int64Default(response.PreviousStartedEventId), common.Int64Default(response.StartedEventId), response.History)
		if err != nil {
			p.Logger.Info("Failing Decision. Decision handler failed with error", tag.Error(err))
			return isQueryTask, nil, p.Engine.RespondDecisionTaskFailed(createContext(), &workflow.RespondDecisionTaskFailedRequest{
				TaskToken: response.TaskToken,
				Cause:     common.DecisionTaskFailedCausePtr(workflow.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure),
				Details:   []byte(err.Error()),
				Identity:  common.StringPtr(p.Identity),
			})
		}

		p.Logger.Info("Completing Decision.  Decisions", tag.Value(decisions))
		if !respondStickyTaskList {
			// non sticky tasklist
			newTask, err := p.Engine.RespondDecisionTaskCompleted(createContext(), &workflow.RespondDecisionTaskCompletedRequest{
				TaskToken:                  response.TaskToken,
				Identity:                   common.StringPtr(p.Identity),
				ExecutionContext:           executionCtx,
				Decisions:                  decisions,
				ReturnNewDecisionTask:      common.BoolPtr(forceCreateNewDecision),
				ForceCreateNewDecisionTask: common.BoolPtr(forceCreateNewDecision),
				QueryResults:               getQueryResults(response.GetQueries(), queryResult),
			})
			return false, newTask, err
		}
		// sticky tasklist
		newTask, err := p.Engine.RespondDecisionTaskCompleted(
			createContext(),
			&workflow.RespondDecisionTaskCompletedRequest{
				TaskToken:        response.TaskToken,
				Identity:         common.StringPtr(p.Identity),
				ExecutionContext: executionCtx,
				Decisions:        decisions,
				StickyAttributes: &workflow.StickyExecutionAttributes{
					WorkerTaskList:                p.StickyTaskList,
					ScheduleToStartTimeoutSeconds: p.StickyScheduleToStartTimeoutSeconds,
				},
				ReturnNewDecisionTask:      common.BoolPtr(forceCreateNewDecision),
				ForceCreateNewDecisionTask: common.BoolPtr(forceCreateNewDecision),
				QueryResults:               getQueryResults(response.GetQueries(), queryResult),
			},
			yarpc.WithHeader(common.LibraryVersionHeaderName, "0.0.1"),
			yarpc.WithHeader(common.FeatureVersionHeaderName, client.GoWorkerConsistentQueryVersion),
			yarpc.WithHeader(common.ClientImplHeaderName, client.GoSDK),
		)

		return false, newTask, err
	}

	return false, nil, matching.ErrNoTasks
}

// HandlePartialDecision for decision task
func (p *TaskPoller) HandlePartialDecision(response *workflow.PollForDecisionTaskResponse) (
	*workflow.RespondDecisionTaskCompletedResponse, error) {
	if response == nil || len(response.TaskToken) == 0 {
		p.Logger.Info("Empty Decision task: Polling again.")
		return nil, nil
	}

	var events []*workflow.HistoryEvent
	history := response.History
	if history == nil {
		p.Logger.Fatal("History is nil")
	}

	events = history.Events
	if events == nil || len(events) == 0 {
		p.Logger.Fatal("History Events are empty")
	}

	executionCtx, decisions, err := p.DecisionHandler(response.WorkflowExecution, response.WorkflowType,
		common.Int64Default(response.PreviousStartedEventId), common.Int64Default(response.StartedEventId), response.History)
	if err != nil {
		p.Logger.Info("Failing Decision. Decision handler failed with error: %v", tag.Error(err))
		return nil, p.Engine.RespondDecisionTaskFailed(createContext(), &workflow.RespondDecisionTaskFailedRequest{
			TaskToken: response.TaskToken,
			Cause:     common.DecisionTaskFailedCausePtr(workflow.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure),
			Details:   []byte(err.Error()),
			Identity:  common.StringPtr(p.Identity),
		})
	}

	p.Logger.Info("Completing Decision.  Decisions: %v", tag.Value(decisions))

	// sticky tasklist
	newTask, err := p.Engine.RespondDecisionTaskCompleted(
		createContext(),
		&workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        response.TaskToken,
			Identity:         common.StringPtr(p.Identity),
			ExecutionContext: executionCtx,
			Decisions:        decisions,
			StickyAttributes: &workflow.StickyExecutionAttributes{
				WorkerTaskList:                p.StickyTaskList,
				ScheduleToStartTimeoutSeconds: p.StickyScheduleToStartTimeoutSeconds,
			},
			ReturnNewDecisionTask:      common.BoolPtr(true),
			ForceCreateNewDecisionTask: common.BoolPtr(true),
		},
		yarpc.WithHeader(common.LibraryVersionHeaderName, "0.0.1"),
		yarpc.WithHeader(common.FeatureVersionHeaderName, client.GoWorkerConsistentQueryVersion),
		yarpc.WithHeader(common.ClientImplHeaderName, client.GoSDK),
	)

	return newTask, err
}

// PollAndProcessActivityTask for activity tasks
func (p *TaskPoller) PollAndProcessActivityTask(dropTask bool) error {
retry:
	for attempt := 0; attempt < 5; attempt++ {
		response, err1 := p.Engine.PollForActivityTask(createContext(), &workflow.PollForActivityTaskRequest{
			Domain:   common.StringPtr(p.Domain),
			TaskList: p.TaskList,
			Identity: common.StringPtr(p.Identity),
		})

		if err1 == history.ErrDuplicate {
			p.Logger.Info("Duplicate Activity task: Polling again.")
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.Logger.Info("Empty Activity task: Polling again.")
			return nil
		}

		if dropTask {
			p.Logger.Info("Dropping Activity task: ")
			return nil
		}
		p.Logger.Debug("Received Activity task", tag.Value(response))

		result, cancel, err2 := p.ActivityHandler(response.WorkflowExecution, response.ActivityType, *response.ActivityId,
			response.Input, response.TaskToken)
		if cancel {
			p.Logger.Info("Executing RespondActivityTaskCanceled")
			return p.Engine.RespondActivityTaskCanceled(createContext(), &workflow.RespondActivityTaskCanceledRequest{
				TaskToken: response.TaskToken,
				Details:   []byte("details"),
				Identity:  common.StringPtr(p.Identity),
			})
		}

		if err2 != nil {
			return p.Engine.RespondActivityTaskFailed(createContext(), &workflow.RespondActivityTaskFailedRequest{
				TaskToken: response.TaskToken,
				Reason:    common.StringPtr(err2.Error()),
				Details:   []byte(err2.Error()),
				Identity:  common.StringPtr(p.Identity),
			})
		}

		return p.Engine.RespondActivityTaskCompleted(createContext(), &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: response.TaskToken,
			Identity:  common.StringPtr(p.Identity),
			Result:    result,
		})
	}

	return matching.ErrNoTasks
}

// PollAndProcessActivityTaskWithID is similar to PollAndProcessActivityTask but using RespondActivityTask...ByID
func (p *TaskPoller) PollAndProcessActivityTaskWithID(dropTask bool) error {
retry:
	for attempt := 0; attempt < 5; attempt++ {
		response, err1 := p.Engine.PollForActivityTask(createContext(), &workflow.PollForActivityTaskRequest{
			Domain:   common.StringPtr(p.Domain),
			TaskList: p.TaskList,
			Identity: common.StringPtr(p.Identity),
		})

		if err1 == history.ErrDuplicate {
			p.Logger.Info("Duplicate Activity task: Polling again.")
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.Logger.Info("Empty Activity task: Polling again.")
			return nil
		}

		if response.GetActivityId() == "" {
			p.Logger.Info("Empty ActivityID")
			return nil
		}

		if dropTask {
			p.Logger.Info("Dropping Activity task: ")
			return nil
		}
		p.Logger.Debug("Received Activity task: %v", tag.Value(response))

		result, cancel, err2 := p.ActivityHandler(response.WorkflowExecution, response.ActivityType, *response.ActivityId,
			response.Input, response.TaskToken)
		if cancel {
			p.Logger.Info("Executing RespondActivityTaskCanceled")
			return p.Engine.RespondActivityTaskCanceledByID(createContext(), &workflow.RespondActivityTaskCanceledByIDRequest{
				Domain:     common.StringPtr(p.Domain),
				WorkflowID: common.StringPtr(response.WorkflowExecution.GetWorkflowId()),
				RunID:      common.StringPtr(response.WorkflowExecution.GetRunId()),
				ActivityID: common.StringPtr(response.GetActivityId()),
				Details:    []byte("details"),
				Identity:   common.StringPtr(p.Identity),
			})
		}

		if err2 != nil {
			return p.Engine.RespondActivityTaskFailedByID(createContext(), &workflow.RespondActivityTaskFailedByIDRequest{
				Domain:     common.StringPtr(p.Domain),
				WorkflowID: common.StringPtr(response.WorkflowExecution.GetWorkflowId()),
				RunID:      common.StringPtr(response.WorkflowExecution.GetRunId()),
				ActivityID: common.StringPtr(response.GetActivityId()),
				Reason:     common.StringPtr(err2.Error()),
				Details:    []byte(err2.Error()),
				Identity:   common.StringPtr(p.Identity),
			})
		}

		return p.Engine.RespondActivityTaskCompletedByID(createContext(), &workflow.RespondActivityTaskCompletedByIDRequest{
			Domain:     common.StringPtr(p.Domain),
			WorkflowID: common.StringPtr(response.WorkflowExecution.GetWorkflowId()),
			RunID:      common.StringPtr(response.WorkflowExecution.GetRunId()),
			ActivityID: common.StringPtr(response.GetActivityId()),
			Identity:   common.StringPtr(p.Identity),
			Result:     result,
		})
	}

	return matching.ErrNoTasks
}

func createContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 90*time.Second)
	return ctx
}

func getQueryResults(queries map[string]*workflow.WorkflowQuery, queryResult *workflow.WorkflowQueryResult) map[string]*workflow.WorkflowQueryResult {
	result := make(map[string]*workflow.WorkflowQueryResult)
	for k := range queries {
		result[k] = queryResult
	}
	return result
}
