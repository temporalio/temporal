// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/matching"
)

type (
	workflowTaskHandler func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error)
	activityTaskHandler func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, takeToken []byte) (*commonpb.Payloads, bool, error)

	queryHandler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error)

	// TaskPoller is used in integration tests to poll workflow or activity task queues.
	TaskPoller struct {
		Engine                       FrontendClient
		Namespace                    string
		TaskQueue                    *taskqueuepb.TaskQueue
		StickyTaskQueue              *taskqueuepb.TaskQueue
		StickyScheduleToStartTimeout time.Duration
		Identity                     string
		WorkflowTaskHandler          workflowTaskHandler
		ActivityTaskHandler          activityTaskHandler
		QueryHandler                 queryHandler
		Logger                       log.Logger
		T                            *testing.T
	}
)

// PollAndProcessWorkflowTask for workflow tasks
func (p *TaskPoller) PollAndProcessWorkflowTask(dumpHistory bool, dropTask bool) (isQueryTask bool, err error) {
	return p.PollAndProcessWorkflowTaskWithAttempt(dumpHistory, dropTask, false, false, 1)
}

// PollAndProcessWorkflowTaskWithSticky for workflow tasks
func (p *TaskPoller) PollAndProcessWorkflowTaskWithSticky(dumpHistory bool, dropTask bool) (isQueryTask bool, err error) {
	return p.PollAndProcessWorkflowTaskWithAttempt(dumpHistory, dropTask, true, true, 1)
}

// PollAndProcessWorkflowTaskWithoutRetry for workflow tasks
func (p *TaskPoller) PollAndProcessWorkflowTaskWithoutRetry(dumpHistory bool, dropTask bool) (isQueryTask bool, err error) {
	return p.PollAndProcessWorkflowTaskWithAttemptAndRetry(dumpHistory, dropTask, false, false, 1, 1)
}

// PollAndProcessWorkflowTaskWithAttempt for workflow tasks
func (p *TaskPoller) PollAndProcessWorkflowTaskWithAttempt(
	dumpHistory bool,
	dropTask bool,
	pollStickyTaskQueue bool,
	respondStickyTaskQueue bool,
	workflowTaskAttempt int32,
) (isQueryTask bool, err error) {

	return p.PollAndProcessWorkflowTaskWithAttemptAndRetry(
		dumpHistory,
		dropTask,
		pollStickyTaskQueue,
		respondStickyTaskQueue,
		workflowTaskAttempt,
		5)
}

// PollAndProcessWorkflowTaskWithAttemptAndRetry for workflow tasks
func (p *TaskPoller) PollAndProcessWorkflowTaskWithAttemptAndRetry(
	dumpHistory bool,
	dropTask bool,
	pollStickyTaskQueue bool,
	respondStickyTaskQueue bool,
	workflowTaskAttempt int32,
	retryCount int,
) (isQueryTask bool, err error) {

	isQueryTask, _, err = p.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		dumpHistory,
		dropTask,
		pollStickyTaskQueue,
		respondStickyTaskQueue,
		workflowTaskAttempt,
		retryCount,
		false,
		nil)
	return isQueryTask, err
}

// PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask for workflow tasks
func (p *TaskPoller) PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
	dumpHistory bool,
	dropTask bool,
	pollStickyTaskQueue bool,
	respondStickyTaskQueue bool,
	workflowTaskAttempt int32,
	retryCount int,
	forceCreateNewWorkflowTask bool,
	queryResult *querypb.WorkflowQueryResult,
) (isQueryTask bool, newTask *workflowservice.RespondWorkflowTaskCompletedResponse, err error) {
Loop:
	for attempt := 1; attempt <= retryCount; attempt++ {

		taskQueue := p.TaskQueue
		if pollStickyTaskQueue {
			taskQueue = p.StickyTaskQueue
		}

		response, err1 := p.Engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: p.Namespace,
			TaskQueue: taskQueue,
			Identity:  p.Identity,
		})

		if !common.IsServiceTransientError(err1) {
			return false, nil, err1
		}

		if err1 == consts.ErrDuplicate {
			p.Logger.Info("Duplicate Workflow task: Polling again")
			continue Loop
		}

		if err1 != nil {
			return false, nil, err1
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.Logger.Info("Empty Workflow task: Polling again")
			continue Loop
		}

		var events []*historypb.HistoryEvent
		if response.Query == nil || !pollStickyTaskQueue {
			// if not query task, should have some history events
			// for non sticky query, there should be events returned
			history := response.History
			if history == nil {
				p.Logger.Fatal("History is nil")
				return false, nil, errors.New("history is nil")
			}

			events = history.Events
			if len(events) == 0 {
				p.Logger.Fatal("History Events are empty")
				return false, nil, errors.New("history events are empty")
			}

			nextPageToken := response.NextPageToken
			for nextPageToken != nil {
				resp, err2 := p.Engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
					Namespace:     p.Namespace,
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
			p.Logger.Info("Dropping Workflow task: ")
			return false, nil, nil
		}

		if dumpHistory {
			common.PrettyPrintHistory(response.History, p.Logger)
		}

		// handle query task response
		if response.Query != nil {
			blob, err := p.QueryHandler(response)

			completeRequest := &workflowservice.RespondQueryTaskCompletedRequest{TaskToken: response.TaskToken}
			if err != nil {
				completeType := enumspb.QUERY_RESULT_TYPE_FAILED
				completeRequest.CompletedType = completeType
				completeRequest.ErrorMessage = err.Error()
			} else {
				completeType := enumspb.QUERY_RESULT_TYPE_ANSWERED
				completeRequest.CompletedType = completeType
				completeRequest.QueryResult = blob
			}

			_, err = p.Engine.RespondQueryTaskCompleted(NewContext(), completeRequest)
			return true, nil, err
		}

		// handle normal workflow task / non query task response
		var lastWorkflowTaskScheduleEvent *historypb.HistoryEvent
		for _, e := range events {
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				lastWorkflowTaskScheduleEvent = e
			}
		}
		if lastWorkflowTaskScheduleEvent != nil && workflowTaskAttempt > 1 {
			require.Equal(p.T, workflowTaskAttempt, lastWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt())
		}

		commands, err := p.WorkflowTaskHandler(response.WorkflowExecution, response.WorkflowType, response.PreviousStartedEventId, response.StartedEventId, response.History)
		if err != nil {
			p.Logger.Error("Failing workflow task. Workflow task handler failed with error", tag.Error(err))
			_, err = p.Engine.RespondWorkflowTaskFailed(NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
				TaskToken: response.TaskToken,
				Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
				Failure:   newApplicationFailure(err, false, nil),
				Identity:  p.Identity,
			})
			return isQueryTask, nil, err
		}

		p.Logger.Info("Completing Commands. Commands ", tag.Value(commands))
		if !respondStickyTaskQueue {
			// non sticky taskqueue
			newTask, err := p.Engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken:                  response.TaskToken,
				Identity:                   p.Identity,
				Commands:                   commands,
				ReturnNewWorkflowTask:      forceCreateNewWorkflowTask,
				ForceCreateNewWorkflowTask: forceCreateNewWorkflowTask,
				QueryResults:               getQueryResults(response.GetQueries(), queryResult),
			})
			return false, newTask, err
		}
		// sticky taskqueue
		newTask, err := p.Engine.RespondWorkflowTaskCompleted(
			NewContext(),
			&workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: response.TaskToken,
				Identity:  p.Identity,
				Commands:  commands,
				StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
					WorkerTaskQueue:        p.StickyTaskQueue,
					ScheduleToStartTimeout: &p.StickyScheduleToStartTimeout,
				},
				ReturnNewWorkflowTask:      forceCreateNewWorkflowTask,
				ForceCreateNewWorkflowTask: forceCreateNewWorkflowTask,
				QueryResults:               getQueryResults(response.GetQueries(), queryResult),
			},
		)

		return false, newTask, err
	}

	return false, nil, matching.ErrNoTasks
}

// HandlePartialWorkflowTask for workflow task
func (p *TaskPoller) HandlePartialWorkflowTask(response *workflowservice.PollWorkflowTaskQueueResponse) (
	*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	if response == nil || len(response.TaskToken) == 0 {
		p.Logger.Info("Empty Workflow task: Polling again")
		return nil, nil
	}

	var events []*historypb.HistoryEvent
	history := response.History
	if history == nil {
		p.Logger.Fatal("History is nil")
		return nil, errors.New("history is nil")
	}

	events = history.Events
	if len(events) == 0 {
		p.Logger.Fatal("History Events are empty")
		return nil, errors.New("history events are empty")
	}

	commands, err := p.WorkflowTaskHandler(response.WorkflowExecution, response.WorkflowType,
		response.PreviousStartedEventId, response.StartedEventId, response.History)
	if err != nil {
		p.Logger.Error("Failing workflow task. Workflow task handler failed with error", tag.Error(err))
		_, err = p.Engine.RespondWorkflowTaskFailed(NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
			TaskToken: response.TaskToken,
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Failure:   newApplicationFailure(err, false, nil),
			Identity:  p.Identity,
		})
		return nil, err
	}

	p.Logger.Info("Completing Commands", tag.Value(commands))

	// sticky taskqueue
	newTask, err := p.Engine.RespondWorkflowTaskCompleted(
		NewContext(),
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: response.TaskToken,
			Identity:  p.Identity,
			Commands:  commands,
			StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
				WorkerTaskQueue:        p.StickyTaskQueue,
				ScheduleToStartTimeout: &p.StickyScheduleToStartTimeout,
			},
			ReturnNewWorkflowTask:      true,
			ForceCreateNewWorkflowTask: true,
		},
	)

	return newTask, err
}

// PollAndProcessActivityTask for activity tasks
func (p *TaskPoller) PollAndProcessActivityTask(dropTask bool) error {
retry:
	for attempt := 1; attempt <= 5; attempt++ {
		response, err := p.Engine.PollActivityTaskQueue(NewContext(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: p.Namespace,
			TaskQueue: p.TaskQueue,
			Identity:  p.Identity,
		})

		if err == consts.ErrDuplicate {
			p.Logger.Info("Duplicate Activity task: Polling again")
			continue retry
		}

		if err != nil {
			return err
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.Logger.Info("Empty Activity task: Polling again")
			continue retry
		}

		if dropTask {
			p.Logger.Info("Dropping Activity task: ")
			return nil
		}
		p.Logger.Debug("Received Activity task", tag.Value(response))

		result, cancel, err2 := p.ActivityTaskHandler(response.WorkflowExecution, response.ActivityType, response.ActivityId,
			response.Input, response.TaskToken)
		if cancel {
			p.Logger.Info("Executing RespondActivityTaskCanceled")
			_, err := p.Engine.RespondActivityTaskCanceled(NewContext(), &workflowservice.RespondActivityTaskCanceledRequest{
				TaskToken: response.TaskToken,
				Details:   payloads.EncodeString("details"),
				Identity:  p.Identity,
			})
			return err
		}

		if err2 != nil {
			_, err := p.Engine.RespondActivityTaskFailed(NewContext(), &workflowservice.RespondActivityTaskFailedRequest{
				TaskToken: response.TaskToken,
				Failure:   newApplicationFailure(err2, false, nil),
				Identity:  p.Identity,
			})
			return err
		}

		_, err = p.Engine.RespondActivityTaskCompleted(NewContext(), &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: response.TaskToken,
			Identity:  p.Identity,
			Result:    result,
		})
		return err
	}

	return matching.ErrNoTasks
}

// PollAndProcessActivityTaskWithID is similar to PollAndProcessActivityTask but using RespondActivityTask...ByID
func (p *TaskPoller) PollAndProcessActivityTaskWithID(dropTask bool) error {
retry:
	for attempt := 1; attempt <= 5; attempt++ {
		response, err1 := p.Engine.PollActivityTaskQueue(NewContext(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: p.Namespace,
			TaskQueue: p.TaskQueue,
			Identity:  p.Identity,
		})

		if err1 == consts.ErrDuplicate {
			p.Logger.Info("Duplicate Activity task: Polling again")
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.Logger.Info("Empty Activity task: Polling again")
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
		p.Logger.Debug("Received Activity task", tag.Value(response))

		result, cancel, err2 := p.ActivityTaskHandler(response.WorkflowExecution, response.ActivityType, response.ActivityId,
			response.Input, response.TaskToken)
		if cancel {
			p.Logger.Info("Executing RespondActivityTaskCanceled")
			_, err := p.Engine.RespondActivityTaskCanceledById(NewContext(), &workflowservice.RespondActivityTaskCanceledByIdRequest{
				Namespace:  p.Namespace,
				WorkflowId: response.WorkflowExecution.GetWorkflowId(),
				RunId:      response.WorkflowExecution.GetRunId(),
				ActivityId: response.GetActivityId(),
				Details:    payloads.EncodeString("details"),
				Identity:   p.Identity,
			})
			return err
		}

		if err2 != nil {
			_, err := p.Engine.RespondActivityTaskFailedById(NewContext(), &workflowservice.RespondActivityTaskFailedByIdRequest{
				Namespace:  p.Namespace,
				WorkflowId: response.WorkflowExecution.GetWorkflowId(),
				RunId:      response.WorkflowExecution.GetRunId(),
				ActivityId: response.GetActivityId(),
				Failure:    newApplicationFailure(err2, false, nil),
				Identity:   p.Identity,
			})
			return err
		}

		_, err := p.Engine.RespondActivityTaskCompletedById(NewContext(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  p.Namespace,
			WorkflowId: response.WorkflowExecution.GetWorkflowId(),
			RunId:      response.WorkflowExecution.GetRunId(),
			ActivityId: response.GetActivityId(),
			Identity:   p.Identity,
			Result:     result,
		})
		return err
	}

	return matching.ErrNoTasks
}

func getQueryResults(queries map[string]*querypb.WorkflowQuery, queryResult *querypb.WorkflowQueryResult) map[string]*querypb.WorkflowQueryResult {
	result := make(map[string]*querypb.WorkflowQueryResult)
	for k := range queries {
		result[k] = queryResult
	}
	return result
}

func newApplicationFailure(err error, nonRetryable bool, details *commonpb.Payloads) *failurepb.Failure {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		nonRetryable = applicationErr.NonRetryable()
	}

	f := &failurepb.Failure{
		Message: err.Error(),
		Source:  "IntegrationTests",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         getErrorType(err),
			NonRetryable: nonRetryable,
			Details:      details,
		}},
	}

	return f
}

func getErrorType(err error) string {
	var t reflect.Type
	for t = reflect.TypeOf(err); t.Kind() == reflect.Ptr; t = t.Elem() {
	}

	return t.Name()
}
