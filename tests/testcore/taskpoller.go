package testcore

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
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	WorkflowTaskHandler func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error)
	ActivityTaskHandler func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error)
	QueryHandler        func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error)
	MessageHandler      func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error)

	// Deprecated: TaskPoller is deprecated. Use taskpoller.TaskPoller instead.
	// TaskPoller is used in functional tests to poll workflow or activity task queues.
	TaskPoller struct {
		Client                       workflowservice.WorkflowServiceClient
		Namespace                    string
		TaskQueue                    *taskqueuepb.TaskQueue
		StickyTaskQueue              *taskqueuepb.TaskQueue
		StickyScheduleToStartTimeout time.Duration
		Identity                     string
		WorkflowTaskHandler          WorkflowTaskHandler
		ActivityTaskHandler          ActivityTaskHandler
		QueryHandler                 QueryHandler
		MessageHandler               MessageHandler
		Logger                       log.Logger
		T                            *testing.T
	}

	PollAndProcessWorkflowTaskOptions struct {
		DumpHistory          bool
		DumpCommands         bool
		DropTask             bool
		PollSticky           bool
		RespondSticky        bool
		ExpectedAttemptCount int
		Retries              int
		ForceNewWorkflowTask bool
		QueryResult          *querypb.WorkflowQueryResult
	}

	PollAndProcessWorkflowTaskOptionFunc func(*PollAndProcessWorkflowTaskOptions)

	PollAndProcessWorkflowTaskResponse struct {
		IsQueryTask bool
		NewTask     *workflowservice.RespondWorkflowTaskCompletedResponse
	}
)

var (
	ErrNoTasks = errors.New("no tasks")

	defaultPollAndProcessWorkflowTaskOptions = PollAndProcessWorkflowTaskOptions{
		DumpHistory:          false,
		DumpCommands:         true,
		DropTask:             false,
		PollSticky:           false,
		RespondSticky:        false,
		ExpectedAttemptCount: 1,
		Retries:              5,
		ForceNewWorkflowTask: false,
		QueryResult:          nil,
	}
)

func WithDumpHistory(o *PollAndProcessWorkflowTaskOptions)    { o.DumpHistory = true }
func WithNoDumpCommands(o *PollAndProcessWorkflowTaskOptions) { o.DumpCommands = false }
func WithDropTask(o *PollAndProcessWorkflowTaskOptions)       { o.DropTask = true }
func WithPollSticky(o *PollAndProcessWorkflowTaskOptions)     { o.PollSticky = true }
func WithRespondSticky(o *PollAndProcessWorkflowTaskOptions)  { o.RespondSticky = true }
func WithExpectedAttemptCount(c int) PollAndProcessWorkflowTaskOptionFunc {
	return func(o *PollAndProcessWorkflowTaskOptions) { o.ExpectedAttemptCount = c }
}
func WithRetries(c int) PollAndProcessWorkflowTaskOptionFunc {
	return func(o *PollAndProcessWorkflowTaskOptions) { o.Retries = c }
}
func WithoutRetries(o *PollAndProcessWorkflowTaskOptions)           { o.Retries = 1 }
func WithForceNewWorkflowTask(o *PollAndProcessWorkflowTaskOptions) { o.ForceNewWorkflowTask = true }

func (p *TaskPoller) PollAndProcessWorkflowTask(funcs ...PollAndProcessWorkflowTaskOptionFunc) (res PollAndProcessWorkflowTaskResponse, err error) {
	opts := defaultPollAndProcessWorkflowTaskOptions
	for _, f := range funcs {
		f(&opts)
	}
	return p.PollAndProcessWorkflowTaskWithOptions(&opts)
}

func (p *TaskPoller) PollAndProcessWorkflowTaskWithOptions(opts *PollAndProcessWorkflowTaskOptions) (res PollAndProcessWorkflowTaskResponse, err error) {
Loop:
	for attempt := 1; attempt <= opts.Retries; attempt++ {

		taskQueue := p.TaskQueue
		if opts.PollSticky {
			taskQueue = p.StickyTaskQueue
		}

		response, err1 := p.Client.PollWorkflowTaskQueue(NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
			Namespace: p.Namespace,
			TaskQueue: taskQueue,
			Identity:  p.Identity,
		}.Build())

		if !common.IsServiceTransientError(err1) {
			return PollAndProcessWorkflowTaskResponse{}, err1
		}

		if err1 == consts.ErrDuplicate {
			p.Logger.Info("Duplicate Workflow task: Polling again")
			continue Loop
		}

		if err1 != nil {
			return PollAndProcessWorkflowTaskResponse{}, err1
		}

		if response == nil || len(response.GetTaskToken()) == 0 {
			p.Logger.Info("Empty Workflow task: Polling again")
			continue Loop
		}

		var events []*historypb.HistoryEvent
		if !response.HasQuery() || !opts.PollSticky {
			// if not query task, should have some history events
			// for non sticky query, there should be events returned
			history := response.GetHistory()
			if history == nil {
				p.Logger.Fatal("History is nil")
				return PollAndProcessWorkflowTaskResponse{}, errors.New("history is nil")
			}

			events = history.GetEvents()
			if len(events) == 0 {
				p.Logger.Fatal("History Events are empty")
				return PollAndProcessWorkflowTaskResponse{}, errors.New("history events are empty")
			}

			nextPageToken := response.GetNextPageToken()
			for nextPageToken != nil {
				resp, err2 := p.Client.GetWorkflowExecutionHistory(NewContext(), workflowservice.GetWorkflowExecutionHistoryRequest_builder{
					Namespace:     p.Namespace,
					Execution:     response.GetWorkflowExecution(),
					NextPageToken: nextPageToken,
				}.Build())

				if err2 != nil {
					return PollAndProcessWorkflowTaskResponse{}, err2
				}

				events = append(events, resp.GetHistory().GetEvents()...)
				nextPageToken = resp.GetNextPageToken()
			}
		} else {
			// for sticky query, there should be NO events returned
			// since worker side already has the state machine and we do not intend to update that.
			history := response.GetHistory()
			nextPageToken := response.GetNextPageToken()
			if !(history == nil || (len(history.GetEvents()) == 0 && nextPageToken == nil)) {
				// if history is not nil, and contains events or next token
				p.Logger.Fatal("History is not empty for sticky query")
			}
		}

		if opts.DropTask {
			p.Logger.Info("Dropping Workflow task: ")
			return PollAndProcessWorkflowTaskResponse{}, nil
		}

		if opts.DumpHistory {
			common.PrettyPrint(response.GetHistory().GetEvents())
		}

		// handle query task response
		if response.HasQuery() {
			blob, err := p.QueryHandler(response)

			completeRequest := workflowservice.RespondQueryTaskCompletedRequest_builder{
				Namespace: p.Namespace,
				TaskToken: response.GetTaskToken(),
			}.Build()
			if err != nil {
				completeType := enumspb.QUERY_RESULT_TYPE_FAILED
				completeRequest.SetCompletedType(completeType)
				completeRequest.SetErrorMessage(err.Error())
			} else {
				completeType := enumspb.QUERY_RESULT_TYPE_ANSWERED
				completeRequest.SetCompletedType(completeType)
				completeRequest.SetQueryResult(blob)
			}

			_, err = p.Client.RespondQueryTaskCompleted(NewContext(), completeRequest)
			return PollAndProcessWorkflowTaskResponse{IsQueryTask: true}, err
		}

		// Handle messages.
		var workerToServerMessages []*protocolpb.Message
		if p.MessageHandler != nil {
			workerToServerMessages, err = p.MessageHandler(response)
			if err != nil {
				p.Logger.Error("Failing workflow task. Workflow messages handler failed with error", tag.Error(err))
				_, err = p.Client.RespondWorkflowTaskFailed(NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
					Namespace: p.Namespace,
					TaskToken: response.GetTaskToken(),
					Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
					Failure:   newApplicationFailure(err, false, nil),
					Identity:  p.Identity,
				}.Build())
				return PollAndProcessWorkflowTaskResponse{}, err
			}
		}

		// handle normal workflow task / non query task response
		var lastWorkflowTaskScheduleEvent *historypb.HistoryEvent
		for _, e := range events {
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				lastWorkflowTaskScheduleEvent = e
			}
		}
		if lastWorkflowTaskScheduleEvent != nil && opts.ExpectedAttemptCount > 1 {
			require.Equal(p.T, opts.ExpectedAttemptCount, int(lastWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt()))
		}

		commands, err := p.WorkflowTaskHandler(response)
		if err != nil {
			p.Logger.Error("Failing workflow task. Workflow task handler failed with error", tag.Error(err))
			_, err = p.Client.RespondWorkflowTaskFailed(NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
				Namespace: p.Namespace,
				TaskToken: response.GetTaskToken(),
				Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
				Failure:   newApplicationFailure(err, false, nil),
				Identity:  p.Identity,
			}.Build())
			return PollAndProcessWorkflowTaskResponse{}, err
		}
		if opts.DumpCommands {
			if len(commands) > 0 {
				common.PrettyPrint(commands, "Send commands to server using RespondWorkflowTaskCompleted:")
			}
			if len(workerToServerMessages) > 0 {
				common.PrettyPrint(workerToServerMessages, "Send messages to server using RespondWorkflowTaskCompleted:")
			}
		}

		if !opts.RespondSticky {
			// non sticky taskqueue
			newTask, err := p.Client.RespondWorkflowTaskCompleted(NewContext(), workflowservice.RespondWorkflowTaskCompletedRequest_builder{
				Namespace:                  p.Namespace,
				TaskToken:                  response.GetTaskToken(),
				Identity:                   p.Identity,
				Commands:                   commands,
				Messages:                   workerToServerMessages,
				ReturnNewWorkflowTask:      true,
				ForceCreateNewWorkflowTask: opts.ForceNewWorkflowTask,
				QueryResults:               getQueryResults(response.GetQueries(), opts.QueryResult),
			}.Build())
			return PollAndProcessWorkflowTaskResponse{NewTask: newTask}, err
		}
		// sticky taskqueue
		newTask, err := p.Client.RespondWorkflowTaskCompleted(
			NewContext(),
			workflowservice.RespondWorkflowTaskCompletedRequest_builder{
				Namespace: p.Namespace,
				TaskToken: response.GetTaskToken(),
				Identity:  p.Identity,
				Commands:  commands,
				StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
					WorkerTaskQueue:        p.StickyTaskQueue,
					ScheduleToStartTimeout: durationpb.New(p.StickyScheduleToStartTimeout),
				}.Build(),
				ReturnNewWorkflowTask:      true,
				ForceCreateNewWorkflowTask: opts.ForceNewWorkflowTask,
				QueryResults:               getQueryResults(response.GetQueries(), opts.QueryResult),
			}.Build(),
		)

		return PollAndProcessWorkflowTaskResponse{NewTask: newTask}, err
	}

	return PollAndProcessWorkflowTaskResponse{}, ErrNoTasks
}

// HandlePartialWorkflowTask for workflow task
func (p *TaskPoller) HandlePartialWorkflowTask(response *workflowservice.PollWorkflowTaskQueueResponse, forceCreateNewWorkflowTask bool) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	if response == nil || len(response.GetTaskToken()) == 0 {
		p.Logger.Info("Empty Workflow task: Polling again")
		return nil, nil
	}

	var events []*historypb.HistoryEvent
	history := response.GetHistory()
	if history == nil {
		p.Logger.Fatal("History is nil")
		return nil, errors.New("history is nil")
	}

	events = history.GetEvents()
	if len(events) == 0 {
		p.Logger.Fatal("History Events are empty")
		return nil, errors.New("history events are empty")
	}

	// Handle messages.
	var workerToServerMessages []*protocolpb.Message
	if p.MessageHandler != nil {
		var err error
		workerToServerMessages, err = p.MessageHandler(response)
		if err != nil {
			p.Logger.Error("Failing workflow task. Workflow messages handler failed with error", tag.Error(err))
			_, err = p.Client.RespondWorkflowTaskFailed(NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
				Namespace: p.Namespace,
				TaskToken: response.GetTaskToken(),
				Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
				Failure:   newApplicationFailure(err, false, nil),
				Identity:  p.Identity,
			}.Build())
			return nil, err
		}
	}

	commands, err := p.WorkflowTaskHandler(response)
	if err != nil {
		p.Logger.Error("Failing workflow task. Workflow task handler failed with error", tag.Error(err))
		_, err = p.Client.RespondWorkflowTaskFailed(NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
			Namespace: p.Namespace,
			TaskToken: response.GetTaskToken(),
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Failure:   newApplicationFailure(err, false, nil),
			Identity:  p.Identity,
		}.Build())
		return nil, err
	}
	if len(commands) > 0 {
		common.PrettyPrint(commands, "Send commands to server using RespondWorkflowTaskCompleted:")
	}
	if len(workerToServerMessages) > 0 {
		common.PrettyPrint(workerToServerMessages, "Send messages to server using RespondWorkflowTaskCompleted:")
	}

	// sticky taskqueue
	newTask, err := p.Client.RespondWorkflowTaskCompleted(
		NewContext(),
		workflowservice.RespondWorkflowTaskCompletedRequest_builder{
			Namespace: p.Namespace,
			TaskToken: response.GetTaskToken(),
			Identity:  p.Identity,
			Commands:  commands,
			Messages:  workerToServerMessages,
			StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
				WorkerTaskQueue:        p.StickyTaskQueue,
				ScheduleToStartTimeout: durationpb.New(p.StickyScheduleToStartTimeout),
			}.Build(),
			ReturnNewWorkflowTask:      true,
			ForceCreateNewWorkflowTask: forceCreateNewWorkflowTask,
		}.Build(),
	)

	return newTask, err
}

// PollAndProcessActivityTask for activity tasks
func (p *TaskPoller) PollAndProcessActivityTask(dropTask bool) error {
retry:
	for attempt := 1; attempt <= 5; attempt++ {
		response, err := p.Client.PollActivityTaskQueue(NewContext(), workflowservice.PollActivityTaskQueueRequest_builder{
			Namespace: p.Namespace,
			TaskQueue: p.TaskQueue,
			Identity:  p.Identity,
		}.Build())

		if err == consts.ErrDuplicate {
			p.Logger.Info("Duplicate Activity task: Polling again")
			continue retry
		}

		if err != nil {
			return err
		}

		if response == nil || len(response.GetTaskToken()) == 0 {
			p.Logger.Info("Empty Activity task: Polling again")
			continue retry
		}

		if dropTask {
			p.Logger.Info("Dropping Activity task: ")
			return nil
		}
		p.Logger.Debug("Received Activity task", tag.Value(response))

		result, cancel, err2 := p.ActivityTaskHandler(response)
		if cancel {
			p.Logger.Info("Executing RespondActivityTaskCanceled")
			_, err := p.Client.RespondActivityTaskCanceled(NewContext(), workflowservice.RespondActivityTaskCanceledRequest_builder{
				Namespace: p.Namespace,
				TaskToken: response.GetTaskToken(),
				Details:   payloads.EncodeString("details"),
				Identity:  p.Identity,
			}.Build())
			return err
		}

		if err2 != nil {
			_, err := p.Client.RespondActivityTaskFailed(NewContext(), workflowservice.RespondActivityTaskFailedRequest_builder{
				Namespace: p.Namespace,
				TaskToken: response.GetTaskToken(),
				Failure:   newApplicationFailure(err2, false, nil),
				Identity:  p.Identity,
			}.Build())
			return err
		}

		_, err = p.Client.RespondActivityTaskCompleted(NewContext(), workflowservice.RespondActivityTaskCompletedRequest_builder{
			Namespace: p.Namespace,
			TaskToken: response.GetTaskToken(),
			Identity:  p.Identity,
			Result:    result,
		}.Build())
		return err
	}

	return ErrNoTasks
}

// PollAndProcessActivityTaskWithID is similar to PollAndProcessActivityTask but using RespondActivityTask...ByID
func (p *TaskPoller) PollAndProcessActivityTaskWithID(dropTask bool) error {
retry:
	for attempt := 1; attempt <= 5; attempt++ {
		response, err1 := p.Client.PollActivityTaskQueue(NewContext(), workflowservice.PollActivityTaskQueueRequest_builder{
			Namespace: p.Namespace,
			TaskQueue: p.TaskQueue,
			Identity:  p.Identity,
		}.Build())

		if err1 == consts.ErrDuplicate {
			p.Logger.Info("Duplicate Activity task: Polling again")
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || len(response.GetTaskToken()) == 0 {
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

		result, cancel, err2 := p.ActivityTaskHandler(response)
		if cancel {
			p.Logger.Info("Executing RespondActivityTaskCanceled")
			_, err := p.Client.RespondActivityTaskCanceledById(NewContext(), workflowservice.RespondActivityTaskCanceledByIdRequest_builder{
				Namespace:  p.Namespace,
				WorkflowId: response.GetWorkflowExecution().GetWorkflowId(),
				RunId:      response.GetWorkflowExecution().GetRunId(),
				ActivityId: response.GetActivityId(),
				Details:    payloads.EncodeString("details"),
				Identity:   p.Identity,
			}.Build())
			return err
		}

		if err2 != nil {
			_, err := p.Client.RespondActivityTaskFailedById(NewContext(), workflowservice.RespondActivityTaskFailedByIdRequest_builder{
				Namespace:  p.Namespace,
				WorkflowId: response.GetWorkflowExecution().GetWorkflowId(),
				RunId:      response.GetWorkflowExecution().GetRunId(),
				ActivityId: response.GetActivityId(),
				Failure:    newApplicationFailure(err2, false, nil),
				Identity:   p.Identity,
			}.Build())
			return err
		}

		_, err := p.Client.RespondActivityTaskCompletedById(NewContext(), workflowservice.RespondActivityTaskCompletedByIdRequest_builder{
			Namespace:  p.Namespace,
			WorkflowId: response.GetWorkflowExecution().GetWorkflowId(),
			RunId:      response.GetWorkflowExecution().GetRunId(),
			ActivityId: response.GetActivityId(),
			Identity:   p.Identity,
			Result:     result,
		}.Build())
		return err
	}

	return ErrNoTasks
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

	f := failurepb.Failure_builder{
		Message: err.Error(),
		Source:  "Functional Tests",
		ApplicationFailureInfo: failurepb.ApplicationFailureInfo_builder{
			Type:         getErrorType(err),
			NonRetryable: nonRetryable,
			Details:      details,
		}.Build(),
	}.Build()

	return f
}

func getErrorType(err error) string {
	var t reflect.Type
	for t = reflect.TypeOf(err); t.Kind() == reflect.Ptr; t = t.Elem() {
	}

	return t.Name()
}
