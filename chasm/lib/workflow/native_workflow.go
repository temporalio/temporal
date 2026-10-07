package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/timer"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/failure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A native workflow holds its history events and its workflow task in its CHASM tree, rather than
// in mutable state. It supports activities and timers, and completing and failing the workflow;
// it has no sticky queues and records no worker version stamps. The server does not create native
// workflows: only the in-memory local server does, until the server's workflow implementation
// moves to CHASM.

type nativeWorkflowState = chasmworkflowpb.NativeWorkflowState

const defaultNativeWorkflowTaskTimeout = 10 * time.Second

// WorkflowTaskDispatcher receives the scheduled workflow tasks of native workflows.
type WorkflowTaskDispatcher interface {
	AddWorkflowTask(ctx context.Context, taskQueue *taskqueuepb.TaskQueue, ref chasm.ComponentRef, stamp int32) error
}

// NewNativeWorkflow starts a native workflow.
func NewNativeWorkflow(
	ctx chasm.MutableContext,
	request *workflowservice.StartWorkflowExecutionRequest,
) (*Workflow, error) {
	taskTimeout := request.GetWorkflowTaskTimeout()
	if taskTimeout.AsDuration() == 0 {
		taskTimeout = durationpb.New(defaultNativeWorkflowTaskTimeout)
	}
	state := &nativeWorkflowState{
		Status:              enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		WorkflowType:        request.GetWorkflowType().GetName(),
		TaskQueue:           request.GetTaskQueue().GetName(),
		WorkflowTaskTimeout: taskTimeout,
		WorkflowRunTimeout:  request.GetWorkflowRunTimeout(),
		NextEventId:         1,
	}
	w := newNativeWorkflow(ctx, state)
	key := ctx.ExecutionKey()
	w.appendEvent(ctx, state, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:             request.GetWorkflowType(),
				TaskQueue:                request.GetTaskQueue(),
				Input:                    request.GetInput(),
				WorkflowExecutionTimeout: request.GetWorkflowExecutionTimeout(),
				WorkflowRunTimeout:       request.GetWorkflowRunTimeout(),
				WorkflowTaskTimeout:      taskTimeout,
				Identity:                 request.GetIdentity(),
				Header:                   request.GetHeader(),
				Attempt:                  1,
				FirstWorkflowTaskBackoff: durationpb.New(0),
				OriginalExecutionRunId:   key.RunID,
				FirstExecutionRunId:      key.RunID,
				WorkflowId:               key.BusinessID,
				Priority:                 request.GetPriority(),
			},
		}
	})
	w.scheduleWorkflowTask(ctx, state, 1)
	return w, nil
}

// NewImportedNativeWorkflow creates a native workflow from the history of a run that a server
// started: its WorkflowExecutionStarted event and its first scheduled workflow task.
func NewImportedNativeWorkflow(
	ctx chasm.MutableContext,
	events []*historypb.HistoryEvent,
) (*Workflow, error) {
	if len(events) != 2 ||
		events[0].GetEventId() != 1 || events[0].GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED ||
		events[1].GetEventId() != 2 || events[1].GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
		return nil, serviceerror.NewInvalidArgument("imported history must be a started event followed by a scheduled workflow task")
	}
	started := events[0].GetWorkflowExecutionStartedEventAttributes()
	state := &nativeWorkflowState{
		Status:              enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		WorkflowType:        started.GetWorkflowType().GetName(),
		TaskQueue:           started.GetTaskQueue().GetName(),
		WorkflowTaskTimeout: started.GetWorkflowTaskTimeout(),
		WorkflowRunTimeout:  started.GetWorkflowRunTimeout(),
		NextEventId:         1,
	}
	w := newNativeWorkflow(ctx, state)
	w.appendEvents(ctx, state, events)
	w.setWorkflowTaskScheduled(ctx, state, 2, events[1].GetWorkflowTaskScheduledEventAttributes().GetAttempt())
	return w, nil
}

func newNativeWorkflow(ctx chasm.MutableContext, state *nativeWorkflowState) *Workflow {
	return &Workflow{
		Native:        chasm.NewDataField(ctx, state),
		NativeHistory: chasm.Map[int64, *historypb.HistoryEvent]{},
		Activities:    chasm.Map[int64, *activity.Activity]{},
		Timers:        chasm.Map[string, *timer.Timer]{},
	}
}

func nativeLifecycleState(state *nativeWorkflowState) chasm.LifecycleState {
	switch state.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return chasm.LifecycleStateCompleted
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// scheduleWorkflowTask adds a WorkflowTaskScheduled event and a task to dispatch it, unless a
// workflow task is already scheduled.
func (w *Workflow) scheduleWorkflowTask(ctx chasm.MutableContext, state *nativeWorkflowState, attempt int32) {
	if state.WorkflowTaskScheduledEventId != 0 || state.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return
	}
	event := w.appendEvent(ctx, state, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           nativeTaskQueue(state),
				StartToCloseTimeout: state.WorkflowTaskTimeout,
				Attempt:             attempt,
			},
		}
	})
	w.setWorkflowTaskScheduled(ctx, state, event.EventId, attempt)
}

func (w *Workflow) setWorkflowTaskScheduled(ctx chasm.MutableContext, state *nativeWorkflowState, eventID int64, attempt int32) {
	state.WorkflowTaskScheduledEventId = eventID
	state.WorkflowTaskAttempt = attempt
	state.WorkflowTaskStamp++
	ctx.AddTask(w, chasm.TaskAttributes{}, &chasmworkflowpb.WorkflowTaskDispatchTask{Stamp: state.WorkflowTaskStamp})
}

// StartWorkflowTask records that a worker polled the scheduled workflow task and returns the poll
// response, carrying the full history.
func (w *Workflow) StartWorkflowTask(
	ctx chasm.MutableContext,
	request *workflowservice.PollWorkflowTaskQueueRequest,
	stamp int32,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	state := w.Native.Get(ctx)
	if state.WorkflowTaskScheduledEventId == 0 || state.WorkflowTaskStartedEventId != 0 || stamp != state.WorkflowTaskStamp {
		return nil, serviceerror.NewNotFound("workflow task not found")
	}
	scheduledEvent := w.NativeHistory[state.WorkflowTaskScheduledEventId].Get(ctx)
	startedEvent := w.appendEvent(ctx, state, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: state.WorkflowTaskScheduledEventId,
				Identity:         request.GetIdentity(),
				HistorySizeBytes: state.HistorySizeBytes,
			},
		}
	})
	state.WorkflowTaskStartedEventId = startedEvent.EventId

	ref, err := ctx.Ref(w)
	if err != nil {
		return nil, err
	}
	key := ctx.ExecutionKey()
	token, err := (&tokenspb.Task{
		NamespaceId:      key.NamespaceID,
		WorkflowId:       key.BusinessID,
		RunId:            key.RunID,
		ScheduledEventId: state.WorkflowTaskScheduledEventId,
		StartedEventId:   state.WorkflowTaskStartedEventId,
		Attempt:          state.WorkflowTaskAttempt,
		ComponentRef:     ref,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	return &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:                  token,
		WorkflowExecution:          &commonpb.WorkflowExecution{WorkflowId: key.BusinessID, RunId: key.RunID},
		WorkflowType:               &commonpb.WorkflowType{Name: state.WorkflowType},
		PreviousStartedEventId:     state.LastCompletedWorkflowTaskStartedEventId,
		StartedEventId:             state.WorkflowTaskStartedEventId,
		Attempt:                    state.WorkflowTaskAttempt,
		History:                    &historypb.History{Events: w.History(ctx)},
		WorkflowExecutionTaskQueue: nativeTaskQueue(state),
		ScheduledTime:              scheduledEvent.GetEventTime(),
		StartedTime:                startedEvent.GetEventTime(),
	}, nil
}

// CompleteWorkflowTask applies the worker's commands. Commands other than completing or failing
// the workflow are handled by the command handlers of the workflow Registry, which get no
// Validator, so payload size limits are not enforced. A FailWorkflowTaskError from a handler is
// returned: the caller must discard this transaction and fail the workflow task.
func (w *Workflow) CompleteWorkflowTask(
	ctx chasm.MutableContext,
	token *tokenspb.Task,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) error {
	state := w.Native.Get(ctx)
	if err := validateWorkflowTaskToken(state, token); err != nil {
		return err
	}
	if len(state.BufferedEvents) > 0 && closesWorkflow(request.GetCommands()) {
		// As in the server: the workflow must see the buffered events before it may close.
		return FailWorkflowTaskError{Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND}
	}
	completedEvent := w.appendEvent(ctx, state, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: state.WorkflowTaskScheduledEventId,
				StartedEventId:   state.WorkflowTaskStartedEventId,
				Identity:         request.GetIdentity(),
				BinaryChecksum:   request.GetBinaryChecksum(), //nolint:staticcheck // deprecated, but the server still records it
				SdkMetadata:      request.GetSdkMetadata(),
				MeteringMetadata: request.GetMeteringMetadata(),
			},
		}
	})
	state.LastCompletedWorkflowTaskStartedEventId = state.WorkflowTaskStartedEventId
	clearWorkflowTask(state)

	registry := workflowContextFromChasm(ctx).registry
	opts := CommandHandlerOptions{WorkflowTaskCompletedEventID: completedEvent.EventId}
	for _, command := range request.GetCommands() {
		if err := w.handleNativeCommand(ctx, state, registry, command, opts); err != nil {
			return err
		}
	}
	if w.flushBufferedEvents(ctx, state) || request.GetForceCreateNewWorkflowTask() {
		w.scheduleWorkflowTask(ctx, state, 1)
	}
	return nil
}

func (w *Workflow) handleNativeCommand(
	ctx chasm.MutableContext,
	state *nativeWorkflowState,
	registry *Registry,
	command *commandpb.Command,
	opts CommandHandlerOptions,
) error {
	switch command.GetCommandType() {
	case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		w.appendEvent(ctx, state, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
					Result:                       command.GetCompleteWorkflowExecutionCommandAttributes().GetResult(),
					WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
				},
			}
		})
		w.close(state, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
		return nil
	case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
		w.appendEvent(ctx, state, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
				WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					Failure:                      command.GetFailWorkflowExecutionCommandAttributes().GetFailure(),
					RetryState:                   enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
					WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
				},
			}
		})
		w.close(state, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED)
		return nil
	default:
		handler, ok := registry.CommandHandler(command.GetCommandType())
		if !ok {
			return serviceerror.NewUnimplementedf("command %v is not supported", command.GetCommandType())
		}
		return handler(ctx, w, nil, command, opts)
	}
}

// close removes the workflow's activities and timers, so that they neither run nor add events
// after the workflow closed.
func (w *Workflow) close(state *nativeWorkflowState, status enumspb.WorkflowExecutionStatus) {
	state.Status = status
	clear(w.Activities)
	clear(w.Timers)
}

// FailWorkflowTask adds a WorkflowTaskFailed event and schedules the next attempt. The server does
// not write events for attempts after the first (transient workflow tasks); this does.
func (w *Workflow) FailWorkflowTask(
	ctx chasm.MutableContext,
	token *tokenspb.Task,
	cause enumspb.WorkflowTaskFailedCause,
	taskFailure *failurepb.Failure,
	identity string,
) error {
	state := w.Native.Get(ctx)
	if err := validateWorkflowTaskToken(state, token); err != nil {
		return err
	}
	w.appendEvent(ctx, state, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				ScheduledEventId: state.WorkflowTaskScheduledEventId,
				StartedEventId:   state.WorkflowTaskStartedEventId,
				Cause:            cause,
				Failure:          taskFailure,
				Identity:         identity,
			},
		}
	})
	attempt := state.WorkflowTaskAttempt + 1
	clearWorkflowTask(state)
	w.flushBufferedEvents(ctx, state)
	w.scheduleWorkflowTask(ctx, state, attempt)
	return nil
}

// WorkflowTaskFailure returns the cause and failure with which to fail a workflow task whose
// completion returned err, as the server does, and whether err requires that.
func WorkflowTaskFailure(err error) (enumspb.WorkflowTaskFailedCause, *failurepb.Failure, bool) {
	failErr, ok := errors.AsType[FailWorkflowTaskError](err)
	if !ok {
		return enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, nil, false
	}
	message := failErr.Cause.String()
	if failErr.Message != "" {
		message = fmt.Sprintf("%v: %v", failErr.Cause, failErr.Message)
	}
	return failErr.Cause, failure.NewServerFailure(message, false), true
}

func nativeTaskQueue(state *nativeWorkflowState) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{Name: state.GetTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
}

func clearWorkflowTask(state *nativeWorkflowState) {
	state.WorkflowTaskScheduledEventId = 0
	state.WorkflowTaskStartedEventId = 0
}

func validateWorkflowTaskToken(state *nativeWorkflowState, token *tokenspb.Task) error {
	if state.WorkflowTaskStartedEventId == 0 ||
		token.GetScheduledEventId() != state.WorkflowTaskScheduledEventId ||
		token.GetStartedEventId() != state.WorkflowTaskStartedEventId {
		return serviceerror.NewNotFound("workflow task not found")
	}
	return nil
}

func closesWorkflow(commands []*commandpb.Command) bool {
	for _, command := range commands {
		switch command.GetCommandType() {
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
			return true
		default:
		}
	}
	return false
}

// workflowTaskDispatchTaskHandler hands a native workflow's scheduled workflow task to its task
// queue.
type workflowTaskDispatchTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*chasmworkflowpb.WorkflowTaskDispatchTask]
	dispatcher WorkflowTaskDispatcher
}

func (h *workflowTaskDispatchTaskHandler) Validate(
	ctx chasm.Context,
	w *Workflow,
	_ chasm.TaskInvocation,
	task *chasmworkflowpb.WorkflowTaskDispatchTask,
) (bool, error) {
	state := w.Native.Get(ctx)
	return state.WorkflowTaskScheduledEventId != 0 &&
		state.WorkflowTaskStartedEventId == 0 &&
		task.GetStamp() == state.WorkflowTaskStamp, nil
}

func (h *workflowTaskDispatchTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *chasmworkflowpb.WorkflowTaskDispatchTask,
) error {
	taskQueue, err := chasm.ReadComponent(ctx, ref, func(w *Workflow, ctx chasm.Context, _ struct{}) (*taskqueuepb.TaskQueue, error) {
		return nativeTaskQueue(w.Native.Get(ctx)), nil
	}, struct{}{})
	if err != nil {
		return err
	}
	return h.dispatcher.AddWorkflowTask(ctx, taskQueue, ref, task.GetStamp())
}
