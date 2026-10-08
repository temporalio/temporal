// Package localserver serves the worker-facing and client-facing RPCs needed to run a workflow
// that uses activities and timers, against an in-memory CHASM engine running native workflows
// (workflow.NewNativeWorkflow). Polls do not block: when no task is available they return an
// empty response, and the host decides when to poll again.
package localserver

import (
	"context"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/timer"
	"go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc"
)

// maxIDLengthLimit is the server's default limit.maxIDLength.
const maxIDLengthLimit = 1000

type Server struct {
	engine        *engine
	workflowTasks map[string][]queuedWorkflowTask
	activityTasks map[string][]*matchingservice.AddActivityTaskRequest
	newRunID      func() string
}

type queuedWorkflowTask struct {
	ref   chasm.ComponentRef
	stamp int32
}

func New(now time.Time, newRunID func() string) (*Server, error) {
	s := &Server{
		workflowTasks: map[string][]queuedWorkflowTask{},
		activityTasks: map[string][]*matchingservice.AddActivityTaskRequest{},
		newRunID:      newRunID,
	}
	workflowRegistry := workflow.NewRegistry()
	for _, lib := range []workflow.Library{
		workflow.NewActivityLibrary(workflow.NewConfig(dynamicconfig.NewNoopCollection())),
		workflow.NewTimerLibrary(),
	} {
		if err := workflowRegistry.Register(lib); err != nil {
			return nil, err
		}
	}
	registry := chasm.NewRegistry(log.NewNoopLogger())
	for _, lib := range []chasm.Library{
		&chasm.CoreLibrary{},
		activity.NewLibrary(matchingClient{server: s}, &activity.Config{
			BreakdownMetricsByTaskQueue:               func(string, string, enumspb.TaskQueueType) bool { return false },
			MutableStateActivityFailureSizeLimitError: func(string) int { return 4 << 20 },
			StartDelayEnabled:                         func(string) bool { return false },
		}),
		timer.NewLibrary(),
		workflow.NewNativeLibrary(workflowRegistry, s),
		callback.NewNilLibrary(),
		nexusoperation.NewNilLibrary(),
	} {
		if err := registry.Register(lib); err != nil {
			return nil, err
		}
	}
	s.engine = newEngine(registry, now)
	return s, nil
}

func (s *Server) ctx(ctx context.Context) context.Context {
	return chasm.NewEngineContext(ctx, s.engine)
}

// AdvanceTime sets the server clock and fires due timers and timeouts.
func (s *Server) AdvanceTime(ctx context.Context, now time.Time) error {
	return s.engine.advanceTime(s.ctx(ctx), now)
}

func (s *Server) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	if err := tqid.NormalizeAndValidateUserDefined(request.GetTaskQueue(), "", "", maxIDLengthLimit); err != nil {
		return nil, err
	}
	result, err := chasm.StartExecution(
		s.ctx(ctx),
		chasm.ExecutionKey{NamespaceID: request.GetNamespace(), BusinessID: request.GetWorkflowId(), RunID: s.newRunID()},
		workflow.NewNativeWorkflow,
		request,
	)
	if err != nil {
		return nil, err
	}
	return &workflowservice.StartWorkflowExecutionResponse{RunId: result.ExecutionKey.RunID, Started: result.Created}, nil
}

// NextDeadline returns the time at which the next task is due, if any.
func (s *Server) NextDeadline() (time.Time, bool) {
	return s.engine.nextDeadline()
}

func (s *Server) GetSystemInfo(
	context.Context,
	*workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{
		Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{
			ActivityFailureIncludeHeartbeat: true,
			EncodedFailureAttributes:        true,
			SdkMetadata:                     true,
		},
	}, nil
}

// DescribeNamespace describes any namespace as registered: the local server has no namespace
// registry.
func (s *Server) DescribeNamespace(
	_ context.Context,
	request *workflowservice.DescribeNamespaceRequest,
) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespacepb.NamespaceInfo{
			Name:  request.GetNamespace(),
			Id:    request.GetNamespace(),
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
	}, nil
}

// ShutdownWorker does nothing: the local server has no sticky queues.
func (s *Server) ShutdownWorker(
	context.Context,
	*workflowservice.ShutdownWorkerRequest,
) (*workflowservice.ShutdownWorkerResponse, error) {
	return &workflowservice.ShutdownWorkerResponse{}, nil
}

// DeleteWorkflowExecution removes a closed workflow. Unlike the server, it does not terminate a
// running workflow first.
func (s *Server) DeleteWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DeleteWorkflowExecutionRequest,
) (*workflowservice.DeleteWorkflowExecutionResponse, error) {
	key := chasm.ExecutionKey{
		NamespaceID: request.GetNamespace(),
		BusinessID:  request.GetWorkflowExecution().GetWorkflowId(),
		RunID:       request.GetWorkflowExecution().GetRunId(),
	}
	if err := s.engine.deleteClosedExecution(key); err != nil {
		return nil, err
	}
	return &workflowservice.DeleteWorkflowExecutionResponse{}, nil
}

// GetWorkflowExecutionHistory returns the whole history, or with the close-event filter only the
// close event, which is absent while the workflow is running. It does not wait for new events.
func (s *Server) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	ref := chasm.NewComponentRef[*workflow.Workflow](chasm.ExecutionKey{
		NamespaceID: request.GetNamespace(),
		BusinessID:  request.GetExecution().GetWorkflowId(),
		RunID:       request.GetExecution().GetRunId(),
	})
	events, err := chasm.ReadComponent(s.ctx(ctx), ref, func(w *workflow.Workflow, ctx chasm.Context, _ struct{}) ([]*historypb.HistoryEvent, error) {
		return w.History(ctx), nil
	}, struct{}{})
	if err != nil {
		return nil, err
	}
	if request.GetHistoryEventFilterType() == enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT {
		events = slices.DeleteFunc(events, func(e *historypb.HistoryEvent) bool { return !isCloseEvent(e) })
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: events}}, nil
}

func (s *Server) PollWorkflowTaskQueue(
	ctx context.Context,
	request *workflowservice.PollWorkflowTaskQueueRequest,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	taskQueue := request.GetTaskQueue().GetName()
	for len(s.workflowTasks[taskQueue]) > 0 {
		task := s.workflowTasks[taskQueue][0]
		s.workflowTasks[taskQueue] = s.workflowTasks[taskQueue][1:]
		response, _, err := chasm.UpdateComponent(s.ctx(ctx), task.ref, func(
			w *workflow.Workflow,
			ctx chasm.MutableContext,
			request *workflowservice.PollWorkflowTaskQueueRequest,
		) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
			return w.StartWorkflowTask(ctx, request, task.stamp)
		}, request)
		if _, notFound := err.(*serviceerror.NotFound); notFound {
			continue // The task is stale.
		}
		return response, err
	}
	return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
}

func (s *Server) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	token, ref, err := s.decodeToken(request.GetTaskToken())
	if err != nil {
		return nil, err
	}
	_, _, err = chasm.UpdateComponent(s.ctx(ctx), ref, func(w *workflow.Workflow, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
		return struct{}{}, w.CompleteWorkflowTask(ctx, token, request)
	}, struct{}{})
	if cause, taskFailure, ok := workflow.WorkflowTaskFailure(err); ok {
		// As in the server, the workflow task fails in a new transaction.
		err = s.failWorkflowTask(ctx, ref, token, cause, taskFailure, request.GetIdentity())
	}
	if err != nil {
		return nil, err
	}
	return &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil
}

func (s *Server) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
) (*workflowservice.RespondWorkflowTaskFailedResponse, error) {
	token, ref, err := s.decodeToken(request.GetTaskToken())
	if err != nil {
		return nil, err
	}
	if err := s.failWorkflowTask(ctx, ref, token, request.GetCause(), request.GetFailure(), request.GetIdentity()); err != nil {
		return nil, err
	}
	return &workflowservice.RespondWorkflowTaskFailedResponse{}, nil
}

func (s *Server) failWorkflowTask(
	ctx context.Context,
	ref []byte,
	token *tokenspb.Task,
	cause enumspb.WorkflowTaskFailedCause,
	taskFailure *failurepb.Failure,
	identity string,
) error {
	_, _, err := chasm.UpdateComponent(s.ctx(ctx), ref, func(w *workflow.Workflow, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
		return struct{}{}, w.FailWorkflowTask(ctx, token, cause, taskFailure, identity)
	}, struct{}{})
	return err
}

func (s *Server) PollActivityTaskQueue(
	ctx context.Context,
	request *workflowservice.PollActivityTaskQueueRequest,
) (*workflowservice.PollActivityTaskQueueResponse, error) {
	taskQueue := request.GetTaskQueue().GetName()
	for len(s.activityTasks[taskQueue]) > 0 {
		task := s.activityTasks[taskQueue][0]
		s.activityTasks[taskQueue] = s.activityTasks[taskQueue][1:]
		response, _, err := chasm.UpdateComponent(s.ctx(ctx), task.ComponentRef, startActivityTask, &historyservice.RecordActivityTaskStartedRequest{
			RequestId:   s.newRunID(),
			Stamp:       task.Stamp,
			PollRequest: request,
		})
		if _, obsolete := err.(*serviceerror.NotFound); obsolete {
			continue
		}
		return response, err
	}
	return &workflowservice.PollActivityTaskQueueResponse{}, nil
}

func (s *Server) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	token, ref, err := s.decodeToken(request.GetTaskToken())
	if err != nil {
		return nil, err
	}
	_, _, err = chasm.UpdateComponent(s.ctx(ctx), ref, (*activity.Activity).HandleCompleted, activity.RespondCompletedEvent{
		Token:   token,
		Request: &historyservice.RespondActivityTaskCompletedRequest{NamespaceId: token.GetNamespaceId(), CompleteRequest: request},
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}

func (s *Server) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	token, ref, err := s.decodeToken(request.GetTaskToken())
	if err != nil {
		return nil, err
	}
	_, _, err = chasm.UpdateComponent(s.ctx(ctx), ref, (*activity.Activity).HandleFailed, activity.RespondFailedEvent{
		Token:   token,
		Request: &historyservice.RespondActivityTaskFailedRequest{NamespaceId: token.GetNamespaceId(), FailedRequest: request},
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.RespondActivityTaskFailedResponse{}, nil
}

// matchingClient queues the activity tasks that the activity library dispatches. It implements
// only AddActivityTask.
type matchingClient struct {
	matchingservice.MatchingServiceClient
	server *Server
}

func (c matchingClient) AddActivityTask(
	_ context.Context,
	request *matchingservice.AddActivityTaskRequest,
	_ ...grpc.CallOption,
) (*matchingservice.AddActivityTaskResponse, error) {
	taskQueue := request.GetTaskQueue().GetName()
	c.server.activityTasks[taskQueue] = append(c.server.activityTasks[taskQueue], request)
	return &matchingservice.AddActivityTaskResponse{}, nil
}

// AddWorkflowTask implements workflow.WorkflowTaskDispatcher.
func (s *Server) AddWorkflowTask(_ context.Context, taskQueue *taskqueuepb.TaskQueue, ref chasm.ComponentRef, stamp int32) error {
	name := taskQueue.GetName()
	s.workflowTasks[name] = append(s.workflowTasks[name], queuedWorkflowTask{ref: ref, stamp: stamp})
	return nil
}

func isCloseEvent(event *historypb.HistoryEvent) bool {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return true
	default:
		return false
	}
}

func (s *Server) decodeToken(serialized []byte) (*tokenspb.Task, []byte, error) {
	token := &tokenspb.Task{}
	if err := token.Unmarshal(serialized); err != nil {
		return nil, nil, serviceerror.NewInvalidArgument("malformed task token")
	}
	return token, token.GetComponentRef(), nil
}

func startActivityTask(
	a *activity.Activity,
	ctx chasm.MutableContext,
	request *historyservice.RecordActivityTaskStartedRequest,
) (*workflowservice.PollActivityTaskQueueResponse, error) {
	started, err := a.HandleStarted(ctx, request)
	if err != nil {
		return nil, err
	}
	w, ok := a.Store.Get(ctx).(*workflow.Workflow)
	if !ok {
		return nil, serviceerror.NewInternal("activity is not embedded in a workflow")
	}
	scheduledEventID, _ := w.ActivityScheduledEventID(ctx, a)
	scheduledEvent, ok := w.HistoryEvent(ctx, scheduledEventID)
	if !ok {
		return nil, serviceerror.NewInternal("activity not found in workflow")
	}
	scheduled := scheduledEvent.GetActivityTaskScheduledEventAttributes()
	ref, err := ctx.Ref(a)
	if err != nil {
		return nil, err
	}
	key := ctx.ExecutionKey()
	token, err := (&tokenspb.Task{
		NamespaceId:          key.NamespaceID,
		WorkflowId:           key.BusinessID,
		RunId:                key.RunID,
		ScheduledEventId:     scheduledEvent.GetEventId(),
		Attempt:              started.Attempt,
		ActivityId:           scheduled.GetActivityId(),
		ActivityType:         scheduled.GetActivityType().GetName(),
		ComponentRef:         ref,
		ActivityAttemptStamp: a.LastAttempt.Get(ctx).GetStartedStamp(),
	}).Marshal()
	if err != nil {
		return nil, err
	}
	return &workflowservice.PollActivityTaskQueueResponse{
		TaskToken:                   token,
		WorkflowNamespace:           key.NamespaceID,
		WorkflowType:                &commonpb.WorkflowType{Name: w.Native.Get(ctx).GetWorkflowType()},
		WorkflowExecution:           &commonpb.WorkflowExecution{WorkflowId: key.BusinessID, RunId: key.RunID},
		ActivityType:                scheduled.GetActivityType(),
		ActivityId:                  scheduled.GetActivityId(),
		Header:                      scheduled.GetHeader(),
		Input:                       scheduled.GetInput(),
		HeartbeatDetails:            started.HeartbeatDetails,
		ScheduledTime:               scheduledEvent.GetEventTime(),
		CurrentAttemptScheduledTime: started.CurrentAttemptScheduledTime,
		StartedTime:                 started.StartedTime,
		Attempt:                     started.Attempt,
		ScheduleToCloseTimeout:      scheduled.GetScheduleToCloseTimeout(),
		StartToCloseTimeout:         scheduled.GetStartToCloseTimeout(),
		HeartbeatTimeout:            scheduled.GetHeartbeatTimeout(),
		RetryPolicy:                 started.RetryPolicy,
		Priority:                    started.Priority,
	}, nil
}
