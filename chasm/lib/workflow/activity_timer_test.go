package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// componentTestEnv runs a Workflow root in a CHASM tree whose backend records history events, with
// the activity command library registered.
type componentTestEnv struct {
	t        *testing.T
	node     *chasm.Node
	backend  *chasm.MockNodeBackend
	registry *Registry
	now      time.Time
	history  []*historypb.HistoryEvent
	matching *recordingMatchingClient
}

type recordingMatchingClient struct {
	matchingservice.MatchingServiceClient
	requests []*matchingservice.AddActivityTaskRequest
}

func (c *recordingMatchingClient) AddActivityTask(
	_ context.Context,
	request *matchingservice.AddActivityTaskRequest,
	_ ...grpc.CallOption,
) (*matchingservice.AddActivityTaskResponse, error) {
	c.requests = append(c.requests, request)
	return &matchingservice.AddActivityTaskResponse{}, nil
}

func newComponentTestEnv(t *testing.T) *componentTestEnv {
	env := &componentTestEnv{
		t:        t,
		now:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		registry: NewRegistry(),
		matching: &recordingMatchingClient{},
	}
	require.NoError(t, env.registry.Register(NewActivityLibrary(NewConfig(dynamicconfig.NewNoopCollection()))))

	chasmRegistry := newChasmTestRegistry(t, NewLibrary(env.registry), env.matching)

	execInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowId:  "workflow-id",
		TaskQueue:   "workflow-task-queue",
	}
	nextEventID := int64(1)
	env.backend = &chasm.MockNodeBackend{
		HandleGetExecutionInfo: func() *persistencespb.WorkflowExecutionInfo { return execInfo },
		HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState {
			return &persistencespb.WorkflowExecutionState{RunId: "run-id"}
		},
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(tests.NamespaceID.String(), "workflow-id", "run-id")
		},
		HandleGetNamespaceEntry: func() *namespace.Namespace { return tests.LocalNamespaceEntry },
		HandleNow:               func() time.Time { return env.now },
		HandleAddHistoryEvent: func(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
			event := &historypb.HistoryEvent{EventId: nextEventID, EventType: t, EventTime: timestamppb.New(env.now)}
			nextEventID++
			setAttributes(event)
			env.history = append(env.history, event)
			return event
		},
	}
	env.node = chasm.NewEmptyTree(chasmRegistry, env.backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	env.update(func(ctx chasm.MutableContext) {
		require.NoError(t, env.node.SetRootComponent(NewWorkflow(ctx, chasm.NewMSPointer(env.backend))))
	})
	return env
}

func newChasmTestRegistry(t *testing.T, workflowLibrary chasm.Library, matching matchingservice.MatchingServiceClient) *chasm.Registry {
	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	for _, lib := range []chasm.Library{
		&chasm.CoreLibrary{},
		workflowLibrary,
		activity.NewLibrary(matching, &activity.Config{
			BreakdownMetricsByTaskQueue:               func(string, string, enumspb.TaskQueueType) bool { return false },
			MutableStateActivityFailureSizeLimitError: func(string) int { return 1 << 20 },
			StartDelayEnabled:                         func(string) bool { return false },
		}),
		callback.NewNilLibrary(),
		nexusoperation.NewNilLibrary(),
	} {
		require.NoError(t, chasmRegistry.Register(lib))
	}
	return chasmRegistry
}

func (env *componentTestEnv) context() context.Context {
	return context.Background()
}

// update runs fn in a transaction on the tree.
func (env *componentTestEnv) update(fn func(ctx chasm.MutableContext)) {
	fn(chasm.NewMutableContext(env.context(), env.node))
	_, err := env.node.CloseTransaction()
	require.NoError(env.t, err)
}

func (env *componentTestEnv) workflow(ctx chasm.Context) *Workflow {
	component, err := env.node.ComponentByPath(ctx, nil)
	require.NoError(env.t, err)
	//nolint:revive // unchecked-type-assertion: the root is always a Workflow in these tests
	return component.(*Workflow)
}

func (env *componentTestEnv) handleCommand(ctx chasm.MutableContext, command *commandpb.Command) error {
	handler, ok := env.registry.CommandHandler(command.GetCommandType())
	require.True(env.t, ok)
	return handler(ctx, env.workflow(ctx), commandValidator{maxPayloadSize: 1 << 20}, command, CommandHandlerOptions{WorkflowTaskCompletedEventID: 100})
}

// runPureTasks advances the clock and runs the pure tasks that are due.
func (env *componentTestEnv) runPureTasks(now time.Time) {
	env.now = now
	require.NoError(env.t, env.node.EachPureTask(now, func(handler chasm.NodePureTask, attrs chasm.TaskAttributes, task any) (bool, error) {
		return handler.ExecutePureTask(env.context(), attrs, task)
	}))
	_, err := env.node.CloseTransaction()
	require.NoError(env.t, err)
}

func scheduleActivityCommand() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:          "activity-id",
				ActivityType:        &commonpb.ActivityType{Name: "activity-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "activity-task-queue"},
				Input:               payloads.EncodeString("input"),
				StartToCloseTimeout: durationpb.New(time.Minute),
			},
		},
	}
}

func TestScheduleActivityCommand(t *testing.T) {
	env := newComponentTestEnv(t)
	env.update(func(ctx chasm.MutableContext) {
		require.NoError(t, env.handleCommand(ctx, scheduleActivityCommand()))
	})

	require.Len(t, env.history, 1)
	scheduled := env.history[0]
	require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, scheduled.GetEventType())
	attrs := scheduled.GetActivityTaskScheduledEventAttributes()
	require.Equal(t, int64(100), attrs.GetWorkflowTaskCompletedEventId())
	// As in the server, the task queue kind and missing timeouts are filled in.
	require.Equal(t, "activity-task-queue", attrs.GetTaskQueue().GetName())
	require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, attrs.GetTaskQueue().GetKind())
	require.Equal(t, time.Minute, attrs.GetStartToCloseTimeout().AsDuration())
	require.NotNil(t, attrs.GetRetryPolicy().GetInitialInterval())

	ctx := chasm.NewContext(env.context(), env.node)
	wf := env.workflow(ctx)
	require.Len(t, wf.Activities, 1)
	a := wf.Activities[scheduled.GetEventId()].Get(ctx)
	id, ok := wf.ActivityScheduledEventID(ctx, a)
	require.True(t, ok)
	require.Equal(t, scheduled.GetEventId(), id)
	require.Len(t, env.backend.TasksByCategory[tasks.CategoryTransfer], 1, "dispatch task")
}

func TestScheduleActivityCommand_InvalidAttributes(t *testing.T) {
	env := newComponentTestEnv(t)
	command := scheduleActivityCommand()
	command.GetScheduleActivityTaskCommandAttributes().StartToCloseTimeout = nil
	env.update(func(ctx chasm.MutableContext) {
		err := env.handleCommand(ctx, command)
		var failErr FailWorkflowTaskError
		require.ErrorAs(t, err, &failErr)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, failErr.Cause)
	})
	require.Empty(t, env.history)
}

// startActivity starts the activity's attempt and returns a token for responding to it.
func (env *componentTestEnv) startActivity(scheduledEventID int64) *tokenspb.Task {
	var token *tokenspb.Task
	env.update(func(ctx chasm.MutableContext) {
		a := env.workflow(ctx).Activities[scheduledEventID].Get(ctx)
		_, err := a.HandleStarted(ctx, &historyservice.RecordActivityTaskStartedRequest{
			RequestId:   "start-request-id",
			Stamp:       a.LastAttempt.Get(ctx).GetStamp(),
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{Identity: "worker"},
		})
		require.NoError(env.t, err)
		ref, err := ctx.Ref(a)
		require.NoError(env.t, err)
		token = &tokenspb.Task{
			NamespaceId:          tests.NamespaceID.String(),
			Attempt:              a.LastAttempt.Get(ctx).GetCount(),
			ActivityAttemptStamp: a.LastAttempt.Get(ctx).GetStamp(),
			ComponentRef:         ref,
		}
	})
	return token
}

func TestActivityCompleted(t *testing.T) {
	env := newComponentTestEnv(t)
	env.update(func(ctx chasm.MutableContext) {
		require.NoError(t, env.handleCommand(ctx, scheduleActivityCommand()))
	})
	scheduledEventID := env.history[0].GetEventId()
	token := env.startActivity(scheduledEventID)
	require.Len(t, env.history, 1, "the started event is written with the outcome")

	result := payloads.EncodeString("result")
	env.update(func(ctx chasm.MutableContext) {
		a := env.workflow(ctx).Activities[scheduledEventID].Get(ctx)
		_, err := a.HandleCompleted(ctx, activity.RespondCompletedEvent{
			Token: token,
			Request: &historyservice.RespondActivityTaskCompletedRequest{
				NamespaceId:     tests.NamespaceID.String(),
				CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{Result: result, Identity: "worker"},
			},
		})
		require.NoError(t, err)
	})

	require.Len(t, env.history, 3)
	started, completed := env.history[1], env.history[2]
	require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, started.GetEventType())
	require.Equal(t, scheduledEventID, started.GetActivityTaskStartedEventAttributes().GetScheduledEventId())
	require.Equal(t, "worker", started.GetActivityTaskStartedEventAttributes().GetIdentity())
	require.Equal(t, "start-request-id", started.GetActivityTaskStartedEventAttributes().GetRequestId())
	require.Equal(t, int32(1), started.GetActivityTaskStartedEventAttributes().GetAttempt())
	require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, completed.GetEventType())
	completedAttrs := completed.GetActivityTaskCompletedEventAttributes()
	require.Equal(t, scheduledEventID, completedAttrs.GetScheduledEventId())
	require.Equal(t, started.GetEventId(), completedAttrs.GetStartedEventId())
	require.Equal(t, result, completedAttrs.GetResult())
	require.Equal(t, "worker", completedAttrs.GetIdentity())

	ctx := chasm.NewContext(env.context(), env.node)
	require.Empty(t, env.workflow(ctx).Activities)
}

func TestActivityScheduleToStartTimeout(t *testing.T) {
	env := newComponentTestEnv(t)
	command := scheduleActivityCommand()
	command.GetScheduleActivityTaskCommandAttributes().ScheduleToStartTimeout = durationpb.New(time.Second)
	command.GetScheduleActivityTaskCommandAttributes().ScheduleToCloseTimeout = durationpb.New(time.Second)
	env.update(func(ctx chasm.MutableContext) {
		require.NoError(t, env.handleCommand(ctx, command))
	})
	scheduledEventID := env.history[0].GetEventId()

	env.runPureTasks(env.now.Add(2 * time.Second))

	require.Len(t, env.history, 2, "an attempt that never started has no started event")
	timedOut := env.history[1]
	require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, timedOut.GetEventType())
	attrs := timedOut.GetActivityTaskTimedOutEventAttributes()
	require.Equal(t, scheduledEventID, attrs.GetScheduledEventId())
	require.Zero(t, attrs.GetStartedEventId())
	require.NotNil(t, attrs.GetFailure().GetTimeoutFailureInfo())

	ctx := chasm.NewContext(env.context(), env.node)
	require.Empty(t, env.workflow(ctx).Activities)
}
