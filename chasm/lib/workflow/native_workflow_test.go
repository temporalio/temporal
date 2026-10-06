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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/tests"
)

type nativeTestEnv struct {
	t    *testing.T
	node *chasm.Node
	now  time.Time
}

type nopWorkflowTaskDispatcher struct{}

func (nopWorkflowTaskDispatcher) AddWorkflowTask(context.Context, *taskqueuepb.TaskQueue, chasm.ComponentRef, int32) error {
	return nil
}

func newNativeTestEnv(t *testing.T) *nativeTestEnv {
	env := &nativeTestEnv{t: t, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	registry := NewRegistry()
	require.NoError(t, registry.Register(NewActivityLibrary(NewConfig(dynamicconfig.NewNoopCollection()))))
	require.NoError(t, registry.Register(NewTimerLibrary()))
	chasmRegistry := newChasmTestRegistry(t, NewNativeLibrary(registry, nopWorkflowTaskDispatcher{}), &recordingMatchingClient{})
	backend := &chasm.MockNodeBackend{
		HandleGetExecutionInfo: func() *persistencespb.WorkflowExecutionInfo { return &persistencespb.WorkflowExecutionInfo{} },
		HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState {
			return &persistencespb.WorkflowExecutionState{RunId: "run-id"}
		},
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(tests.NamespaceID.String(), "workflow-id", "run-id")
		},
		HandleGetNamespaceEntry: func() *namespace.Namespace { return tests.LocalNamespaceEntry },
		HandleNow:               func() time.Time { return env.now },
	}
	env.node = chasm.NewEmptyTree(chasmRegistry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	env.update(func(ctx chasm.MutableContext) {
		w, err := NewNativeWorkflow(ctx, &workflowservice.StartWorkflowExecutionRequest{
			WorkflowType: &commonpb.WorkflowType{Name: "workflow-type"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "workflow-task-queue"},
		})
		require.NoError(t, err)
		require.NoError(t, env.node.SetRootComponent(w))
	})
	return env
}

func (env *nativeTestEnv) update(fn func(ctx chasm.MutableContext)) {
	fn(chasm.NewMutableContext(context.Background(), env.node))
	_, err := env.node.CloseTransaction()
	require.NoError(env.t, err)
}

func (env *nativeTestEnv) workflow(ctx chasm.Context) *Workflow {
	component, err := env.node.ComponentByPath(ctx, nil)
	require.NoError(env.t, err)
	//nolint:revive // unchecked-type-assertion: the root is always a Workflow in these tests
	return component.(*Workflow)
}

func (env *nativeTestEnv) history() []*historypb.HistoryEvent {
	return env.workflow(chasm.NewContext(context.Background(), env.node)).History(chasm.NewContext(context.Background(), env.node))
}

func (env *nativeTestEnv) eventTypes() []enumspb.EventType {
	var types []enumspb.EventType
	for _, event := range env.history() {
		types = append(types, event.GetEventType())
	}
	return types
}

func (env *nativeTestEnv) startWorkflowTask() *tokenspb.Task {
	var token *tokenspb.Task
	env.update(func(ctx chasm.MutableContext) {
		w := env.workflow(ctx)
		response, err := w.StartWorkflowTask(ctx, &workflowservice.PollWorkflowTaskQueueRequest{Identity: "worker"}, w.Native.Get(ctx).GetWorkflowTaskStamp())
		require.NoError(env.t, err)
		token = &tokenspb.Task{}
		require.NoError(env.t, token.Unmarshal(response.GetTaskToken()))
	})
	return token
}

func (env *nativeTestEnv) completeWorkflowTask(token *tokenspb.Task, commands ...*commandpb.Command) error {
	var err error
	env.update(func(ctx chasm.MutableContext) {
		err = env.workflow(ctx).CompleteWorkflowTask(ctx, token, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity: "worker",
			Commands: commands,
		})
	})
	return err
}

// completeActivity starts and completes the activity scheduled by the given event.
func (env *nativeTestEnv) completeActivity(scheduledEventID int64) {
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
		_, err = a.HandleCompleted(ctx, activity.RespondCompletedEvent{
			Token: &tokenspb.Task{
				Attempt:              a.LastAttempt.Get(ctx).GetCount(),
				ActivityAttemptStamp: a.LastAttempt.Get(ctx).GetStamp(),
				ComponentRef:         ref,
			},
			Request: &historyservice.RespondActivityTaskCompletedRequest{
				NamespaceId:     tests.NamespaceID.String(),
				CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{Result: payloads.EncodeString("result")},
			},
		})
		require.NoError(env.t, err)
	})
}

func scheduleActivityCommandWithID(activityID string) *commandpb.Command {
	command := scheduleActivityCommand()
	command.GetScheduleActivityTaskCommandAttributes().ActivityId = activityID
	return command
}

func completeWorkflowCommand() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	}
}

func TestNativeWorkflow_EventsArrivingDuringWorkflowTaskAreBuffered(t *testing.T) {
	env := newNativeTestEnv(t)
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
	}, env.eventTypes())

	require.NoError(t, env.completeWorkflowTask(env.startWorkflowTask(),
		scheduleActivityCommandWithID("a"), scheduleActivityCommandWithID("b")))
	require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, env.history()[4].GetEventType())
	require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, env.history()[5].GetEventType())

	env.completeActivity(5)
	token := env.startWorkflowTask()
	env.completeActivity(6)
	require.Len(t, env.history(), 10, "the second activity's events are buffered")

	require.NoError(t, env.completeWorkflowTask(token))
	history := env.history()
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
	}, env.eventTypes())
	require.Equal(t, int64(7), history[7].GetActivityTaskCompletedEventAttributes().GetStartedEventId())
	require.Equal(t, int64(12), history[12].GetActivityTaskCompletedEventAttributes().GetStartedEventId(),
		"a buffered outcome event refers to the started event's ID assigned on flush")
}

func TestNativeWorkflow_CloseWithBufferedEventsFailsWorkflowTask(t *testing.T) {
	env := newNativeTestEnv(t)
	require.NoError(t, env.completeWorkflowTask(env.startWorkflowTask(),
		scheduleActivityCommandWithID("a"), scheduleActivityCommandWithID("b")))
	env.completeActivity(5)
	token := env.startWorkflowTask()
	env.completeActivity(6)

	err := env.completeWorkflowTask(token, completeWorkflowCommand())
	cause, _, ok := WorkflowTaskFailure(err)
	require.True(t, ok)
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, cause)
}

func TestNativeWorkflow_InvalidCommandFailsWorkflowTask(t *testing.T) {
	env := newNativeTestEnv(t)
	command := scheduleActivityCommand()
	command.GetScheduleActivityTaskCommandAttributes().StartToCloseTimeout = nil
	token := env.startWorkflowTask()
	err := env.completeWorkflowTask(token, command)
	cause, failure, ok := WorkflowTaskFailure(err)
	require.True(t, ok)
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, cause)
	require.Contains(t, failure.GetMessage(), "BadScheduleActivityAttributes: ")
}

func TestNativeWorkflow_Completes(t *testing.T) {
	env := newNativeTestEnv(t)
	require.NoError(t, env.completeWorkflowTask(env.startWorkflowTask(), scheduleActivityCommand(), completeWorkflowCommand()))
	ctx := chasm.NewContext(context.Background(), env.node)
	w := env.workflow(ctx)
	require.Equal(t, chasm.LifecycleStateCompleted, w.LifecycleState(ctx))
	require.Empty(t, w.Activities, "activities are removed when the workflow closes")
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, env.history()[len(env.history())-1].GetEventType())
}

func TestNativeWorkflow_TimerFires(t *testing.T) {
	env := newNativeTestEnv(t)
	require.NoError(t, env.completeWorkflowTask(env.startWorkflowTask(), startTimerCommand("timer-id", time.Minute)))
	env.now = env.now.Add(time.Minute)
	require.NoError(t, env.node.EachPureTask(env.now, func(handler chasm.NodePureTask, attrs chasm.TaskAttributes, task any) (bool, error) {
		return handler.ExecutePureTask(context.Background(), attrs, task)
	}))
	_, err := env.node.CloseTransaction()
	require.NoError(t, err)
	types := env.eventTypes()
	require.Equal(t, []enumspb.EventType{
		enumspb.EVENT_TYPE_TIMER_FIRED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
	}, types[len(types)-2:])
}
