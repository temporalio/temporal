package wci

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const fakeAlgorithm iface.ScalingAlgorithmType = "wci-chasm-test-fake"

// fakeAlgo returns scripted responses; tests set the fields they need.
type fakeAlgo struct {
	taskAddCalls int
	actions      []scalingalgorithm.ScalingAction
	status       iface.ScalingAlgorithmStatus
	nextPoll     *time.Duration
	seen         *scalingalgorithm.ScalingMetricsSnapshot
	validateErr  error
	regActions   []scalingalgorithm.ScalingAction
}

var fake = &fakeAlgo{}

func init() {
	// Also the default for test-invoke, so specs without Scaling pass iface Validate() and reach the fake.
	scalingalgorithm.RegisterScalingAlgorithm(fakeAlgorithm, func(context.Context) (scalingalgorithm.ScalingAlgorithm, error) { return fake, nil }, iface.ComputeProviderTypeTestInvoke)
}

func (f *fakeAlgo) CompatibleLaunchStrategies() []compute.LaunchStrategy { return nil }
func (f *fakeAlgo) ValidateConfig(context.Context, iface.ScalingAlgorithmConfig) error {
	return f.validateErr
}
func (f *fakeAlgo) ProcessTaskAdd(context.Context, iface.ScalingAlgorithmConfig, iface.ScalingAlgorithmStatus, iface.SignalTaskAddRequest) (*scalingalgorithm.TaskAddResponse, error) {
	f.taskAddCalls++
	return &scalingalgorithm.TaskAddResponse{Actions: f.actions, Status: f.status}, nil
}
func (f *fakeAlgo) ProcessDeferredScalingDecision(_ context.Context, _ iface.ScalingAlgorithmConfig, _ iface.ScalingAlgorithmStatus, _ iface.SignalTaskAddRequest, get scalingalgorithm.ScalingMetricsSnapshotGetter) (*scalingalgorithm.TaskAddResponse, error) {
	snapshot, err := get()
	f.seen = snapshot
	return &scalingalgorithm.TaskAddResponse{Actions: f.actions, Status: f.status}, err
}
func (f *fakeAlgo) ProcessMetricsPoll(_ context.Context, _ iface.ScalingAlgorithmConfig, _ iface.ScalingAlgorithmStatus, snapshot scalingalgorithm.ScalingMetricsSnapshot) (*scalingalgorithm.MetricsPollResponse, error) {
	f.seen = &snapshot
	return &scalingalgorithm.MetricsPollResponse{Actions: f.actions, Status: f.status, NextPoll: f.nextPoll}, nil
}
func (f *fakeAlgo) TaskQueueRegistrationActions(context.Context, iface.ScalingAlgorithmConfig, iface.ScalingAlgorithmStatus) (*scalingalgorithm.TaskQueueRegistrationResponse, error) {
	return &scalingalgorithm.TaskQueueRegistrationResponse{Actions: f.regActions}, nil
}

var testNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func newDecisionTest(t *testing.T, script fakeAlgo) (*WorkerControllerInstance, *ScalingGroup, *chasm.MockMutableContext) {
	t.Helper()
	*fake = script
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return testNow }}}
	group := NewScalingGroup("g")
	w := &WorkerControllerInstance{
		WorkerControllerInstanceState: &wcipb.WorkerControllerInstanceState{
			Status: wcipb.WCI_STATUS_RUNNING,
			ScalingGroupSpecs: map[string]*wcipb.ScalingGroupSpec{"g": {
				TaskTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
				Compute:   &wcipb.ComputeProviderSpec{ProviderType: "test-invoke"},
				Scaling:   &wcipb.ScalingAlgorithmSpec{ScalingAlgorithm: string(fakeAlgorithm)},
			}},
		},
		ScalingGroups: chasm.Map[string, *ScalingGroup]{"g": chasm.NewComponentField(ctx, group)},
	}
	return w, group, ctx
}

func count(n int32) *int32 { return &n }

func taskTypes(ctx *chasm.MockMutableContext) []any {
	var out []any
	for _, task := range ctx.Tasks {
		out = append(out, task.Payload)
	}
	return out
}

func TestProcessTaskAdd_DecidesAndStoresStatus(t *testing.T) {
	w, group, ctx := newDecisionTest(t, fakeAlgo{
		actions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(3)}},
		status:  iface.ScalingAlgorithmStatus{"n": float64(1)},
	})

	require.NoError(t, w.ProcessTaskAdd(ctx, &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}))

	require.Equal(t, wcipb.SCALING_GROUP_STATUS_SCALING, group.Status)
	require.EqualValues(t, 3, group.DesiredSize)
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, "test-invoke", ctx.Tasks[0].Attributes.Destination)
	status, err := group.algorithmStatus()
	require.NoError(t, err)
	require.Equal(t, iface.ScalingAlgorithmStatus{"n": float64(1)}, status)
}

func TestProcessTaskAdd_IgnoresOtherTaskTypes(t *testing.T) {
	w, _, ctx := newDecisionTest(t, fakeAlgo{})
	require.NoError(t, w.ProcessTaskAdd(ctx, &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY}))
	require.Zero(t, fake.taskAddCalls)
	require.Empty(t, ctx.Tasks)
}

func TestProcessTaskAdd_DefersWithTheEvent(t *testing.T) {
	w, _, ctx := newDecisionTest(t, fakeAlgo{
		actions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeDeferredScalingDecision}},
	})
	event := &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, TaskQueueName: "tq"}

	require.NoError(t, w.ProcessTaskAdd(ctx, event))

	require.Len(t, ctx.Tasks, 1)
	task := ctx.Tasks[0].Payload.(*wcipb.DeferredDecisionTask)
	require.Equal(t, "g", task.GetScalingGroupId())
	require.Equal(t, "tq", task.GetTaskAdd().GetTaskQueueName())
}

func TestProcessMetricsPoll_RearmsAtEarliestAndDropsDeferral(t *testing.T) {
	next := time.Minute
	w, _, ctx := newDecisionTest(t, fakeAlgo{
		actions:  []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeDeferredScalingDecision}},
		nextPoll: &next,
	})

	require.NoError(t, w.ProcessMetricsPoll(ctx, &scalingalgorithm.ScalingMetricsSnapshot{}, nil))

	require.Equal(t, testNow.Add(time.Minute), w.NextPollTime.AsTime())
	require.Len(t, ctx.Tasks, 1, "only the re-armed timer, the nested deferral is dropped")
	require.Equal(t, testNow.Add(time.Minute), ctx.Tasks[0].Attributes.ScheduledTime)
	require.IsType(t, &wcipb.MetricsPollTimerTask{}, ctx.Tasks[0].Payload)
}

func TestProcessMetricsPoll_ClampsAndHandlesFailedFetch(t *testing.T) {
	tooSoon := time.Second
	w, _, ctx := newDecisionTest(t, fakeAlgo{nextPoll: &tooSoon})
	require.NoError(t, w.ProcessMetricsPoll(ctx, &scalingalgorithm.ScalingMetricsSnapshot{}, nil))
	require.Equal(t, testNow.Add(minPollInterval), w.NextPollTime.AsTime())

	w, _, ctx = newDecisionTest(t, fakeAlgo{})
	require.NoError(t, w.ProcessMetricsPoll(ctx, nil, nil))
	require.Equal(t, testNow.Add(maxPollInterval), w.NextPollTime.AsTime())
	require.Nil(t, fake.seen, "a failed fetch skips the algorithms")
}

func TestProcessDeferredScalingDecision_UsesPrefetchedSnapshot(t *testing.T) {
	w, group, ctx := newDecisionTest(t, fakeAlgo{
		actions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(2)}},
	})
	snapshot := &scalingalgorithm.ScalingMetricsSnapshot{
		Workflow: &iface.QueueTypeScalingMetrics{LastBacklogCount: 7},
		Activity: &iface.QueueTypeScalingMetrics{LastBacklogCount: 9},
	}

	require.NoError(t, w.ProcessDeferredScalingDecision(ctx, "g", &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}, snapshot))

	require.EqualValues(t, 7, fake.seen.Workflow.LastBacklogCount)
	require.Nil(t, fake.seen.Activity, "filtered to the group's task types")
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_SCALING, group.Status)
}

// A task from a superseded timer, or from a state the root has left, fails Validate.
func TestTimerAndFetchValidation(t *testing.T) {
	current := timestamppb.New(testNow)
	stale := timestamppb.New(testNow.Add(-time.Minute))
	for _, tc := range []struct {
		name   string
		status wcipb.WciStatus
		time   *timestamppb.Timestamp
		want   bool
	}{
		{name: "current", status: wcipb.WCI_STATUS_RUNNING, time: current, want: true},
		{name: "superseded", status: wcipb.WCI_STATUS_RUNNING, time: stale},
		{name: "migrating", status: wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW, time: current},
		{name: "deleted", status: wcipb.WCI_STATUS_DELETED, time: current},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := rootAt(tc.status)
			w.NextPollTime = current
			w.NextValidationTime = current

			ok, err := (&MetricsPollTimerTaskHandler{}).Validate(nil, w, chasm.TaskInvocation{}, &wcipb.MetricsPollTimerTask{PollTime: tc.time})
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
			ok, err = (&FetchSnapshotTaskHandler{}).Validate(nil, w, chasm.TaskInvocation{}, &wcipb.FetchSnapshotTask{PollTime: tc.time})
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
			ok, err = (&PeriodicValidationTimerTaskHandler{}).Validate(nil, w, chasm.TaskInvocation{}, &wcipb.PeriodicValidationTimerTask{ValidationTime: tc.time})
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
		})
	}
}

func TestDeferredDecisionValidation(t *testing.T) {
	w, _, _ := newDecisionTest(t, fakeAlgo{})
	ok, err := (&DeferredDecisionTaskHandler{}).Validate(nil, w, chasm.TaskInvocation{}, &wcipb.DeferredDecisionTask{ScalingGroupId: "g"})
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = (&DeferredDecisionTaskHandler{}).Validate(nil, w, chasm.TaskInvocation{}, &wcipb.DeferredDecisionTask{ScalingGroupId: "removed"})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPollTimerSchedulesFetch(t *testing.T) {
	w, _, ctx := newDecisionTest(t, fakeAlgo{})
	pollTime := timestamppb.New(testNow)
	require.NoError(t, (&MetricsPollTimerTaskHandler{}).Execute(ctx, w, chasm.TaskAttributes{}, &wcipb.MetricsPollTimerTask{PollTime: pollTime}))
	require.Equal(t, []any{&wcipb.FetchSnapshotTask{PollTime: pollTime}}, taskTypes(ctx))
}

func TestValidationTimerRearmsBeforeValidating(t *testing.T) {
	w, _, ctx := newDecisionTest(t, fakeAlgo{})
	require.NoError(t, (&PeriodicValidationTimerTaskHandler{}).Execute(ctx, w, chasm.TaskAttributes{}, &wcipb.PeriodicValidationTimerTask{}))

	require.Equal(t, testNow.Add(validationInterval), w.NextValidationTime.AsTime())
	require.Len(t, ctx.Tasks, 2)
	require.IsType(t, &wcipb.PeriodicValidationTimerTask{}, ctx.Tasks[0].Payload)
	validate := ctx.Tasks[1].Payload.(*wcipb.ValidateSpecTask)
	require.Equal(t, w.NextValidationTime.AsTime(), validate.GetValidationTime().AsTime())
}

func TestConstructorsArmTimers(t *testing.T) {
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return testNow }}}
	w := NewWorkerControllerInstance(ctx, "dep", "build", nil)
	require.Equal(t, testNow.Add(maxPollInterval), w.NextPollTime.AsTime())
	require.Equal(t, testNow.Add(validationInterval), w.NextValidationTime.AsTime())

	// A migrated instance arms no cadence until activated; then a past-due poll fires now.
	ctx.Tasks = nil
	m := NewWorkerControllerInstanceFromMigration(ctx, &wcipb.WorkerControllerInstanceState{NextPollTime: timestamppb.New(testNow.Add(-time.Hour))}, nil, "run-1", testNow.Add(time.Minute))
	require.Equal(t, wcipb.WCI_STATUS_PENDING_ACTIVATION, m.Status)
	var timers []string
	for _, task := range ctx.Tasks {
		switch task.Payload.(type) {
		case *wcipb.PendingExpiryTask, *wcipb.MetricsPollTimerTask, *wcipb.PeriodicValidationTimerTask:
			timers = append(timers, fmt.Sprintf("%T", task.Payload))
		default:
		}
	}
	require.Equal(t, []string{"*wcipb.PendingExpiryTask"}, timers)
	require.NoError(t, TransitionMigrationActivated.Apply(m, ctx, EventMigrationActivated{}))
	require.Equal(t, testNow, m.NextPollTime.AsTime())
	require.Equal(t, testNow.Add(validationInterval), m.NextValidationTime.AsTime())
	require.Nil(t, m.PendingExpireTime)
}

func TestSnapshotFromDescribe(t *testing.T) {
	stats := func(backlog int64, age time.Duration) *taskqueuepb.TaskQueueStats {
		return &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: backlog, ApproximateBacklogAge: durationpb.New(age), TasksAddRate: 1, TasksDispatchRate: 2}
	}
	snapshot := snapshotFromDescribe(&workflowservice.DescribeWorkerDeploymentVersionResponse{
		VersionTaskQueues: []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{
			{Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW, Stats: stats(3, time.Second)},
			{Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW, Stats: stats(4, time.Minute)},
			{Type: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		},
	})
	require.EqualValues(t, 7, snapshot.Workflow.LastBacklogCount)
	require.Equal(t, time.Minute, snapshot.Workflow.LastBacklogAge)
	require.InDelta(t, 2, snapshot.Workflow.LastArrivalRate, 1e-9)
	require.Zero(t, snapshot.Activity.LastBacklogCount)
}

func TestProcessTaskAdd_RecordsLastTaskAdd(t *testing.T) {
	w, group, ctx := newDecisionTest(t, fakeAlgo{})
	event := &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, TaskQueueName: "orders", IsSyncMatch: true}

	require.NoError(t, w.ProcessTaskAdd(ctx, event))

	o := group.GetObservations()
	require.Equal(t, "orders", o.GetLastTaskAdd().GetTaskQueueName())
	require.True(t, o.GetLastTaskAdd().GetIsSyncMatch())
	require.Equal(t, testNow, o.GetLastTaskAddTime().AsTime())
	event.TaskQueueName = "mutated"
	require.Equal(t, "orders", o.GetLastTaskAdd().GetTaskQueueName(), "the event is cloned, not aliased")
}

func TestProcessMetricsPoll_RecordsGroupTaskQueues(t *testing.T) {
	w, group, ctx := newDecisionTest(t, fakeAlgo{})
	observed := observedTaskQueuesFromDescribe(&workflowservice.DescribeWorkerDeploymentVersionResponse{
		VersionTaskQueues: []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{
			{Name: "small", Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW, Stats: &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: 1}},
			{Name: "acts", Type: enumspb.TASK_QUEUE_TYPE_ACTIVITY, Stats: &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: 100}},
			{Name: "big", Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW, Stats: &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: 9, ApproximateBacklogAge: durationpb.New(time.Minute)}},
			{Name: "nostats", Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		},
	})

	require.NoError(t, w.ProcessMetricsPoll(ctx, &scalingalgorithm.ScalingMetricsSnapshot{}, observed))

	o := group.GetObservations()
	require.Equal(t, testNow, o.GetPolledAt().AsTime())
	var names []string
	for _, tq := range o.GetTaskQueues() {
		names = append(names, tq.GetName())
	}
	require.Equal(t, []string{"big", "small", "nostats"}, names, "only the group's types, largest backlog first")
	require.Equal(t, time.Minute, o.GetTaskQueues()[0].GetBacklogAge().AsDuration())
}

func TestProcessMetricsPoll_FailedFetchKeepsObservations(t *testing.T) {
	w, group, ctx := newDecisionTest(t, fakeAlgo{})
	observed := []*wcipb.ObservedTaskQueue{{Name: "tq", Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW}}
	require.NoError(t, w.ProcessMetricsPoll(ctx, &scalingalgorithm.ScalingMetricsSnapshot{}, observed))

	require.NoError(t, w.ProcessMetricsPoll(ctx, nil, nil))

	require.Len(t, group.GetObservations().GetTaskQueues(), 1, "polled_at shows how stale it is")
}

func TestObservePoll_Caps(t *testing.T) {
	observed := make([]*wcipb.ObservedTaskQueue, maxObservedTaskQueues+10)
	for i := range observed {
		observed[i] = &wcipb.ObservedTaskQueue{Name: fmt.Sprint(i), Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW}
	}
	g := NewScalingGroup("g")
	g.observePoll(testNow, observed, []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW})
	require.Len(t, g.GetObservations().GetTaskQueues(), maxObservedTaskQueues)
}
