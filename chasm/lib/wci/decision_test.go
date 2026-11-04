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
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/proto"
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
	// Returned for a group that serves nothing in the host region.
	inactive      *scalingalgorithm.MetricsPollResponse
	inactiveCalls int
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

func (f *fakeAlgo) InactiveGroupActions(context.Context, iface.ScalingAlgorithmConfig, iface.ScalingAlgorithmStatus) (*scalingalgorithm.MetricsPollResponse, error) {
	f.inactiveCalls++
	if f.inactive == nil {
		return &scalingalgorithm.MetricsPollResponse{}, nil
	}
	return f.inactive, nil
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

	require.NoError(t, w.ProcessDeferredScalingDecision(ctx, "g", &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}, nil, snapshot))

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
			w.InFlightPollTime = current
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
	fallback := timestamppb.New(testNow.Add(maxPollInterval))
	tasks := taskTypes(ctx)
	require.Len(t, tasks, 2)
	require.True(t, proto.Equal(&wcipb.MetricsPollTimerTask{PollTime: fallback}, tasks[0].(proto.Message)), "a fallback poll is armed first")
	require.True(t, proto.Equal(&wcipb.FetchSnapshotTask{PollTime: pollTime}, tasks[1].(proto.Message)))
	require.True(t, proto.Equal(pollTime, w.InFlightPollTime))
	require.True(t, proto.Equal(fallback, w.NextPollTime))
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

const testEast, testWest = "aws-us-east-1", "aws-us-west-2"

// newRegionalTest has groups "east" and "west", each serving WORKFLOW only in its region.
func newRegionalTest(t *testing.T, script fakeAlgo, host string) (*WorkerControllerInstance, map[string]*ScalingGroup, *chasm.MockMutableContext) {
	t.Helper()
	w, _, ctx := newDecisionTest(t, script)
	groups := map[string]*ScalingGroup{}
	w.ScalingGroupSpecs = map[string]*wcipb.ScalingGroupSpec{}
	w.ScalingGroups = chasm.Map[string, *ScalingGroup]{}
	for id, region := range map[string]string{"east": testEast, "west": testWest} {
		w.ScalingGroupSpecs[id] = &wcipb.ScalingGroupSpec{
			TaskTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			RegionIds: []string{region},
			Compute:   &wcipb.ComputeProviderSpec{ProviderType: "test-invoke"},
			Scaling:   &wcipb.ScalingAlgorithmSpec{ScalingAlgorithm: string(fakeAlgorithm)},
		}
		groups[id] = NewScalingGroup(id)
		w.ScalingGroups[id] = chasm.NewComponentField(ctx, groups[id])
	}
	w.HostRegion = host
	return w, groups, ctx
}

func TestProcessTaskAdd_PicksHostRegionGroup(t *testing.T) {
	for _, tc := range []struct{ host, want string }{{testEast, "east"}, {testWest, "west"}, {"", ""}} {
		t.Run(tc.want, func(t *testing.T) {
			w, groups, ctx := newRegionalTest(t, fakeAlgo{status: iface.ScalingAlgorithmStatus{"seen": true}}, tc.host)
			require.NoError(t, w.ProcessTaskAdd(ctx, &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}))
			for id, g := range groups {
				require.Equal(t, id == tc.want, g.AlgorithmStatus != nil, "only the host region's group decides: %s", id)
			}
		})
	}
}

func TestProcessMetricsPoll_InactiveGroupScalesDown(t *testing.T) {
	w, groups, ctx := newRegionalTest(t, fakeAlgo{
		status:   iface.ScalingAlgorithmStatus{"active": true},
		inactive: &scalingalgorithm.MetricsPollResponse{Actions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(0)}}},
	}, testEast)
	require.NoError(t, groups["west"].setAlgorithmStatus(iface.ScalingAlgorithmStatus{"stale": true}))

	require.NoError(t, w.ProcessMetricsPoll(ctx, &scalingalgorithm.ScalingMetricsSnapshot{}, nil))
	require.Equal(t, 1, fake.inactiveCalls, "only west serves nothing here")
	require.Nil(t, groups["west"].AlgorithmStatus, "a nil status starts the group fresh when it serves again")
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_SCALING, groups["west"].Status, "its worker set scales to 0")
	require.EqualValues(t, 0, groups["west"].DesiredSize)
	status, err := groups["east"].algorithmStatus()
	require.NoError(t, err)
	require.Equal(t, iface.ScalingAlgorithmStatus{"active": true}, status)
}

func TestDeferredDecisionCarriesEffectiveTaskTypes(t *testing.T) {
	w, _, ctx := newRegionalTest(t, fakeAlgo{actions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeDeferredScalingDecision}}}, testEast)
	require.NoError(t, w.ProcessTaskAdd(ctx, &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}))
	var task *wcipb.DeferredDecisionTask
	for _, t := range ctx.Tasks {
		if d, ok := t.Payload.(*wcipb.DeferredDecisionTask); ok {
			task = d
		}
	}
	require.NotNil(t, task)
	require.Equal(t, "east", task.GetScalingGroupId())
	require.Equal(t, []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW}, task.GetEffectiveTaskTypes())

	// The decision uses the carried types, even if the host region has changed since.
	w.HostRegion = testWest
	fake.actions = nil
	snapshot := &scalingalgorithm.ScalingMetricsSnapshot{Workflow: &iface.QueueTypeScalingMetrics{LastBacklogCount: 7}}
	require.NoError(t, w.ProcessDeferredScalingDecision(ctx, "east", task.GetTaskAdd(), task.GetEffectiveTaskTypes(), snapshot))
	require.EqualValues(t, 7, fake.seen.Workflow.LastBacklogCount)
}

func TestRegistrationSkipsGroupsInactiveHere(t *testing.T) {
	w, _, _ := newRegionalTest(t, fakeAlgo{regActions: []scalingalgorithm.ScalingAction{{Action: scalingalgorithm.ActionTypeInvokeWorker}}}, "")
	provider := &fakeCompute{}
	regs, err := registerTaskQueues(context.Background(), context.Background(), &fakeFrontend{}, provider, log.NewNoopLogger(),
		compute.RequestContext{}, w.spec(), nil, testEast)
	require.NoError(t, err)
	require.Len(t, provider.calls, 1, "only the group serving here registers")
	require.Contains(t, regs, "east")
	require.NotContains(t, regs, "west")
}

func TestTaskDestination(t *testing.T) {
	spec := func(regions ...string) *wcipb.ScalingGroupSpec {
		return &wcipb.ScalingGroupSpec{Compute: &wcipb.ComputeProviderSpec{ProviderType: "aws-lambda"}, RegionIds: regions}
	}
	require.Equal(t, "aws-lambda", taskDestination(spec()))
	require.Equal(t, "aws-lambda/aws-us-east-1", taskDestination(spec(testEast)))
	require.Equal(t, taskDestination(spec(testEast, testWest)), taskDestination(spec(testWest, testEast)), "order-insensitive")
	require.NotEqual(t, taskDestination(spec(testEast)), taskDestination(spec(testWest)))
}

// Each region's compute tasks get their own outbound breaker key.
func TestRegionalComputeTasksHaveRegionalDestinations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action scalingalgorithm.ScalingAction
	}{
		{name: "resize", action: scalingalgorithm.ScalingAction{Action: scalingalgorithm.ActionTypeUpdateWorkerSetSize, Count: count(2)}},
		{name: "invoke", action: scalingalgorithm.ScalingAction{Action: scalingalgorithm.ActionTypeInvokeWorker}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _, ctx := newRegionalTest(t, fakeAlgo{actions: []scalingalgorithm.ScalingAction{tc.action}}, testEast)
			require.NoError(t, w.ProcessTaskAdd(ctx, &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}))
			var destinations []string
			for _, task := range ctx.Tasks {
				switch task.Payload.(type) {
				case *wcipb.ResizeTask, *wcipb.InvokeTask:
					destinations = append(destinations, task.Attributes.Destination)
				}
			}
			require.Equal(t, []string{"test-invoke/" + testEast}, destinations)
		})
	}
}
