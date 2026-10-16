package wci

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
)

type transitionCase[S comparable] struct {
	name      string
	sources   []S
	dest      S
	wantTasks []string
	apply     func(ctx chasm.MutableContext, from S) (S, error)
}

// checkTransitions applies every transition from every state: allowed sources reach the
// destination and schedule the expected tasks, all others fail with ErrInvalidTransition.
func checkTransitions[S comparable](t *testing.T, all []S, cases []transitionCase[S]) {
	t.Helper()
	for _, tc := range cases {
		for _, from := range all {
			t.Run(fmt.Sprintf("%s/from_%v", tc.name, from), func(t *testing.T) {
				ctx := &chasm.MockMutableContext{}
				got, err := tc.apply(ctx, from)
				if !slices.Contains(tc.sources, from) {
					require.ErrorIs(t, err, chasm.ErrInvalidTransition)
					require.Equal(t, from, got)
					require.Empty(t, ctx.Tasks)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.dest, got)
				var tasks []string
				for _, task := range ctx.Tasks {
					tasks = append(tasks, fmt.Sprintf("%T", task.Payload))
				}
				require.Equal(t, tc.wantTasks, tasks)
			})
		}
	}
}

var (
	allWciStatuses   = []wcipb.WciStatus{wcipb.WCI_STATUS_RUNNING, wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW, wcipb.WCI_STATUS_MIGRATED, wcipb.WCI_STATUS_DELETED}
	allGroupStatuses = []wcipb.ScalingGroupStatus{wcipb.SCALING_GROUP_STATUS_IDLE, wcipb.SCALING_GROUP_STATUS_SCALING, wcipb.SCALING_GROUP_STATUS_BACKING_OFF}
)

func rootAt(status wcipb.WciStatus) *WorkerControllerInstance {
	return &WorkerControllerInstance{WorkerControllerInstanceState: &wcipb.WorkerControllerInstanceState{Status: status}}
}

func groupAt(status wcipb.ScalingGroupStatus) *ScalingGroup {
	g := NewScalingGroup()
	g.Status = status
	g.Attempt = 1
	return g
}

func TestRootTransitions(t *testing.T) {
	run := func(apply func(*WorkerControllerInstance, chasm.MutableContext) error) func(chasm.MutableContext, wcipb.WciStatus) (wcipb.WciStatus, error) {
		return func(ctx chasm.MutableContext, from wcipb.WciStatus) (wcipb.WciStatus, error) {
			w := rootAt(from)
			err := apply(w, ctx)
			return w.Status, err
		}
	}
	checkTransitions(t, allWciStatuses, []transitionCase[wcipb.WciStatus]{
		{
			name: "migrate_to_workflow", sources: []wcipb.WciStatus{wcipb.WCI_STATUS_RUNNING}, dest: wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW,
			wantTasks: []string{"*wcipb.MigrateToWorkflowTask"},
			apply: run(func(w *WorkerControllerInstance, ctx chasm.MutableContext) error {
				return TransitionMigrateToWorkflow.Apply(w, ctx, EventMigrateToWorkflow{})
			}),
		},
		{
			name: "workflow_started", sources: []wcipb.WciStatus{wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW}, dest: wcipb.WCI_STATUS_MIGRATED,
			apply: run(func(w *WorkerControllerInstance, ctx chasm.MutableContext) error {
				return TransitionWorkflowStarted.Apply(w, ctx, EventWorkflowStarted{})
			}),
		},
		{
			name: "delete", sources: []wcipb.WciStatus{wcipb.WCI_STATUS_RUNNING}, dest: wcipb.WCI_STATUS_DELETED,
			apply: run(func(w *WorkerControllerInstance, ctx chasm.MutableContext) error {
				return TransitionDelete.Apply(w, ctx, EventDelete{Identity: "me"})
			}),
		},
	})
}

func TestScalingGroupTransitions(t *testing.T) {
	run := func(apply func(*ScalingGroup, chasm.MutableContext) error) func(chasm.MutableContext, wcipb.ScalingGroupStatus) (wcipb.ScalingGroupStatus, error) {
		return func(ctx chasm.MutableContext, from wcipb.ScalingGroupStatus) (wcipb.ScalingGroupStatus, error) {
			g := groupAt(from)
			err := apply(g, ctx)
			return g.Status, err
		}
	}
	idle, scaling, backingOff := wcipb.SCALING_GROUP_STATUS_IDLE, wcipb.SCALING_GROUP_STATUS_SCALING, wcipb.SCALING_GROUP_STATUS_BACKING_OFF
	failure := &failurepb.Failure{Message: "boom"}
	checkTransitions(t, allGroupStatuses, []transitionCase[wcipb.ScalingGroupStatus]{
		{
			name: "scale_decided", sources: []wcipb.ScalingGroupStatus{idle}, dest: scaling, wantTasks: []string{"*wcipb.ResizeTask"},
			apply: run(func(g *ScalingGroup, ctx chasm.MutableContext) error {
				return TransitionScaleDecided.Apply(g, ctx, EventScaleDecided{DesiredSize: 3, ProviderType: "aws-lambda"})
			}),
		},
		{
			name: "resize_succeeded", sources: []wcipb.ScalingGroupStatus{scaling}, dest: idle,
			apply: run(func(g *ScalingGroup, ctx chasm.MutableContext) error {
				return TransitionResizeSucceeded.Apply(g, ctx, EventResizeSucceeded{AppliedSize: 3})
			}),
		},
		{
			name: "resize_succeeded_stale", sources: []wcipb.ScalingGroupStatus{scaling}, dest: scaling, wantTasks: []string{"*wcipb.ResizeTask"},
			apply: run(func(g *ScalingGroup, ctx chasm.MutableContext) error {
				return TransitionResizeSucceededStale.Apply(g, ctx, EventResizeSucceededStale{AppliedSize: 2})
			}),
		},
		{
			name: "resize_retryable", sources: []wcipb.ScalingGroupStatus{scaling}, dest: backingOff, wantTasks: []string{"*wcipb.ScalingGroupBackoffTask"},
			apply: run(func(g *ScalingGroup, ctx chasm.MutableContext) error {
				return TransitionResizeRetryable.Apply(g, ctx, EventResizeRetryable{Failure: failure, NextAttemptTime: time.Now().Add(time.Second)})
			}),
		},
		{
			name: "backoff_elapsed", sources: []wcipb.ScalingGroupStatus{backingOff}, dest: scaling, wantTasks: []string{"*wcipb.ResizeTask"},
			apply: run(func(g *ScalingGroup, ctx chasm.MutableContext) error {
				return TransitionBackoffElapsed.Apply(g, ctx, EventBackoffElapsed{})
			}),
		},
		{
			name: "resize_failed", sources: []wcipb.ScalingGroupStatus{scaling}, dest: idle,
			apply: run(func(g *ScalingGroup, ctx chasm.MutableContext) error {
				return TransitionResizeFailed.Apply(g, ctx, EventResizeFailed{Failure: failure})
			}),
		},
	})
}

// The resize task carries the attempt and the provider type as its outbound destination.
func TestResizeAttemptsAndDestination(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	g := NewScalingGroup()

	require.NoError(t, TransitionScaleDecided.Apply(g, ctx, EventScaleDecided{DesiredSize: 3, ProviderType: "aws-lambda"}))
	require.NoError(t, TransitionResizeRetryable.Apply(g, ctx, EventResizeRetryable{NextAttemptTime: time.Now()}))
	require.NoError(t, TransitionBackoffElapsed.Apply(g, ctx, EventBackoffElapsed{ProviderType: "aws-lambda"}))

	require.Len(t, ctx.Tasks, 3)
	require.Equal(t, "aws-lambda", ctx.Tasks[0].Attributes.Destination)
	require.EqualValues(t, 1, ctx.Tasks[0].Payload.(*wcipb.ResizeTask).GetAttempt())
	require.EqualValues(t, 1, ctx.Tasks[1].Payload.(*wcipb.ScalingGroupBackoffTask).GetAttempt())
	require.EqualValues(t, 2, ctx.Tasks[2].Payload.(*wcipb.ResizeTask).GetAttempt())
}

func TestDecideOnlyStartsFromIdle(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	g := NewScalingGroup()

	require.NoError(t, g.Decide(ctx, 3, "aws-lambda"))
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_SCALING, g.Status)

	// While scaling, a new decision only moves the target.
	require.NoError(t, g.Decide(ctx, 5, "aws-lambda"))
	require.EqualValues(t, 5, g.DesiredSize)
	require.Len(t, ctx.Tasks, 1)

	// The landed size (3) is stale against 5, so it resizes again.
	require.NoError(t, g.CompleteResize(ctx, 3, "aws-lambda"))
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_SCALING, g.Status)
	require.NoError(t, g.CompleteResize(ctx, 5, "aws-lambda"))
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_IDLE, g.Status)
	require.EqualValues(t, 5, g.AppliedSize)
}

func TestNewWorkerControllerInstance(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return now }}}

	w := NewWorkerControllerInstance(ctx, "dep", "build", map[string]*wcipb.ScalingGroupSpec{"a": {}, "b": {}})

	require.Equal(t, wcipb.WCI_STATUS_RUNNING, w.Status)
	require.Equal(t, now, w.CreateTime.AsTime())
	require.Len(t, w.ScalingGroups, 2)
	require.Equal(t, chasm.LifecycleStateRunning, w.LifecycleState(ctx))
	memo := w.Memo(ctx).(*wcipb.WorkerControllerInstanceMemo)
	require.Equal(t, "dep", memo.GetDeploymentName())
	require.Equal(t, "build", memo.GetBuildId())
}

func TestLifecycleAndMutability(t *testing.T) {
	for _, tc := range []struct {
		status  wcipb.WciStatus
		closed  bool
		wantErr error
	}{
		{status: wcipb.WCI_STATUS_RUNNING},
		{status: wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW, wantErr: ErrMigrationPending},
		{status: wcipb.WCI_STATUS_MIGRATED, closed: true, wantErr: ErrClosed},
		{status: wcipb.WCI_STATUS_DELETED, closed: true, wantErr: ErrClosed},
	} {
		t.Run(tc.status.String(), func(t *testing.T) {
			w := rootAt(tc.status)
			require.Equal(t, tc.closed, w.LifecycleState(nil).IsClosed())
			require.ErrorIs(t, w.CheckMutable(), tc.wantErr)
		})
	}
}

func TestTerminate(t *testing.T) {
	w := rootAt(wcipb.WCI_STATUS_RUNNING)
	_, err := w.Terminate(&chasm.MockMutableContext{}, chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	require.Equal(t, wcipb.WCI_STATUS_DELETED, w.Status)

	_, err = w.Terminate(&chasm.MockMutableContext{}, chasm.TerminateComponentRequest{})
	require.ErrorIs(t, err, ErrClosed)
}
