package scheduler_test

import (
	"context"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/schedule/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"testing"
	"time"
)

type fakeEnv struct {
	node *hsm.Node
}

func (s fakeEnv) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) error {
	return accessor(s.node)
}

func (fakeEnv) Now() time.Time {
	return time.Now()
}

var _ hsm.Environment = fakeEnv{}

type mutableState struct {
}

func TestProcessScheduleTask(t *testing.T) {
	root := newRoot(t)
	sched := schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}
	schedulerHsm := scheduler.Scheduler{&schedule.HsmSchedulerState{
		Args: &schedspb.StartScheduleArgs{
			Schedule: &sched,
			State: &schedspb.InternalState{
				Namespace:     "myns",
				NamespaceId:   "mynsid",
				ScheduleId:    "myschedule",
				ConflictToken: 1,
			},
		},
		HsmState: enums.SCHEDULER_STATE_WAITING,
	}}

	coll := scheduler.MachineCollection(root)
	node, err := coll.Add("ID", schedulerHsm)
	require.NoError(t, err)
	env := fakeEnv{node}

	reg := hsm.NewRegistry()
	require.NoError(t, scheduler.RegisterExecutor(
		reg,
		scheduler.ActiveExecutorOptions{},
		scheduler.StandbyExecutorOptions{},
		&scheduler.Config{},
	))

	err = reg.ExecuteActiveTimerTask(
		env,
		node,
		scheduler.ScheduleTask{Deadline: env.Now().Add(10 * time.Second)},
	)
	require.NoError(t, err)

	schedulerHsm, err = coll.Data("ID")
	require.NoError(t, err)
	require.Equal(t, enums.SCHEDULER_STATE_WAITING, schedulerHsm.HsmState)
	// TODO(Tianyu): is there any way to inspect the resulting task queue?
}

func newMutableState(t *testing.T) mutableState {
	return mutableState{}
}

func newRoot(t *testing.T) *hsm.Node {
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, scheduler.RegisterStateMachine(reg))
	mutableState := newMutableState(t)

	// Backend is nil because we don't need to generate history events for this test.
	root, err := hsm.NewRoot(reg, workflow.StateMachineType.ID, mutableState, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)
	return root
}
