package scheduler2_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/components/scheduler2"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	namespace   = "ns"
	namespaceID = "ns-id"
	scheduleID  = "sched-id"

	defaultInterval      = 1 * time.Minute
	defaultCatchupWindow = 5 * time.Minute
)

type (
	fakeEnv struct {
		node *hsm.Node
	}

	root struct{}
)

var (
	_ hsm.Environment = fakeEnv{}
)

func (root) IsWorkflowExecutionRunning() bool {
	return true
}

func (s fakeEnv) Access(
	ctx context.Context,
	ref hsm.Ref,
	accessType hsm.AccessType,
	accessor func(*hsm.Node) error) error {
	return accessor(s.node)
}

func (fakeEnv) Now() time.Time {
	return time.Now()
}

func newRegistry(t *testing.T) *hsm.Registry {
	t.Helper()
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, scheduler2.RegisterStateMachines(reg))
	return reg
}

func newRoot(t *testing.T, registry *hsm.Registry, backend *hsmtest.NodeBackend) *hsm.Node {
	root, err := hsm.NewRoot(
		registry,
		workflow.StateMachineType,
		root{},
		make(map[string]*persistence.StateMachineMap),
		backend,
	)
	require.NoError(t, err)
	return root
}

// newSchedulerTree returns the root node for an initialized Scheduler state
// machine tree.
func newSchedulerTree(
	t *testing.T,
	registry *hsm.Registry,
	sched *schedpb.Schedule,
	patch *schedpb.SchedulePatch,
) *hsm.Node {
	backend := &hsmtest.NodeBackend{}
	root := newRoot(t, registry, backend)

	// Add Scheduler root node
	scheduler := scheduler2.NewScheduler(namespace, namespaceID, scheduleID, sched, patch)
	schedulerNode, err := root.AddChild(hsm.Key{
		Type: scheduler2.SchedulerMachineType,
		ID:   scheduleID,
	}, *scheduler)
	require.NoError(t, err)

	// Add Generator sub state machine node
	generator := scheduler2.NewGenerator()
	_, err = schedulerNode.AddChild(scheduler2.GeneratorMachineKey, *generator)
	require.NoError(t, err)

	// Add Executor sub state machine node
	executor := scheduler2.NewExecutor()
	_, err = schedulerNode.AddChild(scheduler2.ExecutorMachineKey, *executor)
	require.NoError(t, err)

	// TODO - add others

	return schedulerNode
}

// defaultSchedule returns a protobuf definition for a schedule matching this
// package's other testing defaults.
func defaultSchedule() *schedpb.Schedule {
	return &schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{
				{
					Interval: durationpb.New(defaultInterval),
					Phase:    durationpb.New(0),
				},
			},
		},
		Action: &schedpb.ScheduleAction{},
		Policies: &schedpb.SchedulePolicies{
			CatchupWindow: durationpb.New(defaultCatchupWindow),
		},
		State: &schedpb.ScheduleState{
			Paused:           false,
			LimitedActions:   false,
			RemainingActions: 0,
		},
	}
}
