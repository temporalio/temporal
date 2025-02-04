// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/components/scheduler"
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
		now  time.Time
	}

	root struct{}
)

var (
	_ hsm.Environment = fakeEnv{}
)

func (root) IsWorkflowExecutionRunning() bool {
	return true
}

func newFakeEnv() *fakeEnv {
	return &fakeEnv{
		now: time.Now(),
	}
}

func (e fakeEnv) Access(
	ctx context.Context,
	ref hsm.Ref,
	accessType hsm.AccessType,
	accessor func(*hsm.Node) error) error {
	return accessor(e.node)
}

func (e fakeEnv) Now() time.Time {
	return e.now
}

func newRegistry(t *testing.T) *hsm.Registry {
	t.Helper()
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, scheduler.RegisterStateMachines(reg))
	return reg
}

func newRoot(t *testing.T, registry *hsm.Registry, backend *hsmtest.NodeBackend) *hsm.Node {
	root, err := hsm.NewRoot(
		registry,
		workflow.StateMachineType,
		root{},
		make(map[string]*persistencespb.StateMachineMap),
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
	root *hsm.Node,
	sched *schedulepb.Schedule,
	patch *schedulepb.SchedulePatch,
) *hsm.Node {
	// Add Scheduler root node
	s := scheduler.NewScheduler(namespace, namespaceID, scheduleID, sched, patch)
	schedulerNode, err := root.AddChild(hsm.Key{
		Type: scheduler.SchedulerMachineType,
		ID:   scheduleID,
	}, *s)
	require.NoError(t, err)

	// Add Generator sub state machine node
	generator := scheduler.NewGenerator()
	_, err = schedulerNode.AddChild(scheduler.GeneratorMachineKey, *generator)
	require.NoError(t, err)

	// Add Executor sub state machine node
	executor := scheduler.NewExecutor()
	_, err = schedulerNode.AddChild(scheduler.ExecutorMachineKey, *executor)
	require.NoError(t, err)

	// TODO - add others

	return schedulerNode
}

// defaultSchedule returns a protobuf definition for a schedule matching this
// package's other testing defaults.
func defaultSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(defaultInterval),
					Phase:    durationpb.New(0),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId: "scheduled-wf",
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			CatchupWindow: durationpb.New(defaultCatchupWindow),
		},
		State: &schedulepb.ScheduleState{
			Paused:           false,
			LimitedActions:   false,
			RemainingActions: 0,
		},
	}
}

func defaultConfig() *scheduler.Config {
	return &scheduler.Config{
		Tweakables: func(_ string) scheduler.Tweakables {
			return scheduler.DefaultTweakables
		},
		ServiceCallTimeout: func() time.Duration {
			return 5 * time.Second
		},
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(1 * time.Second)
		},
	}
}

func opLogTasks(node *hsm.Node) (tasks []hsm.Task, err error) {
	opLog, err := node.OpLog()
	if err != nil {
		return nil, err
	}

	for _, op := range opLog {
		output, ok := op.(hsm.TransitionOperation)
		if ok {
			tasks = append(tasks, output.Output.Tasks...)
		}
	}

	return tasks, nil
}
