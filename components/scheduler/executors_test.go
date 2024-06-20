// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
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
	schedulerHsm := scheduler.Scheduler{HsmSchedulerState: &schedspb.HsmSchedulerState{
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

	node, err := root.AddChild(hsm.Key{Type: scheduler.StateMachineType}, schedulerHsm)
	require.NoError(t, err)
	env := fakeEnv{node}

	reg := hsm.NewRegistry()
	require.NoError(t, scheduler.RegisterExecutor(
		reg,
		scheduler.TaskExecutorOptions{},
		&scheduler.Config{},
	))

	err = reg.ExecuteTimerTask(
		env,
		node,
		scheduler.ScheduleTask{Deadline: env.Now().Add(10 * time.Second)},
	)
	require.NoError(t, err)
	require.Equal(t, enums.SCHEDULER_STATE_WAITING, schedulerHsm.HsmState)
}

func newMutableState(t *testing.T) mutableState {
	return mutableState{}
}

func (mutableState) IsWorkflowExecutionRunning() bool {
	return true
}

func newRoot(t *testing.T) *hsm.Node {
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, scheduler.RegisterStateMachine(reg))
	mutableState := newMutableState(t)

	// Backend is nil because we don't need to generate history events for this test.
	root, err := hsm.NewRoot(reg, workflow.StateMachineType, mutableState, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	return root
}
