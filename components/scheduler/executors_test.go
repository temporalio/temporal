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
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/components/callbacks"
	schedulerhsm "go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestProcessScheduleWaitTask(t *testing.T) {
	root := newRoot(t)
	sched := schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
	}
	schedulerHsm := schedulerhsm.NewScheduler(&schedspb.StartScheduleArgs{
		Schedule: &sched,
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: 1,
		},
	}, &schedulerhsm.DefaultTweakables)

	node, err := root.AddChild(hsm.Key{Type: schedulerhsm.StateMachineType}, schedulerHsm)
	require.NoError(t, err)
	env := fakeEnv{node}

	reg := hsm.NewRegistry()
	require.NoError(t, schedulerhsm.RegisterExecutor(
		reg,
		schedulerhsm.TaskExecutorOptions{},
	))

	err = reg.ExecuteTimerTask(
		env,
		node,
		schedulerhsm.SchedulerWaitTask{Deadline: env.Now()},
	)
	require.NoError(t, err)
	require.Equal(t, enumsspb.SCHEDULER_STATE_EXECUTING, schedulerHsm.HsmState)
}

func TestProcessScheduleRunTask(t *testing.T) {
	root := newRoot(t)
	sched := schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Action: &schedpb.ScheduleAction{
			Action: &schedpb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wid",
					WorkflowType: &commonpb.WorkflowType{Name: "wt"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	schedulerHsm := schedulerhsm.NewScheduler(&schedspb.StartScheduleArgs{
		Schedule: &sched,
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: 1,
		},
	}, &schedulerhsm.DefaultTweakables)
	schedulerHsm.HsmState = enumsspb.SCHEDULER_STATE_EXECUTING
	schedulerHsm.Args.State.LastProcessedTime = timestamppb.New(time.Now().Add(-5 * time.Minute))

	node, err := root.AddChild(hsm.Key{Type: schedulerhsm.StateMachineType, ID: "ID"}, schedulerHsm)
	require.NoError(t, err)
	env := fakeEnv{node}

	reg := hsm.NewRegistry()
	ctrl := gomock.NewController(t)
	frontendClientMock := workflowservicemock.NewMockWorkflowServiceClient(ctrl)

	frontendClientMock.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, in *workflowservice.StartWorkflowExecutionRequest, opts ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
		require.True(t, strings.HasPrefix(in.WorkflowId, "wid"))
		require.Equal(t, "wt", in.WorkflowType.Name)
		require.Equal(t, "queue", in.TaskQueue.Name)
		require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, in.TaskQueue.Kind)
		require.Equal(t, "myns", in.Namespace)
		return &workflowservice.StartWorkflowExecutionResponse{}, nil
	}).Times(1)

	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("mynsid")).Return(
		namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "myns"}, nil, false, nil, 0), nil)

	config := &schedulerhsm.Config{
		Tweakables:       dynamicconfig.GetTypedPropertyFnFilteredByNamespace[schedulerhsm.Tweakables](schedulerhsm.DefaultTweakables),
		ExecutionTimeout: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Second * 10),
	}
	require.NoError(t, schedulerhsm.RegisterExecutor(reg, schedulerhsm.TaskExecutorOptions{
		MetricsHandler:    metrics.NoopMetricsHandler,
		Logger:            log.NewNoopLogger(),
		SpecBuilder:       scheduler.NewSpecBuilder(),
		FrontendClient:    frontendClientMock,
		HistoryClient:     nil,
		NamespaceRegistry: namespaceRegistry,
		Config:            config,
	}))

	err = reg.ExecuteImmediateTask(
		context.Background(),
		env,
		hsm.Ref{
			WorkflowKey: definition.NewWorkflowKey("mynsid", "", ""),
			StateMachineRef: &persistencespb.StateMachineRef{
				Path: []*persistencespb.StateMachineKey{
					{
						Type: callbacks.StateMachineType,
						Id:   "ID",
					},
				},
			}},
		schedulerhsm.SchedulerActivateTask{},
	)
	require.NoError(t, err)
	require.Equal(t, enumsspb.SCHEDULER_STATE_WAITING, schedulerHsm.HsmState)
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
	require.NoError(t, schedulerhsm.RegisterStateMachine(reg))
	mutableState := newMutableState(t)

	// Backend is nil because we don't need to generate history events for this test.
	root, err := hsm.NewRoot(reg, workflow.StateMachineType, mutableState, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	return root
}
