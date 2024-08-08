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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/components/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidTransitions(t *testing.T) {
	// Setup
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
	schedulerHsm := scheduler.NewScheduler(&schedspb.StartScheduleArgs{
		Schedule: &sched,
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: 1,
		},
	}, &scheduler.DefaultTweakables)
	out, err := scheduler.TransitionSchedulerActivate.Apply(schedulerHsm, scheduler.EventSchedulerActivate{})
	require.NoError(t, err)
	require.Equal(t, enumsspb.SCHEDULER_STATE_EXECUTING, schedulerHsm.HsmState)
	require.Equal(t, 1, len(out.Tasks))
	runTask, ok := out.Tasks[0].(scheduler.SchedulerActivateTask)
	require.True(t, ok)
	require.Equal(t, "", runTask.Destination())

	// Manually set the next invocation time and verify that it is scheduled for that
	now := timestamppb.Now()
	schedulerHsm.NextInvocationTime = now
	out, err = scheduler.TransitionSchedulerWait.Apply(schedulerHsm, scheduler.EventSchedulerWait{})
	require.NoError(t, err)
	require.Equal(t, enumsspb.SCHEDULER_STATE_WAITING, schedulerHsm.HsmState)
	require.Equal(t, 1, len(out.Tasks))
	waitTask, ok := out.Tasks[0].(scheduler.SchedulerWaitTask)
	require.True(t, ok)
	require.Equal(t, now.AsTime(), waitTask.Deadline())
}
