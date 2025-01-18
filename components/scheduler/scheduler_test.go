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

package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	v1scheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestUseScheduledActionPeek(t *testing.T) {
	const initialRemainingActions int64 = 10
	const initialConflictToken int64 = 0
	decrement := false

	// Starts paused, with unlimited actions.
	scheduler := Scheduler{
		SchedulerInternal: &schedulespb.SchedulerInternal{
			ConflictToken: initialConflictToken,
			Schedule: &schedulepb.Schedule{
				State: &schedulepb.ScheduleState{
					Paused:           true,
					LimitedActions:   false,
					RemainingActions: initialRemainingActions,
				},
			},
		},
	}

	// Paused schedules should deny and never mutate.
	require.False(t, scheduler.useScheduledAction(decrement))
	require.Equal(t, initialRemainingActions, scheduler.Schedule.State.RemainingActions)
	require.Equal(t, initialConflictToken, scheduler.ConflictToken)

	// Running schedules with unlimited actions should allow and never mutate.
	scheduler.Schedule.State.Paused = false
	require.True(t, scheduler.useScheduledAction(decrement))
	require.Equal(t, initialRemainingActions, scheduler.Schedule.State.RemainingActions)
	require.Equal(t, initialConflictToken, scheduler.ConflictToken)

	// Limit the schedule's actions, and check that we can peek the value.
	scheduler.Schedule.State.LimitedActions = true
	decrement = false
	require.True(t, scheduler.useScheduledAction(decrement))
	require.Equal(t, initialRemainingActions, scheduler.Schedule.State.RemainingActions)
	require.Equal(t, initialConflictToken, scheduler.ConflictToken)

	// When not peeking, we should mutate RemainingActions and ConflictToken.
	decrement = true
	require.True(t, scheduler.useScheduledAction(decrement))
	require.Equal(t, initialRemainingActions-1, scheduler.Schedule.State.RemainingActions)
	require.NotEqual(t, initialConflictToken, scheduler.ConflictToken)

	// False when out of remaining actions.
	scheduler.Schedule.State.RemainingActions = 0
	oldConflictToken := scheduler.ConflictToken
	require.False(t, scheduler.useScheduledAction(decrement))
	require.Equal(t, oldConflictToken, scheduler.ConflictToken)
}

func TestCompiledSpec(t *testing.T) {
	const initialConflictToken int64 = 0
	initialInterval := schedulepb.IntervalSpec{
		Interval: durationpb.New(time.Minute),
		Phase:    durationpb.New(0),
	}

	scheduler := Scheduler{
		SchedulerInternal: &schedulespb.SchedulerInternal{
			ConflictToken: initialConflictToken,
			Schedule: &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					Interval: []*schedulepb.IntervalSpec{&initialInterval},
				},
			},
		},
	}
	specBuilder := v1scheduler.NewSpecBuilder()

	// Test that the same compiled spec is returned between invocations.
	spec, err := scheduler.getCompiledSpec(specBuilder)
	require.NoError(t, err)

	spec2, err := scheduler.getCompiledSpec(specBuilder)
	require.NoError(t, err)
	require.Equal(t, spec, spec2)

	// Test that an update to conflict token will recompile the spec.
	scheduler.updateConflictToken()
	spec3, err := scheduler.getCompiledSpec(specBuilder)
	require.NoError(t, err)
	require.NotSame(t, spec, spec3)
}
