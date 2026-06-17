package workflow

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/components/nexusoperations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *mutableStateSuite) TestPropagateTimeSkippingToNextRun_FastForwardInfo() {
	fixed := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)
	fixedTS := timestamppb.New(fixed)

	s.Run("FastForwardNotYetReached_TargetTimePropagated", func() {
		src := &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &commonpb.TimeSkippingConfig{
					Enabled:     true,
					FastForward: durationpb.New(3 * time.Hour),
				},
				AccumulatedSkippedDuration: durationpb.New(time.Hour),
				FastForwardInfo: &persistencespb.FastForwardInfo{
					TargetTime: fixedTS,
					HasReached: false,
				},
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(src)
		s.Require().NotNil(stateProp.GetFastForwardTargetTime())
		s.Equal(fixed, stateProp.GetFastForwardTargetTime().AsTime())
		s.Require().NotNil(tsc)
		s.True(proto.Equal(tsc, src.GetTimeSkippingInfo().GetConfig()))
	})

	s.Run("FastForwardAlreadyReached_TargetTimeOmitted", func() {
		src := &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &commonpb.TimeSkippingConfig{
					Enabled:     false,
					FastForward: durationpb.New(3 * time.Hour),
				},
				AccumulatedSkippedDuration: durationpb.New(time.Hour),
				FastForwardInfo: &persistencespb.FastForwardInfo{
					TargetTime: fixedTS,
					HasReached: true,
				},
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(src)
		s.Nil(tsc)
		s.False(tsc.GetEnabled(), "disabled config must be propagated to the next run")
		s.NotNil(stateProp)
		s.Nil(stateProp.GetFastForwardTargetTime(), "consumed fast-forward must not be re-propagated")
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
	})

	s.Run("disabled config still propagates virtual time", func() {
		src := &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &commonpb.TimeSkippingConfig{
					Enabled:     false,
					FastForward: durationpb.New(3 * time.Hour),
				},
				AccumulatedSkippedDuration: durationpb.New(time.Hour),
				FastForwardInfo: &persistencespb.FastForwardInfo{
					TargetTime: timestamppb.New(s.mutableState.timeSource.Now().Add(3 * time.Hour)),
					HasReached: true,
				},
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(src)
		s.Nil(tsc)
		s.Nil(stateProp.GetFastForwardTargetTime())
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
	})

	s.Run("nil config still propagates virtual time", func() {
		src := &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config:                     nil,
				AccumulatedSkippedDuration: durationpb.New(time.Hour),
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(src)
		s.Nil(tsc)
		s.Nil(stateProp.GetFastForwardTargetTime())
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
	})
}

func (s *mutableStateSuite) TestSnapshotTimeSkippingInfo_ForChildWorkflows() {
	newSource := func() *persistencespb.WorkflowExecutionInfo {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		return &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &commonpb.TimeSkippingConfig{
					Enabled:     true,
					FastForward: durationpb.New(3 * time.Hour),
				},
				AccumulatedSkippedDuration: durationpb.New(time.Hour),
				FastForwardInfo: &persistencespb.FastForwardInfo{
					TargetTime: timestamppb.New(s.mutableState.timeSource.Now().Add(3 * time.Hour)),
					HasReached: false,
				},
			},
		}
	}

	s.Run("child workflows have no fast-forward", func() {
		tsc, propagatedState := propagateTimeSkippingToChild(newSource())
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Nil(tsc.GetFastForward())
		s.Equal(time.Hour, propagatedState.GetInitialSkippedDuration().AsDuration())
	})

	s.Run("child workflow propagation can be turned off", func() {
		src := newSource()
		src.TimeSkippingInfo.Config.DisableChildPropagation = true
		tsc, propagatedState := propagateTimeSkippingToChild(src)
		s.Nil(tsc)
		s.Require().NotNil(propagatedState)
		s.Equal(time.Hour, propagatedState.GetInitialSkippedDuration().AsDuration(),
			"virtual time is always propagated, even when config propagation is disabled")
	})

	s.Run("nil config still propagates virtual time", func() {
		src := newSource()
		src.TimeSkippingInfo.Config = nil
		tsc, propagatedState := propagateTimeSkippingToChild(src)
		s.Nil(tsc)
		s.Require().NotNil(propagatedState)
		s.Equal(time.Hour, propagatedState.GetInitialSkippedDuration().AsDuration(),
			"virtual time is always propagated, even when config propagation is disabled")
		s.Nil(propagatedState.GetFastForwardTargetTime())
	})

	s.Run("disableChildPropagation still propagates virtual time", func() {
		src := newSource()
		src.TimeSkippingInfo.Config.Enabled = false
		tsc, propagatedState := propagateTimeSkippingToChild(src)
		s.Nil(tsc)
		s.Require().NotNil(propagatedState)
		s.Equal(time.Hour, propagatedState.GetInitialSkippedDuration().AsDuration(),
			"virtual time is always propagated, even when config propagation is disabled")
		s.Nil(propagatedState.GetFastForwardTargetTime())

	})

}

func (s *mutableStateSuite) TestHasInflightWorkToPreventTimeSkipping() {
	// Each s.Run() gets a fresh mutable state via SetupSubTest().

	s.Run("FalseWhenNoPendingWork", func() {
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.False(hasPendingWork)
		s.Empty(reason)
	})

	s.Run("TrueWhenPendingWorkflowTask", func() {
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = 1
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending workflow task", reason)
	})

	s.Run("TrueWhenPendingActivity", func() {
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending activity", reason)
	})

	s.Run("FalseWhenPendingActivityInRetryBackoff", func() {
		now := s.mutableState.Now()
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			Attempt:          2,
			ScheduledTime:    timestamppb.New(now.Add(time.Hour)),
		}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.False(hasPendingWork)
		s.Empty(reason)
	})

	s.Run("TrueWhenActivityStarted", func() {
		now := s.mutableState.Now()
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			Attempt:          2,
			ScheduledTime:    timestamppb.New(now.Add(time.Hour)),
			StartedEventId:   10,
		}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending activity", reason)
	})

	// A running activity that cannot be retried (no retry policy) must still block:
	// the STARTED state short-circuits before the retry-policy check.
	s.Run("TrueWhenActivityStartedNotRetryable", func() {
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   false,
			Attempt:          1,
			StartedEventId:   10,
		}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending activity", reason)
	})

	// A first-attempt scheduled activity that has not failed yet (attempt 1) is not
	// in backoff and must block even if it has a retry policy.
	s.Run("TrueWhenActivityFirstAttemptScheduled", func() {
		now := s.mutableState.Now()
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			Attempt:          1,
			ScheduledTime:    timestamppb.New(now.Add(time.Hour)),
		}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending activity", reason)
	})

	s.Run("TrueWhenActivityPausedInBackoff", func() {
		now := s.mutableState.Now()
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			Attempt:          2,
			ScheduledTime:    timestamppb.New(now.Add(time.Hour)),
			Paused:           true,
		}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending activity", reason)
	})

	s.Run("TrueWhenActivityScheduledNow", func() {
		now := s.mutableState.Now()
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			ScheduledTime:    timestamppb.New(now.Add(-time.Hour)),
		}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending activity", reason)
	})

	s.Run("TrueWhenPendingChildExecution", func() {
		s.mutableState.pendingChildExecutionInfoIDs[1] = &persistencespb.ChildExecutionInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending child execution", reason)
	})

	s.Run("TrueWhenPendingNexusOperation", func() {
		_, err := nexusoperations.AddChild(s.mutableState.HSM(), "op-1", &historypb.HistoryEvent{
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
				NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{},
			},
		}, []byte("token"))
		s.Require().NoError(err)
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending nexus operations", reason)
	})

	s.Run("TrueWhenPendingSignalExternal", func() {
		s.mutableState.pendingSignalInfoIDs[1] = &persistencespb.SignalInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending signal external", reason)
	})

	s.Run("TrueWhenPendingRequestCancelExternal", func() {
		s.mutableState.pendingRequestCancelInfoIDs[1] = &persistencespb.RequestCancelInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWorkToPreventTimeSkipping()
		s.True(hasPendingWork)
		s.Equal("has pending request cancel external", reason)
	})

}

func (s *mutableStateSuite) TestShouldExecuteTimeSkipping() {
	// Each s.Run() gets a fresh mutable state via SetupSubTest().
	// The default state is RUNNING with no pending work.

	s.Run("FalseWhenTimeSkippingInfoNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenConfigNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{Config: nil}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenConfigDisabled", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenWorkflowNotRunning", func() {
		s.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenPendingWorkflowTask", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = 1
		s.True(s.mutableState.HasPendingWorkflowTask())
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenPendingActivity", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenPendingChildExecution", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.pendingChildExecutionInfoIDs[1] = &persistencespb.ChildExecutionInfo{}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenPendingNexusOperation", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		_, err := nexusoperations.AddChild(s.mutableState.HSM(), "op-1", &historypb.HistoryEvent{
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
				NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{},
			},
		}, []byte("token"))
		s.Require().NoError(err)
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenNoPendingTimersAndNoFastForward", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("TrueWhenPendingTimerAndNoFastForward", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.True(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("TrueWhenFastForwardAndNoPendingTimer", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:     true,
				FastForward: durationpb.New(time.Hour),
			},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
			},
		}
		s.True(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("TrueWhenFastForwardAndPendingTimer", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:     true,
				FastForward: durationpb.New(time.Hour),
			},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
			},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.True(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("FalseWhenPaused", func() {
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.shouldExecuteTimeSkipping())
	})

	s.Run("TrueWhenOnlyActivityInRetryBackoff", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		now := s.mutableState.Now()
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			Attempt:          2,
			ScheduledTime:    timestamppb.New(now.Add(time.Hour)),
		}
		s.True(s.mutableState.shouldExecuteTimeSkipping())
	})
}

func (s *mutableStateSuite) TestApplyWorkflowExecutionTimeSkippingTransitionedEvent() {
	// Use fixed UTC times so duration arithmetic is exact.
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	makeEvent := func(eventTime time.Time, targetTime *time.Time, disabledAfterBound bool) *historypb.HistoryEvent {
		attr := &historypb.WorkflowExecutionTimeSkippingTransitionedEventAttributes{
			DisabledAfterFastForward: disabledAfterBound,
		}
		if targetTime != nil {
			attr.TargetTime = timestamppb.New(*targetTime)
		}
		return &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED,
			EventTime: timestamppb.New(eventTime),
			Attributes: &historypb.HistoryEvent_WorkflowExecutionTimeSkippingTransitionedEventAttributes{
				WorkflowExecutionTimeSkippingTransitionedEventAttributes: attr,
			},
		}
	}

	s.Run("ErrorWhenTimeSkippingInfoNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		targetTime := baseTime.Add(2 * time.Hour)
		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, false),
		)
		s.Require().Error(err)
	})

	s.Run("AccumulatesDuration", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.timeSkippingInfoUpdated = false
		targetTime := baseTime.Add(2 * time.Hour)

		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, false),
		)
		s.Require().NoError(err)

		accumulated := s.mutableState.GetExecutionInfo().TimeSkippingInfo.AccumulatedSkippedDuration
		s.Require().Equal(2*time.Hour, accumulated.AsDuration())
		s.Require().True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("AccumulatesDurationAdditively", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config:                     &commonpb.TimeSkippingConfig{Enabled: true},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		targetTime := baseTime.Add(2 * time.Hour)

		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, false),
		)
		s.Require().NoError(err)

		accumulated := s.mutableState.GetExecutionInfo().TimeSkippingInfo.AccumulatedSkippedDuration
		s.Require().Equal(3*time.Hour, accumulated.AsDuration()) // 1h pre-existing + 2h new
	})

	s.Run("ErrorWhenNilTargetTimeAndNotDisabledAfterBound", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.timeSkippingInfoUpdated = false
		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, nil, false),
		)
		s.Require().Error(err)
	})

	s.Run("NilTargetTimeWithDisabledAfterBoundDoesNotAccumulateDuration", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config:                     &commonpb.TimeSkippingConfig{Enabled: true},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.timeSkippingInfoUpdated = false
		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, nil, true),
		)
		s.Require().NoError(err)

		accumulated := s.mutableState.GetExecutionInfo().TimeSkippingInfo.AccumulatedSkippedDuration
		s.Require().Equal(time.Hour, accumulated.AsDuration())
		s.Require().False(s.mutableState.GetExecutionInfo().TimeSkippingInfo.Config.Enabled)
		s.Require().True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("DisabledAfterBoundDisablesConfigAndAccumulatesDuration", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		targetTime := baseTime.Add(2 * time.Hour)

		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, true),
		)
		s.Require().NoError(err)

		s.Require().False(s.mutableState.GetExecutionInfo().TimeSkippingInfo.Config.Enabled)
		accumulated := s.mutableState.GetExecutionInfo().TimeSkippingInfo.AccumulatedSkippedDuration
		s.Require().Equal(2*time.Hour, accumulated.AsDuration())
	})
}

func (s *mutableStateSuite) TestWrapTimeSourceWithTimeSkipping() {
	const skipped = 2 * time.Hour
	fixedBase := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)

	// fixedTimeSource returns fixedBase and is used as the base time source for subtests
	// that need deterministic virtual-time assertions.
	fixedTimeSource := func() *clock.EventTimeSource {
		ts := clock.NewEventTimeSource()
		ts.Update(fixedBase)
		return ts
	}

	s.Run("ZeroOffsetWhenTimeSkippingInfoNil", func() {
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = nil

		s.mutableState.wrapTimeSourceWithTimeSkipping()

		_, isWrapper := s.mutableState.timeSource.(*clock.TimeSkippingTimeSourceWrapper)
		s.True(isWrapper)
		// With nil TimeSkippingInfo the wrapper is present but applies a zero offset.
		s.Equal(fixedBase, s.mutableState.timeSource.Now())
	})

	s.Run("OffsetTracksAccumulatedDuration", func() {
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(skipped),
		}

		s.mutableState.wrapTimeSourceWithTimeSkipping()

		_, isWrapper := s.mutableState.timeSource.(*clock.TimeSkippingTimeSourceWrapper)
		s.True(isWrapper)
		s.Equal(fixedBase.Add(skipped), s.mutableState.timeSource.Now())
	})

	s.Run("OffsetFollowsLateTimeSkippingInfoAssignment", func() {
		// Wrap first with nil TimeSkippingInfo, then assign it — the closure must
		// pick up the new accumulated duration without a re-wrap.
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.mutableState.wrapTimeSourceWithTimeSkipping()

		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(skipped),
		}

		s.Equal(fixedBase.Add(skipped), s.mutableState.timeSource.Now())
	})

	s.Run("IdempotentWhenAlreadyWrapped", func() {
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(skipped),
		}
		s.mutableState.wrapTimeSourceWithTimeSkipping()
		wrappedOnce := s.mutableState.timeSource

		s.mutableState.wrapTimeSourceWithTimeSkipping()

		s.Equal(wrappedOnce, s.mutableState.timeSource, "second call must not double-wrap")
	})

	s.Run("HBuilderUsesVirtualTime", func() {
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(skipped),
		}

		s.mutableState.wrapTimeSourceWithTimeSkipping()

		event := s.mutableState.hBuilder.AddHistoryEvent(
			enumspb.EVENT_TYPE_TIMER_FIRED,
			func(e *historypb.HistoryEvent) {
				e.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{
					TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{TimerId: "t1"},
				}
			},
		)
		s.Equal(fixedBase.Add(skipped), event.GetEventTime().AsTime())
	})
}

// TestApplyFastForward covers the full branch table of applyFastForward:
// MaxElapsedDuration set / nil duration / nil fast-forward / nil config / Enabled=false.
// The first-init virtual-time path is covered separately in
// TestInitTimeSkippingInfo_VirtualTime.
func (s *mutableStateSuite) TestApplyFastForward() {
	s.Run("FastForward_WithNoPropagatedTargetTime", func() {

		fastForwardDuration := 3 * time.Hour
		eventID := int64(1)
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()

		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:     true,
				FastForward: durationpb.New(fastForwardDuration)},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.applyFastForward(eventID, nil)
		fastForward := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(fastForward)
		s.Equal(eventID, fastForward.GetSourceEventId())
		s.False(fastForward.GetHasReached())
		s.WithinDuration(fastForward.GetTargetTime().AsTime(), baseTime.Add(fastForwardDuration), 1*time.Second)
	})

	s.Run("FastForward_WithPropagatedTargetTime", func() {
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		eventID := int64(1)
		propagatedTarget := fixed.Add(2 * time.Hour)
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:     true,
				FastForward: durationpb.New(3 * time.Hour)},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.applyFastForward(eventID, timestamppb.New(propagatedTarget))
		fastForward := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(fastForward)
		s.Equal(propagatedTarget, fastForward.GetTargetTime().AsTime(),
			"propagated target used directly, not recomputed from ff duration")
	})

	s.Run("MaxElapsedDuration_NilDuration_NoOp", func() {
		eventID := int64(1)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled: true},
		}
		s.mutableState.applyFastForward(eventID, nil)
		s.Nil(s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo())
	})

	s.Run("FastForward_ClearsExistingInfo", func() {
		eventID := int64(1)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime:    timestamppb.New(s.mutableState.Now().Add(time.Hour)),
				SourceEventId: 7,
			},
		}
		s.mutableState.applyFastForward(eventID, nil)
		s.Nil(s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo())
	})

	s.Run("FastForward_OverridesExistingInfo", func() {
		eventID := int64(1)
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true, FastForward: durationpb.New(time.Hour)},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime:    timestamppb.New(fixed),
				SourceEventId: 7,
			},
		}
		newTarget := s.mutableState.Now().Add(time.Hour)
		s.mutableState.applyFastForward(eventID, nil)
		s.WithinDuration(s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo().GetTargetTime().AsTime(), newTarget, 1*time.Second)
	})
}

// TestInitTimeSkippingInfo covers 3 basic scenarios this function is called.
func (s *mutableStateSuite) TestInitTimeSkippingInfo() {

	// if the inputs are nil, the caller doesn't need to call the TSI
	// yet we still add this test to ensure the function is safe with an noop implementation
	// to call with nil inputs
	s.Run("SafeInitWithNil_ForExecutionsWithoutTS", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		s.NotPanics(func() {
			s.mutableState.initTimeSkippingInfo(nil, nil, 0)
		})
		s.Nil(s.mutableState.executionInfo.TimeSkippingInfo)
		s.Equal(baseTime, s.mutableState.Now())
	})

	s.Run("InitWithConfigOnly_ForExecutionsWithTSStartedByUser", func() {
		eventID := int64(1)
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		cfg := &commonpb.TimeSkippingConfig{
			Enabled:     true,
			FastForward: durationpb.New(3 * time.Hour)}

		s.mutableState.initTimeSkippingInfo(cfg, nil, eventID)
		s.Equal(baseTime, s.mutableState.Now())
		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.True(proto.Equal(cfg, tsi.GetConfig()))
		s.Require().NotNil(tsi.GetFastForwardInfo())
		s.Require().Nil(tsi.GetAccumulatedSkippedDuration())
		// timestamppb translates to UTC time
		s.Require().Equal(baseTime.Add(3*time.Hour).UTC(),
			tsi.GetFastForwardInfo().GetTargetTime().AsTime())
	})

	s.Run("InitWithPropagation_ForExecutionsWithTSStartedByPropagation", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()

		// all local time
		baseTime := s.mutableState.timeSource.Now()
		targetTime := baseTime.Add(3 * time.Hour)

		hasSkipped := 2 * time.Hour
		fastForward := 3 * time.Hour
		eventID := int64(1)

		cfg := &commonpb.TimeSkippingConfig{
			Enabled:                 true,
			FastForward:             durationpb.New(fastForward),
			DisableChildPropagation: true,
		}
		propagation := &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(hasSkipped),
			FastForwardTargetTime:  timestamppb.New(targetTime),
		}
		s.mutableState.initTimeSkippingInfo(cfg, propagation, eventID)

		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.Equal(baseTime.Add(hasSkipped), s.mutableState.Now())
		s.Equal(hasSkipped, tsi.GetAccumulatedSkippedDuration().AsDuration())
		s.Equal(targetTime.UTC(),
			tsi.GetFastForwardInfo().GetTargetTime().AsTime())
	})
}

func (s *mutableStateSuite) TestUpdateTimeSkippingInfo() {

	s.Run("UpdateTimeSkippingInfo_UpdateWithNil", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentTSI := &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:     true,
				FastForward: durationpb.New(time.Hour),
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime:    timestamppb.New(baseTime.Add(time.Hour)),
				SourceEventId: 7,
				HasReached:    false,
			},
		}
		s.mutableState.executionInfo.TimeSkippingInfo = currentTSI
		s.mutableState.timeSkippingInfoUpdated = false
		newEventID := int64(8)
		s.mutableState.updateTimeSkippingInfo(nil, newEventID)
		newTSI := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(newTSI)
		s.Nil(newTSI.GetConfig())
		s.Nil(newTSI.GetFastForwardInfo())
		s.Equal(currentTSI.GetAccumulatedSkippedDuration(), newTSI.GetAccumulatedSkippedDuration())
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("UpdateTimeSkippingInfo_EnableTS", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentTSI := persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled: false,
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.executionInfo.TimeSkippingInfo = &currentTSI

		// new config
		newConfig := &commonpb.TimeSkippingConfig{
			Enabled:                 true,
			FastForward:             durationpb.New(2 * time.Hour),
			DisableChildPropagation: true,
		}
		newEventID := int64(8)
		s.mutableState.updateTimeSkippingInfo(newConfig, newEventID)
		newTSI := s.mutableState.executionInfo.GetTimeSkippingInfo()

		s.Require().NotNil(newTSI)
		s.True(proto.Equal(newConfig, newTSI.GetConfig()))
		s.Require().NotNil(newTSI.GetFastForwardInfo())
		s.Equal(newEventID, newTSI.GetFastForwardInfo().GetSourceEventId())
		s.Equal(baseTime.Add(2*time.Hour).UTC(), newTSI.GetFastForwardInfo().GetTargetTime().AsTime())
		s.False(newTSI.GetFastForwardInfo().GetHasReached())
		s.Equal(time.Hour, newTSI.GetAccumulatedSkippedDuration().AsDuration())
	})

	s.Run("UpdateTimeSkippingInfo_OverrideFFThenTurnOff", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentTSI := persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:     true,
				FastForward: durationpb.New(time.Hour),
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime:    timestamppb.New(baseTime.Add(time.Hour)),
				SourceEventId: 7,
				HasReached:    false,
			},
		}
		s.mutableState.executionInfo.TimeSkippingInfo = &currentTSI

		// update with new config with a new FF
		tsc2 := &commonpb.TimeSkippingConfig{
			Enabled:     true,
			FastForward: durationpb.New(2 * time.Hour),
		}
		newEventID := int64(8)
		s.mutableState.updateTimeSkippingInfo(tsc2, newEventID)
		tsc2TSI := s.mutableState.executionInfo.GetTimeSkippingInfo()

		s.Require().NotNil(tsc2TSI)
		s.True(proto.Equal(tsc2, tsc2TSI.GetConfig()))
		s.Require().NotNil(tsc2TSI.GetFastForwardInfo())
		s.Equal(newEventID, tsc2TSI.GetFastForwardInfo().GetSourceEventId())
		s.Equal(baseTime.Add(2*time.Hour).UTC(), tsc2TSI.GetFastForwardInfo().GetTargetTime().AsTime())
		s.False(tsc2TSI.GetFastForwardInfo().GetHasReached())
		s.Equal(time.Hour, tsc2TSI.GetAccumulatedSkippedDuration().AsDuration())

		// disable the time skipping
		tsc3 := &commonpb.TimeSkippingConfig{
			Enabled: false,
		}
		s.mutableState.updateTimeSkippingInfo(tsc3, newEventID)
		tsc3TSI := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsc3TSI)
		s.True(proto.Equal(tsc3, tsc3TSI.GetConfig()))
		s.Nil(tsc3TSI.GetFastForwardInfo())
		s.Equal(time.Hour, tsc3TSI.GetAccumulatedSkippedDuration().AsDuration())

	})

}

// TestCalculateTimeSkippingTransition exercises the full candidate-selection
// branch table.
func (s *mutableStateSuite) TestCalculateTimeSkippingTransition() {
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	addTimer := func(id string, expiry time.Time) {
		s.mutableState.pendingTimerInfoIDs[id] = &persistencespb.TimerInfo{
			TimerId:    id,
			ExpiryTime: timestamppb.New(expiry),
		}
	}

	// resetMS gives each subtest a fresh, deterministic MS.
	resetMS := func() {
		ts := clock.NewEventTimeSource().Update(baseTime)
		s.mutableState.timeSource = ts
		s.mutableState.pendingTimerInfoIDs = make(map[string]*persistencespb.TimerInfo)
		s.mutableState.pendingActivityInfoIDs = make(map[int64]*persistencespb.ActivityInfo)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		// Default: workflow has had a workflow task, so the backoff branch is skipped.
		s.mutableState.executionInfo.StartTime = timestamppb.New(baseTime)
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(baseTime)
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = 1
	}

	s.Run("NoCandidates_InvalidTransition", func() {
		resetMS()
		t, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.False(t.isValid())
	})

	s.Run("OneUserTimer_TargetIsTimer", func() {
		resetMS()
		t1 := baseTime.Add(2 * time.Hour)
		addTimer("t1", t1)

		t, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(t1, t.targetTime)
		s.False(t.disabledAfterFastForward)
	})

	s.Run("TwoUserTimers_TargetIsEarliest", func() {
		resetMS()
		t1 := baseTime.Add(2 * time.Hour)
		t2 := baseTime.Add(3 * time.Hour)
		addTimer("t1", t1)
		addTimer("t2", t2)

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(t1, tr.targetTime)
		s.False(tr.disabledAfterFastForward)
	})

	s.Run("UserTimer_PlusEarlierFastForward_TargetIsFastForward", func() {
		resetMS()
		t1 := baseTime.Add(3 * time.Hour)
		fastForwardTarget := baseTime.Add(time.Hour)
		addTimer("t1", t1)
		s.mutableState.executionInfo.TimeSkippingInfo.Config.FastForward = durationpb.New(time.Hour)
		s.mutableState.executionInfo.TimeSkippingInfo.FastForwardInfo =
			&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(fastForwardTarget)}

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(fastForwardTarget, tr.targetTime)
		s.True(tr.disabledAfterFastForward)
	})

	s.Run("ZeroFastForward_NilTarget_NoErrorNoCandidate", func() {
		resetMS()
		// A zero max_elapsed_duration is treated as no fast-forward: it contributes no candidate
		// and must NOT trip the nil-fast-forward corruption check.
		s.mutableState.executionInfo.TimeSkippingInfo.Config.FastForward = durationpb.New(0)
		// FastForward persistence deliberately not set.

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.False(tr.isValid())
	})

	s.Run("Backoff_NotChildAndExecutionTimeFuture_IsCandidate", func() {
		resetMS()
		// !HadOrHasWorkflowTask: clear the last completed WT.
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		// ExecutionTime > StartTime: a real backoff configured.
		execTime := baseTime.Add(time.Hour)
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(execTime)

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(execTime, tr.targetTime)
		s.False(tr.disabledAfterFastForward)
	})

	s.Run("Backoff_ChildWFCase_NotCandidate", func() {
		resetMS()
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		// ExecutionTime == StartTime: child WF "no first WT scheduled yet" case.
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(baseTime)

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.False(tr.isValid(),
			"child WF without backoff and no other candidate must yield invalid transition")
	})

	s.Run("Backoff_CaNWithInheritedSkip_PastExecutionTime_NotCandidate", func() {
		resetMS()
		// Inherited accumulated such that Now() > ExecutionTime: the backoff candidate
		// is in the past relative to virtual now and must not be picked.
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		execTime := baseTime.Add(10 * time.Minute)
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(execTime)
		// Wrap the time source so virtual Now is baseTime + accum (= baseTime + 1h).
		s.mutableState.timeSource = clock.WrapTimeSourceWithTimeSkipping(
			clock.NewEventTimeSource().Update(baseTime),
			func() time.Duration { return time.Hour },
		)
		s.mutableState.executionInfo.TimeSkippingInfo.AccumulatedSkippedDuration = durationpb.New(time.Hour)

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.False(tr.isValid(),
			"backoff in the virtual past must not produce a transition candidate")
	})

	s.Run("ActivityInRetryBackoff_IsCandidate", func() {
		resetMS()
		schedTime := baseTime.Add(30 * time.Minute)
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			Attempt:          2,
			ScheduledTime:    timestamppb.New(schedTime),
		}

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(schedTime, tr.targetTime)
		s.False(tr.disabledAfterFastForward)
	})

	s.Run("TwoActivitiesInBackoff_TargetIsEarliest", func() {
		resetMS()
		early := baseTime.Add(30 * time.Minute)
		late := baseTime.Add(2 * time.Hour)
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1, HasRetryPolicy: true, Attempt: 2,
			ScheduledTime: timestamppb.New(late),
		}
		s.mutableState.pendingActivityInfoIDs[2] = &persistencespb.ActivityInfo{
			ScheduledEventId: 2, HasRetryPolicy: true, Attempt: 2,
			ScheduledTime: timestamppb.New(early),
		}

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(early, tr.targetTime)
	})

	s.Run("ActivityBackoff_PlusEarlierTimer_TargetIsTimer", func() {
		resetMS()
		schedTime := baseTime.Add(2 * time.Hour)
		timerTime := baseTime.Add(time.Hour)
		addTimer("t1", timerTime)
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1, HasRetryPolicy: true, Attempt: 2,
			ScheduledTime: timestamppb.New(schedTime),
		}

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(timerTime, tr.targetTime)
	})

	// Universal cap: skip target must not exceed the run/execution timeout.
	// MaxElapsedDuration's value is irrelevant to calculateTimeSkippingTransition;
	// only FastForward.TargetTime is read. We use a large dummy
	// duration solely to configure the fast-forward.
	const largeFastForward = 24 * time.Hour
	setFastForwardAt := func(target time.Time) {
		s.mutableState.executionInfo.TimeSkippingInfo.Config.FastForward = durationpb.New(largeFastForward)
		s.mutableState.executionInfo.TimeSkippingInfo.FastForwardInfo =
			&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(target)}
	}

	s.Run("FastForward_LargerThanRunTimeout_CappedAtRunTimeout", func() {
		resetMS()
		runExpiry := baseTime.Add(30 * time.Minute)
		fastForwardTarget := baseTime.Add(2 * time.Hour) // fast-forward > run timeout
		setFastForwardAt(fastForwardTarget)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runExpiry)

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(runExpiry, tr.targetTime, "skip must be capped at run timeout")
		s.False(tr.disabledAfterFastForward, "cap fires before fast-forward; fast-forward must not be marked reached")
	})

	s.Run("FastForward_SmallerThanRunTimeout_NoCap_TargetIsFastForward", func() {
		resetMS()
		fastForwardTarget := baseTime.Add(30 * time.Minute)
		runExpiry := baseTime.Add(2 * time.Hour) // run timeout > fast-forward, no cap needed
		setFastForwardAt(fastForwardTarget)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runExpiry)

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(fastForwardTarget, tr.targetTime, "fast-forward is minimum; no cap applies")
		s.True(tr.disabledAfterFastForward, "fast-forward fires before run timeout; fast-forward must be marked reached")
	})

	s.Run("FastForward_LargerThanExecTimeout_NoRunTimeout_CappedAtExecTimeout", func() {
		resetMS()
		execExpiry := baseTime.Add(30 * time.Minute)
		fastForwardTarget := baseTime.Add(2 * time.Hour) // fast-forward > execution timeout
		setFastForwardAt(fastForwardTarget)
		// No WorkflowRunExpirationTime; only execution timeout.
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(execExpiry)

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(execExpiry, tr.targetTime, "skip must be capped at execution timeout")
		s.False(tr.disabledAfterFastForward, "cap fires before fast-forward; fast-forward must not be marked reached")
	})

	s.Run("FastForward_ZeroRunTimeout_TreatedAsNoTimeout_NoCap", func() {
		// A zero-value timestamp means "no timeout configured". The cap must not
		// fire: the fast-forward target should be the skip destination unchanged.
		resetMS()
		fastForwardTarget := baseTime.Add(time.Hour)
		setFastForwardAt(fastForwardTarget)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(time.Time{})
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(time.Time{})

		tr, err := s.mutableState.calculateTimeSkippingTransition()
		s.Require().NoError(err)
		s.Equal(fastForwardTarget, tr.targetTime, "zero timeout must not cap the skip")
		s.True(tr.disabledAfterFastForward)
	})
}
