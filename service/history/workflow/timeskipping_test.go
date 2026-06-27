package workflow

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
func (s *mutableStateSuite) TestWorkflowHasInflightWorkToPreventTimeSkipping() {

	s.Run("FalseWhenNoPendingWork", func() {
		hasPendingWork, reason := s.mutableState.hasInflightWork()
		s.False(hasPendingWork)
		s.Empty(reason)
	})

	s.Run("TrueWhenPendingWorkflowTask", func() {
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = 1
		hasPendingWork, reason := s.mutableState.hasInflightWork()
		s.True(hasPendingWork)
		s.Equal("has pending workflow task", reason)
	})

	s.Run("TrueWhenPendingActivity", func() {
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWork()
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
		hasPendingWork, reason := s.mutableState.hasInflightWork()
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
		hasPendingWork, reason := s.mutableState.hasInflightWork()
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
		hasPendingWork, reason := s.mutableState.hasInflightWork()
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
		hasPendingWork, reason := s.mutableState.hasInflightWork()
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
		hasPendingWork, reason := s.mutableState.hasInflightWork()
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
		hasPendingWork, reason := s.mutableState.hasInflightWork()
		s.True(hasPendingWork)
		s.Equal("has pending activity", reason)
	})

	s.Run("TrueWhenPendingChildExecution", func() {
		s.mutableState.pendingChildExecutionInfoIDs[1] = &persistencespb.ChildExecutionInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWork()
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
		hasPendingWork, reason := s.mutableState.hasInflightWork()
		s.True(hasPendingWork)
		s.Equal("has pending nexus operations", reason)
	})

	s.Run("TrueWhenPendingSignalExternal", func() {
		s.mutableState.pendingSignalInfoIDs[1] = &persistencespb.SignalInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWork()
		s.True(hasPendingWork)
		s.Equal("has pending signal external", reason)
	})

	s.Run("TrueWhenPendingRequestCancelExternal", func() {
		s.mutableState.pendingRequestCancelInfoIDs[1] = &persistencespb.RequestCancelInfo{}
		hasPendingWork, reason := s.mutableState.hasInflightWork()
		s.True(hasPendingWork)
		s.Equal("has pending request cancel external", reason)
	})

}

func (s *mutableStateSuite) TestWorkflowIsSkippable() {

	s.Run("FalseWhenTimeSkippingInfoNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenConfigNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{Config: nil}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenConfigDisabled", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenWorkflowNotRunning", func() {
		s.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingWorkflowTask", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = 1
		s.True(s.mutableState.HasPendingWorkflowTask())
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingActivity", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingChildExecution", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.pendingChildExecutionInfoIDs[1] = &persistencespb.ChildExecutionInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
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
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenNoPendingTimersAndNoFastForward", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("TrueWhenPendingTimerAndNoFastForward", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.True(s.mutableState.isWorkflowSkippable())
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
		s.True(s.mutableState.isWorkflowSkippable())
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
		s.True(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPaused", func() {
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
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
		s.True(s.mutableState.isWorkflowSkippable())
	})
}

func (s *mutableStateSuite) TestShouldExecuteTimeSkipping() {
	// Each s.Run() gets a fresh mutable state via SetupSubTest().
	// The default state is RUNNING with no pending work.

	s.Run("FalseWhenTimeSkippingInfoNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenConfigNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{Config: nil}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenConfigDisabled", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenWorkflowNotRunning", func() {
		s.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingWorkflowTask", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = 1
		s.True(s.mutableState.HasPendingWorkflowTask())
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingActivity", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingChildExecution", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.mutableState.pendingChildExecutionInfoIDs[1] = &persistencespb.ChildExecutionInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
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
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenNoPendingTimersAndNoFastForward", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("TrueWhenPendingTimerAndNoFastForward", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.True(s.mutableState.isWorkflowSkippable())
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
		s.True(s.mutableState.isWorkflowSkippable())
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
		s.True(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPaused", func() {
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
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
		s.True(s.mutableState.isWorkflowSkippable())
	})
}

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

// TestInitTimeSkippingInfo covers 3 basic scenarios this function is called.

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
