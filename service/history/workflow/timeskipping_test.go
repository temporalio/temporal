package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/components/nexusoperations"
	historyi "go.temporal.io/server/service/history/interfaces"
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
		s.Nil(tsc, "disabled config is not propagated")
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

	// Regression: with no accumulated duration the stateProp starts nil; an active
	// (not-yet-reached) fast-forward must still be propagated without a nil deref.
	s.Run("FastForwardPropagatedWhenNoAccumulatedDuration", func() {
		src := &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &commonpb.TimeSkippingConfig{
					Enabled:     true,
					FastForward: durationpb.New(3 * time.Hour),
				},
				// no AccumulatedSkippedDuration
				FastForwardInfo: &persistencespb.FastForwardInfo{
					TargetTime: fixedTS,
					HasReached: false,
				},
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(src)
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Require().NotNil(stateProp)
		s.Nil(stateProp.GetInitialSkippedDuration(), "no virtual time accumulated yet")
		s.Require().NotNil(stateProp.GetFastForwardTargetTime())
		s.Equal(fixed, stateProp.GetFastForwardTargetTime().AsTime())
	})

	s.Run("NothingPropagatedWhenNoAccumAndNoFastForward", func() {
		src := &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &commonpb.TimeSkippingConfig{Enabled: true},
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(src)
		s.Require().NotNil(tsc, "enabled config is still propagated")
		s.True(tsc.GetEnabled())
		s.Nil(stateProp, "no accumulated time and no active fast-forward means nothing to propagate")
	})

	s.Run("NilTimeSkippingInfoPropagatesNothing", func() {
		tsc, stateProp := propagateTimeSkippingToNextRun(&persistencespb.WorkflowExecutionInfo{})
		s.Nil(tsc)
		s.Nil(stateProp)
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

func (s *mutableStateSuite) TestIsWorkflowSkippable() {

	// base case: ensures the other tests modifications are the reason to make the workflow not skippable
	s.Run("BaseCase", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true}}
		s.True(s.mutableState.isWorkflowSkippable())
	})

	// config tests: nil safe, and false safe proof
	s.Run("FalseWhenTimeSkippingInfoNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenConfigNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{Config: nil}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenConfigDisabled", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
		}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	// state and status tests
	s.Run("FalseWhenWorkflowNotRunning", func() {
		s.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPaused", func() {
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	// inflight work tests
	s.Run("FalseWhenPendingWorkflowTask", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = 1
		s.True(s.mutableState.HasPendingWorkflowTask())
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingActivity", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingSignalExternal", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingSignalInfoIDs[1] = &persistencespb.SignalInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingRequestCancelExternal", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingRequestCancelInfoIDs[1] = &persistencespb.RequestCancelInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingChildExecution", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingChildExecutionInfoIDs[1] = &persistencespb.ChildExecutionInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("FalseWhenPendingNexusOperation", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		_, err := nexusoperations.AddChild(s.mutableState.HSM(), "op-1", &historypb.HistoryEvent{
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
				NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{},
			},
		}, []byte("token"))
		s.Require().NoError(err)
		s.False(s.mutableState.isWorkflowSkippable())
	})

	// conditions that won't impact the workflow skippability
	s.Run("TrueWhenPendingTimersExist", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.True(s.mutableState.isWorkflowSkippable())
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

	s.Run("FalseWhenActivityRetryDueNowOrPast", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		now := s.mutableState.Now()
		// A pending retry whose next attempt is already due (<= now) is not a future
		// skip target — it is in-flight work and must block. now is the exact boundary,
		// since the future check (Now().Before(ScheduledTime)) is strict.
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1,
			HasRetryPolicy:   true,
			Attempt:          2,
			ScheduledTime:    timestamppb.New(now),
		}
		s.False(s.mutableState.isWorkflowSkippable())
	})
}

// TestInitTimeSkippingInfo covers the 3 basic scenarios in which the function is called:
// nil inputs (no-op), config-only init (user-started), and init with propagated state.
func (s *mutableStateSuite) TestInitTimeSkippingInfo() {

	// if the inputs are nil, the caller doesn't need to call the TSI
	// yet we still add this test to ensure the function is safe with an noop implementation
	// to call with nil inputs
	s.Run("SafeInitWithNil_ForExecutionsWithoutTS", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		s.NotPanics(func() {
			s.Require().NoError(s.mutableState.initTimeSkippingInfo(nil, nil, 0))
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

		s.Require().NoError(s.mutableState.initTimeSkippingInfo(cfg, nil, eventID))
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
		s.Require().NoError(s.mutableState.initTimeSkippingInfo(cfg, propagation, eventID))

		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.Equal(baseTime.Add(hasSkipped), s.mutableState.Now())
		s.Equal(hasSkipped, tsi.GetAccumulatedSkippedDuration().AsDuration())
		s.Equal(targetTime.UTC(),
			tsi.GetFastForwardInfo().GetTargetTime().AsTime())
	})
}

func (s *mutableStateSuite) TestUpdateTimeSkippingInfo() {

	// update assumes the TS info was already initialized; updating a workflow that never
	// started time skipping is an internal error and must not initialize it as a side effect.
	s.Run("ErrorWhenTimeSkippingInfoNil", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.mutableState.timeSkippingInfoUpdated = false

		err := s.mutableState.updateTimeSkippingInfo(&commonpb.TimeSkippingConfig{Enabled: true}, 8)
		s.Error(err)
		s.Nil(s.mutableState.executionInfo.GetTimeSkippingInfo(), "must not initialize TS info on update")
		s.False(s.mutableState.timeSkippingInfoUpdated, "must not mark updated on a failed update")
	})

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
		s.Require().NoError(s.mutableState.updateTimeSkippingInfo(nil, newEventID))
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
		s.Require().NoError(s.mutableState.updateTimeSkippingInfo(newConfig, newEventID))
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
		s.Require().NoError(s.mutableState.updateTimeSkippingInfo(tsc2, newEventID))
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
		s.Require().NoError(s.mutableState.updateTimeSkippingInfo(tsc3, newEventID))
		tsc3TSI := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsc3TSI)
		s.True(proto.Equal(tsc3, tsc3TSI.GetConfig()))
		s.Nil(tsc3TSI.GetFastForwardInfo())
		s.Equal(time.Hour, tsc3TSI.GetAccumulatedSkippedDuration().AsDuration())

	})
}

// TestWrapExecutionTimes covers the three invariants of wrapExecutionTimes:
// a nil/zero skipped duration is a no-op; a non-zero duration shifts every *set*
// execution timestamp forward by exactly that duration; unset timestamps stay unset.
func (s *mutableStateSuite) TestWrapExecutionTimes() {
	base := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	s.Run("NilOrZeroDurationIsNoOp", func() {
		s.mutableState.executionInfo.StartTime = timestamppb.New(base)
		s.mutableState.wrapExecutionTimes(nil)
		s.Equal(base, s.mutableState.executionInfo.StartTime.AsTime())
		s.mutableState.wrapExecutionTimes(durationpb.New(0))
		s.Equal(base, s.mutableState.executionInfo.StartTime.AsTime())
	})

	s.Run("ShiftsSetTimestampsAndLeavesUnsetUntouched", func() {
		accum := 2 * time.Hour
		s.mutableState.executionState.StartTime = timestamppb.New(base)
		s.mutableState.executionInfo.StartTime = timestamppb.New(base)
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(base.Add(time.Minute))
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(base.Add(time.Hour))
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = nil // unset must stay unset

		s.mutableState.wrapExecutionTimes(durationpb.New(accum))

		s.Equal(base.Add(accum), s.mutableState.executionState.StartTime.AsTime())
		s.Equal(base.Add(accum), s.mutableState.executionInfo.StartTime.AsTime())
		s.Equal(base.Add(time.Minute).Add(accum), s.mutableState.executionInfo.ExecutionTime.AsTime())
		s.Equal(base.Add(time.Hour).Add(accum), s.mutableState.executionInfo.WorkflowRunExpirationTime.AsTime())
		s.Nil(s.mutableState.executionInfo.WorkflowExecutionExpirationTime, "unset timestamp must remain unset")
	})
}

// TestApplyFastForward covers the full branch table of applyFastForward:
// FastForward set / nil duration / nil config / Enabled=false.
// The first-init virtual-time path is covered separately in TestInitTimeSkippingInfo.
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

	s.Run("FastForward_NilDuration_NoOp", func() {
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
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true, FastForward: durationpb.New(time.Hour)},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime:    timestamppb.New(fixed), // stale bound that must be overridden
				SourceEventId: 7,
			},
		}
		s.mutableState.applyFastForward(eventID, nil)
		ff := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(ff)
		s.Equal(fixed.Add(time.Hour), ff.GetTargetTime().AsTime(), "stale bound is replaced by now+FastForward")
		s.Equal(eventID, ff.GetSourceEventId())
	})
}

// TestFindNextSkipTarget exercises the full candidate-selection branch table: pending timers,
// activity retry backoffs, the workflow backoff (ExecutionTime) branch, the run/execution
// timeouts (all valid targets), and the fast-forward gate that both contributes a candidate and
// disables time skipping once its target is at or before now.
func (s *mutableStateSuite) TestFindNextSkipTarget() {
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	addTimer := func(id string, expiry time.Time) {
		s.mutableState.pendingTimerInfoIDs[id] = &persistencespb.TimerInfo{
			TimerId:    id,
			ExpiryTime: timestamppb.New(expiry),
		}
	}
	setFastForward := func(target time.Time) {
		s.mutableState.executionInfo.TimeSkippingInfo.FastForwardInfo =
			&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(target)}
	}

	// resetMS gives each subtest a fresh, deterministic MS.
	resetMS := func() {
		ts := clock.NewEventTimeSource()
		ts.Update(baseTime)
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
		// No timeouts unless a case sets them.
		s.mutableState.executionInfo.WorkflowRunExpirationTime = nil
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = nil
	}

	s.Run("NoCandidates_InvalidTransition", func() {
		resetMS()
		s.Nil(s.mutableState.findNextSkipTarget())
	})

	s.Run("OneUserTimer_TargetIsTimer", func() {
		resetMS()
		t1 := baseTime.Add(2 * time.Hour)
		addTimer("t1", t1)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(t1, tr.TargetTime)
		s.False(tr.DisabledAfterFastForward)
	})

	s.Run("TwoUserTimers_TargetIsEarliest", func() {
		resetMS()
		addTimer("t1", baseTime.Add(2*time.Hour))
		addTimer("t2", baseTime.Add(3*time.Hour))

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(baseTime.Add(2*time.Hour), tr.TargetTime)
	})

	s.Run("UserTimer_PlusEarlierFastForward_TargetIsFastForward", func() {
		resetMS()
		addTimer("t1", baseTime.Add(3*time.Hour))
		fastForwardTarget := baseTime.Add(time.Hour)
		setFastForward(fastForwardTarget)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(fastForwardTarget, tr.TargetTime)
		// The fast-forward is the earliest target (earlier than the timer), so skipping to it
		// consumes the budget and disables time skipping on this transition.
		s.True(tr.DisabledAfterFastForward)
	})

	s.Run("Backoff_NotChildAndExecutionTimeFuture_IsCandidate", func() {
		resetMS()
		// !HadOrHasWorkflowTask: no completed and no pending workflow task.
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		// ExecutionTime > StartTime: a real backoff is configured.
		execTime := baseTime.Add(time.Hour)
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(execTime)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(execTime, tr.TargetTime)
		s.False(tr.DisabledAfterFastForward)
	})

	s.Run("Backoff_ChildWFCase_NotCandidate", func() {
		resetMS()
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		// ExecutionTime == StartTime: child WF "no first WT scheduled yet" case, no backoff.
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(baseTime)

		s.Nil(s.mutableState.findNextSkipTarget(),
			"child WF without backoff and no other candidate must yield invalid transition")
	})

	s.Run("Backoff_CaNWithInheritedSkip_PastExecutionTime_NotCandidate", func() {
		resetMS()
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(baseTime.Add(10 * time.Minute))
		// Inherited accumulated skip puts virtual now (baseTime+1h) past ExecutionTime.
		s.mutableState.timeSource = clock.WrapTimeSourceWithTimeSkipping(
			clock.NewEventTimeSource().Update(baseTime),
			func() time.Duration { return time.Hour },
		)
		s.mutableState.executionInfo.TimeSkippingInfo.AccumulatedSkippedDuration = durationpb.New(time.Hour)

		s.Nil(s.mutableState.findNextSkipTarget(),
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

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(schedTime, tr.TargetTime)
		s.False(tr.DisabledAfterFastForward)
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

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(early, tr.TargetTime)
	})

	s.Run("ActivityBackoff_PlusEarlierTimer_TargetIsTimer", func() {
		resetMS()
		timerTime := baseTime.Add(time.Hour)
		addTimer("t1", timerTime)
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
			ScheduledEventId: 1, HasRetryPolicy: true, Attempt: 2,
			ScheduledTime: timestamppb.New(baseTime.Add(2 * time.Hour)),
		}

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(timerTime, tr.TargetTime)
	})

	s.Run("RunTimeoutIsATarget", func() {
		resetMS()
		runTimeout := baseTime.Add(2 * time.Hour)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runTimeout)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(runTimeout, tr.TargetTime)
	})

	s.Run("ExecutionTimeoutIsATarget", func() {
		resetMS()
		execTimeout := baseTime.Add(3 * time.Hour)
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(execTimeout)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(execTimeout, tr.TargetTime)
	})

	s.Run("EarliestOfRunAndExecutionTimeoutWins", func() {
		resetMS()
		runTimeout := baseTime.Add(2 * time.Hour)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runTimeout)
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(baseTime.Add(5 * time.Hour))

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(runTimeout, tr.TargetTime, "the earlier run timeout must win")
	})

	s.Run("ExpiredTimeoutsAreNotTargets", func() {
		resetMS()
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(baseTime.Add(-time.Hour))
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(baseTime.Add(-2 * time.Hour))

		s.Nil(s.mutableState.findNextSkipTarget(), "timeouts in the past cannot be skip targets")
	})

	s.Run("FastForwardLaterThanRunTimeout_TimeoutWins", func() {
		resetMS()
		runTimeout := baseTime.Add(30 * time.Minute)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runTimeout)
		setFastForward(baseTime.Add(2 * time.Hour)) // fast-forward target is later than the timeout

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(runTimeout, tr.TargetTime, "skip is bounded by the earlier run timeout")
		s.False(tr.DisabledAfterFastForward, "fast-forward not reached: it was not the chosen target")
	})

	s.Run("FastForwardEarlierThanRunTimeout_FastForwardWins", func() {
		resetMS()
		fastForwardTarget := baseTime.Add(30 * time.Minute)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(baseTime.Add(2 * time.Hour))
		setFastForward(fastForwardTarget)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(fastForwardTarget, tr.TargetTime)
		s.True(tr.DisabledAfterFastForward, "fast-forward is the earliest target: skipping to it disables time skipping")
	})

	s.Run("FastForwardLaterThanExecutionTimeout_TimeoutWins", func() {
		resetMS()
		execTimeout := baseTime.Add(30 * time.Minute)
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(execTimeout)
		setFastForward(baseTime.Add(2 * time.Hour))

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(execTimeout, tr.TargetTime, "skip is bounded by the earlier execution timeout")
		s.False(tr.DisabledAfterFastForward)
	})

	s.Run("ZeroValuedTimeoutsAreIgnored_FastForwardWins", func() {
		resetMS()
		fastForwardTarget := baseTime.Add(time.Hour)
		setFastForward(fastForwardTarget)
		// Zero-valued timestamps mean "no timeout configured" and must not be candidates.
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(time.Time{})
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(time.Time{})

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(fastForwardTarget, tr.TargetTime, "a zero-valued timeout must not cap the skip")
		// The fast-forward is the only (and therefore earliest) target, so it is reached and
		// time skipping is disabled.
		s.True(tr.DisabledAfterFastForward)
	})

	s.Run("FastForwardAtNow_DisablesAfterFastForward", func() {
		resetMS()
		// Fast-forward target is at (virtual) now: it has been reached, so time skipping
		// is disabled and the target is now itself.
		setFastForward(baseTime)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(baseTime, tr.TargetTime)
		s.True(tr.DisabledAfterFastForward)
	})
}

// TestCloseTransactionHandleWorkflowTimeSkipping verifies the skippability gate is wired into the
// active-policy path: even when a valid skip target exists (a user timer), a non-skippable workflow
// (here, nil time-skipping config) must not skip — needRegenTasks is false and no state changes.
func (s *mutableStateSuite) TestCloseTransactionHandleWorkflowTimeSkipping() {
	// A valid skip target exists: a user timer one hour in the (virtual) future.
	s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{
		TimerId:    "t1",
		ExpiryTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
	}
	// ...but time skipping is not enabled (nil config), so the workflow is not skippable.
	s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{Config: nil}
	s.mutableState.timeSkippingInfoUpdated = false

	// Make the intent explicit: the target is real, but the gate must reject the workflow.
	s.Require().True(s.mutableState.findNextSkipTarget().IsValid(), "the user timer is a valid skip target")
	s.Require().False(s.mutableState.isWorkflowSkippable(), "nil config means not skippable")

	needRegen := s.mutableState.closeTransactionHandleWorkflowTimeSkipping(
		context.Background(), historyi.TransactionPolicyActive)

	s.False(needRegen, "the gate must prevent skipping even though a valid target exists")
	s.False(s.mutableState.timeSkippingInfoUpdated, "a gated-out transaction must not change state")
}

// TestTimeSkippingTransition covers the pure timeSkippingTransition data structure:
// its constructor, validity check, earliest-future-time tracking, and fast-forward
// gating. It needs no mutable state, so it is a plain test rather than a suite method.
func TestTimeSkippingTransition(t *testing.T) {
	t.Parallel()
	base := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("New sets only the current time", func(t *testing.T) {
		tr := NewTimeSkippingTransition(base)
		require.Equal(t, base, tr.CurrentTime)
		require.True(t, tr.TargetTime.IsZero())
		require.False(t, tr.DisabledAfterFastForward)
		require.False(t, tr.IsValid(), "a transition with no target and no disable signal is invalid")
	})

	// Invariant 1: every method is nil-safe — on a nil receiver, and (for GateByFastForward)
	// on a nil/absent fast-forward argument.
	t.Run("nil safe", func(t *testing.T) {
		var nilTr *timeSkippingTransition
		require.False(t, nilTr.IsValid(), "nil transition is never valid")
		require.NotPanics(t, func() { nilTr.TrackEarliestFutureTime(base.Add(time.Hour)) })
		require.NotPanics(t, func() {
			nilTr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
		})

		// A nil or empty fast-forward must be a no-op, not a spurious disable. A nil proto
		// timestamp's AsTime() is the Unix epoch (not the Go zero time), so this guards against
		// treating "no fast-forward" as a past target.
		tr := NewTimeSkippingTransition(base)
		require.NotPanics(t, func() { tr.GateByFastForward(nil) })
		tr.GateByFastForward(nil)
		require.True(t, tr.TargetTime.IsZero())
		require.False(t, tr.DisabledAfterFastForward)

		tr.GateByFastForward(&persistencespb.FastForwardInfo{}) // non-nil ff, nil target time
		require.True(t, tr.TargetTime.IsZero())
		require.False(t, tr.DisabledAfterFastForward)
		require.False(t, tr.IsValid())
	})

	// Invariant 2: TrackEarliestFutureTime keeps the earliest strictly-trackable future time
	// and ignores anything that is not a usable future skip target.
	t.Run("earliest future time", func(t *testing.T) {
		t.Run("ignores zero and past candidates", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(time.Time{})          // zero candidate
			tr.TrackEarliestFutureTime(base.Add(-time.Hour)) // past candidate
			require.True(t, tr.TargetTime.IsZero())
		})

		t.Run("keeps the earliest of several future candidates", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(3 * time.Hour))
			require.Equal(t, base.Add(3*time.Hour), tr.TargetTime)

			tr.TrackEarliestFutureTime(base.Add(time.Hour)) // earlier wins
			require.Equal(t, base.Add(time.Hour), tr.TargetTime)

			tr.TrackEarliestFutureTime(base.Add(2 * time.Hour)) // later is ignored
			require.Equal(t, base.Add(time.Hour), tr.TargetTime)
		})

		t.Run("accepts a candidate equal to the current time", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base)
			require.Equal(t, base, tr.TargetTime)
		})
	})

	// Invariant 3: the fast-forward target is taken — and disables time skipping — exactly when
	// it is the earliest target (nothing earlier tracked). When a real candidate is earlier the
	// fast-forward is not reached and skipping stays enabled. An absent/reached/zero fast-forward
	// is a no-op.
	t.Run("fast-forward fallback and gating", func(t *testing.T) {
		t.Run("taken as the target and disables when nothing earlier exists", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, base.Add(time.Hour).Equal(tr.TargetTime))
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})

		t.Run("an earlier tracked target wins over a later fast-forward", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(time.Hour))
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(3 * time.Hour))})
			require.Equal(t, base.Add(time.Hour), tr.TargetTime)
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("an earlier fast-forward wins over a later tracked target and disables", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(3 * time.Hour))
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, base.Add(time.Hour).Equal(tr.TargetTime))
			require.True(t, tr.DisabledAfterFastForward)
		})

		t.Run("ignores an already-reached fast-forward", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{
				HasReached: true,
				TargetTime: timestamppb.New(base.Add(time.Hour)),
			})
			require.True(t, tr.TargetTime.IsZero())
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("ignores a zero-valued target time", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(time.Time{})})
			require.True(t, tr.TargetTime.IsZero())
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("target equal to current disables and sets the target", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base)})
			require.True(t, base.Equal(tr.TargetTime))
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})

		t.Run("past target disables as a bare signal", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(-time.Hour))})
			require.True(t, tr.TargetTime.IsZero(), "a past target is not a future skip target")
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})
	})

	// Invariant 4: without a current time the transition is always invalid and no setter can
	// make it valid — every field is relative to the current time.
	t.Run("no current time always invalid", func(t *testing.T) {
		t.Run("a directly-set target is still invalid", func(t *testing.T) {
			tr := &timeSkippingTransition{TargetTime: base.Add(time.Hour)}
			require.False(t, tr.IsValid())
		})

		t.Run("a directly-set disable signal is still invalid", func(t *testing.T) {
			tr := &timeSkippingTransition{DisabledAfterFastForward: true}
			require.False(t, tr.IsValid())
		})

		t.Run("setters are no-ops without a current time", func(t *testing.T) {
			tr := &timeSkippingTransition{}
			tr.TrackEarliestFutureTime(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, tr.TargetTime.IsZero())
			require.False(t, tr.DisabledAfterFastForward)
			require.False(t, tr.IsValid())
		})
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
