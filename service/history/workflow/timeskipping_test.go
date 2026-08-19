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
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/components/nexusoperations"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *mutableStateSuite) TestPropagateTimeSkippingToNextRun() {
	fixed := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)
	fixedTS := timestamppb.New(fixed)

	s.Run("PendingFastForward_PropagatesTargetTime", func() {
		tsi := &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)},
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: fixedTS,
				HasReached: false,
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(tsi)
		s.Require().NotNil(stateProp.GetFastForwardTargetTime())
		s.Equal(fixed, stateProp.GetFastForwardTargetTime().AsTime())
		s.Require().NotNil(tsc)
		s.True(proto.Equal(tsc, tsi.GetConfig()))
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("ReachedFastForward_StillPropagatesTargetTime", func() {
		tsi := &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           false,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)},
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: fixedTS,
				HasReached: true,
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(tsi)
		s.Require().NotNil(tsc, "config is propagated regardless of enabled")
		s.True(proto.Equal(tsc, tsi.GetConfig()))
		s.NotNil(stateProp)
		s.Require().NotNil(stateProp.GetFastForwardTargetTime(),
			"a completed fast-forward is still propagated so the next run can report completion")
		s.Equal(fixed, stateProp.GetFastForwardTargetTime().AsTime())
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("NilConfig_PropagatesVirtualTime", func() {
		tsi := &persistencespb.TimeSkippingInfo{
			Config:                     nil,
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(tsi)
		s.Nil(tsc)
		s.Nil(stateProp.GetFastForwardTargetTime())
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
		s.requireInitNoPanic(tsc, stateProp)
	})

	// Regression: with no accumulated duration the stateProp starts nil; an active
	// (not-yet-reached) fast-forward must still be propagated without a nil deref.
	s.Run("NoAccumulatedSkip_StillPropagatesFastForwardTarget", func() {
		tsi := &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)},
			},
			// no AccumulatedSkippedDuration
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: fixedTS,
				HasReached: false,
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(tsi)
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Require().NotNil(stateProp)
		s.Zero(stateProp.GetInitialSkippedDuration().AsDuration(), "no virtual time accumulated yet")
		s.Require().NotNil(stateProp.GetFastForwardTargetTime())
		s.Equal(fixed, stateProp.GetFastForwardTargetTime().AsTime())
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("NoAccumulatedSkipNoFastForward_EmptyState", func() {
		tsi := &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(tsi)
		s.Require().NotNil(tsc, "enabled config is still propagated")
		s.True(tsc.GetEnabled())
		// stateProp is always returned; it just carries nothing meaningful here.
		s.Require().NotNil(stateProp)
		s.Zero(stateProp.GetInitialSkippedDuration().AsDuration())
		s.Nil(stateProp.GetFastForwardTargetTime())
		s.Zero(stateProp.GetInitialSkipCount())
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("NilTimeSkippingInfo_PropagatesNothing", func() {
		tsc, stateProp := propagateTimeSkippingToNextRun(nil)
		s.Nil(tsc)
		s.Nil(stateProp)
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("SkipCountOnly_PropagatesSkipCount", func() {
		tsc, stateProp := propagateTimeSkippingToNextRun(&persistencespb.TimeSkippingInfo{
			Config:           &commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: 50},
			SessionSkipCount: 2,
		})
		s.Require().NotNil(tsc)
		s.Require().NotNil(stateProp)
		s.Zero(stateProp.GetInitialSkippedDuration().AsDuration())
		s.Equal(int32(2), stateProp.GetInitialSkipCount())
		s.Equal(int32(50), tsc.MaxSessionSkipCount)
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("FullState_PropagatesEverything", func() {
		tsc, stateProp := propagateTimeSkippingToNextRun(&persistencespb.TimeSkippingInfo{
			Config:                     &commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: 50},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			SessionSkipCount:           3,
		})
		s.Require().NotNil(tsc)
		s.Equal(int32(50), tsc.GetMaxSessionSkipCount())
		s.Require().NotNil(stateProp)
		s.Equal(int32(3), stateProp.GetInitialSkipCount())
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("DisabledConfig_StillPropagatesState", func() {
		tsc, stateProp := propagateTimeSkippingToNextRun(&persistencespb.TimeSkippingInfo{
			Config:                     &commonpb.TimeSkippingConfig{Enabled: false, MaxSessionSkipCount: 50},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			SessionSkipCount:           3,
		})
		// Everything propagates regardless of enabled so the next run reflects the latest state.
		s.Require().NotNil(tsc, "config is propagated regardless of enabled")
		s.False(tsc.GetEnabled())
		s.Require().NotNil(stateProp)
		s.Equal(int32(3), stateProp.GetInitialSkipCount())
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("EmptyInfo_NilConfigAndEmptyState", func() {
		// Internal config nil (but TimeSkippingInfo present): must not panic; config is nil,
		// stateProp is still returned and empty.
		tsc, stateProp := propagateTimeSkippingToNextRun(&persistencespb.TimeSkippingInfo{})
		s.Nil(tsc, "nil config -> nil propagated config")
		s.Require().NotNil(stateProp)
		s.Zero(stateProp.GetInitialSkippedDuration().AsDuration())
		s.Nil(stateProp.GetFastForwardTargetTime())
		s.Zero(stateProp.GetInitialSkipCount())
		s.requireInitNoPanic(tsc, stateProp)
	})

	s.Run("DisablePropagationFlag_PreservedInChainOfRuns", func() {
		// Chain-of-runs clones the full config, so DisablePropagation is preserved. Contrast with
		// propagateTimeSkippingToOtherExecution, which never carries the flag onto a child config.
		tsi := &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:             true,
				DisablePropagation:  true,
				MaxSessionSkipCount: 5,
			},
		}
		tsc, stateProp := propagateTimeSkippingToNextRun(tsi)
		s.Require().NotNil(tsc)
		s.True(tsc.GetDisablePropagation())
		s.Equal(int32(5), tsc.GetMaxSessionSkipCount())
		s.requireInitNoPanic(tsc, stateProp)
	})
}

func (s *mutableStateSuite) TestPropagateTimeSkippingToOtherExecution() {
	accumSkip := time.Hour
	sessionMaxSkipCount := int32(10)
	newTSI := func() *persistencespb.TimeSkippingInfo {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		return &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:             true,
				FastForwardConfig:   &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)},
				MaxSessionSkipCount: sessionMaxSkipCount,
			},
			AccumulatedSkippedDuration: durationpb.New(accumSkip),
			SessionSkipCount:           3,
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(s.mutableState.timeSource.Now().Add(3 * time.Hour)),
				HasReached: false,
			},
		}
	}

	s.Run("NilTimeSkippingInfo_PropagatesNothing", func() {
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(nil)
		s.Nil(tsc)
		s.Nil(propagatedState)
		s.requireInitNoPanic(tsc, propagatedState)
	})

	s.Run("FullState_PropagatesConfigAndVirtualTime", func() {
		tsi := newTSI()
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(tsi)
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		// no fast-forward
		s.Nil(tsc.GetFastForwardConfig().GetDuration())
		// has virtual time
		s.Equal(accumSkip, propagatedState.GetInitialSkippedDuration().AsDuration())
		// has config of max skip
		s.Equal(sessionMaxSkipCount, tsc.GetMaxSessionSkipCount())
		// no accumulated session skip
		s.Equal(int32(0), propagatedState.GetInitialSkipCount())
		s.requireInitNoPanic(tsc, propagatedState)
	})

	s.Run("FastForward_NeverPropagatedToChild", func() {
		tsi := newTSI() // enabled parent carrying an active, unreached fast-forward
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(tsi)
		s.Require().NotNil(tsc)
		s.Nil(tsc.GetFastForwardConfig(), "child never inherits the fast-forward config")
		s.Require().NotNil(propagatedState)
		s.Nil(propagatedState.GetFastForwardTargetTime(),
			"fast-forward is per-execution; a child never inherits the fast-forward target time")
		s.requireInitNoPanic(tsc, propagatedState)
	})

	s.Run("DisablePropagationFlag_NeverCarriedToChild", func() {
		// Config only propagates when !DisablePropagation, so the flag is structurally always
		// false on a propagated child config -- there is nothing to carry down the tree.
		tsi := newTSI()
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(tsi)
		s.Require().NotNil(tsc)
		s.False(tsc.GetDisablePropagation())
		s.requireInitNoPanic(tsc, propagatedState)
	})

	s.Run("DisablePropagationSet_NoConfigPropagated", func() {
		tsi := newTSI()
		tsi.Config.DisablePropagation = true
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(tsi)
		s.Nil(tsc)
		s.Require().NotNil(propagatedState)
		s.Equal(accumSkip, propagatedState.GetInitialSkippedDuration().AsDuration())
		s.Equal(int32(0), propagatedState.GetInitialSkipCount())
		s.Nil(propagatedState.GetFastForwardTargetTime())
		s.requireInitNoPanic(tsc, propagatedState)
	})

	s.Run("NilConfig_PropagatesVirtualTime", func() {
		tsi := newTSI()
		tsi.Config = nil
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(tsi)
		s.Nil(tsc)
		s.Require().NotNil(propagatedState)
		s.Equal(int32(0), propagatedState.GetInitialSkipCount())
		s.Equal(time.Hour, propagatedState.GetInitialSkippedDuration().AsDuration(),
			"virtual time is always propagated, even when config propagation is disabled")
		s.Nil(propagatedState.GetFastForwardTargetTime())
		s.requireInitNoPanic(tsc, propagatedState)
	})

	s.Run("DisabledParentConfig_PropagatesDisabledConfig", func() {
		tsi := newTSI()
		tsi.Config.Enabled = false
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(tsi)
		// enabled is copied through: a disabled-but-propagating parent hands the child a disabled config.
		s.Require().NotNil(tsc)
		s.False(tsc.GetEnabled())
		s.Equal(sessionMaxSkipCount, tsc.GetMaxSessionSkipCount())
		s.Nil(tsc.GetFastForwardConfig())
		s.Require().NotNil(propagatedState)
		s.Equal(time.Hour, propagatedState.GetInitialSkippedDuration().AsDuration(),
			"virtual time is always propagated regardless of enabled")
		s.Nil(propagatedState.GetFastForwardTargetTime())
		s.Equal(int32(0), propagatedState.GetInitialSkipCount())
		s.requireInitNoPanic(tsc, propagatedState)
	})

	s.Run("ZeroAccumulatedSkip_ConfigButNilState", func() {
		tsi := newTSI()
		tsi.AccumulatedSkippedDuration = nil
		tsc, propagatedState := propagateTimeSkippingToOtherExecution(tsi)
		s.Require().NotNil(tsc, "an enabled config still propagates with no accumulated skip")
		s.True(tsc.GetEnabled())
		s.Nil(propagatedState, "no accumulated skip -> no state to propagate")
		s.requireInitNoPanic(tsc, propagatedState)
	})
}

func (s *mutableStateSuite) TestIsWorkflowSkippable() {

	// base case: ensures the other tests modifications are the reason to make the workflow not skippable
	s.Run("EnabledAndIdle_Skippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true}}
		s.True(s.mutableState.isWorkflowSkippable())
	})

	// config tests: nil safe, and false safe proof
	s.Run("NilTimeSkippingInfo_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("NilConfig_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{Config: nil}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("DisabledConfig_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
		}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	// state and status tests
	s.Run("WorkflowNotRunning_NotSkippable", func() {
		s.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("WorkflowPaused_NotSkippable", func() {
		s.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	// inflight work tests
	s.Run("PendingWorkflowTask_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = 1
		s.True(s.mutableState.HasPendingWorkflowTask())
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("PendingActivity_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("PendingSignalExternal_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingSignalInfoIDs[1] = &persistencespb.SignalInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("PendingRequestCancelExternal_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingRequestCancelInfoIDs[1] = &persistencespb.RequestCancelInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("PendingChildExecution_NotSkippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingChildExecutionInfoIDs[1] = &persistencespb.ChildExecutionInfo{}
		s.False(s.mutableState.isWorkflowSkippable())
	})

	s.Run("PendingNexusOperation_NotSkippable", func() {
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
	s.Run("PendingTimers_Skippable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{TimerId: "t1"}
		s.True(s.mutableState.isWorkflowSkippable())
	})

	s.Run("ActivityInRetryBackoff_Skippable", func() {
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

	s.Run("ActivityRetryDueNowOrPast_NotSkippable", func() {
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

func (s *mutableStateSuite) requireInitNoPanic(
	cfg *commonpb.TimeSkippingConfig,
	sp *commonpb.TimeSkippingStatePropagation,
) {
	s.mutableState.timeSource = clock.NewEventTimeSource()
	s.mutableState.executionInfo.TimeSkippingInfo = nil
	s.mutableState.PopTasks()
	s.NotPanics(func() { s.mutableState.initTimeSkippingInfo(cfg, sp) })
}

func (s *mutableStateSuite) TestInitTimeSkippingInfo() {
	s.Run("NilConfigAndNilPropagation_NoOp", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		s.NotPanics(func() {
			s.mutableState.initTimeSkippingInfo(nil, nil)
		})
		s.Nil(s.mutableState.executionInfo.TimeSkippingInfo)
		s.Equal(baseTime, s.mutableState.Now())
		s.False(s.mutableState.timeSkippingInfoUpdated, "a no-op init must not mark updated")
	})

	s.Run("ConfigOnlyNoPropagation_SetsFastForwardInfo", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 5}
		nextVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 6}
		s.mutableState.executionInfo.TransitionHistory = []*persistencespb.VersionedTransition{currentVT}
		cfg := &commonpb.TimeSkippingConfig{
			Enabled:           true,
			FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)}}

		s.mutableState.initTimeSkippingInfo(cfg, nil)
		s.Equal(baseTime, s.mutableState.Now())
		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.True(proto.Equal(cfg, tsi.GetConfig()))
		s.Require().NotNil(tsi.GetFastForwardInfo())
		s.Require().Nil(tsi.GetAccumulatedSkippedDuration())
		// timestamppb translates to UTC time
		s.Require().Equal(baseTime.Add(3*time.Hour).UTC(),
			tsi.GetFastForwardInfo().GetTargetTime().AsTime())
		// the fast-forward records the current versioned transition
		s.Require().True(proto.Equal(nextVT, tsi.GetFastForwardInfoLastUpdateVersionedTransition()))
		s.True(s.mutableState.timeSkippingInfoUpdated, "a real init marks updated")
	})

	s.Run("WithPropagatedState_ShiftsClockAndSetsFastForward", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()

		// all local time
		baseTime := s.mutableState.timeSource.Now()
		targetTime := baseTime.Add(3 * time.Hour)

		hasSkipped := 2 * time.Hour
		fastForward := 3 * time.Hour

		cfg := &commonpb.TimeSkippingConfig{
			Enabled:            true,
			FastForwardConfig:  &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward)},
			DisablePropagation: true,
		}
		propagation := &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(hasSkipped),
			FastForwardTargetTime:  timestamppb.New(targetTime),
		}
		s.mutableState.initTimeSkippingInfo(cfg, propagation)

		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.Equal(baseTime.Add(hasSkipped), s.mutableState.Now())
		s.Equal(hasSkipped, tsi.GetAccumulatedSkippedDuration().AsDuration())
		s.Equal(targetTime.UTC(),
			tsi.GetFastForwardInfo().GetTargetTime().AsTime())
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})

	// Invariant 4: propagation may carry an enabled config whose fast-forward target is already due
	// once the virtual clock (real + accumulated skip) is established at init. Init must disable time
	// skipping, matching the runtime flip-enabled model.
	s.Run("EnabledConfigWithDueFastForwardTarget_DisablesSkipping", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		baseTime := s.mutableState.timeSource.Now()
		hasSkipped := 2 * time.Hour
		dueTarget := baseTime.Add(time.Hour) // < virtual now (baseTime + 2h)

		cfg := &commonpb.TimeSkippingConfig{
			Enabled:           true,
			FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)},
		}
		propagation := &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(hasSkipped),
			FastForwardTargetTime:  timestamppb.New(dueTarget),
		}
		s.mutableState.PopTasks()
		s.mutableState.initTimeSkippingInfo(cfg, propagation)

		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.False(tsi.GetConfig().GetEnabled(), "a due fast-forward at init disables time skipping")
		s.Require().NotNil(tsi.GetFastForwardInfo())
		s.True(tsi.GetFastForwardInfo().GetHasReached())
		s.Empty(s.mutableState.PopTasks()[tasks.CategoryTimer], "a reached target schedules no timer")
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("InheritedSkipOnlyNoConfig_ShiftsClock", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		baseTime := s.mutableState.timeSource.Now()
		hasSkipped := 2 * time.Hour
		propagation := &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(hasSkipped),
		}

		s.mutableState.initTimeSkippingInfo(nil, propagation)

		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.Nil(tsi.GetConfig())
		s.Equal(hasSkipped, tsi.GetAccumulatedSkippedDuration().AsDuration())
		s.Equal(baseTime.Add(hasSkipped), s.mutableState.Now(), "inherited skip still shifts the virtual clock")
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("WithPropagatedSkipCount_SeedsSessionSkipCount", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		cfg := &commonpb.TimeSkippingConfig{Enabled: true}
		propagation := &commonpb.TimeSkippingStatePropagation{InitialSkipCount: 4}
		s.mutableState.initTimeSkippingInfo(cfg, propagation)
		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.Equal(int32(4), tsi.GetSessionSkipCount())
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("WithoutPropagation_SessionSkipCountIsZero", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		cfg := &commonpb.TimeSkippingConfig{Enabled: true}
		s.mutableState.initTimeSkippingInfo(cfg, nil)
		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.Equal(int32(0), tsi.GetSessionSkipCount())
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})
}

func (s *mutableStateSuite) TestUpdateTimeSkippingInfo() {
	s.Run("UpdateWithNilConfig_ClearsConfigAndFastForward", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentTSI := &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour)},
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(baseTime.Add(time.Hour)),
				HasReached: false,
			},
			FastForwardInfoLastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 7},
		}
		s.mutableState.executionInfo.TimeSkippingInfo = currentTSI
		s.mutableState.timeSkippingInfoUpdated = false
		s.mutableState.updateTimeSkippingInfo(nil)
		newTSI := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(newTSI)
		s.Nil(newTSI.GetConfig())
		s.Nil(newTSI.GetFastForwardInfo())
		// clearing the fast-forward still stamps the versioned transition; the field is never cleared
		s.Require().NotNil(newTSI.GetFastForwardInfoLastUpdateVersionedTransition())
		s.Equal(s.mutableState.NextTransitionCount(), newTSI.GetFastForwardInfoLastUpdateVersionedTransition().GetTransitionCount())
		s.Equal(currentTSI.GetAccumulatedSkippedDuration(), newTSI.GetAccumulatedSkippedDuration())
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("EnableWithFastForward_InstallsFastForwardInfo", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 5}
		nextVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 6}
		s.mutableState.executionInfo.TransitionHistory = []*persistencespb.VersionedTransition{currentVT}
		currentTSI := persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled: false,
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.executionInfo.TimeSkippingInfo = &currentTSI

		// new config
		newConfig := &commonpb.TimeSkippingConfig{
			Enabled:            true,
			FastForwardConfig:  &commonpb.FastForwardConfig{Duration: durationpb.New(2 * time.Hour)},
			DisablePropagation: true,
		}
		s.mutableState.updateTimeSkippingInfo(newConfig)
		newTSI := s.mutableState.executionInfo.GetTimeSkippingInfo()

		s.Require().NotNil(newTSI)
		s.True(proto.Equal(newConfig, newTSI.GetConfig()))
		s.Require().NotNil(newTSI.GetFastForwardInfo())
		// re-installing the fast-forward records the current versioned transition, so a task
		// emitted here validates against a stable reference after a failover.
		s.True(proto.Equal(nextVT, newTSI.GetFastForwardInfoLastUpdateVersionedTransition()))
		s.Equal(baseTime.Add(2*time.Hour).UTC(), newTSI.GetFastForwardInfo().GetTargetTime().AsTime())
		s.False(newTSI.GetFastForwardInfo().GetHasReached())
		s.Equal(time.Hour, newTSI.GetAccumulatedSkippedDuration().AsDuration())
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("OverrideFastForwardThenDisable_ReinstallsThenClears", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 8}
		nextVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 9}
		s.mutableState.executionInfo.TransitionHistory = []*persistencespb.VersionedTransition{currentVT}
		currentTSI := persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour)},
			},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(baseTime.Add(time.Hour)),
				HasReached: false,
			},
			FastForwardInfoLastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 3, TransitionCount: 5},
		}
		s.mutableState.executionInfo.TimeSkippingInfo = &currentTSI

		// update with new config with a new FF
		tsc2 := &commonpb.TimeSkippingConfig{
			Enabled:           true,
			FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(2 * time.Hour)},
		}
		s.mutableState.updateTimeSkippingInfo(tsc2)
		tsc2TSI := s.mutableState.executionInfo.GetTimeSkippingInfo()

		s.Require().NotNil(tsc2TSI)
		s.True(proto.Equal(tsc2, tsc2TSI.GetConfig()))
		s.Require().NotNil(tsc2TSI.GetFastForwardInfo())
		// re-applying records the current versioned transition, replacing the previous one
		s.True(proto.Equal(nextVT, tsc2TSI.GetFastForwardInfoLastUpdateVersionedTransition()))
		s.Equal(baseTime.Add(2*time.Hour).UTC(), tsc2TSI.GetFastForwardInfo().GetTargetTime().AsTime())
		s.False(tsc2TSI.GetFastForwardInfo().GetHasReached())
		s.Equal(time.Hour, tsc2TSI.GetAccumulatedSkippedDuration().AsDuration())

		// disable the time skipping
		tsc3 := &commonpb.TimeSkippingConfig{
			Enabled: false,
		}
		s.mutableState.updateTimeSkippingInfo(tsc3)
		tsc3TSI := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsc3TSI)
		s.True(proto.Equal(tsc3, tsc3TSI.GetConfig()))
		s.Nil(tsc3TSI.GetFastForwardInfo())
		s.True(proto.Equal(nextVT, tsc3TSI.GetFastForwardInfoLastUpdateVersionedTransition()),
			"clearing the fast-forward re-stamps the versioned transition rather than clearing it")
		s.Equal(time.Hour, tsc3TSI.GetAccumulatedSkippedDuration().AsDuration())
	})

	// Updating the config restarts the skip session: the per-session skip counter from the
	// previous session must be cleared.
	s.Run("Update_ResetsSessionSkipCount", func() {
		s.mutableState.timeSource = clock.NewEventTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:             false,
				MaxSessionSkipCount: 10,
			},
			SessionSkipCount: 7,
		}

		s.mutableState.updateTimeSkippingInfo(&commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: 10})

		tsi := s.mutableState.executionInfo.GetTimeSkippingInfo()
		s.Require().NotNil(tsi)
		s.Equal(int32(0), tsi.GetSessionSkipCount())
		s.True(s.mutableState.timeSkippingInfoUpdated)
	})
}

// TestApplyFastForward covers the full branch table of applyFastForward:
// FastForward set / nil duration / nil config / Enabled=false.
// The first-init virtual-time path is covered separately in TestInitTimeSkippingInfo.
func (s *mutableStateSuite) TestApplyFastForward() {
	s.Run("NilTimeSkippingInfo_NoOp", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		s.NotPanics(func() { s.mutableState.applyFastForward(nil) })
		s.Nil(s.mutableState.executionInfo.GetTimeSkippingInfo())
	})

	s.Run("NilConfig_ClearsFastForwardInfo", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: nil,
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
			},
		}
		s.NotPanics(func() { s.mutableState.applyFastForward(nil) })
		s.Nil(s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo(),
			"no ff config (nil config) -> ff info cleared")
	})

	s.Run("NoPropagatedTarget_SetsTargetFromDuration", func() {

		fastForwardDuration := 3 * time.Hour
		s.mutableState.timeSource = clock.NewEventTimeSource()
		baseTime := s.mutableState.timeSource.Now()
		currentVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 5}
		nextVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: tests.Version, TransitionCount: 6}

		s.mutableState.executionInfo.TransitionHistory = []*persistencespb.VersionedTransition{currentVT}

		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForwardDuration)}},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.PopTasks()
		s.mutableState.applyFastForward(nil)
		fastForward := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(fastForward)
		s.False(fastForward.GetHasReached())
		s.WithinDuration(fastForward.GetTargetTime().AsTime(), baseTime.Add(fastForwardDuration), 0)
		s.True(proto.Equal(nextVT, s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfoLastUpdateVersionedTransition()))

		timers := s.mutableState.PopTasks()[tasks.CategoryTimer]
		s.Require().Len(timers, 1, "an enabled config with a pending target schedules a fast-forward timer")
		s.WithinDuration(s.mutableState.ToRealTime(fastForward.GetTargetTime().AsTime()), timers[0].GetVisibilityTime(), 0)
	})

	s.Run("FuturePropagatedTarget_SchedulesTimer", func() {
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		propagatedTarget := fixed.Add(2 * time.Hour)
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)}},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.PopTasks()
		s.mutableState.applyFastForward(timestamppb.New(propagatedTarget))
		fastForward := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(fastForward)
		s.Equal(propagatedTarget, fastForward.GetTargetTime().AsTime(),
			"propagated target used directly, not recomputed from ff duration")
		s.NotNil(s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfoLastUpdateVersionedTransition())
		s.False(fastForward.GetHasReached(), "a future target has not been reached yet")

		timers := s.mutableState.PopTasks()[tasks.CategoryTimer]
		s.Require().Len(timers, 1, "a future target schedules a fast-forward timer")
		// AddTasks converts the virtual target to real wall-clock (minus accumulated skip).
		s.Equal(s.mutableState.ToRealTime(propagatedTarget), timers[0].GetVisibilityTime())
	})

	s.Run("FuturePropagatedTargetDisabledConfig_NoTimer", func() {
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		propagatedTarget := fixed.Add(2 * time.Hour)
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           false,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)}},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.PopTasks()
		s.mutableState.applyFastForward(timestamppb.New(propagatedTarget))
		fastForward := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(fastForward)
		s.Equal(propagatedTarget, fastForward.GetTargetTime().AsTime(),
			"propagated target used directly, not recomputed from ff duration")
		s.False(fastForward.GetHasReached(), "a future target has not been reached yet")
		s.Empty(s.mutableState.PopTasks()[tasks.CategoryTimer], "a disabled config schedules no timer")
	})

	s.Run("StalePropagatedTarget_MarkedReachedAndDisablesSkipping", func() {
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		staleTarget := fixed.Add(-2 * time.Hour)
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)}},
			AccumulatedSkippedDuration: durationpb.New(time.Hour),
		}
		s.mutableState.PopTasks()
		s.mutableState.applyFastForward(timestamppb.New(staleTarget))
		tsi := s.mutableState.executionInfo.TimeSkippingInfo
		fastForward := tsi.GetFastForwardInfo()
		s.Require().NotNil(fastForward)
		s.Equal(staleTarget, fastForward.GetTargetTime().AsTime())
		s.True(fastForward.GetHasReached(), "an already-due propagated target is treated as reached")
		s.False(tsi.GetConfig().GetEnabled(),
			"an already-due target completes the fast-forward, so time skipping is disabled")
		s.Empty(s.mutableState.PopTasks()[tasks.CategoryTimer], "a reached target schedules no timer")
	})

	s.Run("DisabledConfigWithReachedTarget_PreservesFastForwardInfo", func() {
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		completedTarget := fixed.Add(-2 * time.Hour)
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           false, // completing a fast-forward disables skipping
				FastForwardConfig: &commonpb.FastForwardConfig{Id: "ff-1", Duration: durationpb.New(3 * time.Hour)}},
		}
		s.mutableState.PopTasks()
		s.mutableState.applyFastForward(timestamppb.New(completedTarget))
		fastForward := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(fastForward, "a completed fast-forward is preserved even when disabled")
		s.True(fastForward.GetHasReached())
		s.Empty(s.mutableState.PopTasks()[tasks.CategoryTimer], "disabled config schedules no timer")
	})

	s.Run("DisabledConfigNoPropagatedTarget_InertFastForwardInfo", func() {
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           false,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour)}},
		}
		s.mutableState.PopTasks()
		s.mutableState.applyFastForward(nil)
		// Invariant: an existing ff config keeps ff-info alive, but a disabled config makes it inert
		// (fresh target, not reached, no timer).
		ff := s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo()
		s.Require().NotNil(ff)
		s.Equal(fixed.Add(3*time.Hour), ff.GetTargetTime().AsTime())
		s.False(ff.GetHasReached())
		s.Empty(s.mutableState.PopTasks()[tasks.CategoryTimer], "disabled config schedules no timer")
	})

	s.Run("NoFastForwardConfig_ClearsFastForwardInfo", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled: true},
		}
		s.mutableState.applyFastForward(nil)
		s.Nil(s.mutableState.executionInfo.TimeSkippingInfo.GetFastForwardInfo())
	})

	s.Run("NoFastForwardConfigWithExistingInfo_ClearsIt", func() {
		staleVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 7}
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
			},
			FastForwardInfoLastUpdateVersionedTransition: staleVT,
		}
		s.mutableState.applyFastForward(nil)
		tsi := s.mutableState.executionInfo.TimeSkippingInfo
		s.Nil(tsi.GetFastForwardInfo())
		ffVT := tsi.GetFastForwardInfoLastUpdateVersionedTransition()
		s.Require().NotNil(ffVT)
		s.False(proto.Equal(staleVT, ffVT), "removing the fast-forward re-stamps the versioned transition")
		s.Equal(s.mutableState.NextTransitionCount(), ffVT.GetTransitionCount())
	})

	s.Run("FastForward_AlreadyNil_LeavesVersionedTransitionUntouched", func() {
		staleVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 7}
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
			FastForwardInfoLastUpdateVersionedTransition: staleVT,
		}
		s.mutableState.applyFastForward(nil)
		tsi := s.mutableState.executionInfo.TimeSkippingInfo
		s.Nil(tsi.GetFastForwardInfo())
		s.True(proto.Equal(staleVT, tsi.GetFastForwardInfoLastUpdateVersionedTransition()))
	})

	s.Run("StaleExistingFastForwardInfo_Overridden", func() {
		fixed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		s.mutableState.timeSource = clock.NewEventTimeSource().Update(fixed)
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour)}},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(fixed), // stale bound that must be overridden
			},
			FastForwardInfoLastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 3, TransitionCount: 5},
		}
		s.mutableState.applyFastForward(nil)
		tsi := s.mutableState.executionInfo.TimeSkippingInfo
		ff := tsi.GetFastForwardInfo()
		s.Require().NotNil(ff)
		s.Equal(fixed.Add(time.Hour), ff.GetTargetTime().AsTime(), "stale bound is replaced by now+FastForward")
		ffVT := tsi.GetFastForwardInfoLastUpdateVersionedTransition()
		s.Equal(tests.Version, ffVT.GetNamespaceFailoverVersion())
		s.Equal(s.mutableState.NextTransitionCount(), ffVT.GetTransitionCount())
	})
}

func (s *mutableStateSuite) TestFindNextSkipTarget() {
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	fixedTimeSource := clock.NewEventTimeSource()
	fixedTimeSource.Update(baseTime)
	s.mutableState.timeSource = fixedTimeSource

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

	s.Run("NoCandidates_NoTarget", func() {
		resetMS()
		s.Nil(s.mutableState.findNextSkipTarget())
	})

	s.Run("NilTimeSkippingInfo_NoPanic", func() {
		resetMS()
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		var tr *timeSkippingTransition
		s.NotPanics(func() { tr = s.mutableState.findNextSkipTarget() })
		s.Nil(tr, "nil TimeSkippingInfo with no candidates yields no transition")
	})

	s.Run("NoWorkflowTaskYet_TimeoutsAreNotTargets", func() {
		resetMS()
		// !HadOrHasWorkflowTask: start event applied but the first workflow task is not scheduled yet.
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		// ExecutionTime == StartTime, so the backoff branch contributes no candidate either.
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(baseTime)
		// Both timeouts are in the future but must not be skip targets until a task has run --
		// otherwise a fresh child would skip to its run timeout before ever setting its timers.
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(baseTime.Add(2 * time.Hour))
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(baseTime.Add(3 * time.Hour))

		s.Nil(s.mutableState.findNextSkipTarget(),
			"run/execution timeouts must not be skip targets before the first workflow task")
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

	s.Run("UserTimerAndEarlierFastForward_TargetIsFastForward", func() {
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

	s.Run("FutureWorkflowBackoff_IsCandidate", func() {
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

	s.Run("ChildWithoutBackoff_NoCandidate", func() {
		resetMS()
		s.mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
		s.mutableState.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
		// ExecutionTime == StartTime: child WF "no first WT scheduled yet" case, no backoff.
		s.mutableState.executionInfo.ExecutionTime = timestamppb.New(baseTime)

		s.Nil(s.mutableState.findNextSkipTarget(),
			"child WF without backoff and no other candidate must yield invalid transition")
	})

	s.Run("PastExecutionTime_NoCandidate", func() {
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

	s.Run("ActivityBackoffAndEarlierTimer_TargetIsTimer", func() {
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

	s.Run("RunTimeout_IsTarget", func() {
		resetMS()
		runTimeout := baseTime.Add(2 * time.Hour)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runTimeout)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(runTimeout, tr.TargetTime)
	})

	s.Run("ExecutionTimeout_IsTarget", func() {
		resetMS()
		execTimeout := baseTime.Add(3 * time.Hour)
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(execTimeout)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(execTimeout, tr.TargetTime)
	})

	s.Run("RunAndExecutionTimeout_EarliestWins", func() {
		resetMS()
		runTimeout := baseTime.Add(2 * time.Hour)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runTimeout)
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(baseTime.Add(5 * time.Hour))

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(runTimeout, tr.TargetTime, "the earlier run timeout must win")
	})

	s.Run("ExpiredTimeouts_NotTargets", func() {
		resetMS()
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(baseTime.Add(-time.Hour))
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(baseTime.Add(-2 * time.Hour))

		s.Nil(s.mutableState.findNextSkipTarget(), "timeouts in the past cannot be skip targets")
	})

	s.Run("FastForwardAfterRunTimeout_TimeoutWins", func() {
		resetMS()
		runTimeout := baseTime.Add(30 * time.Minute)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(runTimeout)
		setFastForward(baseTime.Add(2 * time.Hour)) // fast-forward target is later than the timeout

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(runTimeout, tr.TargetTime, "skip is bounded by the earlier run timeout")
		s.False(tr.DisabledAfterFastForward, "fast-forward not reached: it was not the chosen target")
	})

	s.Run("FastForwardBeforeRunTimeout_FastForwardWins", func() {
		resetMS()
		fastForwardTarget := baseTime.Add(30 * time.Minute)
		s.mutableState.executionInfo.WorkflowRunExpirationTime = timestamppb.New(baseTime.Add(2 * time.Hour))
		setFastForward(fastForwardTarget)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(fastForwardTarget, tr.TargetTime)
		s.True(tr.DisabledAfterFastForward, "fast-forward is the earliest target: skipping to it disables time skipping")
	})

	s.Run("FastForwardAfterExecutionTimeout_TimeoutWins", func() {
		resetMS()
		execTimeout := baseTime.Add(30 * time.Minute)
		s.mutableState.executionInfo.WorkflowExecutionExpirationTime = timestamppb.New(execTimeout)
		setFastForward(baseTime.Add(2 * time.Hour))

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.Equal(execTimeout, tr.TargetTime, "skip is bounded by the earlier execution timeout")
		s.False(tr.DisabledAfterFastForward)
	})

	s.Run("ZeroValuedTimeouts_FastForwardWins", func() {
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

	s.Run("FastForwardTargetAtNow_DisablesSkipping", func() {
		resetMS()
		// Fast-forward target is at (virtual) now: already due, so it is a bare disable
		// signal with no skip target.
		setFastForward(baseTime)

		tr := s.mutableState.findNextSkipTarget()
		s.Require().NotNil(tr)
		s.True(tr.TargetTime.IsZero())
		s.True(tr.DisabledAfterFastForward)
	})
}

func (s *mutableStateSuite) TestCloseTransactionHandleWorkflowTimeSkipping() {
	// A valid skip target exists: a user timer one hour in the (virtual) future.
	s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{
		TimerId:    "t1",
		ExpiryTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
	}
	s.Require().True(s.mutableState.findNextSkipTarget().IsValid(), "the user timer is a valid skip target")

	s.Run("NilConfig_NoSkip", func() {
		// Time skipping is not enabled (nil config), so the workflow is not skippable.
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{Config: nil}
		s.mutableState.timeSkippingInfoUpdated = false
		s.Require().False(s.mutableState.isWorkflowSkippable(), "nil config means not skippable")

		needRegen := s.mutableState.closeTransactionHandleWorkflowTimeSkipping(
			context.Background(), historyi.TransactionPolicyActive)

		s.False(needRegen, "the gate must prevent skipping even though a valid target exists")
		s.False(s.mutableState.timeSkippingInfoUpdated, "a gated-out transaction must not change state")
	})

	s.Run("NonWorkflow_NoSkip", func() {
		// Enable time skipping so the only thing rejecting the skip is the non-workflow archetype.
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.timeSkippingInfoUpdated = false

		mockChasmTree := historyi.NewMockChasmTree(s.controller)
		mockChasmTree.EXPECT().ArchetypeID().Return(activity.ArchetypeID).AnyTimes()
		s.mutableState.chasmTree = mockChasmTree
		s.Require().False(s.mutableState.IsWorkflow(), "a non-workflow archetype must not report as a workflow")

		for _, policy := range []historyi.TransactionPolicy{
			historyi.TransactionPolicyActive,
			historyi.TransactionPolicyPassive,
		} {
			needRegen := s.mutableState.closeTransactionHandleWorkflowTimeSkipping(context.Background(), policy)
			s.False(needRegen, "a non-workflow must never skip time (policy %v)", policy)
			s.False(s.mutableState.timeSkippingInfoUpdated, "a non-workflow must not change state (policy %v)", policy)
		}
	})

	// The primary use case: an active, skippable workflow with a valid target skips time,
	// emits the transitioned event, and asks for task regeneration.
	s.Run("ActiveWithValidTarget_SkipsAndRegeneratesTasks", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{
			TimerId:    "t1",
			ExpiryTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
		}
		s.mutableState.timeSkippingInfoUpdated = false
		s.Require().True(s.mutableState.IsWorkflow())
		s.Require().True(s.mutableState.isWorkflowSkippable())

		needRegen := s.mutableState.closeTransactionHandleWorkflowTimeSkipping(
			context.Background(), historyi.TransactionPolicyActive)

		s.True(needRegen, "a skippable workflow with a valid target must skip time")
		s.True(s.mutableState.timeSkippingInfoUpdated, "a successful skip mutates state")
		s.Positive(s.mutableState.executionInfo.TimeSkippingInfo.GetAccumulatedSkippedDuration().AsDuration(),
			"the skip advances virtual time toward the timer target")
	})

	s.Run("ActiveWithoutValidTarget_NoSkip", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.timeSkippingInfoUpdated = false
		s.Require().True(s.mutableState.isWorkflowSkippable())
		s.Require().False(s.mutableState.findNextSkipTarget().IsValid(), "no candidate -> invalid transition")

		needRegen := s.mutableState.closeTransactionHandleWorkflowTimeSkipping(
			context.Background(), historyi.TransactionPolicyActive)
		s.False(needRegen, "no valid skip target -> no skip")
		s.False(s.mutableState.timeSkippingInfoUpdated, "no skip -> no state change")
	})

	s.Run("PassivePolicy_NoSkip", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		s.mutableState.pendingTimerInfoIDs["t1"] = &persistencespb.TimerInfo{
			TimerId:    "t1",
			ExpiryTime: timestamppb.New(s.mutableState.Now().Add(time.Hour)),
		}
		s.mutableState.timeSkippingInfoUpdated = false

		needRegen := s.mutableState.closeTransactionHandleWorkflowTimeSkipping(
			context.Background(), historyi.TransactionPolicyPassive)
		s.False(needRegen, "passive policy never skips")
		s.False(s.mutableState.timeSkippingInfoUpdated, "passive policy makes no state change")
	})
}

func TestTimeSkippingTransition(t *testing.T) {
	t.Parallel()
	base := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("New_SetsOnlyCurrentTime", func(t *testing.T) {
		tr := NewTimeSkippingTransition(base)
		require.Equal(t, base, tr.CurrentTime)
		require.True(t, tr.TargetTime.IsZero())
		require.False(t, tr.DisabledAfterFastForward)
		require.False(t, tr.IsValid(), "a transition with no target and no disable signal is invalid")
	})

	// Invariant 1: every method is nil-safe — on a nil receiver, and (for GateByFastForward)
	// on a nil/absent fast-forward argument.
	t.Run("NilReceiver_NoPanic", func(t *testing.T) {
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
	t.Run("TrackEarliestFutureTime", func(t *testing.T) {
		t.Run("ZeroOrPastCandidate_Ignored", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(time.Time{})          // zero candidate
			tr.TrackEarliestFutureTime(base.Add(-time.Hour)) // past candidate
			require.True(t, tr.TargetTime.IsZero())
		})

		t.Run("MultipleFutureCandidates_KeepsEarliest", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(3 * time.Hour))
			require.Equal(t, base.Add(3*time.Hour), tr.TargetTime)

			tr.TrackEarliestFutureTime(base.Add(time.Hour)) // earlier wins
			require.Equal(t, base.Add(time.Hour), tr.TargetTime)

			tr.TrackEarliestFutureTime(base.Add(2 * time.Hour)) // later is ignored
			require.Equal(t, base.Add(time.Hour), tr.TargetTime)
		})

		t.Run("CandidateEqualToCurrentTime_Accepted", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base)
			require.Equal(t, base, tr.TargetTime)
		})
	})

	// Invariant 3: the fast-forward target is taken — and disables time skipping — exactly when
	// it is the earliest target (nothing earlier tracked). When a real candidate is earlier the
	// fast-forward is not reached and skipping stays enabled. An absent/reached/zero fast-forward
	// is a no-op.
	t.Run("GateByFastForward", func(t *testing.T) {
		t.Run("NoEarlierTarget_TakesFastForwardTargetAndDisables", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, base.Add(time.Hour).Equal(tr.TargetTime))
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})

		t.Run("EarlierTrackedTarget_WinsOverFastForward", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(time.Hour))
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(3 * time.Hour))})
			require.Equal(t, base.Add(time.Hour), tr.TargetTime)
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("EarlierFastForward_WinsOverTrackedAndDisables", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(3 * time.Hour))
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(time.Hour))})
			require.True(t, base.Add(time.Hour).Equal(tr.TargetTime))
			require.True(t, tr.DisabledAfterFastForward)
		})

		t.Run("ReachedFastForward_Ignored", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{
				HasReached: true,
				TargetTime: timestamppb.New(base.Add(time.Hour)),
			})
			require.True(t, tr.TargetTime.IsZero())
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("ZeroFastForwardTarget_Ignored", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(time.Time{})})
			require.True(t, tr.TargetTime.IsZero())
			require.False(t, tr.DisabledAfterFastForward)
		})

		t.Run("TargetEqualToCurrentTime_BareDisableSignal", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base)})
			require.True(t, tr.TargetTime.IsZero(), "a target at now is already due, not a future skip target")
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})

		t.Run("PastFastForwardTarget_BareDisableSignal", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(-time.Hour))})
			require.True(t, tr.TargetTime.IsZero(), "a past target is not a future skip target")
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid())
		})

		t.Run("StaleFastForward_ClearsTrackedTargetAndDisables", func(t *testing.T) {
			tr := NewTimeSkippingTransition(base)
			tr.TrackEarliestFutureTime(base.Add(time.Hour))
			tr.GateByFastForward(&persistencespb.FastForwardInfo{TargetTime: timestamppb.New(base.Add(-time.Hour))})
			require.True(t, tr.TargetTime.IsZero(), "a stale fast-forward clears the tracked target")
			require.True(t, tr.DisabledAfterFastForward)
			require.True(t, tr.IsValid(), "a bare disable signal is still a valid transition")
		})
	})

	// Invariant 4: without a current time the transition is always invalid and no setter can
	// make it valid — every field is relative to the current time.
	t.Run("NoCurrentTime", func(t *testing.T) {
		t.Run("DirectlySetTarget_StillInvalid", func(t *testing.T) {
			tr := &timeSkippingTransition{TargetTime: base.Add(time.Hour)}
			require.False(t, tr.IsValid())
		})

		t.Run("DirectlySetDisableSignal_StillInvalid", func(t *testing.T) {
			tr := &timeSkippingTransition{DisabledAfterFastForward: true}
			require.False(t, tr.IsValid())
		})

		t.Run("SettersWithoutCurrentTime_NoOp", func(t *testing.T) {
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

	s.Run("NilTimeSkippingInfo_ReturnsError", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = nil
		targetTime := baseTime.Add(2 * time.Hour)
		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, false),
		)
		s.Require().Error(err)
	})

	s.Run("WithTargetTime_AccumulatesDuration", func() {
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

	s.Run("WithExistingSkip_AccumulatesAdditively", func() {
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

	s.Run("NilTargetTimeAndNotDisabledAfterFastForward_ReturnsError", func() {
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

	// disabledAfterFastForward only disables when a FastForwardInfo is present (the nil-guard in
	// applyEvent). A large MaxSessionSkipCount keeps the skip-count cap from being the thing that
	// disables, so these two isolate the ff==nil guard: Enabled must stay true.
	s.Run("NilTargetTimeWithDisabledFlag_NoAccumulation", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config:                     &commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: 100},
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
		s.Require().True(s.mutableState.GetExecutionInfo().TimeSkippingInfo.Config.Enabled,
			"disabledAfterFastForward with nil FastForwardInfo must not disable")
		s.Require().True(s.mutableState.timeSkippingInfoUpdated)
	})

	s.Run("DisabledFlagWithNilFastForwardInfo_DoesNotDisable", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: 100},
		}
		targetTime := baseTime.Add(2 * time.Hour)

		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, true),
		)
		s.Require().NoError(err)

		s.Require().True(s.mutableState.GetExecutionInfo().TimeSkippingInfo.Config.Enabled,
			"disabledAfterFastForward with nil FastForwardInfo must not disable")
		accumulated := s.mutableState.GetExecutionInfo().TimeSkippingInfo.AccumulatedSkippedDuration
		s.Require().Equal(2*time.Hour, accumulated.AsDuration())
	})

	// Regression: a fast-forward-completion transition must both disable the config and record
	// the fast-forward as reached. A previous bug left Enabled unchanged on this path.
	s.Run("FastForwardCompletion_DisablesSkipping", func() {
		targetTime := baseTime.Add(2 * time.Hour)
		staleVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 3}
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(2 * time.Hour)},
			},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(targetTime),
				HasReached: false,
			},
			FastForwardInfoLastUpdateVersionedTransition: staleVT,
		}

		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, true),
		)
		s.Require().NoError(err)

		tsi := s.mutableState.GetExecutionInfo().TimeSkippingInfo
		s.Require().False(tsi.GetConfig().GetEnabled())
		s.Require().True(tsi.GetFastForwardInfo().GetHasReached())
		// reaching the target is an update to the fast-forward, so the stamp advances too
		ffVT := tsi.GetFastForwardInfoLastUpdateVersionedTransition()
		s.Require().NotNil(ffVT)
		s.False(proto.Equal(staleVT, ffVT))
		s.Equal(s.mutableState.NextTransitionCount(), ffVT.GetTransitionCount())
	})

	// Every applied transition increments SessionSkipCount by one, and once the count reaches
	// the per-session cap the config is disabled.
	s.Run("SkipCountReachesCap_DisablesSkipping", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:             true,
				MaxSessionSkipCount: 2,
			},
		}
		targetTime := baseTime.Add(time.Hour)

		// first skip: count 1, below cap, stays enabled
		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, false),
		)
		s.Require().NoError(err)
		tsi := s.mutableState.GetExecutionInfo().TimeSkippingInfo
		s.Require().Equal(int32(1), tsi.GetSessionSkipCount())
		s.Require().True(tsi.GetConfig().GetEnabled())

		// second skip: count reaches the cap of 2, disables
		err = s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, false),
		)
		s.Require().NoError(err)
		s.Require().Equal(int32(2), tsi.GetSessionSkipCount())
		s.Require().False(tsi.GetConfig().GetEnabled())
	})

	// A MaxSkipPerSession of 0 caps on the very first skip (1 >= 0), disabling immediately.
	// The frontend guarantees a populated config carries at least 1, so a 0 reaching the
	// runtime (e.g. a config that never had the cap populated) kills skipping right away.
	s.Run("MaxSessionSkipUnset_DisablesOnFirstSkip", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		targetTime := baseTime.Add(time.Hour)

		err := s.mutableState.ApplyWorkflowExecutionTimeSkippingTransitionedEvent(
			context.Background(),
			makeEvent(baseTime, &targetTime, false),
		)
		s.Require().NoError(err)

		tsi := s.mutableState.GetExecutionInfo().TimeSkippingInfo
		s.Require().Equal(int32(1), tsi.GetSessionSkipCount())
		s.Require().False(tsi.GetConfig().GetEnabled())
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

	s.Run("NilTimeSkippingInfo_ZeroOffset", func() {
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = nil

		s.mutableState.wrapTimeSourceWithTimeSkipping()

		_, isWrapper := s.mutableState.timeSource.(*clock.TimeSkippingTimeSourceWrapper)
		s.True(isWrapper)
		// With nil TimeSkippingInfo the wrapper is present but applies a zero offset.
		s.Equal(fixedBase, s.mutableState.timeSource.Now())
	})

	s.Run("AccumulatedSkip_OffsetsClock", func() {
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(skipped),
		}

		s.mutableState.wrapTimeSourceWithTimeSkipping()

		_, isWrapper := s.mutableState.timeSource.(*clock.TimeSkippingTimeSourceWrapper)
		s.True(isWrapper)
		s.Equal(fixedBase.Add(skipped), s.mutableState.timeSource.Now())
	})

	s.Run("LateTimeSkippingInfoAssignment_OffsetFollows", func() {
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

	s.Run("AlreadyWrapped_Idempotent", func() {
		s.mutableState.timeSource = fixedTimeSource()
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(skipped),
		}
		s.mutableState.wrapTimeSourceWithTimeSkipping()
		wrappedOnce := s.mutableState.timeSource

		s.mutableState.wrapTimeSourceWithTimeSkipping()

		s.Equal(wrappedOnce, s.mutableState.timeSource, "second call must not double-wrap")
	})

	s.Run("HistoryBuilder_UsesVirtualTime", func() {
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

func (s *mutableStateSuite) TestTimeSkippingInfoUtil() {
	s.Run("NilTimeSkippingInfo_NilSafe", func() {
		util := NewTimeSkippingInfoUtil(nil)
		s.False(util.HasPendingFastForward())
		s.False(util.IsEnabled())
		s.Zero(util.GetAccumulatedSkippedDuration())
		s.Nil(util.GetFastForwardTargetTime())
		s.Nil(util.ToDescribeInfo(time.Now()))
		s.Nil(util.ToFastForwardInfo())
	})

	s.Run("NilReceiver_NilSafe", func() {
		var util *TimeSkippingInfoUtil
		s.False(util.IsEnabled())
		s.False(util.HasPendingFastForward())
		s.Zero(util.GetAccumulatedSkippedDuration())
		s.Nil(util.GetFastForwardTargetTime())
		s.Nil(util.ToDescribeInfo(time.Now()))
		s.Nil(util.ToFastForwardInfo())
	})

	s.Run("GetFastForwardTarget_NilFastForwardInfo_ReturnsNil", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true}})
		s.Nil(util.GetFastForwardTargetTime())
	})

	s.Run("GetFastForwardTarget_NilTargetTime_ReturnsNil", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			FastForwardInfo: &persistencespb.FastForwardInfo{TargetTime: nil}})
		s.Nil(util.GetFastForwardTargetTime())
	})

	s.Run("GetFastForwardTarget_ZeroTargetTime_ReturnsNil", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			FastForwardInfo: &persistencespb.FastForwardInfo{TargetTime: timestamppb.New(time.Time{})}})
		s.Nil(util.GetFastForwardTargetTime())
	})

	s.Run("GetFastForwardTarget_ValidTarget_ReturnsTarget", func() {
		target := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			FastForwardInfo: &persistencespb.FastForwardInfo{TargetTime: timestamppb.New(target)}})
		got := util.GetFastForwardTargetTime()
		s.Require().NotNil(got)
		s.Equal(target, got.AsTime())
	})

	s.Run("ToFastForwardInfo_Populated_IncludesIdAndDuration", func() {
		target := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				FastForwardConfig: &commonpb.FastForwardConfig{Id: "ff-9", Duration: durationpb.New(3 * time.Hour)}},
			FastForwardInfo: &persistencespb.FastForwardInfo{TargetTime: timestamppb.New(target), HasReached: true},
		})
		got := util.ToFastForwardInfo()
		s.Require().NotNil(got)
		s.Equal(target, got.GetTargetTime().AsTime())
		s.True(got.GetHasCompleted())
		s.Equal("ff-9", got.GetFastForwardId())
		s.Equal(3*time.Hour, got.GetFastForwardDuration().AsDuration())
	})

	s.Run("ToFastForwardInfo_NilConfig_EmptyIdAndDuration", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config:          nil,
			FastForwardInfo: &persistencespb.FastForwardInfo{TargetTime: timestamppb.New(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))},
		})
		got := util.ToFastForwardInfo()
		s.Require().NotNil(got, "ff present -> non-nil even with nil config")
		s.Empty(got.GetFastForwardId())
		s.Nil(got.GetFastForwardDuration())
	})

	s.Run("IsEnabled_DisabledConfig_False", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false}})
		s.False(util.IsEnabled())
	})

	s.Run("HasPendingFastForward_NilFastForwardInfo_False", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config:          &commonpb.TimeSkippingConfig{Enabled: true},
			FastForwardInfo: nil,
		}
		util := NewTimeSkippingInfoUtil(s.mutableState.executionInfo.TimeSkippingInfo)
		s.False(util.HasPendingFastForward())
	})

	s.Run("HasPendingFastForward_NilTargetTime_False", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config:          &commonpb.TimeSkippingConfig{Enabled: true},
			FastForwardInfo: &persistencespb.FastForwardInfo{HasReached: false, TargetTime: nil},
		})
		s.False(util.HasPendingFastForward())
	})

	s.Run("HasPendingFastForward_ZeroTargetTime_False", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config:          &commonpb.TimeSkippingConfig{Enabled: true},
			FastForwardInfo: &persistencespb.FastForwardInfo{HasReached: false, TargetTime: timestamppb.New(time.Time{})},
		})
		s.False(util.HasPendingFastForward())
	})

	s.Run("HasPendingFastForward_AlreadyReached_False", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				HasReached: true,
				TargetTime: timestamppb.New(time.Now()),
			},
			FastForwardInfoLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: 1,
			},
		}
		util := NewTimeSkippingInfoUtil(s.mutableState.executionInfo.TimeSkippingInfo)
		s.False(util.HasPendingFastForward())
	})

	s.Run("HasPendingFastForward_DisabledConfig_False", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: false},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				HasReached: false,
				TargetTime: timestamppb.New(time.Now()),
			},
			FastForwardInfoLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: 1,
			},
		}
		util := NewTimeSkippingInfoUtil(s.mutableState.executionInfo.TimeSkippingInfo)
		s.False(util.HasPendingFastForward())
	})

	s.Run("HasPendingFastForward_ActiveTarget_True", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				HasReached: false,
				TargetTime: timestamppb.New(time.Now()),
			},
			FastForwardInfoLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: 1,
			},
		}
		util := NewTimeSkippingInfoUtil(s.mutableState.executionInfo.TimeSkippingInfo)
		s.True(util.HasPendingFastForward())
	})

	s.Run("IsEnabled_EnabledConfig_True", func() {
		s.mutableState.executionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		}
		util := NewTimeSkippingInfoUtil(s.mutableState.executionInfo.TimeSkippingInfo)
		s.True(util.IsEnabled())
	})

	s.Run("IsEnabled_NilConfig_False", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{Config: nil})
		s.False(util.IsEnabled())
	})

	s.Run("GetAccumulatedSkippedDuration_ReturnsValue", func() {
		s.Zero(NewTimeSkippingInfoUtil(nil).GetAccumulatedSkippedDuration(), "nil info")
		s.Zero(NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{}).GetAccumulatedSkippedDuration(), "unset duration")
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(90 * time.Minute),
		})
		s.Equal(90*time.Minute, util.GetAccumulatedSkippedDuration())
	})

	s.Run("ToDescribeInfo_NilTimeSkippingInfo_ReturnsNil", func() {
		s.Nil(NewTimeSkippingInfoUtil(nil).ToDescribeInfo(time.Now()))
	})

	s.Run("ToDescribeInfo_EnabledNoFastForward_NilFastForwardInfo", func() {
		now := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{Enabled: true},
		})
		info := util.ToDescribeInfo(now)
		s.NotNil(info)
		s.True(info.GetEffectiveConfig().GetEnabled())
		s.Equal(now, info.GetCurrentTime().AsTime())
		s.Nil(info.GetFastForwardInfo(), "no fast-forward set")
	})

	s.Run("ToDescribeInfo_WithFastForward_IncludesFastForwardInfo", func() {
		now := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)
		targetTime := now.Add(time.Hour)
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:           true,
				FastForwardConfig: &commonpb.FastForwardConfig{Id: "ff-1"},
			},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime: timestamppb.New(targetTime),
				HasReached: true,
			},
		})
		info := util.ToDescribeInfo(now)
		s.NotNil(info)
		ff := info.GetFastForwardInfo()
		s.NotNil(ff)
		s.Equal(targetTime, ff.GetTargetTime().AsTime())
		s.True(ff.GetHasCompleted())
		s.Equal("ff-1", ff.GetFastForwardId())
	})

	s.Run("ToDescribeInfo_DisabledConfig_ReflectsConfig", func() {
		util := NewTimeSkippingInfoUtil(&persistencespb.TimeSkippingInfo{
			Config:                     &commonpb.TimeSkippingConfig{Enabled: false},
			AccumulatedSkippedDuration: durationpb.New(2 * time.Hour),
		})
		msNow := time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC)
		info := util.ToDescribeInfo(msNow)
		s.NotNil(info)
		s.False(info.GetEffectiveConfig().GetEnabled())
		s.Equal(msNow, info.GetCurrentTime().AsTime())
	})

}

func TestAdjustNowWithTimeSkipping(t *testing.T) {
	now := time.Date(2026, 8, 19, 9, 0, 0, 0, time.UTC)

	testCases := []struct {
		name             string
		statePropagation *commonpb.TimeSkippingStatePropagation
		want             time.Time
	}{
		{
			name: "nil propagation",
			want: now,
		},
		{
			name:             "nil initial skipped duration",
			statePropagation: &commonpb.TimeSkippingStatePropagation{},
			want:             now,
		},
		{
			name: "zero initial skipped duration",
			statePropagation: &commonpb.TimeSkippingStatePropagation{
				InitialSkippedDuration: durationpb.New(0),
			},
			want: now,
		},
		{
			name: "positive initial skipped duration",
			statePropagation: &commonpb.TimeSkippingStatePropagation{
				InitialSkippedDuration: durationpb.New(2 * time.Hour),
			},
			want: now.Add(2 * time.Hour),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, AdjustNowWithTimeSkipping(now, tc.statePropagation))
		})
	}
}
