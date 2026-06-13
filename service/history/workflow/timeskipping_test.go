package workflow

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
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
