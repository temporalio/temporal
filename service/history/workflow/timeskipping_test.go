package workflow

import (
	"time"

	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *mutableStateSuite) TestSnapshotTimeSkippingInfo_ContinuationVsChild() {
	newSource := func() *persistencespb.WorkflowExecutionInfo {
		return &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &workflowpb.TimeSkippingConfig{
					Enabled:     true,
					FastForward: durationpb.New(3 * time.Hour),
				},
				AccumulatedSkippedDuration: durationpb.New(time.Hour),
			},
		}
	}

	s.Run("continuation keeps MaxElapsedDuration and Enabled", func() {
		tsc, initialSkip := propagateTimeSkippingToNextRun(newSource())
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Equal(3*time.Hour, tsc.GetFastForward().AsDuration())
		s.Equal(time.Hour, initialSkip.AsDuration())
	})

	s.Run("child does not propagate config when Enabled is false", func() {
		src := newSource()
		src.TimeSkippingInfo.Config.Enabled = false
		tsc, initialSkip := propagateTimeSkippingToChild(src)
		s.Nil(tsc, "Enabled=false → no config propagated to the child")
		s.Require().NotNil(initialSkip)
		s.Equal(time.Hour, initialSkip.AsDuration(),
			"virtual time is always propagated, even when config propagation is disabled")
	})

	s.Run("child clears MaxElapsedDuration and inherits Enabled", func() {
		tsc, initialSkip := propagateTimeSkippingToChild(newSource())
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Nil(tsc.GetFastForward(), "MaxElapsedDuration never cascades into children")
		s.Equal(time.Hour, initialSkip.AsDuration())
	})

	s.Run("execution-chain snapshot does not mutate the source config", func() {
		src := newSource()
		tsc, _ := propagateTimeSkippingToNextRun(src)
		s.Require().NotNil(tsc)
		tsc.Enabled = false
		tsc.Bound = nil
		s.True(src.GetTimeSkippingInfo().GetConfig().GetEnabled(), "source Enabled must not be mutated")
		s.Equal(3*time.Hour, src.GetTimeSkippingInfo().GetConfig().GetFastForward().AsDuration(),
			"source MaxElapsedDuration must not be mutated")
	})
}
