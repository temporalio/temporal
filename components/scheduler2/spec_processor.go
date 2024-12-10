package scheduler2

import (
	"time"

	schedpb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	scheduler1 "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// SpecProcessor is used by the Generator and Backfiller to generate buffered
	// actions according to the schedule spec.
	SpecProcessor struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		Logger         log.Logger
		SpecBuilder    *scheduler1.SpecBuilder
	}

	ProcessedTimeRange struct {
		NextWakeupTime time.Time
		LastActionTime time.Time
		BufferedStarts []*schedpb.BufferedStart
	}
)

// ProcessTimeRange generates buffered actions according to the schedule spec for
// the given time range.
//
// The parameter manual is propagated to the returned BufferedStarts. When the limit
// is set to a non-nil pointer, it will be decremented for each buffered start, and
// the function will return early should limit reach 0.
func (s SpecProcessor) ProcessTimeRange(
	scheduler Scheduler,
	start, end time.Time,
	manual bool,
	limit *int,
) (*ProcessedTimeRange, error) {
	tweakables := s.Config.Tweakables(scheduler.Namespace)
	overlapPolicy := scheduler.OverlapPolicy()

	s.Logger.Debug("ProcessTimeRange",
		tag.NewTimeTag("start", start),
		tag.NewTimeTag("end", end),
		tag.NewAnyTag("overlap-policy", overlapPolicy),
		tag.NewBoolTag("manual", manual))

	// Peek at paused/remaining actions state and don't bother if we're not going to
	// take an action now. (Don't count as missed catchup window either.)
	// Skip over entire time range if paused or no actions can be taken.
	//
	// Manual (backfill/patch) runs are always buffered here.
	if !scheduler.UseScheduledAction(false) && !manual {
		// Use end as last action time so that we don't reprocess time spent paused.
		next, err := s.getNextTime(scheduler, end)
		if err != nil {
			return nil, err
		}

		return &ProcessedTimeRange{
			NextWakeupTime: next.Next,
			LastActionTime: end,
			BufferedStarts: nil,
		}, nil
	}

	catchupWindow := catchupWindow(scheduler, tweakables)
	lastAction := start
	var next scheduler1.GetNextTimeResult
	var err error
	var bufferedStarts []*schedpb.BufferedStart
	for next, err = s.getNextTime(scheduler, start); err == nil && !(next.Next.IsZero() || next.Next.After(end)); next, err = s.getNextTime(scheduler, next.Next) {
		if scheduler.Info.UpdateTime.AsTime().After(next.Next) {
			// If we've received an update that took effect after the LastProcessedTime high
			// water mark, discard actions that were scheduled to kick off before the update.
			continue
		}

		if end.Sub(next.Next) > catchupWindow {
			s.Logger.Warn("Schedule missed catchup window",
				tag.NewTimeTag("now", end),
				tag.NewTimeTag("time", next.Next))
			s.MetricsHandler.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(1)

			// TODO - update Info.MissedCatchupWindow
			// s.Info.MissedCatchupWindow++
			// or write that to the generator's persisted state?
			continue
		}

		bufferedStarts = append(bufferedStarts, &schedpb.BufferedStart{
			NominalTime:   timestamppb.New(next.Nominal),
			ActualTime:    timestamppb.New(next.Next),
			OverlapPolicy: overlapPolicy,
			Manual:        manual,
			RequestId:     GenerateRequestID(scheduler, "", next.Nominal, next.Next),
		})
		lastAction = next.Next

		if limit != nil {
			if (*limit)--; *limit <= 0 {
				break
			}
		}
	}

	return &ProcessedTimeRange{
		NextWakeupTime: next.Next,
		LastActionTime: lastAction,
		BufferedStarts: bufferedStarts,
	}, nil
}

func catchupWindow(s Scheduler, tweakables Tweakables) time.Duration {
	cw := s.Schedule.Policies.CatchupWindow
	if cw == nil {
		return tweakables.DefaultCatchupWindow
	}

	return max(cw.AsDuration(), tweakables.MinCatchupWindow)
}

// getNextTime returns the next time result, or an error if the schedule cannot be compiled.
func (s SpecProcessor) getNextTime(scheduler Scheduler, after time.Time) (scheduler1.GetNextTimeResult, error) {
	spec, err := scheduler.CompiledSpec(s.SpecBuilder)
	if err != nil {
		s.Logger.Error("Invalid schedule", tag.Error(err))
		return scheduler1.GetNextTimeResult{}, err
	}

	return spec.GetNextTime(scheduler.JitterSeed(), after), nil
}
