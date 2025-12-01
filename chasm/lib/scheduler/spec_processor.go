package scheduler

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination spec_processor_mock.go

type (
	// SpecProcessor is used by the Generator and Backfiller to generate buffered
	// actions according to the schedule spec.
	SpecProcessor interface {
		// ProcessTimeRange generates buffered actions according to the schedule spec for
		// the given time range.
		//
		// The parameter manual is propagated to the returned BufferedStarts. When the limit
		// is set to a non-nil pointer, it will be decremented for each buffered start, and
		// the function will return early should limit reach 0.
		//
		// If backfillID is set, it will be used to generate request IDs.
		ProcessTimeRange(
			scheduler *Scheduler,
			start, end time.Time,
			overlapPolicy enumspb.ScheduleOverlapPolicy,
			workflowID string,
			backfillID string,
			manual bool,
			limit *int,
		) (*ProcessedTimeRange, error)

		// NextTime provides a peek at the next time in the spec following 'after'.
		NextTime(scheduler *Scheduler, after time.Time) (legacyscheduler.GetNextTimeResult, error)
	}

	SpecProcessorImpl struct {
		config         *Config
		metricsHandler metrics.Handler
		logger         log.Logger
		specBuilder    *legacyscheduler.SpecBuilder
	}

	ProcessedTimeRange struct {
		NextWakeupTime time.Time
		LastActionTime time.Time
		BufferedStarts []*schedulespb.BufferedStart
	}
)

func NewSpecProcessor(
	config *Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
	specBuilder *legacyscheduler.SpecBuilder,
) *SpecProcessorImpl {
	return &SpecProcessorImpl{
		config:         config,
		metricsHandler: metricsHandler,
		logger:         logger,
		specBuilder:    specBuilder,
	}
}

func (s *SpecProcessorImpl) ProcessTimeRange(
	scheduler *Scheduler,
	start, end time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	workflowID string,
	backfillID string,
	manual bool,
	limit *int,
) (*ProcessedTimeRange, error) {
	tweakables := s.config.Tweakables(scheduler.Namespace)
	overlapPolicy = scheduler.resolveOverlapPolicy(overlapPolicy)

	s.logger.Debug("ProcessTimeRange",
		tag.NewTimeTag("start", start),
		tag.NewTimeTag("end", end),
		tag.NewAnyTag("overlap-policy", overlapPolicy),
		tag.NewBoolTag("manual", manual))

	// Peek at paused/remaining actions state and don't bother if we're not going to
	// take an action now. (Don't count as missed catchup window either.)
	// Skip over entire time range if paused or no actions can be taken.
	//
	// Manual (backfill/patch) runs are always buffered here.
	if !scheduler.useScheduledAction(false) && !manual {
		// Use end as last action time so that we don't reprocess time spent paused.
		next, err := s.NextTime(scheduler, end)
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

	// lastAction is used to set the high water mark for future ProcessTimeRange
	// invocations. The code below will set a "last action" even when none is taken,
	// simply to indicate that processing can permanently skip that period of time
	// (e.g., it was prior to an update or past a catchup).
	lastAction := end

	var next legacyscheduler.GetNextTimeResult
	var err error
	var bufferedStarts []*schedulespb.BufferedStart
	for next, err = s.NextTime(scheduler, start); err == nil && (!next.Next.IsZero() && !next.Next.After(end)); next, err = s.NextTime(scheduler, next.Next) {
		lastAction = next.Next

		if scheduler.Info.UpdateTime.AsTime().After(next.Next) {
			// If we've received an update that took effect after the LastProcessedTime high
			// water mark, discard actions that were scheduled to kick off before the update.
			s.logger.Warn("ProcessBuffer skipped an action due to update time",
				tag.NewTimeTag("updateTime", scheduler.Info.UpdateTime.AsTime()),
				tag.NewTimeTag("droppedActionTime", next.Next))
			continue
		}

		if !manual && end.Sub(next.Next) > catchupWindow {
			s.logger.Warn("Schedule missed catchup window",
				tag.NewTimeTag("now", end),
				tag.NewTimeTag("time", next.Next))
			s.metricsHandler.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(1)

			scheduler.Info.MissedCatchupWindow++
			continue
		}

		nominalTimeSec := next.Nominal.Truncate(time.Second)
		bufferedStarts = append(bufferedStarts, &schedulespb.BufferedStart{
			NominalTime:   timestamppb.New(next.Nominal),
			ActualTime:    timestamppb.New(next.Next),
			OverlapPolicy: overlapPolicy,
			Manual:        manual,
			RequestId:     generateRequestID(scheduler, backfillID, next.Nominal, next.Next),
			WorkflowId:    fmt.Sprintf("%s-%s", workflowID, nominalTimeSec.Format(time.RFC3339)),
		})

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

func catchupWindow(s *Scheduler, tweakables Tweakables) time.Duration {
	cw := s.Schedule.GetPolicies().GetCatchupWindow()
	if cw == nil {
		return tweakables.DefaultCatchupWindow
	}

	return max(cw.AsDuration(), tweakables.MinCatchupWindow)
}

// NextTime returns the next time result, or an error if the schedule cannot be compiled.
func (s *SpecProcessorImpl) NextTime(scheduler *Scheduler, after time.Time) (legacyscheduler.GetNextTimeResult, error) {
	spec, err := scheduler.getCompiledSpec(s.specBuilder)
	if err != nil {
		s.logger.Error("Invalid schedule", tag.Error(err))
		return legacyscheduler.GetNextTimeResult{}, err
	}

	return spec.GetNextTime(scheduler.jitterSeed(), after), nil
}
