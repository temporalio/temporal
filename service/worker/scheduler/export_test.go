package scheduler

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SchedulerWorkflowWithSpecBuilder exposes the internal workflow constructor to the external
// (scheduler_test) package so replay tests can vary the compute-limit configuration by injecting
// a SpecBuilder with specific Max/WarnIterations accessors. Compiled only under test.
func SchedulerWorkflowWithSpecBuilder(b *SpecBuilder) func(workflow.Context, *schedulespb.StartScheduleArgs) error {
	return func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, args, b, schedulerDynamicConfig{})
	}
}

// ScheduleGenerationForTest contains the observable results of one generation pass.
type ScheduleGenerationForTest struct {
	BufferedStarts []*schedulespb.BufferedStart
	NextWakeupTime time.Time
}

// GenerateScheduledActionsForTest returns the actions and next wakeup at each
// supplied wakeup. Cursor bookkeeping stays inside the adapter.
func GenerateScheduledActionsForTest(
	ctx workflow.Context,
	args *schedulespb.StartScheduleArgs,
	ends []time.Time,
) ([]ScheduleGenerationForTest, error) {
	builder := NewSpecBuilder(dynamicconfig.GetIntPropertyFn(0), dynamicconfig.GetIntPropertyFn(0))
	spec, err := builder.NewCompiledSpec(args.Schedule.Spec)
	if err != nil {
		return nil, err
	}
	s := &scheduler{
		StartScheduleArgs: args,
		ctx:               ctx,
		logger:            workflow.GetLogger(ctx),
		metrics:           workflow.GetMetricsHandler(ctx),
		specBuilder:       builder,
		cspec:             spec,
		tweakables: TweakablePolicies{
			Version:              UseLastAction,
			DefaultCatchupWindow: time.Hour,
			NextTimeCacheV2Size:  10,
		},
	}
	var results []ScheduleGenerationForTest
	for _, end := range ends {
		previousCount := len(s.State.BufferedStarts)
		nextWakeup, lastAction := s.processTimeRange(
			s.State.LastProcessedTime.AsTime(), end,
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, false, nil,
		)
		s.State.LastProcessedTime = timestamppb.New(lastAction)
		results = append(results, ScheduleGenerationForTest{
			BufferedStarts: s.State.BufferedStarts[previousCount:],
			NextWakeupTime: nextWakeup,
		})
	}
	return results, nil
}
