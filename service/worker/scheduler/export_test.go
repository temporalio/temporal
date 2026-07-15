package scheduler

import (
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
)

// SchedulerWorkflowWithSpecBuilder exposes the internal workflow constructor to the external
// (scheduler_test) package so replay tests can vary the compute-limit configuration by injecting
// a SpecBuilder with specific Max/WarnIterations accessors. Compiled only under test.
func SchedulerWorkflowWithSpecBuilder(b *SpecBuilder) func(workflow.Context, *schedulespb.StartScheduleArgs) error {
	return func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(
			ctx, args, b,
			func() bool { return false }, // enableCHASMMigration
			func() bool { return false }, // migrateWithRunningWorkflows
		)
	}
}
