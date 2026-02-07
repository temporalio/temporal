package dummy

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	DummyWFTypeName = "temporal-sys-dummy-workflow"

	// dummyDuration only needs to account for the duration in which the key needs
	// to stay reserved.
	dummyDuration = 1 * time.Hour
)

// DummyWorkflow is a sentinel workflow that hangs open for a fixed duration.
// It is used to reserve a particular key in the traditional workflow ID-space,
// as opposed to the CHASM archetype-specific ID spaces.
//
// CHASM Scheduler creates a DummyWorkflow to block a V1 schedule of the same
// logical schedule ID from being created in the workflow ID-space. The key stays
// reserved for a period to account for a fleet's potentially inconsistent view of
// the dynamic config feature flags controlling which stack schedulers are being
// created on.
func DummyWorkflow(ctx workflow.Context) error {
	return workflow.Sleep(ctx, dummyDuration)
}
