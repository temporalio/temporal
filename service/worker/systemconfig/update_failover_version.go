package systemconfig

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	updateFailoverVersionIncrementWorkflowName = "update-failover-version-increment"
)

type (
	UpdateFailoverVersionIncrementParams struct {
		CurrentFVI int64
		NewFVI     int64
	}
)

func UpdateFailoverVersionIncrementWorkflow(
	ctx workflow.Context,
	params UpdateFailoverVersionIncrementParams,
) error {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	}
	future := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, activityOptions), "UpdateFVI", UpdateFailoverVersionIncrementInput{
		CurrentFVI: params.CurrentFVI,
		NewFVI:     params.NewFVI,
	})
	return future.Get(ctx, nil)
}
