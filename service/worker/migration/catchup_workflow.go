package migration

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	catchupWorkflowName = "catchup"
)

type (
	CatchUpParams struct {
		Namespace      string
		CatchupCluster string
		TargetCluster  string
	}

	CatchUpOutput struct{}
)

func CatchupWorkflow(ctx workflow.Context, params CatchUpParams) (CatchUpOutput, error) {
	if err := validateCatchupParams(&params); err != nil {
		return CatchUpOutput{}, err
	}

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		MaximumInterval:    time.Second,
		BackoffCoefficient: 1,
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx1 := workflow.WithActivityOptions(ctx, ao)

	var a *activities
	err := workflow.ExecuteActivity(ctx1, a.WaitCatchup, params).Get(ctx, nil)
	if err != nil {
		return CatchUpOutput{}, err
	}

	return CatchUpOutput{}, err
}

func validateCatchupParams(params *CatchUpParams) error {
	if len(params.Namespace) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: Namespace is required", "InvalidArgument", nil)
	}
	if len(params.CatchupCluster) == 0 {
		return temporal.NewNonRetryableApplicationError("InvalidArgument: CatchupCluster is required", "InvalidArgument", nil)
	}

	return nil
}
