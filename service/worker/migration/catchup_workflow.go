package migration

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/wideevents"
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

func CatchupWorkflow(ctx workflow.Context, params CatchUpParams) (_ CatchUpOutput, retErr error) {
	if err := validateCatchupParams(&params); err != nil {
		return CatchUpOutput{}, err
	}
	if workflow.GetVersion(ctx, migrationWorkflowLifecycleVersion, workflow.DefaultVersion, 1) > workflow.DefaultVersion {
		lifecycle := newMigrationWorkflowLifecycle(
			ctx,
			params.Namespace,
			wideevents.PhaseNamespaceCatchupStarted,
			wideevents.PhaseNamespaceCatchupFinished,
			map[string]any{
				"catchup_cluster": params.CatchupCluster,
				"target_cluster":  params.TargetCluster,
			},
		)
		defer func() { lifecycle.emitFinished(ctx, retErr, nil) }()
		lifecycle.emitStarted(ctx)
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
	retErr = workflow.ExecuteActivity(ctx1, a.WaitCatchup, params).Get(ctx, nil)
	if retErr != nil {
		return CatchUpOutput{}, retErr
	}

	return CatchUpOutput{}, nil
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
