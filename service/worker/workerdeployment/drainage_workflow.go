package workerdeployment

import (
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultVisibilityRefresh = 5 * time.Minute
	defaultVisibilityGrace   = 3 * time.Minute
)

func DrainageWorkflow(
	ctx workflow.Context,
	unsafeRefreshIntervalGetter func() any,
	unsafeVisibilityGracePeriodGetter func() any,
	args *deploymentspb.DrainageWorkflowArgs,
) error {
	if args.Version == nil {
		return serviceerror.NewInvalidArgument("version cannot be nil")
	}
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var a *DrainageActivities

	// listen for done signal sent by parent if started accepting new executions or continued-as-new
	done := false
	workflow.Go(ctx, func(ctx workflow.Context) {
		terminateChan := workflow.GetSignalChannel(ctx, TerminateDrainageSignal)
		terminateChan.Receive(ctx, nil)
		done = true
	})

	// Set status = DRAINING and then sleep for visibilityGracePeriod (to let recently-started workflows arrive in visibility)
	if !args.IsCan { // skip if resuming after the parent continued-as-new
		v := workflow.GetVersion(ctx, "Step1", workflow.DefaultVersion, 1)
		if v == workflow.DefaultVersion { // needs patching because we removed a Signal call
			parentWf := workflow.GetInfo(ctx).ParentWorkflowExecution
			now := timestamppb.New(workflow.Now(ctx))
			drainingInfo := &deploymentpb.VersionDrainageInfo{
				Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
				LastChangedTime: now,
				LastCheckedTime: now,
			}
			err := workflow.SignalExternalWorkflow(ctx, parentWf.ID, parentWf.RunID, SyncDrainageSignalName, drainingInfo).Get(ctx, nil)
			if err != nil {
				return err
			}
		}
		grace, err := getSafeDurationConfig(ctx, "getVisibilityGracePeriod", unsafeVisibilityGracePeriodGetter, defaultVisibilityGrace)
		if err != nil {
			return err
		}
		_ = workflow.Sleep(ctx, grace)
	}

	for {
		if done {
			return nil
		}
		var info *deploymentpb.VersionDrainageInfo
		err := workflow.ExecuteActivity(
			activityCtx,
			a.GetVersionDrainageStatus,
			args.Version,
		).Get(ctx, &info)
		if err != nil {
			return err
		}

		parentWf := workflow.GetInfo(ctx).ParentWorkflowExecution
		err = workflow.SignalExternalWorkflow(ctx, parentWf.ID, parentWf.RunID, SyncDrainageSignalName, info).Get(ctx, nil)
		if err != nil {
			return err
		}
		if info.Status == enumspb.VERSION_DRAINAGE_STATUS_DRAINED {
			return nil
		}
		refresh, err := getSafeDurationConfig(ctx, "getDrainageRefreshInterval", unsafeRefreshIntervalGetter, defaultVisibilityRefresh)
		if err != nil {
			return err
		}
		_ = workflow.Sleep(ctx, refresh)

		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			args.IsCan = true
			return workflow.NewContinueAsNewError(ctx, WorkerDeploymentDrainageWorkflowType, args)
		}
	}
}
