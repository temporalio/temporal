package deployment

import (
	"github.com/google/uuid"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// DeploymentWorkflowRunner holds the local state while running a deployment-series workflow
	DeploymentSeriesWorkflowRunner struct {
		*deploymentspb.DeploymentSeriesWorkflowArgs
		a              *DeploymentSeriesActivities
		logger         sdklog.Logger
		metrics        sdkclient.MetricsHandler
		lock           workflow.Mutex
		pendingUpdates int
	}
)

func DeploymentSeriesWorkflow(ctx workflow.Context, args *deploymentspb.DeploymentSeriesWorkflowArgs) error {
	deploymentWorkflowNameRunner := &DeploymentSeriesWorkflowRunner{
		DeploymentSeriesWorkflowArgs: args,

		a:       nil,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.NamespaceName}),
		lock:    workflow.NewMutex(ctx),
	}
	return deploymentWorkflowNameRunner.run(ctx)
}

func (d *DeploymentSeriesWorkflowRunner) run(ctx workflow.Context) error {
	if d.State == nil {
		d.State = &deploymentspb.SeriesLocalState{}
	}

	var pendingUpdates int

	err := workflow.SetQueryHandler(ctx, QueryCurrentDeployment, func() (string, error) {
		return d.State.CurrentBuildId, nil
	})
	if err != nil {
		d.logger.Info("SetQueryHandler failed for DeploymentSeries workflow with error: " + err.Error())
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		SetCurrentDeployment,
		d.handleSetCurrent,
		workflow.UpdateHandlerOptions{
			Validator: d.validateSetCurrent,
		},
	); err != nil {
		return err
	}

	// Wait until we can continue as new or are cancelled.
	err = workflow.Await(ctx, func() bool { return workflow.GetInfo(ctx).GetContinueAsNewSuggested() && pendingUpdates == 0 })
	if err != nil {
		return err
	}

	// Continue as new when there are no pending updates and history size is greater than requestsBeforeContinueAsNew.
	// Note, if update requests come in faster than they
	// are handled, there will not be a moment where the workflow has
	// nothing pending which means this will run forever.
	return workflow.NewContinueAsNewError(ctx, DeploymentSeriesWorkflow, d.DeploymentSeriesWorkflowArgs)
}

func (d *DeploymentSeriesWorkflowRunner) validateSetCurrent(args *deploymentspb.SetCurrentDeploymentArgs) error {
	if d.State.CurrentBuildId != args.BuildId {
		return nil
	}

	return temporal.NewApplicationError("no change", errNoChangeType)
}

func (d *DeploymentSeriesWorkflowRunner) handleSetCurrent(ctx workflow.Context, args *deploymentspb.SetCurrentDeploymentArgs) (*deploymentspb.SetCurrentDeploymentResponse, error) {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return nil, serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
		d.lock.Unlock()
	}()

	var curState, prevState *deploymentspb.DeploymentLocalState
	prevCurrent := d.State.CurrentBuildId

	// update local state
	d.State.CurrentBuildId = args.BuildId
	d.State.CurrentChangedTime = timestamppb.New(workflow.Now(ctx))

	// update memo
	if err = d.updateMemo(ctx); err != nil {
		return nil, err
	}

	// tell new current that it's current
	if curState, err = d.syncDeployment(ctx, d.State.CurrentBuildId, args.UpdateMetadata); err != nil {
		return nil, err
	}

	if prevCurrent != "" {
		// tell previous current that it's no longer current
		if prevState, err = d.syncDeployment(ctx, prevCurrent, nil); err != nil {
			return nil, err
		}
	}

	return &deploymentspb.SetCurrentDeploymentResponse{
		CurrentDeploymentState:  curState,
		PreviousDeploymentState: prevState,
	}, nil
}

func (d *DeploymentSeriesWorkflowRunner) syncDeployment(ctx workflow.Context, buildId string, updateMetadata *deploymentpb.UpdateDeploymentMetadata) (*deploymentspb.DeploymentLocalState, error) {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	setCur := &deploymentspb.SyncDeploymentStateArgs_SetCurrent{}
	if d.State.CurrentBuildId == buildId {
		setCur.LastBecameCurrentTime = d.State.CurrentChangedTime
	}

	var res deploymentspb.SyncDeploymentStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.SyncDeployment, &deploymentspb.SyncDeploymentStateActivityArgs{
		Deployment: &deploymentpb.Deployment{
			SeriesName: d.SeriesName,
			BuildId:    buildId,
		},
		Args: &deploymentspb.SyncDeploymentStateArgs{
			SetCurrent:     setCur,
			UpdateMetadata: updateMetadata,
		},
		RequestId: d.newUUID(ctx),
	}).Get(ctx, &res)
	return res.State, err
}

func (d *DeploymentSeriesWorkflowRunner) newUUID(ctx workflow.Context) string {
	var val string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.NewString()
	}).Get(&val)
	return val
}

func (d *DeploymentSeriesWorkflowRunner) updateMemo(ctx workflow.Context) error {
	return workflow.UpsertMemo(ctx, map[string]any{
		DeploymentSeriesMemoField: &deploymentspb.DeploymentSeriesWorkflowMemo{
			SeriesName:         d.SeriesName,
			CurrentBuildId:     d.State.CurrentBuildId,
			CurrentChangedTime: d.State.CurrentChangedTime,
		},
	})
}
