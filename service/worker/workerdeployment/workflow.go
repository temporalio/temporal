package workerdeployment

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/pborman/uuid"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// The actual limit is set in dynamic configs, this is only used in case we cannot read the DC.
	defaultMaxVersions = 100
)

type (
	// SignalHandler encapsulates the signal handling logic
	SignalHandler struct {
		signalSelector    workflow.Selector
		processingSignals int
	}

	// WorkflowRunner holds the local state while running a deployment-series workflow
	WorkflowRunner struct {
		*deploymentspb.WorkerDeploymentWorkflowArgs
		a                *Activities
		logger           sdklog.Logger
		metrics          sdkclient.MetricsHandler
		lock             workflow.Mutex
		conflictToken    []byte
		deleteDeployment bool
		unsafeMaxVersion func() int
		// stateChanged is used to track if the state of the workflow has undergone a local state change since the last signal/update.
		// This prevents a workflow from continuing-as-new if the state has not changed.
		stateChanged  bool
		signalHandler *SignalHandler
		forceCAN      bool
	}
)

// This workflow is implemented in a way such that it always CaNs after some
// history events are added to it and when it has no pending work to do. This is to keep the
// history clean so that we have less concern about backwards and forwards compatibility.
// In steady state (i.e. absence of ongoing updates or signals) the wf should only have
// a single wft in the history.
func Workflow(ctx workflow.Context, unsafeMaxVersion func() int, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
	workflowRunner := &WorkflowRunner{
		WorkerDeploymentWorkflowArgs: args,

		a:                nil,
		logger:           sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.NamespaceName),
		metrics:          workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.NamespaceName}),
		lock:             workflow.NewMutex(ctx),
		unsafeMaxVersion: unsafeMaxVersion,
		signalHandler: &SignalHandler{
			signalSelector: workflow.NewSelector(ctx),
		},
	}

	return workflowRunner.run(ctx)
}

func (d *WorkflowRunner) listenToSignals(ctx workflow.Context) {
	forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
	syncVersionSummaryChannel := workflow.GetSignalChannel(ctx, SyncVersionSummarySignal)

	d.signalHandler.signalSelector.AddReceive(forceCANSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		d.signalHandler.processingSignals++
		defer func() { d.signalHandler.processingSignals-- }()
		c.Receive(ctx, nil)
		d.forceCAN = true
	})
	d.signalHandler.signalSelector.AddReceive(syncVersionSummaryChannel, func(c workflow.ReceiveChannel, more bool) {
		d.signalHandler.processingSignals++
		defer func() { d.signalHandler.processingSignals-- }()
		var summary *deploymentspb.WorkerDeploymentVersionSummary
		c.Receive(ctx, &summary)
		d.syncVersionSummaryFromVersionWorkflow(summary)
		d.setStateChanged()
	})

	// Keep waiting for signals, when it's time to CaN the main goroutine will exit.
	for {
		d.signalHandler.signalSelector.Select(ctx)
	}
}

// syncVersionSummary ensures the version summary in the deployment workflow stays consistent
// with the version workflow. This helps prevent discrepancies if they ever fall out of sync.
func (d *WorkflowRunner) syncVersionSummaryFromVersionWorkflow(summary *deploymentspb.WorkerDeploymentVersionSummary) {
	if _, ok := d.getVersionSummary(summary.GetDeploymentVersion()); !ok {
		d.logger.Error("received summary for a non-existing version, ignoring it", "version", summary.GetDeploymentVersion())
		return
	}

	d.setVersionSummary(summary.GetDeploymentVersion(), summary)
}

func (d *WorkflowRunner) updateVersionSummary(summary *deploymentspb.WorkerDeploymentVersionSummary) {
	if _, ok := d.getVersionSummary(summary.GetDeploymentVersion()); !ok {
		d.logger.Error("received summary for a non-existing version, ignoring it", "version", summary.GetDeploymentVersion())
		return
	}

	// Preserve create_time and first_activation_time if they exist in current summary. This is to ensure that if the version
	// had already been activated before, we don't override the first activation time by setting it to a wrong value.
	if existingSummary, _ := d.getVersionSummary(summary.GetDeploymentVersion()); existingSummary.GetCreateTime() != nil {
		summary.CreateTime = existingSummary.GetCreateTime()

		if existingSummary.GetFirstActivationTime() != nil {
			summary.FirstActivationTime = existingSummary.GetFirstActivationTime()
		}
	}

	d.setVersionSummary(summary.GetDeploymentVersion(), summary)
}

func (d *WorkflowRunner) run(ctx workflow.Context) error {
	// TODO(carlydf): remove verbose logging
	d.logger.Info("Raw workflow state at start",
		"state_nil", d.State == nil,
		"create_time_nil", d.GetState().GetCreateTime() == nil,
		"routing_config_nil", d.GetState().GetRoutingConfig() == nil,
		"raw_state", d.State,
		"workflow_id", workflow.GetInfo(ctx).WorkflowExecution.ID,
		"run_id", workflow.GetInfo(ctx).WorkflowExecution.RunID)

	if d.GetState().GetCreateTime() == nil ||
		d.GetState().GetRoutingConfig() == nil ||
		d.GetState().GetConflictToken() == nil {
		if d.State == nil {
			d.State = &deploymentspb.WorkerDeploymentLocalState{}
		}
		if d.State.CreateTime == nil {
			d.State.CreateTime = timestamppb.New(workflow.Now(ctx))
		}
		if d.State.RoutingConfig == nil {
			d.State.RoutingConfig = &deploymentpb.RoutingConfig{}
		}
		if d.State.ConflictToken == nil {
			d.State.ConflictToken, _ = workflow.Now(ctx).MarshalBinary()
		}

		// updating the memo since the RoutingConfig is updated
		if err := d.updateMemo(ctx); err != nil {
			return err
		}

		d.metrics.Counter(metrics.WorkerDeploymentCreated.Name()).Inc(1)
	}
	if d.State.Versions == nil {
		d.State.Versions = make(map[string]*deploymentspb.WorkerDeploymentVersionSummary)
	}

	// TODO(carlydf): remove verbose logging
	d.logger.Info("Starting workflow run",
		"create_time", d.State.GetCreateTime(),
		"routing_config", d.State.GetRoutingConfig(),
		"current_version", d.State.GetRoutingConfig().GetCurrentDeploymentVersion(),
		"ramping_version", d.State.GetRoutingConfig().GetRampingDeploymentVersion())

	err := workflow.SetQueryHandler(ctx, QueryDescribeDeployment, func() (*deploymentspb.QueryDescribeWorkerDeploymentResponse, error) {
		return &deploymentspb.QueryDescribeWorkerDeploymentResponse{
			State: d.State,
		}, nil
	})
	if err != nil {
		d.logger.Info("SetQueryHandler failed for WorkerDeployment workflow with error: " + err.Error())
		return err
	}

	if err := workflow.SetUpdateHandler(
		ctx,
		RegisterWorkerInWorkerDeployment,
		d.handleRegisterWorker,
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		SetCurrentVersion,
		d.handleSetCurrent,
		workflow.UpdateHandlerOptions{
			Validator: d.validateSetCurrent,
		},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		SetRampingVersion,
		d.handleSetRampingVersion,
		workflow.UpdateHandlerOptions{
			Validator: d.validateSetRampingVersion,
		},
	); err != nil {
		return err
	}

	// to-be-deprecated
	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		AddVersionToWorkerDeployment,
		d.handleAddVersionToWorkerDeployment,
		workflow.UpdateHandlerOptions{
			Validator: d.validateAddVersionToWorkerDeployment,
		},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		DeleteVersion,
		d.handleDeleteVersion,
		workflow.UpdateHandlerOptions{
			Validator: d.validateDeleteVersion,
		},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		DeleteDeployment,
		d.handleDeleteDeployment,
		workflow.UpdateHandlerOptions{
			Validator: d.validateDeleteDeployment,
		},
	); err != nil {
		return err
	}

	// Listen to signals in a different goroutine to make business logic clearer
	workflow.Go(ctx, d.listenToSignals)

	// Wait until we can continue as new or are cancelled. The workflow will continue-as-new iff
	// there are no pending updates/signals and the state has changed.
	err = workflow.Await(ctx, func() bool {
		canContinue := d.deleteDeployment || // deployment is deleted -> it's ok to drop all signals and updates.
			// There is no pending signal or update, but the state is dirty or forceCaN is requested:
			(!d.signalHandler.signalSelector.HasPending() && d.signalHandler.processingSignals == 0 && workflow.AllHandlersFinished(ctx) &&
				(d.forceCAN || d.stateChanged))

		// TODO(carlydf): remove verbose logging
		if canContinue {
			d.logger.Info("Workflow can continue as new",
				"workflow_id", workflow.GetInfo(ctx).WorkflowExecution.ID,
				"run_id", workflow.GetInfo(ctx).WorkflowExecution.RunID,
				"delete_deployment", d.deleteDeployment,
				"has_pending_signals", d.signalHandler.signalSelector.HasPending(),
				"processing_signals", d.signalHandler.processingSignals,
				"all_handlers_finished", workflow.AllHandlersFinished(ctx),
				"force_can", d.forceCAN,
				"state_changed", d.stateChanged,
				"routing_config", d.State.GetRoutingConfig())
		}
		return canContinue
	})
	if err != nil {
		return err
	}

	if d.deleteDeployment {
		return nil
	}

	// TODO(carlydf): remove verbose logging
	d.logger.Info("Continuing workflow as new",
		"create_time", d.State.GetCreateTime(),
		"routing_config", d.State.GetRoutingConfig(),
		"current_version", d.State.GetRoutingConfig().GetCurrentDeploymentVersion(),
		"ramping_version", d.State.GetRoutingConfig().GetRampingDeploymentVersion(),
		"state_changed", d.stateChanged,
		"force_can", d.forceCAN,
		"workflow_id", workflow.GetInfo(ctx).WorkflowExecution.ID,
		"run_id", workflow.GetInfo(ctx).WorkflowExecution.RunID)

	// We perform a continue-as-new after each update and signal is handled to ensure compatibility
	// even if the server rolls back to a previous minor version. By continuing-as-new,
	// we pass the current state as input to the next workflow execution, resulting in a new
	// workflow history with just two initial events. This minimizes the risk of NDE (Non-Deterministic Execution)
	// errors during server rollbacks.
	return workflow.NewContinueAsNewError(ctx, WorkerDeploymentWorkflowType, d.WorkerDeploymentWorkflowArgs)
}

func (d *WorkflowRunner) addVersionToWorkerDeployment(ctx workflow.Context, args *deploymentspb.AddVersionUpdateArgs) error {
	if d.State.Versions == nil {
		return nil
	}

	for _, k := range workflow.DeterministicKeys(d.State.Versions) {
		v := d.State.Versions[k]
		// only check build id because within the same deployment, we know deployment name is the same
		if v.DeploymentVersion.GetBuildId() == args.DeploymentVersion.GetBuildId() {
			return nil
		}
	}

	maxVersions := d.getMaxVersions(ctx)

	if len(d.State.Versions) >= maxVersions {
		err := d.tryDeleteVersion(ctx)
		if err != nil {
			return temporal.NewApplicationError(fmt.Sprintf("cannot add version '%v' since maximum number of versions (%d) have been registered in the deployment", args.DeploymentVersion, maxVersions), errTooManyVersions)
		}
	}

	d.setVersionSummary(args.DeploymentVersion, &deploymentspb.WorkerDeploymentVersionSummary{
		DeploymentVersion: args.DeploymentVersion,
		CreateTime:        args.CreateTime,
		Status:            enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
	})
	d.metrics.Counter(metrics.WorkerDeploymentVersionCreated.Name()).Inc(1)
	return nil
}

func (d *WorkflowRunner) handleRegisterWorker(ctx workflow.Context, args *deploymentspb.RegisterWorkerInWorkerDeploymentArgs) error {

	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	defer func() {
		// Even if the update doesn't change the state we mark it as dirty because of created history events.
		d.setStateChanged()
		d.lock.Unlock()
	}()

	// Add version to local state of the workflow, if not already present.
	err = d.addVersionToWorkerDeployment(ctx, &deploymentspb.AddVersionUpdateArgs{
		DeploymentVersion: args.DeploymentVersion,
		CreateTime:        timestamppb.New(workflow.Now(ctx)),
	})
	if err != nil {
		return err
	}

	// Register task-queue worker in version workflow.
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	err = workflow.ExecuteActivity(activityCtx, d.a.RegisterWorkerInVersion, &deploymentspb.RegisterWorkerInVersionArgs{
		TaskQueueName:     args.TaskQueueName,
		TaskQueueType:     args.TaskQueueType,
		MaxTaskQueues:     args.MaxTaskQueues,
		DeploymentVersion: args.DeploymentVersion,
	}).Get(ctx, nil)
	if err != nil {
		var appError *temporal.ApplicationError
		if errors.As(err, &appError) {
			if appError.Type() == errMaxTaskQueuesInVersionType {
				return temporal.NewApplicationError(
					fmt.Sprintf("cannot add task queue %v since maximum number of task queues (%d) have been registered in deployment", args.TaskQueueName, args.MaxTaskQueues),
					errMaxTaskQueuesInVersionType,
				)
			}
		}
		return err
	}

	// update memo
	return d.updateMemo(ctx)
}

func (d *WorkflowRunner) validateDeleteDeployment() error {
	if len(d.State.Versions) > 0 {
		return serviceerror.NewFailedPrecondition("deployment has versions, can't be deleted")
	}
	return nil
}

func (d *WorkflowRunner) handleDeleteDeployment(ctx workflow.Context) error {
	// Even if the update doesn't change the state we mark it as dirty because of created history events.
	defer d.setStateChanged()

	if len(d.State.Versions) == 0 {
		d.deleteDeployment = true
	}
	return nil
}

func (d *WorkflowRunner) validateStateBeforeAcceptingRampingUpdate(args *deploymentspb.SetRampingVersionArgs) error {
	if args.DeploymentVersion.GetBuildId() == d.State.GetRoutingConfig().GetRampingDeploymentVersion().GetBuildId() &&
		args.Percentage == d.State.GetRoutingConfig().GetRampingVersionPercentage() &&
		args.Identity == d.State.GetLastModifierIdentity() {
		return temporal.NewApplicationError("version already ramping, no change", errNoChangeType, d.State.GetConflictToken())
	}

	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.GetConflictToken()) {
		return temporal.NewApplicationError("conflict token mismatch", errFailedPrecondition)
	}
	if args.DeploymentVersion.GetBuildId() == d.State.GetRoutingConfig().GetCurrentDeploymentVersion().GetBuildId() &&
		args.DeploymentVersion != nil {
		d.logger.Info("version can't be set to ramping since it is already current")
		return temporal.NewApplicationError(fmt.Sprintf("requested ramping version '%v' is already current", args.DeploymentVersion), errFailedPrecondition)
	}

	if _, ok := d.getVersionSummary(args.DeploymentVersion); !ok && args.DeploymentVersion != nil {
		d.logger.Info("version not found in deployment")
		return temporal.NewApplicationError(fmt.Sprintf("requested ramping version '%v' not found in deployment", args.DeploymentVersion), errFailedPrecondition)
	}

	return nil
}

func (d *WorkflowRunner) validateSetRampingVersion(args *deploymentspb.SetRampingVersionArgs) error {
	return d.validateStateBeforeAcceptingRampingUpdate(args)
}

//revive:disable-next-line:cognitive-complexity
func (d *WorkflowRunner) handleSetRampingVersion(ctx workflow.Context, args *deploymentspb.SetRampingVersionArgs) (*deploymentspb.SetRampingVersionResponse, error) {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return nil, serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	defer func() {
		// Even if the update doesn't change the state we mark it as dirty because of created history events.
		d.setStateChanged()
		d.lock.Unlock()
	}()

	// Validating the state before starting the SetRampingVersion operation. This is required due to the following reason:
	// The validator accepts/rejects updates based on the state of the deployment workflow. Theoretically, two concurrent update requests
	// might be accepted by the validator since the state of the workflow, at that point in time, is valid for the updates to take place. Since this update handler
	// enforces sequential updates, after the first update completes, the local state of the deployment workflow will change. The second update,
	// now already accepted by the validator, should now not be allowed to run since the state of the workflow is different.
	err = d.validateStateBeforeAcceptingRampingUpdate(args)
	if err != nil {
		return nil, err
	}

	prevRampingVersion := d.State.RoutingConfig.RampingDeploymentVersion
	prevRampingVersionPercentage := d.State.RoutingConfig.RampingVersionPercentage

	newRampingVersion := args.DeploymentVersion
	routingUpdateTime := timestamppb.New(workflow.Now(ctx))

	var rampingSinceTime *timestamppb.Timestamp
	var rampingVersionUpdateTime *timestamppb.Timestamp

	if prevRampingVersion.GetBuildId() == newRampingVersion.GetBuildId() { // the version was already ramping, user changing ramp %
		rampingSinceTime = d.State.RoutingConfig.RampingVersionChangedTime
		rampingVersionUpdateTime = d.State.RoutingConfig.RampingVersionChangedTime
	} else {
		// version ramping for the first time
		currentVersion := d.State.RoutingConfig.CurrentDeploymentVersion
		if !args.IgnoreMissingTaskQueues &&
			currentVersion != nil &&
			newRampingVersion != nil {
			isMissingTaskQueues, err := d.isVersionMissingTaskQueues(ctx, currentVersion, newRampingVersion)
			if err != nil {
				d.logger.Info("Error verifying poller presence in version", "error", err)
				return nil, err
			}
			if isMissingTaskQueues {
				return nil, serviceerror.NewFailedPrecondition(ErrRampingVersionDoesNotHaveAllTaskQueues)
			}
		}
		rampingSinceTime = routingUpdateTime
		rampingVersionUpdateTime = routingUpdateTime

		// Erase summary drainage status immediately, so it is not draining/drained.
		d.setDrainageStatus(newRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED, routingUpdateTime)
	}

	setRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
		RoutingUpdateTime: routingUpdateTime,
		RampingSinceTime:  rampingSinceTime,
		RampPercentage:    args.Percentage,
	}
	if newRampingVersion != nil {
		if _, err := d.syncVersion(ctx, newRampingVersion, setRampUpdateArgs, true); err != nil {
			return nil, err
		}
	} else {
		if err := d.syncUnversionedRamp(ctx, setRampUpdateArgs); err != nil {
			return nil, err
		}
	}

	// tell previous ramping version that it's no longer ramping
	// Note: "unsetting" ramp means a user will pass (rampingDeploymentVersion = nil, percentage = 0).
	// if that occurs, we must:
	//   1. tell the previously ramping version the new config
	//   2. set the drainage status for the previously ramping version to draining
	// that is taken care of here.
	if prevRampingVersion.GetBuildId() != newRampingVersion.GetBuildId() {
		unsetRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: routingUpdateTime,
			RampingSinceTime:  nil, // remove ramp
			RampPercentage:    0,   // remove ramp
		}
		if prevRampingVersion != nil {
			if _, err := d.syncVersion(ctx, prevRampingVersion, unsetRampUpdateArgs, false); err != nil {
				return nil, err
			}
		} else if prevRampingVersionPercentage > 0 {
			if err := d.syncUnversionedRamp(ctx, unsetRampUpdateArgs); err != nil {
				return nil, err
			}
		}
		// Set summary drainage status immediately to draining.
		// We know prevRampingVersion cannot have been current, so it must now be draining
		d.setDrainageStatus(prevRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING, routingUpdateTime)
	}

	// update local state
	d.State.RoutingConfig.RampingDeploymentVersion = newRampingVersion
	d.State.RoutingConfig.RampingVersionPercentage = args.Percentage
	d.State.RoutingConfig.RampingVersionChangedTime = rampingVersionUpdateTime
	d.State.ConflictToken, _ = routingUpdateTime.AsTime().MarshalBinary()
	d.State.LastModifierIdentity = args.Identity

	// update memo
	if err = d.updateMemo(ctx); err != nil {
		return nil, err
	}

	return &deploymentspb.SetRampingVersionResponse{
		PreviousDeploymentVersion: prevRampingVersion,
		PreviousPercentage:        prevRampingVersionPercentage,
		ConflictToken:             d.State.ConflictToken,
	}, nil

}

func (d *WorkflowRunner) setDrainageStatus(version *deploymentpb.WorkerDeploymentVersion, status enumspb.VersionDrainageStatus, routingUpdateTime *timestamppb.Timestamp) {
	if summary, _ := d.getVersionSummary(version); summary != nil {
		summary.DrainageStatus = status
		summary.DrainageInfo = &deploymentpb.VersionDrainageInfo{
			Status:          status,
			LastChangedTime: routingUpdateTime,
			LastCheckedTime: routingUpdateTime,
		}
	}
}

func (d *WorkflowRunner) validateDeleteVersion(args *deploymentspb.DeleteVersionArgs) error {
	if _, ok := d.getVersionSummary(args.DeploymentVersion); !ok {
		return temporal.NewApplicationError("version not found in deployment", errVersionNotFound)
	}

	// Check if the version is not current or ramping. This condition is better to be checked in the
	// deployment workflow because that's the source of truth for routing config.
	if d.State.RoutingConfig.GetCurrentDeploymentVersion().GetBuildId() == args.DeploymentVersion.GetBuildId() ||
		d.State.RoutingConfig.GetRampingDeploymentVersion().GetBuildId() == args.DeploymentVersion.GetBuildId() {
		// activity won't retry on this error since version not eligible for deletion
		return serviceerror.NewFailedPrecondition(ErrVersionIsCurrentOrRamping)
	}
	return nil
}

func (d *WorkflowRunner) deleteVersion(ctx workflow.Context, args *deploymentspb.DeleteVersionArgs) error {
	// ask version to delete itself
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.SyncVersionStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.DeleteWorkerDeploymentVersion, &deploymentspb.DeleteVersionActivityArgs{
		Identity:          args.Identity,
		DeploymentVersion: args.DeploymentVersion,
		RequestId:         uuid.New(),
		SkipDrainage:      args.SkipDrainage,
	}).Get(ctx, &res)
	if err != nil {
		return err
	}
	// update local state
	d.deleteVersionSummary(args.DeploymentVersion)
	d.State.LastModifierIdentity = args.Identity
	// update memo
	return d.updateMemo(ctx)
}

func (d *WorkflowRunner) handleDeleteVersion(ctx workflow.Context, args *deploymentspb.DeleteVersionArgs) error {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	defer func() {
		// Even if the update doesn't change the state we mark it as dirty because of created history events.
		d.setStateChanged()
		d.lock.Unlock()
	}()

	// Validating the state before starting the DeleteVersion operation. This is required due to the following reason:
	// The validator accepts/rejects updates based on the state of the deployment workflow. Theoretically, two concurrent delete version requests
	// might be accepted by the validator since the local state of the workflow contains the version which is requested to be deleted. Since this update handler
	// enforces sequential updates, after the first update completes, the version will be removed from the local state of the deployment workflow. The second update,
	// now already accepted by the validator, should now not be allowed to run since the initial workflow state is different.
	err = d.validateDeleteVersion(args)
	if err != nil {
		return err
	}

	return d.deleteVersion(ctx, args)
}

func (d *WorkflowRunner) validateStateBeforeAcceptingSetCurrent(args *deploymentspb.SetCurrentVersionArgs) error {
	if d.State.GetRoutingConfig().GetCurrentDeploymentVersion().GetBuildId() == args.DeploymentVersion.GetBuildId() && d.State.GetLastModifierIdentity() == args.Identity {
		return temporal.NewApplicationError("no change", errNoChangeType, d.State.ConflictToken)
	}
	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.ConflictToken) {
		return temporal.NewApplicationError("conflict token mismatch", errFailedPrecondition)
	}
	if _, ok := d.getVersionSummary(args.DeploymentVersion); !ok && args.DeploymentVersion != nil {
		d.logger.Info("version not found in deployment")
		return temporal.NewApplicationError(fmt.Sprintf("version '%v' not found in deployment", args.DeploymentVersion), errFailedPrecondition)
	}
	return nil
}

func (d *WorkflowRunner) validateSetCurrent(args *deploymentspb.SetCurrentVersionArgs) error {
	return d.validateStateBeforeAcceptingSetCurrent(args)
}

func (d *WorkflowRunner) handleSetCurrent(ctx workflow.Context, args *deploymentspb.SetCurrentVersionArgs) (*deploymentspb.SetCurrentVersionResponse, error) {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return nil, serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	defer func() {
		// Even if the update doesn't change the state we mark it as dirty because of created history events.
		d.setStateChanged()
		d.lock.Unlock()
	}()

	// Log state before update
	// TODO(carlydf): remove verbose logging
	d.logger.Info("Starting SetCurrent update",
		"current_version", d.State.GetRoutingConfig().GetCurrentDeploymentVersion(),
		"new_version", args.DeploymentVersion,
		"routing_config", d.State.GetRoutingConfig())

	// Validating the state before starting the SetCurrent operation. This is required due to the following reason:
	// The validator accepts/rejects updates based on the state of the deployment workflow. Theoretically, two concurrent update requests
	// might be accepted by the validator since the state of the workflow, at that point in time, is valid for the updates to take place. Since this update handler
	// enforces sequential updates, after the first update completes, the local state of the deployment workflow will change. The second update,
	// now already accepted by the validator, should now not be allowed to run since the state of the workflow is different.
	err = d.validateStateBeforeAcceptingSetCurrent(args)
	if err != nil {
		return nil, err
	}

	prevCurrentVersion := d.State.RoutingConfig.CurrentDeploymentVersion
	newCurrentVersion := args.DeploymentVersion
	updateTime := timestamppb.New(workflow.Now(ctx))

	if !args.IgnoreMissingTaskQueues &&
		prevCurrentVersion != nil &&
		newCurrentVersion != nil {
		isMissingTaskQueues, err := d.isVersionMissingTaskQueues(ctx, prevCurrentVersion, newCurrentVersion)
		if err != nil {
			d.logger.Info("Error verifying poller presence in version", "error", err)
			return nil, err
		}
		if isMissingTaskQueues {
			return nil, serviceerror.NewFailedPrecondition(ErrCurrentVersionDoesNotHaveAllTaskQueues)
		}
	}

	if newCurrentVersion != nil {
		// Tell new current version that it's current
		currUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: updateTime,
			CurrentSinceTime:  updateTime,
			RampingSinceTime:  nil, // remove ramp for that version if it was ramping
			RampPercentage:    0,   // remove ramp for that version if it was ramping
		}
		if _, err := d.syncVersion(ctx, newCurrentVersion, currUpdateArgs, true); err != nil {
			return nil, err
		}
		// Erase summary drainage status immediately (in case it was previously drained/draining)
		d.setDrainageStatus(newCurrentVersion, enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED, updateTime)
	}
	// If the new current version is unversioned and there was no unversioned ramp, all we need to
	// do is tell the previous current version that it is not current. Then, the task queues in the
	// previous current version will have no current version and will become unversioned implicitly.

	if prevCurrentVersion != nil {
		// Tell previous current that it's no longer current
		prevUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: updateTime,
			CurrentSinceTime:  nil, // remove current
			RampingSinceTime:  nil, // no change, the prev current was not ramping
			RampPercentage:    0,   // no change, the prev current was not ramping
		}
		if _, err := d.syncVersion(ctx, prevCurrentVersion, prevUpdateArgs, false); err != nil {
			return nil, err
		}
		// Set summary drainage status immediately to draining.
		// We know prevCurrentVersion cannot have been ramping, so it must now be draining
		d.setDrainageStatus(prevCurrentVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING, updateTime)
	}

	if newCurrentVersion == nil && d.State.RoutingConfig.RampingDeploymentVersion == nil && d.State.RoutingConfig.RampingVersionPercentage > 0 {
		// If the new current is unversioned, and it was previously ramping, we need to tell
		// all the task queues with unversioned ramp that they no longer have unversioned ramp.
		// The task queues with unversioned ramp are the task queues of the previous current version.
		// TODO (Carly): Should we ban people from changing the task queues in the current version while they have an unversioned ramp?
		unsetRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: updateTime,
			RampingSinceTime:  nil, // remove ramp
			RampPercentage:    0,   // remove ramp
		}
		if err := d.syncUnversionedRamp(ctx, unsetRampUpdateArgs); err != nil {
			return nil, err
		}
	}

	// If the previous current version was unversioned, there is nothing in the task queues
	// to remove, because they were implicitly unversioned. We don't have to remove any
	// unversioned ramps, because current and ramping cannot both be unversioned.

	// update local state
	d.State.RoutingConfig.CurrentDeploymentVersion = args.DeploymentVersion
	d.State.RoutingConfig.CurrentVersionChangedTime = updateTime
	d.State.ConflictToken, _ = updateTime.AsTime().MarshalBinary()
	d.State.LastModifierIdentity = args.Identity

	// unset ramping version if it was set to current version
	if d.State.RoutingConfig.CurrentDeploymentVersion.GetBuildId() == d.State.RoutingConfig.RampingDeploymentVersion.GetBuildId() {
		d.State.RoutingConfig.RampingDeploymentVersion = nil
		d.State.RoutingConfig.RampingVersionPercentage = 0
		d.State.RoutingConfig.RampingVersionChangedTime = updateTime // since ramp was removed
	}

	// update memo
	if err = d.updateMemo(ctx); err != nil {
		return nil, err
	}

	return &deploymentspb.SetCurrentVersionResponse{
		PreviousDeploymentVersion: prevCurrentVersion,
		ConflictToken:             d.State.ConflictToken,
	}, nil

}

// to-be-deprecated
func (d *WorkflowRunner) validateAddVersionToWorkerDeployment(args *deploymentspb.AddVersionUpdateArgs) error {
	if d.State.Versions == nil {
		return nil
	}

	for _, v := range d.State.Versions {
		if v.DeploymentVersion.GetBuildId() == args.DeploymentVersion.GetBuildId() {
			return temporal.NewApplicationError("deployment version already registered", errVersionAlreadyExistsType)
		}
	}
	return nil
}

func (d *WorkflowRunner) getMaxVersions(ctx workflow.Context) int {
	getMaxVersionsInDeployment := func(ctx workflow.Context) interface{} {
		return d.unsafeMaxVersion()
	}
	intEq := func(a, b interface{}) bool {
		return a == b
	}
	var maxVersions int
	if err := workflow.MutableSideEffect(ctx, "getMaxVersions", getMaxVersionsInDeployment, intEq).Get(&maxVersions); err != nil {
		// This should not happen really. but just in case.
		return defaultMaxVersions
	}
	return maxVersions
}

// to-be-deprecated
func (d *WorkflowRunner) handleAddVersionToWorkerDeployment(ctx workflow.Context, args *deploymentspb.AddVersionUpdateArgs) error {
	// Even if the update doesn't change the state we mark it as dirty because of created history events.
	defer d.setStateChanged()

	maxVersions := d.getMaxVersions(ctx)

	if len(d.State.Versions) >= maxVersions {
		err := d.tryDeleteVersion(ctx)
		if err != nil {
			return temporal.NewApplicationError(fmt.Sprintf("cannot add version, already at max versions %d", maxVersions), errTooManyVersions)
		}
	}

	d.setVersionSummary(args.DeploymentVersion, &deploymentspb.WorkerDeploymentVersionSummary{
		DeploymentVersion: args.DeploymentVersion,
		CreateTime:        args.CreateTime,
	})

	return nil
}

func (d *WorkflowRunner) tryDeleteVersion(ctx workflow.Context) error {
	sortedSummaries := d.sortedSummaries()
	for _, v := range sortedSummaries {
		args := &deploymentspb.DeleteVersionArgs{
			Identity:          "try-delete-for-add-version",
			DeploymentVersion: v.DeploymentVersion,
		}
		if err := d.validateDeleteVersion(args); err == nil {
			// this might hang on the lock
			if err = d.deleteVersion(ctx, args); err == nil {
				return nil
			}
		}
	}
	return serviceerror.NewFailedPrecondition("could not add version: too many versions in deployment and none are eligible for deletion")
}

func (d *WorkflowRunner) syncVersion(ctx workflow.Context, targetVersion *deploymentpb.WorkerDeploymentVersion, versionUpdateArgs *deploymentspb.SyncVersionStateUpdateArgs, activated bool) (*deploymentspb.VersionLocalState, error) {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.SyncVersionStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.SyncWorkerDeploymentVersion, &deploymentspb.SyncVersionStateActivityArgs{
		DeploymentVersion: targetVersion,
		UpdateArgs:        versionUpdateArgs,
		RequestId:         d.newUUID(ctx),
	}).Get(ctx, &res)

	// Update the VersionSummary, stored as part of the WorkerDeploymentLocalState, for this version.
	if err == nil {
		summary := &deploymentspb.WorkerDeploymentVersionSummary{
			DeploymentVersion: targetVersion,
			RoutingUpdateTime: versionUpdateArgs.RoutingUpdateTime,
			CurrentSinceTime:  versionUpdateArgs.CurrentSinceTime,
			RampingSinceTime:  versionUpdateArgs.RampingSinceTime,
		}
		if activated {
			summary.FirstActivationTime = versionUpdateArgs.RoutingUpdateTime
		} else {
			summary.LastDeactivationTime = versionUpdateArgs.RoutingUpdateTime
		}

		// Setting the appropriate status for the version. The status of a version is never set to
		// DRAINED from within the deployment workflow since the version workflow is responsible for
		// querying visibility after which it signals the deployment workflow if the version is drained.
		if summary.CurrentSinceTime != nil {
			summary.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT
		} else if summary.RampingSinceTime != nil {
			summary.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING
		} else {
			summary.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING
		}
		d.updateVersionSummary(summary)
	}
	return res.VersionState, err
}

func (d *WorkflowRunner) syncUnversionedRamp(ctx workflow.Context, versionUpdateArgs *deploymentspb.SyncVersionStateUpdateArgs) error {
	if d.State.RoutingConfig.CurrentDeploymentVersion == nil {
		return nil
	}
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	// DescribeVersion activity to get all the task queues in the current version
	var res deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult
	err := workflow.ExecuteActivity(
		activityCtx,
		d.a.DescribeVersionFromWorkerDeployment,
		&deploymentspb.DescribeVersionFromWorkerDeploymentActivityArgs{
			DeploymentVersion: d.State.RoutingConfig.CurrentDeploymentVersion,
		}).Get(ctx, &res)
	if err != nil {
		return err
	}

	// send in the task-queue families in batches of syncBatchSize
	batches := make([][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)
	syncReqs := make([]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)

	// Grouping by task-queue name
	taskQueuesByName := make(map[string][]enumspb.TaskQueueType)
	for _, tq := range res.GetTaskQueueInfos() {
		taskQueuesByName[tq.GetName()] = append(taskQueuesByName[tq.GetName()], tq.GetType())
	}

	for _, tqName := range workflow.DeterministicKeys(taskQueuesByName) {
		tqTypes := taskQueuesByName[tqName]
		sync := &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			Name:  tqName,
			Types: tqTypes,
			Data: &deploymentspb.DeploymentVersionData{
				DeploymentVersion: nil,
				RoutingUpdateTime: versionUpdateArgs.RoutingUpdateTime,
				RampingSinceTime:  versionUpdateArgs.RampingSinceTime,
				RampPercentage:    versionUpdateArgs.RampPercentage,
			},
		}
		syncReqs = append(syncReqs, sync)

		if len(syncReqs) == int(d.State.SyncBatchSize) {
			batches = append(batches, syncReqs)
			syncReqs = make([]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0) // reset the syncReq.Sync slice for the next batch
		}
	}
	if len(syncReqs) > 0 {
		batches = append(batches, syncReqs)
	}

	// calling SyncDeploymentVersionUserData for each batch
	for _, batch := range batches {
		var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse

		err = workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserDataFromWorkerDeployment, &deploymentspb.SyncDeploymentVersionUserDataRequest{
			DeploymentVersion: nil,
			ForgetVersion:     false,
			Sync:              batch,
		}).Get(ctx, &syncRes)
		if err != nil {
			// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
			return err
		}

		if len(syncRes.TaskQueueMaxVersions) > 0 {
			// wait for propagation
			err = workflow.ExecuteActivity(
				activityCtx,
				d.a.CheckUnversionedRampUserDataPropagation,
				&deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
					TaskQueueMaxVersions: syncRes.TaskQueueMaxVersions,
				}).Get(ctx, nil)
			if err != nil {
				// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
				return err
			}
		}
	}

	return err
}

func (d *WorkflowRunner) isVersionMissingTaskQueues(ctx workflow.Context, prevCurrentVersion, newCurrentVersion *deploymentpb.WorkerDeploymentVersion) (bool, error) {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.IsVersionMissingTaskQueuesResult
	err := workflow.ExecuteActivity(activityCtx, d.a.IsVersionMissingTaskQueues, &deploymentspb.IsVersionMissingTaskQueuesArgs{
		PrevCurrentDeploymentVersion: prevCurrentVersion,
		NewCurrentDeploymentVersion:  newCurrentVersion,
	}).Get(ctx, &res)
	return res.IsMissingTaskQueues, err
}

func (d *WorkflowRunner) newUUID(ctx workflow.Context) string {
	var val string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New()
	}).Get(&val)
	return val
}

func (d *WorkflowRunner) updateMemo(ctx workflow.Context) error {
	// TODO(carlydf): remove verbose logging
	d.logger.Info("Updating workflow memo",
		"routing_config", d.State.GetRoutingConfig(),
		"current_version", d.State.GetRoutingConfig().GetCurrentDeploymentVersion(),
		"ramping_version", d.State.GetRoutingConfig().GetRampingDeploymentVersion())

	return workflow.UpsertMemo(ctx, map[string]any{
		WorkerDeploymentMemoField: &deploymentspb.WorkerDeploymentWorkflowMemo{
			DeploymentName:        d.DeploymentName,
			CreateTime:            d.State.CreateTime,
			RoutingConfig:         d.State.RoutingConfig,
			LatestVersionSummary:  d.getLatestVersionSummary(),
			CurrentVersionSummary: d.getCurrentVersionSummary(),
			RampingVersionSummary: d.getRampingVersionSummary(),
		},
	})
}

func (d *WorkflowRunner) setStateChanged() {
	d.stateChanged = true
}

func (d *WorkflowRunner) sortedSummaries() []*deploymentspb.WorkerDeploymentVersionSummary {
	var sortedSummaries []*deploymentspb.WorkerDeploymentVersionSummary
	for _, k := range workflow.DeterministicKeys(d.State.Versions) {
		s := d.State.Versions[k]
		sortedSummaries = append(sortedSummaries, s)
	}

	slices.SortFunc(sortedSummaries, func(a, b *deploymentspb.WorkerDeploymentVersionSummary) int {
		// sorts in ascending order.
		// cmp(a, b) should return a negative number when a < b, a positive number when a > b,
		// and zero when a == b or a and b are incomparable in the sense of a strict weak ordering.
		if a.GetCreateTime().AsTime().After(b.GetCreateTime().AsTime()) {
			return 1
		} else if a.GetCreateTime().AsTime().Before(b.GetCreateTime().AsTime()) {
			return -1
		}
		return 0
	})
	return sortedSummaries
}

func (d *WorkflowRunner) getLatestVersionSummary() *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary {
	sortedSummaries := d.sortedSummaries()
	if len(sortedSummaries) == 0 {
		return nil
	}
	latest_summary := sortedSummaries[len(sortedSummaries)-1]
	return d.getWorkerDeploymentInfoVersionSummary(latest_summary)
}

func (d *WorkflowRunner) getCurrentVersionSummary() *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary {
	currentVersion := d.GetState().GetRoutingConfig().GetCurrentDeploymentVersion()
	currentVersionSummary, _ := d.getVersionSummary(currentVersion)

	if currentVersionSummary == nil {
		return nil
	}
	return d.getWorkerDeploymentInfoVersionSummary(currentVersionSummary)
}

func (d *WorkflowRunner) getRampingVersionSummary() *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary {
	rampingVersion := d.GetState().GetRoutingConfig().GetRampingDeploymentVersion()
	rampingVersionSummary, _ := d.getVersionSummary(rampingVersion)

	if rampingVersionSummary == nil {
		return nil
	}
	return d.getWorkerDeploymentInfoVersionSummary(rampingVersionSummary)
}

func (d *WorkflowRunner) getWorkerDeploymentInfoVersionSummary(versionSummary *deploymentspb.WorkerDeploymentVersionSummary) *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary {
	return &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		DeploymentVersion:    versionSummary.GetDeploymentVersion(),
		Status:               versionSummary.GetStatus(),
		CreateTime:           versionSummary.GetCreateTime(),
		DrainageInfo:         versionSummary.GetDrainageInfo(),
		CurrentSinceTime:     versionSummary.GetCurrentSinceTime(),
		RampingSinceTime:     versionSummary.GetRampingSinceTime(),
		RoutingUpdateTime:    versionSummary.GetRoutingUpdateTime(),
		FirstActivationTime:  versionSummary.GetFirstActivationTime(),
		LastDeactivationTime: versionSummary.GetLastDeactivationTime(),
	}
}

func (d *WorkflowRunner) setVersionSummary(dv *deploymentpb.WorkerDeploymentVersion, summary *deploymentspb.WorkerDeploymentVersionSummary) {
	d.State.Versions[worker_versioning.ExternalWorkerDeploymentVersionToStringV31(dv)] = summary
}

func (d *WorkflowRunner) getVersionSummary(dv *deploymentpb.WorkerDeploymentVersion) (summary *deploymentspb.WorkerDeploymentVersionSummary, ok bool) {
	summary, ok = d.State.Versions[worker_versioning.ExternalWorkerDeploymentVersionToStringV31(dv)]
	return summary, ok
}

func (d *WorkflowRunner) deleteVersionSummary(dv *deploymentpb.WorkerDeploymentVersion) {
	delete(d.State.Versions, worker_versioning.ExternalWorkerDeploymentVersionToStringV31(dv))
}
