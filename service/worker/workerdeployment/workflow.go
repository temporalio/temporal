package workerdeployment

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
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
		// workflowVersion is set at workflow start based on the dynamic config of the worker
		// that completes the first task. It remains constant for the lifetime of the run and
		// only updates when the workflow performs continue-as-new.
		workflowVersion DeploymentWorkflowVersion
	}
)

// This workflow is implemented in a way such that it always CaNs after some
// history events are added to it and when it has no pending work to do. This is to keep the
// history clean so that we have less concern about backwards and forwards compatibility.
// In steady state (i.e. absence of ongoing updates or signals) the wf should only have
// a single wft in the history.
func Workflow(ctx workflow.Context, unsafeWorkflowVersionGetter func() DeploymentWorkflowVersion, unsafeMaxVersion func() int, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
	workflowRunner := &WorkflowRunner{
		WorkerDeploymentWorkflowArgs: args,
		workflowVersion:              getWorkflowVersion(ctx, unsafeWorkflowVersionGetter),
		a:                            nil,
		logger:                       sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.NamespaceName),
		metrics:                      workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.NamespaceName}),
		lock:                         workflow.NewMutex(ctx),
		unsafeMaxVersion:             unsafeMaxVersion,
		signalHandler: &SignalHandler{
			signalSelector: workflow.NewSelector(ctx),
		},
	}

	return workflowRunner.run(ctx)
}

func getWorkflowVersion(ctx workflow.Context, unsafeWorkflowVersionGetter func() DeploymentWorkflowVersion) DeploymentWorkflowVersion {
	if workflow.GetVersion(ctx, "workflowVersionAdded", workflow.DefaultVersion, 0) >= 0 {
		var ver DeploymentWorkflowVersion
		err := workflow.MutableSideEffect(ctx, "workflowVersion",
			func(_ workflow.Context) interface{} { return unsafeWorkflowVersionGetter() },
			func(a, b interface{}) bool { return a == b }).
			Get(&ver)
		if err == nil {
			return ver
		}
	}
	return 0
}

func (d *WorkflowRunner) hasMinVersion(version DeploymentWorkflowVersion) bool {
	return d.workflowVersion >= version
}

func (d *WorkflowRunner) listenToSignals(ctx workflow.Context) {
	forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
	syncVersionSummaryChannel := workflow.GetSignalChannel(ctx, SyncVersionSummarySignal)
	propagationCompleteChannel := workflow.GetSignalChannel(ctx, PropagationCompleteSignal)

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
	d.signalHandler.signalSelector.AddReceive(propagationCompleteChannel, func(c workflow.ReceiveChannel, more bool) {
		d.signalHandler.processingSignals++
		defer func() { d.signalHandler.processingSignals-- }()
		var completion *deploymentspb.PropagationCompletionInfo
		c.Receive(ctx, &completion)
		d.handlePropagationComplete(completion)
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
	if _, ok := d.State.Versions[summary.GetVersion()]; !ok {
		d.logger.Error("received summary for a non-existing version, ignoring it", "version", summary.GetVersion())
		return
	}

	d.State.Versions[summary.GetVersion()] = summary
}

// handlePropagationComplete handles the propagation complete signal from version workflows
func (d *WorkflowRunner) handlePropagationComplete(completion *deploymentspb.PropagationCompletionInfo) {
	buildID := completion.BuildId
	revisionNumber := completion.RevisionNumber

	if d.State.PropagatingRevisions == nil {
		d.logger.Error("Received propagation complete signal, but no in-progress propagations found",
			"revision", revisionNumber,
			"build_id", buildID)
		return
	}

	// Remove this revision from in-progress tracking for this build
	if revisions, ok := d.State.PropagatingRevisions[buildID]; ok {
		found := false
		// Find and remove the revision number
		filteredRevisions := make([]int64, 0, len(revisions.RevisionNumbers))
		for _, rev := range revisions.RevisionNumbers {
			if rev != revisionNumber {
				filteredRevisions = append(filteredRevisions, rev)
			} else {
				found = true
			}
		}
		if !found {
			d.logger.Error("Received propagation complete signal, but this propagation is not in-progress",
				"revision", revisionNumber,
				"build_id", buildID)
		}

		if len(filteredRevisions) == 0 {
			// Clean up empty build id entries
			delete(d.State.PropagatingRevisions, buildID)
		} else {
			revisions.RevisionNumbers = filteredRevisions
		}
	} else {
		d.logger.Error("Received propagation complete signal, but no in-progress propagations found for this version",
			"revision", revisionNumber,
			"build_id", buildID)
	}

	d.logger.Info("Propagation completed for revision",
		"revision", revisionNumber,
		"build_id", buildID)
}

func (d *WorkflowRunner) updateVersionSummary(summary *deploymentspb.WorkerDeploymentVersionSummary) {
	if _, ok := d.State.Versions[summary.GetVersion()]; !ok {
		d.logger.Error("received summary for a non-existing version, ignoring it", "version", summary.GetVersion())
		return
	}

	// Preserve create_time and first_activation_time if they exist in current summary. This is to ensure that if the version
	// had already been activated before, we don't override the first activation time by setting it to a wrong value.
	if existingSummary := d.State.Versions[summary.GetVersion()]; existingSummary.GetCreateTime() != nil {
		summary.CreateTime = existingSummary.GetCreateTime()

		if existingSummary.GetFirstActivationTime() != nil {
			summary.FirstActivationTime = existingSummary.GetFirstActivationTime()
		}
	}

	d.State.Versions[summary.GetVersion()] = summary
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
			d.State.RoutingConfig = &deploymentpb.RoutingConfig{CurrentVersion: worker_versioning.UnversionedVersionId}
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
		//nolint:staticcheck // SA1019: worker versioning v0.31
		"current_version", d.State.GetRoutingConfig().GetCurrentVersion(),
		//nolint:staticcheck // SA1019: worker versioning v0.31
		"ramping_version", d.State.GetRoutingConfig().GetRampingVersion())

	err := workflow.SetQueryHandler(ctx, QueryDescribeDeployment, func() (*deploymentspb.QueryDescribeWorkerDeploymentResponse, error) {
		if d.deleteDeployment {
			return nil, errors.New(errDeploymentDeleted)
		}
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

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		SetManagerIdentity,
		d.handleSetManager,
		workflow.UpdateHandlerOptions{
			Validator: d.validateSetManager,
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
		//nolint:staticcheck // SA1019: worker versioning v0.31
		"current_version", d.State.GetRoutingConfig().GetCurrentVersion(),
		//nolint:staticcheck // SA1019: worker versioning v0.31
		"ramping_version", d.State.GetRoutingConfig().GetRampingVersion(),
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

func (d *WorkflowRunner) preUpdateChecks(ctx workflow.Context) error {
	err := d.ensureNotDeleted()
	if err != nil {
		return err
	}

	if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
		// History is too large, do not accept new updates until wf CaNs.
		// Since this needs workflow context we cannot do it in validators.
		return temporal.NewApplicationError(errLongHistory, errLongHistory)
	}
	return nil
}

func (d *WorkflowRunner) ensureNotDeleted() error {
	if d.deleteDeployment {
		return temporal.NewNonRetryableApplicationError(errDeploymentDeleted, errDeploymentDeleted, nil)
	}
	return nil
}

func (d *WorkflowRunner) addVersionToWorkerDeployment(ctx workflow.Context, args *deploymentspb.AddVersionUpdateArgs) error {
	if d.State.Versions == nil {
		return nil
	}

	for _, k := range workflow.DeterministicKeys(d.State.Versions) {
		v := d.State.Versions[k]
		if v.Version == args.Version {
			return nil
		}
	}

	maxVersions := d.getMaxVersions(ctx)

	if len(d.State.Versions) >= maxVersions {
		err := d.tryDeleteVersion(ctx)
		if err != nil {
			return temporal.NewApplicationError(fmt.Sprintf("cannot add version %s since maximum number of versions (%d) have been registered in the deployment", args.Version, maxVersions), errTooManyVersions)
		}
	}

	d.State.Versions[args.Version] = &deploymentspb.WorkerDeploymentVersionSummary{
		Version:    args.Version,
		CreateTime: args.CreateTime,
		Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
	}
	d.metrics.Counter(metrics.WorkerDeploymentVersionCreated.Name()).Inc(1)
	return nil
}

func (d *WorkflowRunner) handleRegisterWorker(ctx workflow.Context, args *deploymentspb.RegisterWorkerInWorkerDeploymentArgs) error {
	// TODO: there is a small race condition where the deployment is just deleted and got a register update before closing itself.
	// In that case, we should ideally not reject the update, but revive the workflow so that the caller does not need to retry.
	// In practice this should be fine because the polls will retry and Deployment workflows are short-lived.
	// Same principle applies for Version workflows, but they can be slightly more long-lived of they are handling long propagations.
	// Hence, the revive logic is implemented in Version workflow but not here yet.
	if err := d.ensureNotDeleted(); err != nil {
		return err
	}

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
		Version:    worker_versioning.WorkerDeploymentVersionToStringV31(args.Version),
		CreateTime: timestamppb.New(workflow.Now(ctx)),
	})
	if err != nil {
		return err
	}
	var routingConfigToSync *deploymentpb.RoutingConfig
	if d.hasMinVersion(AsyncSetCurrentAndRamping) {
		routingConfigToSync = d.GetState().GetRoutingConfig()
	}

	// Register task-queue worker in version workflow.
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	err = workflow.ExecuteActivity(activityCtx, d.a.RegisterWorkerInVersion, &deploymentspb.RegisterWorkerInVersionArgs{
		TaskQueueName: args.TaskQueueName,
		TaskQueueType: args.TaskQueueType,
		MaxTaskQueues: args.MaxTaskQueues,
		Version:       worker_versioning.WorkerDeploymentVersionToStringV31(args.Version),
		RoutingConfig: routingConfigToSync,
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

	if len(d.State.Versions) == 0 {
		d.deleteDeployment = true
	}
	return nil
}

func (d *WorkflowRunner) rampingVersionStringUnversioned(s string) bool {
	return s == worker_versioning.UnversionedVersionId || s == ""
}

func (d *WorkflowRunner) validateStateBeforeAcceptingRampingUpdate(args *deploymentspb.SetRampingVersionArgs) error {
	//nolint:staticcheck // SA1019: worker versioning v0.31
	if args.Version == d.State.GetRoutingConfig().GetRampingVersion() &&
		args.Percentage == d.State.GetRoutingConfig().GetRampingVersionPercentage() &&
		args.Identity == d.State.GetLastModifierIdentity() {
		return temporal.NewApplicationError("version already ramping, no change", errNoChangeType, d.State.GetConflictToken())
	}

	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.GetConflictToken()) {
		return temporal.NewApplicationError("conflict token mismatch", errFailedPrecondition)
	}

	//nolint:staticcheck // SA1019: worker versioning v0.31
	if args.Version == d.State.GetRoutingConfig().GetCurrentVersion() &&
		!(args.Version == worker_versioning.UnversionedVersionId && args.Percentage == 0) {
		d.logger.Info("version can't be set to ramping since it is already current")
		return temporal.NewApplicationError(fmt.Sprintf("requested ramping version %s is already current", args.Version), errFailedPrecondition)
	}

	if _, ok := d.State.GetVersions()[args.Version]; !ok &&
		args.Version != worker_versioning.UnversionedVersionId &&
		args.Version != "" &&
		!args.GetAllowNoPollers() {
		d.logger.Info("version not found in deployment")
		return temporal.NewApplicationError(fmt.Sprintf("requested ramping version %s not found in deployment", args.Version), errVersionNotFound)
	}

	if d.State.ManagerIdentity != "" && d.State.ManagerIdentity != args.Identity {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf(ErrManagerIdentityMismatch, d.State.ManagerIdentity, args.Identity))
	}
	return nil
}

func (d *WorkflowRunner) validateSetRampingVersion(args *deploymentspb.SetRampingVersionArgs) error {
	if err := d.ensureNotDeleted(); err != nil {
		return err
	}
	return d.validateStateBeforeAcceptingRampingUpdate(args)
}

//revive:disable-next-line:cognitive-complexity
//nolint:staticcheck // deprecated stuff will be cleaned
func (d *WorkflowRunner) handleSetRampingVersion(ctx workflow.Context, args *deploymentspb.SetRampingVersionArgs) (*deploymentspb.SetRampingVersionResponse, error) {
	if err := d.preUpdateChecks(ctx); err != nil {
		return nil, err
	}

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

	prevRampingVersion := d.State.RoutingConfig.RampingVersion
	prevRampingVersionPercentage := d.State.RoutingConfig.RampingVersionPercentage

	newRampingVersion := args.Version
	routingUpdateTime := timestamppb.New(workflow.Now(ctx))

	if _, ok := d.State.Versions[args.Version]; !ok &&
		args.Version != worker_versioning.UnversionedVersionId &&
		args.Version != "" &&
		args.GetAllowNoPollers() {
		d.logger.Info("version not found in deployment, but AllowNoPollers is true, so we will create the version")
		if err := d.addVersionToWorkerDeployment(ctx, &deploymentspb.AddVersionUpdateArgs{Version: newRampingVersion, CreateTime: routingUpdateTime}); err != nil {
			return nil, err // only possible error is errTooManyVersions
		}
		v, err := worker_versioning.WorkerDeploymentVersionFromStringV31(newRampingVersion)
		if err != nil {
			return nil, err // this would never happen, because version string formatting was already checked earlier
		}
		if err := d.startVersion(ctx, &deploymentspb.StartWorkerDeploymentVersionRequest{
			DeploymentName: v.GetDeploymentName(),
			BuildId:        v.GetBuildId(),
			RequestId:      d.newUUID(ctx),
		}); err != nil {
			return nil, err
		}
	}

	var rampingSinceTime *timestamppb.Timestamp
	var rampingVersionUpdateTime *timestamppb.Timestamp

	asyncMode := d.hasMinVersion(AsyncSetCurrentAndRamping)

	if prevRampingVersion != newRampingVersion || prevRampingVersionPercentage != args.Percentage || !asyncMode {
		// In async mode we do not touch routing config and versions if this is not changing the
		// ramping version or percentage (but could still come here because modifier identity is changing).

		// Determine timestamps based on whether we're setting or unsetting ramp
		if newRampingVersion == "" {
			// unsetting ramp
			rampingVersionUpdateTime = routingUpdateTime
		} else if prevRampingVersion == newRampingVersion {
			// version was already ramping, user changing ramp %
			rampingSinceTime = d.State.RoutingConfig.RampingVersionChangedTime
			rampingVersionUpdateTime = d.State.RoutingConfig.RampingVersionChangedTime
		} else {
			// version ramping for the first time
			rampingSinceTime = routingUpdateTime
			rampingVersionUpdateTime = routingUpdateTime
		}

		// Build pending routing config with the updated ramping version
		// Initialize for both sync and async modes to simplify state update logic
		// Ensure CurrentDeploymentVersion is populated from deprecated field if needed for backward compatibility
		currentDeploymentVersion := d.State.RoutingConfig.CurrentDeploymentVersion
		if currentDeploymentVersion == nil {
			currentDeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(d.State.RoutingConfig.CurrentVersion)
		}
		pendingRoutingConfig := &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:            currentDeploymentVersion,
			CurrentVersion:                      d.State.RoutingConfig.CurrentVersion,
			RampingDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(newRampingVersion),
			RampingVersion:                      newRampingVersion,
			RampingVersionPercentage:            args.Percentage,
			CurrentVersionChangedTime:           d.State.RoutingConfig.CurrentVersionChangedTime,
			RampingVersionChangedTime:           rampingVersionUpdateTime,
			RampingVersionPercentageChangedTime: routingUpdateTime,
			RevisionNumber:                      d.State.RoutingConfig.RevisionNumber,
		}

		var routingConfigToSync *deploymentpb.RoutingConfig

		if asyncMode {
			pendingRoutingConfig.RevisionNumber++
			// only setting it in the request if it's async mode
			routingConfigToSync = pendingRoutingConfig
		}

		if newRampingVersion == "" {
			err = d.unsetRamp(ctx, routingUpdateTime, routingConfigToSync, prevRampingVersion, asyncMode, pendingRoutingConfig)
			if err != nil {
				return nil, err
			}
		} else {
			err = d.setRamp(ctx,
				args,
				prevRampingVersion,
				newRampingVersion,
				routingUpdateTime,
				rampingSinceTime,
				routingConfigToSync,
				asyncMode,
				pendingRoutingConfig)
			if err != nil {
				return nil, err
			}
		}

		// update local state - use pendingRoutingConfig (initialized for both sync and async modes)
		d.State.RoutingConfig = pendingRoutingConfig
	}

	d.State.ConflictToken, _ = routingUpdateTime.AsTime().MarshalBinary()
	d.State.LastModifierIdentity = args.Identity

	// update memo
	if err = d.updateMemo(ctx); err != nil {
		return nil, err
	}

	return &deploymentspb.SetRampingVersionResponse{
		PreviousVersion:    prevRampingVersion,
		PreviousPercentage: prevRampingVersionPercentage,
		ConflictToken:      d.State.ConflictToken,
	}, nil

}

//nolint:staticcheck // SA1019
func (d *WorkflowRunner) unsetRamp(
	ctx workflow.Context,
	routingUpdateTime *timestamppb.Timestamp,
	routingConfigToSync *deploymentpb.RoutingConfig,
	prevRampingVersion string,
	asyncMode bool,
	pendingRoutingConfig *deploymentpb.RoutingConfig,
) error {
	unsetRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
		RoutingUpdateTime: routingUpdateTime,
		RampingSinceTime:  nil, // remove ramp
		RampPercentage:    0,   // remove ramp
		RoutingConfig:     routingConfigToSync,
	}

	if !d.rampingVersionStringUnversioned(prevRampingVersion) {
		if _, err := d.syncVersion(ctx, prevRampingVersion, unsetRampUpdateArgs); err != nil {
			return err
		}
	} else if asyncMode {
		// Here, we are unsetting unversioned ramp in async mode. This only can happen if there IS a current version, so we
		// propagate the ramp status in routing config through the current version.
		if _, err := d.syncVersion(ctx, pendingRoutingConfig.CurrentVersion, unsetRampUpdateArgs); err != nil {
			return err
		}
	} else {
		// Only should call this in sync mode
		if err := d.syncUnversionedRamp(ctx, unsetRampUpdateArgs); err != nil {
			return err
		}
	}

	// Set summary drainage status immediately to draining.
	// We know prevRampingVersion cannot have been current, so it must now be draining
	d.setDrainageStatus(prevRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING, routingUpdateTime)
	return nil
}

//nolint:staticcheck // SA1019
func (d *WorkflowRunner) setRamp(
	ctx workflow.Context,
	args *deploymentspb.SetRampingVersionArgs,
	prevRampingVersion string,
	newRampingVersion string,
	routingUpdateTime *timestamppb.Timestamp,
	rampingSinceTime *timestamppb.Timestamp,
	routingConfigToSync *deploymentpb.RoutingConfig,
	asyncMode bool,
	pendingRoutingConfig *deploymentpb.RoutingConfig,
) error {
	if prevRampingVersion != newRampingVersion {
		// version ramping for the first time - need to check for missing task queues

		currentVersion := d.State.RoutingConfig.CurrentVersion
		if !args.IgnoreMissingTaskQueues &&
			currentVersion != worker_versioning.UnversionedVersionId &&
			newRampingVersion != worker_versioning.UnversionedVersionId {
			isMissingTaskQueues, err := d.isVersionMissingTaskQueues(ctx, currentVersion, newRampingVersion)
			if err != nil {
				d.logger.Info("Error verifying poller presence in version", "error", err)
				return err
			}
			if isMissingTaskQueues {
				newRampingVersionObj, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(newRampingVersion)
				return serviceerror.NewFailedPreconditionf(
					ErrRampingVersionDoesNotHaveAllTaskQueues,
					worker_versioning.WorkerDeploymentVersionToStringV32(newRampingVersionObj),
				)
			}
		}
		if !asyncMode {
			// Erase summary drainage status immediately, so it is not draining/drained.
			d.setDrainageStatus(newRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED, routingUpdateTime)
		}
	}

	setRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
		RoutingUpdateTime: routingUpdateTime,
		RampingSinceTime:  rampingSinceTime,
		RampPercentage:    args.Percentage,
		RoutingConfig:     routingConfigToSync,
	}
	if !d.rampingVersionStringUnversioned(newRampingVersion) {
		if _, err := d.syncVersion(ctx, newRampingVersion, setRampUpdateArgs); err != nil {
			return err
		}
	} else if asyncMode {
		// Here, we are setting unversioned ramp in async mode. This only can happen if there IS a current version, so we
		// propagate the ramp status in routing config through the current version.
		if _, err := d.syncVersion(ctx, pendingRoutingConfig.CurrentVersion, setRampUpdateArgs); err != nil {
			return err
		}
	} else {
		// Only should call this in sync mode
		if err := d.syncUnversionedRamp(ctx, setRampUpdateArgs); err != nil {
			return err
		}
	}

	// tell previous ramping version, if present, that it's no longer ramping
	if prevRampingVersion != "" && prevRampingVersion != newRampingVersion {
		err := d.unsetPreviousRamp(ctx, routingUpdateTime, routingConfigToSync, prevRampingVersion, asyncMode)
		if err != nil {
			return err
		}
	}
	return nil
}

//nolint:staticcheck // SA1019
func (d *WorkflowRunner) unsetPreviousRamp(
	ctx workflow.Context,
	routingUpdateTime *timestamppb.Timestamp,
	routingConfigToSync *deploymentpb.RoutingConfig,
	prevRampingVersion string,
	asyncMode bool,
) error {
	unsetRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
		RoutingUpdateTime: routingUpdateTime,
		RampingSinceTime:  nil, // remove ramp
		RampPercentage:    0,   // remove ramp
		RoutingConfig:     routingConfigToSync,
	}
	if !d.rampingVersionStringUnversioned(prevRampingVersion) {
		if _, err := d.syncVersion(ctx, prevRampingVersion, unsetRampUpdateArgs); err != nil {
			return err
		}
	} else if asyncMode {
		// Here, we are setting a versioned ramp on top of an unversioned ramp in async mode. In this case we
		// already synced the ramp status through the new ramping version so there is no need for another sync.
	} else {
		// Only should call this in sync mode
		if err := d.syncUnversionedRamp(ctx, unsetRampUpdateArgs); err != nil {
			return err
		}
	}
	if !asyncMode {
		// Set summary drainage status immediately to draining.
		// We know prevRampingVersion cannot have been current, so it must now be draining
		d.setDrainageStatus(prevRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING, routingUpdateTime)
	}
	return nil
}

func (d *WorkflowRunner) setDrainageStatus(version string, status enumspb.VersionDrainageStatus, routingUpdateTime *timestamppb.Timestamp) {
	if summary := d.State.GetVersions()[version]; summary != nil {
		summary.DrainageStatus = status
		summary.DrainageInfo = &deploymentpb.VersionDrainageInfo{
			Status:          status,
			LastChangedTime: routingUpdateTime,
			LastCheckedTime: routingUpdateTime,
		}
	}
}

func (d *WorkflowRunner) validateDeleteVersion(args *deploymentspb.DeleteVersionArgs) error {
	if err := d.ensureNotDeleted(); err != nil {
		return err
	}

	if _, ok := d.State.Versions[args.Version]; !ok {
		return temporal.NewApplicationError("version not found in deployment", errVersionNotFound)
	}

	// Check if the version is not current or ramping. This condition is better to be checked in the
	// deployment workflow because that's the source of truth for routing config.
	//nolint:staticcheck // SA1019: worker versioning v0.31
	if d.State.RoutingConfig.CurrentVersion == args.Version || d.State.RoutingConfig.RampingVersion == args.Version {
		versionObj, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(args.Version)
		// activity won't retry on this error since version not eligible for deletion
		return serviceerror.NewFailedPreconditionf(ErrVersionIsCurrentOrRamping, worker_versioning.WorkerDeploymentVersionToStringV32(versionObj))
	}

	// Ignore the manager identity check if the delete operation is initiated by the server internally
	if !args.GetServerDelete() && d.State.ManagerIdentity != "" && d.State.ManagerIdentity != args.Identity {
		return serviceerror.NewFailedPreconditionf(ErrManagerIdentityMismatch, d.State.ManagerIdentity, args.Identity)
	}
	return nil
}

func (d *WorkflowRunner) deleteVersion(ctx workflow.Context, args *deploymentspb.DeleteVersionArgs) error {
	// ask version to delete itself
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.SyncVersionStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.DeleteWorkerDeploymentVersion, &deploymentspb.DeleteVersionActivityArgs{
		Identity:         args.Identity,
		DeploymentName:   d.DeploymentName,
		Version:          args.Version,
		RequestId:        uuid.NewString(),
		SkipDrainage:     args.SkipDrainage,
		AsyncPropagation: d.hasMinVersion(AsyncSetCurrentAndRamping),
	}).Get(ctx, &res)
	if err != nil {
		var activityError *temporal.ActivityError
		var applicationError *temporal.ApplicationError
		if errors.As(err, &activityError) && errors.As(activityError.Unwrap(), &applicationError) &&
			(applicationError.Type() == errVersionHasPollers || applicationError.Type() == errVersionIsDraining) {
			return serviceerror.NewFailedPrecondition(applicationError.Message())
		}
		return err
	}
	// update local state
	delete(d.State.Versions, args.Version)
	// remove from propagating versions if it's there
	delete(d.State.PropagatingRevisions, worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(args.GetVersion()).GetBuildId())
	if !args.GetServerDelete() {
		d.State.LastModifierIdentity = args.Identity
	}
	// update memo
	return d.updateMemo(ctx)
}

func (d *WorkflowRunner) handleDeleteVersion(ctx workflow.Context, args *deploymentspb.DeleteVersionArgs) error {
	if err := d.preUpdateChecks(ctx); err != nil {
		return err
	}

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

func (d *WorkflowRunner) validateStateBeforeAcceptingSetManager(args *deploymentspb.SetManagerIdentityArgs) error {
	if d.State.GetManagerIdentity() == args.ManagerIdentity && d.State.GetLastModifierIdentity() == args.Identity {
		return temporal.NewApplicationError("no change", errNoChangeType, d.State.ConflictToken)
	}
	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.ConflictToken) {
		return temporal.NewApplicationError("conflict token mismatch", errFailedPrecondition)
	}
	return nil
}

func (d *WorkflowRunner) validateSetManager(args *deploymentspb.SetManagerIdentityArgs) error {
	if err := d.ensureNotDeleted(); err != nil {
		return err
	}
	return d.validateStateBeforeAcceptingSetManager(args)
}

func (d *WorkflowRunner) handleSetManager(ctx workflow.Context, args *deploymentspb.SetManagerIdentityArgs) (*deploymentspb.SetManagerIdentityResponse, error) {
	if err := d.preUpdateChecks(ctx); err != nil {
		return nil, err
	}

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

	err = d.validateStateBeforeAcceptingSetManager(args)
	if err != nil {
		return nil, err
	}

	prevManager := d.State.ManagerIdentity

	// update local state
	d.State.ManagerIdentity = args.ManagerIdentity
	d.State.LastModifierIdentity = args.Identity
	d.State.ConflictToken, _ = workflow.Now(ctx).MarshalBinary()

	// no need to update memo because identity and manager identity are not in it
	return &deploymentspb.SetManagerIdentityResponse{
		PreviousManagerIdentity: prevManager,
		ConflictToken:           d.State.ConflictToken,
	}, nil
}

func (d *WorkflowRunner) validateStateBeforeAcceptingSetCurrent(args *deploymentspb.SetCurrentVersionArgs) error {
	//nolint:staticcheck // SA1019: worker versioning v0.31
	if d.State.GetRoutingConfig().GetCurrentVersion() == args.Version && d.State.GetLastModifierIdentity() == args.Identity {
		return temporal.NewApplicationError("no change", errNoChangeType, d.State.ConflictToken)
	}
	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.ConflictToken) {
		return temporal.NewApplicationError("conflict token mismatch", errFailedPrecondition)
	}
	if _, ok := d.State.Versions[args.Version]; !ok &&
		args.Version != worker_versioning.UnversionedVersionId &&
		!args.GetAllowNoPollers() {
		d.logger.Info("version not found in deployment")
		return temporal.NewApplicationError(fmt.Sprintf("version %s not found in deployment", args.Version), errVersionNotFound)
	}
	if d.State.ManagerIdentity != "" && d.State.ManagerIdentity != args.Identity {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf(ErrManagerIdentityMismatch, d.State.ManagerIdentity, args.Identity))
	}
	return nil
}

func (d *WorkflowRunner) validateSetCurrent(args *deploymentspb.SetCurrentVersionArgs) error {
	if err := d.ensureNotDeleted(); err != nil {
		return err
	}

	return d.validateStateBeforeAcceptingSetCurrent(args)
}

//nolint:staticcheck // deprecated stuff will be cleaned
func (d *WorkflowRunner) handleSetCurrent(ctx workflow.Context, args *deploymentspb.SetCurrentVersionArgs) (*deploymentspb.SetCurrentVersionResponse, error) {
	if err := d.preUpdateChecks(ctx); err != nil {
		return nil, err
	}

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
		//nolint:staticcheck // SA1019: worker versioning v0.31
		"current_version", d.State.GetRoutingConfig().GetCurrentVersion(),
		"new_version", args.Version,
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

	prevCurrentVersion := d.State.RoutingConfig.CurrentVersion
	newCurrentVersion := args.Version
	updateTime := timestamppb.New(workflow.Now(ctx))

	if _, ok := d.State.Versions[args.Version]; !ok &&
		args.Version != worker_versioning.UnversionedVersionId &&
		args.GetAllowNoPollers() {
		d.logger.Info("version not found in deployment, but AllowNoPollers is true, so we will create the version")
		if err := d.addVersionToWorkerDeployment(ctx, &deploymentspb.AddVersionUpdateArgs{Version: newCurrentVersion, CreateTime: updateTime}); err != nil {
			return nil, err // only possible error is errTooManyVersions
		}
		v, err := worker_versioning.WorkerDeploymentVersionFromStringV31(newCurrentVersion)
		if err != nil {
			return nil, err // this would never happen, because version string formatting was already checked earlier
		}
		if err := d.startVersion(ctx, &deploymentspb.StartWorkerDeploymentVersionRequest{
			DeploymentName: v.GetDeploymentName(),
			BuildId:        v.GetBuildId(),
			RequestId:      d.newUUID(ctx),
		}); err != nil {
			return nil, err
		}
	}

	if !args.IgnoreMissingTaskQueues &&
		prevCurrentVersion != worker_versioning.UnversionedVersionId &&
		newCurrentVersion != worker_versioning.UnversionedVersionId {
		isMissingTaskQueues, err := d.isVersionMissingTaskQueues(ctx, prevCurrentVersion, newCurrentVersion)
		if err != nil {
			d.logger.Info("Error verifying poller presence in version", "error", err)
			return nil, err
		}
		if isMissingTaskQueues {
			newCurrentVersionObj, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(newCurrentVersion)
			return nil, serviceerror.NewFailedPreconditionf(
				ErrCurrentVersionDoesNotHaveAllTaskQueues,
				worker_versioning.WorkerDeploymentVersionToStringV32(newCurrentVersionObj),
			)
		}
	}

	asyncMode := d.hasMinVersion(AsyncSetCurrentAndRamping)

	if prevCurrentVersion != newCurrentVersion || !asyncMode {
		// In async mode we do not touch routing config and versions if this is not changing the
		// current version (but could still come here because modifier identity is changing).

		// Build pending routing config with the updated current version
		// Initialize for both sync and async modes to simplify state update logic
		// Ensure RampingDeploymentVersion is populated from deprecated field if needed for backward compatibility
		rampingDeploymentVersion := d.State.RoutingConfig.RampingDeploymentVersion
		if rampingDeploymentVersion == nil {
			rampingDeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(d.State.RoutingConfig.RampingVersion)
		}
		pendingRoutingConfig := &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(newCurrentVersion),
			CurrentVersion:                      newCurrentVersion,
			RampingDeploymentVersion:            rampingDeploymentVersion,
			RampingVersion:                      d.State.RoutingConfig.RampingVersion,
			RampingVersionPercentage:            d.State.RoutingConfig.RampingVersionPercentage,
			CurrentVersionChangedTime:           updateTime,
			RampingVersionChangedTime:           d.State.RoutingConfig.RampingVersionChangedTime,
			RampingVersionPercentageChangedTime: d.State.RoutingConfig.RampingVersionPercentageChangedTime,
			RevisionNumber:                      d.State.RoutingConfig.RevisionNumber,
		}

		var routingConfigToSync *deploymentpb.RoutingConfig
		if asyncMode {
			pendingRoutingConfig.RevisionNumber++
			// only setting it in the request if it's async mode
			routingConfigToSync = pendingRoutingConfig
		}

		// Unset ramping if it's being promoted to current
		if newCurrentVersion == d.State.RoutingConfig.RampingVersion {
			pendingRoutingConfig.RampingVersion = ""
			pendingRoutingConfig.RampingDeploymentVersion = nil
			pendingRoutingConfig.RampingVersionPercentage = 0
			pendingRoutingConfig.RampingVersionChangedTime = updateTime
			pendingRoutingConfig.RampingVersionPercentageChangedTime = updateTime
		}

		// TODO (Shivam): remove the empty string check once canary stops flaking out
		if newCurrentVersion != worker_versioning.UnversionedVersionId && newCurrentVersion != "" {
			// Tell new current version that it's current
			currUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
				RoutingUpdateTime: updateTime,
				CurrentSinceTime:  updateTime,
				RampingSinceTime:  nil, // remove ramp for that version if it was ramping
				RampPercentage:    0,   // remove ramp for that version if it was ramping
				RoutingConfig:     routingConfigToSync,
			}
			if _, err := d.syncVersion(ctx, newCurrentVersion, currUpdateArgs); err != nil {
				return nil, err
			}
			// Erase summary drainage status immediately (in case it was previously drained/draining)
			d.setDrainageStatus(newCurrentVersion, enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED, updateTime)
		}
		// If the new current version is unversioned and there was no unversioned ramp, all we need to
		// do is tell the previous current version that it is not current. Then, the task queues in the
		// previous current version will have no current version and will become unversioned implicitly.

		// TODO (Shivam): remove the empty string check once canary stops flaking out
		if prevCurrentVersion != worker_versioning.UnversionedVersionId && prevCurrentVersion != "" {
			// Tell previous current that it's no longer current
			prevUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
				RoutingUpdateTime: updateTime,
				CurrentSinceTime:  nil, // remove current
				RampingSinceTime:  nil, // no change, the prev current was not ramping
				RampPercentage:    0,   // no change, the prev current was not ramping
				RoutingConfig:     routingConfigToSync,
			}
			if _, err := d.syncVersion(ctx, prevCurrentVersion, prevUpdateArgs); err != nil {
				return nil, err
			}
			if !asyncMode {
				// Set summary drainage status immediately to draining.
				// We know prevCurrentVersion cannot have been ramping, so it must now be draining
				d.setDrainageStatus(prevCurrentVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING, updateTime)
			}
		}

		//nolint:staticcheck // deprecated stuff will be cleaned
		if newCurrentVersion == worker_versioning.UnversionedVersionId && d.State.RoutingConfig.RampingVersion == worker_versioning.UnversionedVersionId &&
			// this step is not needed in async mode because the task queues got full routing info (including removed ramp) through the previous version.
			!asyncMode {
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

		// update local state - use pendingRoutingConfig (initialized for both sync and async modes)
		d.State.RoutingConfig = pendingRoutingConfig
	}

	d.State.ConflictToken, _ = updateTime.AsTime().MarshalBinary()
	d.State.LastModifierIdentity = args.Identity

	// update memo
	if err = d.updateMemo(ctx); err != nil {
		return nil, err
	}

	return &deploymentspb.SetCurrentVersionResponse{
		PreviousVersion: prevCurrentVersion,
		ConflictToken:   d.State.ConflictToken,
	}, nil
}

// to-be-deprecated
func (d *WorkflowRunner) validateAddVersionToWorkerDeployment(args *deploymentspb.AddVersionUpdateArgs) error {
	if d.State.Versions == nil {
		return nil
	}

	for _, v := range d.State.Versions {
		if v.Version == args.Version {
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

func (d *WorkflowRunner) tryDeleteVersion(ctx workflow.Context) error {
	sortedSummaries := d.sortedSummaries()
	for _, v := range sortedSummaries {
		args := &deploymentspb.DeleteVersionArgs{
			Identity:     serverDeleteVersionIdentity,
			Version:      v.Version,
			ServerDelete: true,
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

func (d *WorkflowRunner) syncVersion(ctx workflow.Context, targetVersion string, versionUpdateArgs *deploymentspb.SyncVersionStateUpdateArgs) (*deploymentspb.VersionLocalState, error) {
	bld := worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(targetVersion).GetBuildId()
	revisionNumber := versionUpdateArgs.GetRoutingConfig().GetRevisionNumber()
	if revisionNumber > 0 {
		// track revision number until propagation finishes
		if len(d.State.PropagatingRevisions) == 0 {
			d.State.PropagatingRevisions = make(map[string]*deploymentspb.PropagatingRevisions)
		}
		revs, ok := d.State.PropagatingRevisions[bld]
		if !ok {
			revs = &deploymentspb.PropagatingRevisions{}
			d.State.PropagatingRevisions[bld] = revs
		}
		revs.RevisionNumbers = append(revs.RevisionNumbers, revisionNumber)
	}

	var reqID string
	if d.hasMinVersion(VersionDataRevisionNumber) && revisionNumber > 0 {
		// Don't send repetitive sync requests for the same revision number to the same version.
		reqID = fmt.Sprintf("SyncWorkerDeploymentVersion-%d", revisionNumber)
	} else {
		reqID = d.newUUID(ctx)
	}
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.SyncVersionStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.SyncWorkerDeploymentVersion, &deploymentspb.SyncVersionStateActivityArgs{
		DeploymentName: d.DeploymentName,
		Version:        targetVersion,
		UpdateArgs:     versionUpdateArgs,
		RequestId:      reqID,
	}).Get(ctx, &res)

	if err == nil {
		if sum := res.GetSummary(); sum != nil {
			d.updateVersionSummary(sum)
		} else {
			//nolint:staticcheck // SA1019
			d.updateVersionSummary(versionStateToSummary(res.GetVersionState()))
		}
	} else if revisionNumber > 0 {
		// Activity failed meaning the synchronous part of the sync request failed. we need to untrack the revision number.
		d.handlePropagationComplete(&deploymentspb.PropagationCompletionInfo{RevisionNumber: revisionNumber, BuildId: bld})
	}
	return res.VersionState, err
}

// syncUnversionedRamp should not be called in async mode
func (d *WorkflowRunner) syncUnversionedRamp(ctx workflow.Context, versionUpdateArgs *deploymentspb.SyncVersionStateUpdateArgs) error {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	// DescribeVersion activity to get all the task queues in the current version, or the ramping version if current is nil
	version := d.State.RoutingConfig.CurrentVersion //nolint:staticcheck // SA1019: worker versioning v0.31
	if version == worker_versioning.UnversionedVersionId {
		version = d.State.RoutingConfig.RampingVersion //nolint:staticcheck // SA1019: worker versioning v0.31
	}

	if d.rampingVersionStringUnversioned(version) {
		return nil
	}

	var res deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult
	err := workflow.ExecuteActivity(
		activityCtx,
		d.a.DescribeVersionFromWorkerDeployment,
		&deploymentspb.DescribeVersionFromWorkerDeploymentActivityArgs{
			Version: version,
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
				Version:           nil,
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
			DeploymentName: d.DeploymentName,
			Version:        nil,
			ForgetVersion:  false,
			Sync:           batch,
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

func (d *WorkflowRunner) isVersionMissingTaskQueues(ctx workflow.Context, prevCurrentVersion string, newCurrentVersion string) (bool, error) {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.IsVersionMissingTaskQueuesResult
	err := workflow.ExecuteActivity(activityCtx, d.a.IsVersionMissingTaskQueues, &deploymentspb.IsVersionMissingTaskQueuesArgs{
		PrevCurrentVersion: prevCurrentVersion,
		NewCurrentVersion:  newCurrentVersion,
	}).Get(ctx, &res)
	return res.IsMissingTaskQueues, err
}

func (d *WorkflowRunner) startVersion(ctx workflow.Context, args *deploymentspb.StartWorkerDeploymentVersionRequest) error {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	return workflow.ExecuteActivity(activityCtx, d.a.StartWorkerDeploymentVersionWorkflow, args).Get(ctx, nil)
}

func (d *WorkflowRunner) newUUID(ctx workflow.Context) string {
	var val string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.NewString()
	}).Get(&val)
	return val
}

func (d *WorkflowRunner) updateMemo(ctx workflow.Context) error {
	// TODO(carlydf): remove verbose logging
	d.logger.Info("Updating workflow memo",
		"routing_config", d.State.GetRoutingConfig(),
		//nolint:staticcheck // SA1019: worker versioning v0.31
		"current_version", d.State.GetRoutingConfig().GetCurrentVersion(),
		//nolint:staticcheck // SA1019: worker versioning v0.31
		"ramping_version", d.State.GetRoutingConfig().GetRampingVersion())

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
	// The deployment workflow still uses the deprecated fields from v0.31. Hence, the current version is read from
	// CurrentVersion and not CurrentDeploymentVersion. This shall change before GA.
	currentVersion := d.GetState().GetRoutingConfig().GetCurrentVersion() //nolint:staticcheck
	currentVersionSummary := d.GetState().GetVersions()[currentVersion]

	if currentVersionSummary == nil {
		return nil
	}
	return d.getWorkerDeploymentInfoVersionSummary(currentVersionSummary)
}

func (d *WorkflowRunner) getRampingVersionSummary() *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary {
	// The deployment workflow still uses the deprecated fields from v0.31. Hence, the ramping version is read from
	// RampingVersion and not RampingDeploymentVersion. This shall change before GA.
	rampingVersion := d.GetState().GetRoutingConfig().GetRampingVersion() //nolint:staticcheck
	rampingVersionSummary := d.GetState().GetVersions()[rampingVersion]

	if rampingVersionSummary == nil {
		return nil
	}
	return d.getWorkerDeploymentInfoVersionSummary(rampingVersionSummary)
}

func (d *WorkflowRunner) getWorkerDeploymentInfoVersionSummary(versionSummary *deploymentspb.WorkerDeploymentVersionSummary) *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary {
	return &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              versionSummary.GetVersion(),
		DeploymentVersion:    worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(versionSummary.GetVersion()),
		Status:               versionSummary.GetStatus(),
		CreateTime:           versionSummary.GetCreateTime(),
		DrainageInfo:         versionSummary.GetDrainageInfo(),
		CurrentSinceTime:     versionSummary.GetCurrentSinceTime(),
		RampingSinceTime:     versionSummary.GetRampingSinceTime(),
		RoutingUpdateTime:    versionSummary.GetRoutingUpdateTime(),
		FirstActivationTime:  versionSummary.GetFirstActivationTime(),
		LastCurrentTime:      versionSummary.GetLastCurrentTime(),
		LastDeactivationTime: versionSummary.GetLastDeactivationTime(),
	}
}
