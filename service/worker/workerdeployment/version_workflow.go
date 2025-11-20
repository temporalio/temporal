package workerdeployment

import (
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// This key is used by controllers who manage deployment versions.
	metadataKeyController    = "temporal.io/controller"
	defaultVisibilityRefresh = 5 * time.Minute
	defaultVisibilityGrace   = 3 * time.Minute
)

type (
	// VersionWorkflowRunner holds the local state for a deployment workflow
	VersionWorkflowRunner struct {
		*deploymentspb.WorkerDeploymentVersionWorkflowArgs
		a                                 *VersionActivities
		logger                            sdklog.Logger
		metrics                           sdkclient.MetricsHandler
		lock                              workflow.Mutex
		unsafeRefreshIntervalGetter       func() any
		unsafeVisibilityGracePeriodGetter func() any
		deleteVersion                     bool
		// stateChanged is used to track if the state of the workflow has undergone a local state change since the last signal/update.
		// This prevents a workflow from continuing-as-new if the state has not changed.
		stateChanged                 bool
		signalHandler                *SignalHandler
		drainageStatusSyncInProgress bool
		forceCAN                     bool
		// Track if async propagations are in progress (prevents CaN)
		asyncPropagationsInProgress int
		// When true, all the ongoing propagations should cancel themselves
		cancelPropagations          bool
		unsafeWorkflowVersionGetter func() DeploymentWorkflowVersion
	}
)

// VersionWorkflow is implemented in a way where it always CaNs after some
// history events are added to it and it has no pending work to do. This is to keep the
// history clean so that we have less concern about backwards and forwards compatibility.
// In steady state (i.e. absence of ongoing updates or signals) the wf should only have
// a single wft in the history. For draining versions, the workflow history should only
// have a single wft in history followed by a scheduled timer for refreshing drainage
// info.
func VersionWorkflow(
	ctx workflow.Context,
	unsafeWorkflowVersionGetter func() DeploymentWorkflowVersion,
	unsafeRefreshIntervalGetter func() any,
	unsafeVisibilityGracePeriodGetter func() any,
	versionWorkflowArgs *deploymentspb.WorkerDeploymentVersionWorkflowArgs,
) error {
	versionWorkflowRunner := &VersionWorkflowRunner{
		WorkerDeploymentVersionWorkflowArgs: versionWorkflowArgs,

		a:                                 nil,
		logger:                            sdklog.With(workflow.GetLogger(ctx), "wf-namespace", versionWorkflowArgs.NamespaceName),
		metrics:                           workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": versionWorkflowArgs.NamespaceName}),
		lock:                              workflow.NewMutex(ctx),
		unsafeWorkflowVersionGetter:       unsafeWorkflowVersionGetter,
		unsafeRefreshIntervalGetter:       unsafeRefreshIntervalGetter,
		unsafeVisibilityGracePeriodGetter: unsafeVisibilityGracePeriodGetter,
		signalHandler: &SignalHandler{
			signalSelector: workflow.NewSelector(ctx),
		},
	}
	return versionWorkflowRunner.run(ctx)
}

func (d *VersionWorkflowRunner) listenToSignals(ctx workflow.Context) {
	// Fetch signal channels
	forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
	drainageStatusSignalChannel := workflow.GetSignalChannel(ctx, SyncDrainageSignalName)

	d.signalHandler.signalSelector.AddReceive(forceCANSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		d.signalHandler.processingSignals++
		defer func() { d.signalHandler.processingSignals-- }()
		// Process Signal
		c.Receive(ctx, nil)
		d.forceCAN = true
	})
	d.signalHandler.signalSelector.AddReceive(drainageStatusSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		d.signalHandler.processingSignals++
		defer func() { d.signalHandler.processingSignals-- }()

		var newInfo *deploymentpb.VersionDrainageInfo
		c.Receive(ctx, &newInfo)

		// if the version is current or ramping, ignore drainage signal since it could have come late
		if d.VersionState.GetRampingSinceTime() != nil || d.VersionState.GetCurrentSinceTime() != nil {
			return
		}

		mergedInfo := &deploymentpb.VersionDrainageInfo{}
		mergedInfo.LastCheckedTime = newInfo.LastCheckedTime
		if d.VersionState.GetDrainageInfo().GetStatus() != newInfo.Status {
			mergedInfo.Status = newInfo.Status
			mergedInfo.LastChangedTime = newInfo.LastCheckedTime
			d.VersionState.DrainageInfo = mergedInfo
		} else {
			mergedInfo.Status = d.VersionState.DrainageInfo.Status
			mergedInfo.LastChangedTime = d.VersionState.DrainageInfo.LastChangedTime
			d.VersionState.DrainageInfo = mergedInfo
		}

		if d.VersionState.GetDrainageInfo().GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_DRAINED {
			d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED
		}
		d.syncSummary(ctx)
	})

	// Keep waiting for signals, when it's time to CaN the main goroutine will exit.
	for {
		d.signalHandler.signalSelector.Select(ctx)
	}
}

func (d *VersionWorkflowRunner) run(ctx workflow.Context) error {
	if d.GetVersionState().Version == nil {
		return fmt.Errorf("version cannot be nil on start")
	}
	if d.VersionState.GetCreateTime() == nil {
		d.VersionState.CreateTime = timestamppb.New(workflow.Now(ctx))
	}
	if d.VersionState.Status == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_UNSPECIFIED {
		d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE
	}

	// if we were draining and just continued-as-new, do another drainage check after waiting for appropriate time
	if d.VersionState.GetDrainageInfo().GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_DRAINING {
		workflow.Go(ctx, d.refreshDrainageInfo)
	}

	// Set up Query Handlers here:
	if err := workflow.SetQueryHandler(ctx, QueryDescribeVersion, d.handleDescribeQuery); err != nil {
		d.logger.Error("Failed while setting up query handler")
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		RegisterWorkerInDeploymentVersion,
		d.handleRegisterWorker,
		workflow.UpdateHandlerOptions{
			Validator: d.validateRegisterWorker,
		},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		SyncVersionState,
		d.handleSyncState,
		workflow.UpdateHandlerOptions{
			Validator: d.validateSyncState,
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
		UpdateVersionMetadata,
		d.handleUpdateVersionMetadata,
		workflow.UpdateHandlerOptions{
			Validator: d.validateUpdateVersionMetadata,
		},
	); err != nil {
		return err
	}

	// First ensure deployment workflow is running
	if !d.VersionState.StartedDeploymentWorkflow {
		activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
		err := workflow.ExecuteActivity(activityCtx, d.a.StartWorkerDeploymentWorkflow, &deploymentspb.StartWorkerDeploymentRequest{
			DeploymentName: d.VersionState.Version.DeploymentName,
			RequestId:      d.newUUID(ctx),
		}).Get(ctx, nil)
		if err != nil {
			return err
		}
		d.VersionState.StartedDeploymentWorkflow = true
	}

	// Listen to signals in a different goroutine to make business logic clearer
	workflow.Go(ctx, d.listenToSignals)

	// Wait until we can continue as new or are cancelled. The workflow will continue-as-new iff
	// there are no pending updates/signals and the state has changed.
	err := workflow.Await(ctx, func() bool {
		return (d.deleteVersion && d.asyncPropagationsInProgress == 0) || // version is deleted -> it's ok to drop all signals and updates.
			// There is no pending signal or update, but the state is dirty or forceCaN is requested:
			(!d.signalHandler.signalSelector.HasPending() && d.signalHandler.processingSignals == 0 && workflow.AllHandlersFinished(ctx) &&
				// And there is a force CaN or a propagated state change
				(d.forceCAN || (d.stateChanged && d.asyncPropagationsInProgress == 0)))
	})
	if err != nil {
		return err
	}

	if d.deleteVersion {
		return nil
	}

	d.logger.Debug("Version doing continue-as-new")
	nextArgs := d.WorkerDeploymentVersionWorkflowArgs
	nextArgs.VersionState = d.VersionState
	return workflow.NewContinueAsNewError(ctx, WorkerDeploymentVersionWorkflowType, nextArgs)
}

func (d *VersionWorkflowRunner) validateUpdateVersionMetadata(args *deploymentspb.UpdateVersionMetadataArgs) error {
	if d.deleteVersion {
		// Deployment workflow should not call this function if version is marked for deletion, but still checking for safety.
		return temporal.NewNonRetryableApplicationError(errVersionDeleted, errVersionDeleted, nil)
	}
	return nil
}

func (d *VersionWorkflowRunner) handleUpdateVersionMetadata(ctx workflow.Context, args *deploymentspb.UpdateVersionMetadataArgs) (*deploymentspb.UpdateVersionMetadataResponse, error) {
	if d.VersionState.Metadata == nil && args.UpsertEntries != nil {
		d.VersionState.Metadata = &deploymentpb.VersionMetadata{}
		d.VersionState.Metadata.Entries = make(map[string]*commonpb.Payload)
	}

	for _, key := range workflow.DeterministicKeys(args.UpsertEntries) {
		if key == metadataKeyController && d.VersionState.Metadata.Entries[key] == nil {
			// adding the controller identifier key-value for the first time, counting as a controller-managed version
			// TODO: also check and potentially emit the controller id
			d.metrics.Counter(metrics.WorkerDeploymentVersionCreatedManagedByController.Name()).Inc(1)
		}

		payload := args.UpsertEntries[key]
		d.VersionState.Metadata.Entries[key] = payload
	}

	for _, key := range args.RemoveEntries {
		delete(d.VersionState.Metadata.Entries, key)
	}

	// although the handler might have not changed the metadata at all, still
	// it's better to CaN because some history events are built now.
	d.setStateChanged()

	return &deploymentspb.UpdateVersionMetadataResponse{
		Metadata: d.VersionState.Metadata,
	}, nil
}

func (d *VersionWorkflowRunner) startDrainage(ctx workflow.Context) {
	if d.VersionState.GetDrainageInfo().GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED {
		now := timestamppb.New(workflow.Now(ctx))
		d.VersionState.DrainageInfo = &deploymentpb.VersionDrainageInfo{
			Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
			LastChangedTime: now,
			LastCheckedTime: now,
		}
		if workflow.GetVersion(ctx, "no-draining-signal", workflow.DefaultVersion, 1) == workflow.DefaultVersion {
			// this is not needed because startDrainage is called only from syncVersionState which sends the summary back to deployment.
			// TODO: cleanup with sync mode
			d.syncSummary(ctx)
		}
		d.setStateChanged()
	}
}

func (d *VersionWorkflowRunner) buildSearchAttributes() temporal.SearchAttributes {
	return temporal.NewSearchAttributes(
		temporal.NewSearchAttributeKeyString(searchattribute.TemporalNamespaceDivision).ValueSet(WorkerDeploymentNamespaceDivision),
	)
}

func (d *VersionWorkflowRunner) validateDeleteVersion(args *deploymentspb.DeleteVersionArgs) error {
	// We can't call DescribeTaskQueue here because that would be an Activity call / non-deterministic.
	// Once we have PollersStatus on the version, we can check it here.
	return nil
}

func (d *VersionWorkflowRunner) handleDeleteVersion(ctx workflow.Context, args *deploymentspb.DeleteVersionArgs) error {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	defer func() {
		// although the handler might have not changed the state and had returned an error, still
		// it's better to CaN because some history events are built now.
		d.setStateChanged()
		d.lock.Unlock()
	}()

	// wait until deployment workflow started
	err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before worker deployment workflow started")
		return serviceerror.NewDeadlineExceeded("Update canceled before worker deployment workflow started")
	}

	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	// Manual deletion of versions is only possible when:
	// 1. The version is not current or ramping (checked in the deployment wf)
	// 2. The version is not draining. (check skipped when `skip-drainage=true` )
	// 3. The version has no active pollers.

	// 2. Check if the version is draining.
	if !args.SkipDrainage {
		if d.GetVersionState().GetDrainageInfo().GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_DRAINING {
			// activity won't retry on this error since version not eligible for deletion
			return serviceerror.NewFailedPrecondition(ErrVersionIsDraining)
		}
	}

	// 3. Check if the version has any active pollers.
	hasPollers, err := d.doesVersionHaveActivePollers(ctx)
	if hasPollers {
		// activity won't retry on this error since version not eligible for deletion
		return serviceerror.NewFailedPrecondition(ErrVersionHasPollers)
	}
	if err != nil {
		// some other error allowing activity retries
		return err
	}

	if args.AsyncPropagation {
		d.asyncPropagationsInProgress++
		workflow.Go(ctx, d.deleteVersionFromTaskQueuesAsync)
	} else {
		err = d.deleteVersionFromTaskQueues(ctx, activityCtx)
	}
	if err != nil {
		return err
	}

	d.deleteVersion = true
	return nil
}

//nolint:revive,errcheck // In async mode the activities retry indefinitely so this function should not return error
func (d *VersionWorkflowRunner) deleteVersionFromTaskQueuesAsync(ctx workflow.Context) {
	// If there are propagations in progress, we ask them to cancel and wait for them to do so.
	// The reason is that the ongoing upsert propagation may overwrite the delete that we want to send here, unintentionally undoing it.
	d.cancelPropagations = true
	workflow.Await(ctx, func() bool { return d.asyncPropagationsInProgress == 1 }) // delete itself is counted as one
	d.cancelPropagations = false                                                   // need to unset this in case the version is revived

	d.deleteVersionFromTaskQueues(ctx, workflow.WithActivityOptions(ctx, propagationActivityOptions))
	d.asyncPropagationsInProgress--
}

func (d *VersionWorkflowRunner) deleteVersionFromTaskQueues(ctx workflow.Context, activityCtx workflow.Context) error {
	state := d.GetVersionState()

	// sync version removal to task queues
	syncReq := &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version:       state.GetVersion(),
		ForgetVersion: true,
	}

	for _, tqName := range workflow.DeterministicKeys(state.TaskQueueFamilies) {
		byType := state.TaskQueueFamilies[tqName]
		var types []enumspb.TaskQueueType
		for _, tqType := range workflow.DeterministicKeys(byType.TaskQueues) {
			types = append(types, enumspb.TaskQueueType(tqType))
		}
		syncReq.Sync = append(syncReq.Sync, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			Name:  tqName,
			Types: types,
		})
	}

	var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse
	err := workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, syncReq).Get(ctx, &syncRes)
	if err != nil {
		return err
	}

	// wait for propagation
	if len(syncRes.TaskQueueMaxVersions) > 0 {
		err = workflow.ExecuteActivity(
			activityCtx,
			d.a.CheckWorkerDeploymentUserDataPropagation,
			&deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
				TaskQueueMaxVersions: syncRes.TaskQueueMaxVersions,
			}).Get(ctx, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// doesVersionHaveActivePollers returns true if the version has active pollers.
func (d *VersionWorkflowRunner) doesVersionHaveActivePollers(ctx workflow.Context) (bool, error) {

	// describe all task queues in the deployment, if any have pollers, then cannot delete

	tqNameToTypes := make(map[string]*deploymentspb.CheckTaskQueuesHavePollersActivityArgs_TaskQueueTypes)
	for _, tqName := range workflow.DeterministicKeys(d.VersionState.TaskQueueFamilies) {
		tqFamilyData := d.VersionState.TaskQueueFamilies[tqName]
		var tqTypes []enumspb.TaskQueueType
		for _, tqType := range workflow.DeterministicKeys(tqFamilyData.TaskQueues) {
			tqTypes = append(tqTypes, enumspb.TaskQueueType(tqType))
		}
		tqNameToTypes[tqName] = &deploymentspb.CheckTaskQueuesHavePollersActivityArgs_TaskQueueTypes{Types: tqTypes}
	}
	checkPollersReq := &deploymentspb.CheckTaskQueuesHavePollersActivityArgs{
		TaskQueuesAndTypes:      tqNameToTypes,
		WorkerDeploymentVersion: d.VersionState.Version,
	}
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var hasPollers bool
	err := workflow.ExecuteActivity(activityCtx, d.a.CheckIfTaskQueuesHavePollers, checkPollersReq).Get(ctx, &hasPollers)
	if err != nil {
		return false, err
	}
	return hasPollers, nil
}

func (d *VersionWorkflowRunner) validateRegisterWorker(args *deploymentspb.RegisterWorkerInVersionArgs) error {
	if _, ok := d.VersionState.TaskQueueFamilies[args.TaskQueueName].GetTaskQueues()[int32(args.TaskQueueType)]; ok {
		return temporal.NewApplicationError("task queue already exists in deployment version", errNoChangeType)
	}
	if len(d.VersionState.TaskQueueFamilies) >= int(args.MaxTaskQueues) {
		return temporal.NewApplicationError(
			fmt.Sprintf("maximum number of task queues (%d) have been registered in deployment", args.MaxTaskQueues),
			errMaxTaskQueuesInVersionType,
		)
	}
	return nil
}

func (d *VersionWorkflowRunner) handleRegisterWorker(ctx workflow.Context, args *deploymentspb.RegisterWorkerInVersionArgs) error {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return err
	}
	defer func() {
		// although the handler might have not changed the state and had returned an error, still
		// it's better to CaN because some history events are built now.
		d.setStateChanged()
		d.lock.Unlock()
	}()

	// In case this version just got deleted, we wait until it finished propagating delete to all task queues before reviving it
	err = workflow.Await(ctx, func() bool {
		return d.asyncPropagationsInProgress == 0
	})
	if err != nil {
		return err
	}
	// In case it was marked as deleted we make it undeleted
	d.deleteVersion = false

	// Add the task queue to the local state.
	if d.VersionState.TaskQueueFamilies == nil {
		d.VersionState.TaskQueueFamilies = make(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData)
	}
	if d.VersionState.TaskQueueFamilies[args.TaskQueueName] == nil {
		d.VersionState.TaskQueueFamilies[args.TaskQueueName] = &deploymentspb.VersionLocalState_TaskQueueFamilyData{}
	}
	if d.VersionState.TaskQueueFamilies[args.TaskQueueName].TaskQueues == nil {
		d.VersionState.TaskQueueFamilies[args.TaskQueueName].TaskQueues = make(map[int32]*deploymentspb.TaskQueueVersionData)
	}
	d.VersionState.TaskQueueFamilies[args.TaskQueueName].TaskQueues[int32(args.TaskQueueType)] = &deploymentspb.TaskQueueVersionData{}

	// initial data
	var data *deploymentspb.DeploymentVersionData
	if args.GetRoutingConfig() == nil {
		// Sync mode
		data = &deploymentspb.DeploymentVersionData{
			Version:           d.VersionState.Version,
			RoutingUpdateTime: d.VersionState.RoutingUpdateTime,
			CurrentSinceTime:  d.VersionState.CurrentSinceTime,
			RampingSinceTime:  d.VersionState.RampingSinceTime,
			RampPercentage:    d.VersionState.RampPercentage,
			Status:            d.VersionState.Status,
		}
	}

	activityCtx := workflow.WithActivityOptions(ctx, propagationActivityOptions)

	// sync to user data
	var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse
	err = workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version:             d.VersionState.Version,
		UpdateRoutingConfig: args.GetRoutingConfig(),
		UpsertVersionData:   d.versionDataToSync(),
		Sync: []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			{
				Name:  args.TaskQueueName,
				Types: []enumspb.TaskQueueType{args.TaskQueueType},
				Data:  data,
			},
		},
	}).Get(ctx, &syncRes)
	if err != nil {
		return err
	}

	if len(syncRes.TaskQueueMaxVersions) > 0 {
		// wait for propagation
		err = workflow.ExecuteActivity(
			activityCtx,
			d.a.CheckWorkerDeploymentUserDataPropagation,
			&deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
				TaskQueueMaxVersions: syncRes.TaskQueueMaxVersions,
			}).Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *VersionWorkflowRunner) versionDataToSync() *deploymentspb.WorkerDeploymentVersionData {
	return &deploymentspb.WorkerDeploymentVersionData{Status: d.VersionState.Status}
}

// If routing update time has changed then we want to let the update through.
func (d *VersionWorkflowRunner) validateSyncState(args *deploymentspb.SyncVersionStateUpdateArgs) error {
	if d.deleteVersion {
		// Deployment workflow should not call this function if version is marked for deletion, but still checking for safety.
		return temporal.NewNonRetryableApplicationError(errVersionDeleted, errVersionDeleted, nil)
	}
	res := &deploymentspb.SyncVersionStateResponse{VersionState: d.VersionState}
	if args.GetRoutingUpdateTime().AsTime().Equal(d.GetVersionState().GetRoutingUpdateTime().AsTime()) {
		return temporal.NewApplicationError("no change", errNoChangeType, res)
	}
	return nil
}

//nolint:staticcheck // SA1019
func (d *VersionWorkflowRunner) handleSyncState(ctx workflow.Context, args *deploymentspb.SyncVersionStateUpdateArgs) (*deploymentspb.SyncVersionStateResponse, error) {
	// use lock to enforce only one update at a time
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return nil, serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}
	defer func() {
		// although the handler might have not changed the state and had returned an error, still
		// it's better to CaN because some history events are built now.
		d.setStateChanged()
		d.lock.Unlock()
	}()

	if err = d.validateSyncState(args); err != nil {
		return nil, err
	}

	// wait until deployment workflow started
	err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before worker deployment workflow started")
		return nil, serviceerror.NewDeadlineExceeded("Update canceled before worker deployment workflow started")
	}

	state := d.GetVersionState()
	var newStatus enumspb.WorkerDeploymentVersionStatus

	// Determine propagation mode based on routing config presence
	if rg := args.GetRoutingConfig(); rg != nil {
		// ASYNC MODE: propagate full routing config
		newStatus = d.findNewVersionStatusFromRoutingConfig(rg)
		err = d.syncTaskQueuesAsync(ctx, args.RoutingConfig, newStatus)
		if err != nil {
			return nil, err
		}
		d.updateStateFromRoutingConfig(newStatus, state, rg)
	} else {
		// SYNC MODE: propagate only version data (existing behavior)
		newStatus = d.findNewVersionStatus(args)
		versionData := &deploymentspb.DeploymentVersionData{
			Version:           d.VersionState.Version,
			RoutingUpdateTime: args.RoutingUpdateTime,
			CurrentSinceTime:  args.CurrentSinceTime,
			RampingSinceTime:  args.RampingSinceTime,
			RampPercentage:    args.RampPercentage,
			Status:            newStatus,
		}

		// sync version information to all the task queues
		err = d.syncVersionDataToTaskQueues(ctx, versionData)
		if err != nil {
			// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
			return nil, err
		}
		// apply changes to current and ramping
		state.Status = newStatus
		state.RoutingUpdateTime = args.RoutingUpdateTime
		state.CurrentSinceTime = args.CurrentSinceTime
		state.RampingSinceTime = args.RampingSinceTime
		state.RampPercentage = args.RampPercentage
	}

	// stopped accepting new workflows --> start drainage tracking
	if newStatus == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING {
		// Version deactivated from current/ramping
		state.LastDeactivationTime = args.RoutingUpdateTime
		d.startDrainage(ctx)
	}

	// started accepting new workflows
	if newStatus == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT || newStatus == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING {
		if state.FirstActivationTime == nil {
			// First time this version is activated to current/ramping
			state.FirstActivationTime = args.RoutingUpdateTime
		}

		// Clear drainage information, if present, when a version gets activated.
		// This handles the rollback scenario where a previously draining/drained version
		// is reactivated and should have its drainage information cleared.
		state.DrainageInfo = nil
		state.LastDeactivationTime = nil
	}

	return &deploymentspb.SyncVersionStateResponse{
		Summary: versionStateToSummary(state),
	}, nil
}

func (d *VersionWorkflowRunner) updateStateFromRoutingConfig(
	newStatus enumspb.WorkerDeploymentVersionStatus,
	state *deploymentspb.VersionLocalState,
	rg *deploymentpb.RoutingConfig,
) {
	// apply changes to state based on routing config
	if newStatus != enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_UNSPECIFIED {
		// In UNSPECIFIED case the routing info doesn't affect the status of this version. It just needs to propagate to
		// task queues (not sure if it happens in practice though.)
		state.Status = newStatus
		state.CurrentSinceTime = nil
		state.RampingSinceTime = nil
		state.RampPercentage = 0
		switch newStatus {
		case enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT:
			state.CurrentSinceTime = rg.GetCurrentVersionChangedTime()
			state.RoutingUpdateTime = rg.GetCurrentVersionChangedTime()
		case enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING:
			state.RampingSinceTime = rg.GetRampingVersionChangedTime()
			state.RampPercentage = rg.GetRampingVersionPercentage()
			// Percentage change time is updated even if only ramping version changes, we should use this for RoutingUpdateTime.
			state.RoutingUpdateTime = rg.GetRampingVersionPercentageChangedTime()
		case enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING:
			// Version just became draining. So we need to update RoutingUpdateTime which is the max of the following:
			state.RoutingUpdateTime = rg.GetCurrentVersionChangedTime()
			if rg.GetRampingVersionPercentageChangedTime().AsTime().After(state.RampingSinceTime.AsTime()) {
				state.RoutingUpdateTime = rg.GetRampingVersionPercentageChangedTime()
			}
		default: // Shouldn't happen
		}
	}
}

func (d *VersionWorkflowRunner) handleDescribeQuery() (*deploymentspb.QueryDescribeVersionResponse, error) {
	if d.deleteVersion {
		return nil, errors.New(errVersionDeleted)
	}
	return &deploymentspb.QueryDescribeVersionResponse{
		VersionState: d.VersionState,
	}, nil
}

func (d *VersionWorkflowRunner) newUUID(ctx workflow.Context) string {
	var val string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New()
	}).Get(&val)
	return val
}

// Sync version summary with the WorkerDeployment workflow.
func (d *VersionWorkflowRunner) syncSummary(ctx workflow.Context) {
	err := workflow.SignalExternalWorkflow(ctx,
		worker_versioning.GenerateDeploymentWorkflowID(d.VersionState.Version.DeploymentName),
		"",
		SyncVersionSummarySignal,
		versionStateToSummary(d.GetVersionState()),
	).Get(ctx, nil)
	if err != nil {
		d.logger.Error("could not sync version summary to deployment workflow", "error", err)
	}
}

func versionStateToSummary(s *deploymentspb.VersionLocalState) *deploymentspb.WorkerDeploymentVersionSummary {
	return &deploymentspb.WorkerDeploymentVersionSummary{
		Version:              worker_versioning.WorkerDeploymentVersionToStringV31(s.Version),
		CreateTime:           s.CreateTime,
		DrainageStatus:       s.DrainageInfo.GetStatus(), // deprecated.
		DrainageInfo:         s.DrainageInfo,
		RoutingUpdateTime:    s.RoutingUpdateTime,
		CurrentSinceTime:     s.CurrentSinceTime,
		RampingSinceTime:     s.RampingSinceTime,
		FirstActivationTime:  s.FirstActivationTime,
		LastDeactivationTime: s.LastDeactivationTime,
		Status:               s.Status,
	}
}

func (d *VersionWorkflowRunner) refreshDrainageInfo(ctx workflow.Context) {
	if d.VersionState.GetDrainageInfo().GetStatus() != enumspb.VERSION_DRAINAGE_STATUS_DRAINING {
		return // only refresh when status is draining
	}

	defer func() {
		// regardless of results mark state as dirty so we CaN in the first opportunity now that some
		// history events are made.
		d.setStateChanged()
	}()

	drainage := d.VersionState.GetDrainageInfo()
	var interval time.Duration
	var err error
	if drainage.LastCheckedTime.AsTime() == drainage.LastChangedTime.AsTime() {
		// this is the first update, so we wait according to the grace period config
		interval, err = getSafeDurationConfig(ctx, "getVisibilityGracePeriod", d.unsafeVisibilityGracePeriodGetter, defaultVisibilityGrace)
	} else {
		// this is a subsequent check, we wait according to the refresh interval
		interval, err = getSafeDurationConfig(ctx, "getDrainageRefreshInterval", d.unsafeRefreshIntervalGetter, defaultVisibilityRefresh)
	}
	if err != nil {
		d.logger.Error("could not calculate drainage refresh interval", tag.Error(err))
		return
	}
	timeSinceLastRefresh := workflow.Now(ctx).Sub(drainage.LastCheckedTime.AsTime())
	if interval > timeSinceLastRefresh {
		if err = workflow.Sleep(ctx, interval-timeSinceLastRefresh); err != nil {
			d.logger.Error("error while trying to sleep", tag.Error(err))
			return
		}
	}

	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var a *VersionActivities
	var newInfo *deploymentpb.VersionDrainageInfo
	err = workflow.ExecuteActivity(
		activityCtx,
		a.GetVersionDrainageStatus,
		d.VersionState.Version,
	).Get(ctx, &newInfo)
	if err != nil {
		d.logger.Error("could not get version drainage status", tag.Error(err))
		return
	}

	d.metrics.Counter(metrics.WorkerDeploymentVersionVisibilityQueryCount.Name()).Inc(1)

	if d.VersionState.DrainageInfo == nil {
		d.VersionState.DrainageInfo = &deploymentpb.VersionDrainageInfo{}
	}

	d.VersionState.DrainageInfo.LastCheckedTime = newInfo.LastCheckedTime
	if d.VersionState.GetDrainageInfo().GetStatus() != newInfo.Status {
		d.VersionState.DrainageInfo.Status = newInfo.Status
		d.VersionState.DrainageInfo.LastChangedTime = newInfo.LastCheckedTime

		// Update the status of the version according to the drainage status
		d.updateVersionStatusAfterDrainageStatusChange(ctx, newInfo.Status)
	}
	d.syncSummary(ctx)
}

func (d *VersionWorkflowRunner) setStateChanged() {
	d.stateChanged = true
}

func (d *VersionWorkflowRunner) findNewVersionStatusFromRoutingConfig(rg *deploymentpb.RoutingConfig) enumspb.WorkerDeploymentVersionStatus {
	state := d.GetVersionState()

	wasActive := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil
	isCurrent := rg.GetCurrentDeploymentVersion().GetBuildId() == state.GetVersion().GetBuildId() && rg.GetCurrentDeploymentVersion().GetDeploymentName() == state.GetVersion().GetDeploymentName()
	isRamping := rg.GetRampingDeploymentVersion().GetBuildId() == state.GetVersion().GetBuildId() && rg.GetRampingDeploymentVersion().GetDeploymentName() == state.GetVersion().GetDeploymentName()

	if wasActive && !isCurrent && !isRamping {
		return enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING
	} else if isCurrent {
		return enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT
	} else if isRamping {
		return enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING
	}
	// In this case the routing config change is not impacting the status of this version
	return state.GetStatus()
}

// findNewVersionStatus is a helper to find the new version status a version will have after it a successful sync to
// all its task queues.
func (d *VersionWorkflowRunner) findNewVersionStatus(args *deploymentspb.SyncVersionStateUpdateArgs) enumspb.WorkerDeploymentVersionStatus {
	state := d.GetVersionState()

	wasAcceptingNewWorkflows := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil
	isAcceptingNewWorkflows := args.CurrentSinceTime != nil || args.RampingSinceTime != nil

	if wasAcceptingNewWorkflows && !isAcceptingNewWorkflows {
		return enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING
	} else if args.CurrentSinceTime != nil {
		return enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT
	} else if args.RampingSinceTime != nil {
		return enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING
	}
	// should never happen
	return enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_UNSPECIFIED
}

func (d *VersionWorkflowRunner) updateVersionStatusAfterDrainageStatusChange(ctx workflow.Context, newStatus enumspb.VersionDrainageStatus) {
	if newStatus == enumspb.VERSION_DRAINAGE_STATUS_DRAINED {
		d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED
	} else if newStatus == enumspb.VERSION_DRAINAGE_STATUS_DRAINING {
		d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING
	} else {
		// This should only happen if we encounter an error while checking the drainage status of the version
		d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_UNSPECIFIED
	}

	var err error
	if d.hasMinVersion(ctx, AsyncSetCurrentAndRamping) {
		err = d.syncTaskQueuesAsync(ctx, nil, d.VersionState.Status)
	} else {
		err = d.syncVersionStatusAfterDrainageStatusChange(ctx)
	}
	if err != nil {
		d.logger.Error("failed to sync version status after drainage status change", "error", err)
	}
}

// syncVersionStatusAfterDrainageStatusChange syncs the current version status to all task queues.
// This function acquires the workflow lock the same way as the update handlers do so that
// there are no race conditions during task-queue sync. It is called internally when the status
// of the version becomes draining or drained.
func (d *VersionWorkflowRunner) syncVersionStatusAfterDrainageStatusChange(ctx workflow.Context) error {
	err := d.lock.Lock(ctx)
	if err != nil {
		d.logger.Error("Could not acquire workflow lock")
		return serviceerror.NewDeadlineExceeded("Could not acquire workflow lock")
	}

	// Processing the drainage status change.
	d.asyncPropagationsInProgress++
	defer func() {
		d.asyncPropagationsInProgress--
		d.lock.Unlock()
	}()

	// TODO: update this to new format once the old format is deleted.
	// Build version data with current state
	versionData := &deploymentspb.DeploymentVersionData{
		Version:           d.VersionState.Version,
		RoutingUpdateTime: d.VersionState.RoutingUpdateTime,
		CurrentSinceTime:  d.VersionState.CurrentSinceTime,
		RampingSinceTime:  d.VersionState.RampingSinceTime,
		RampPercentage:    d.VersionState.RampPercentage,
		Status:            d.VersionState.Status,
	}

	return d.syncVersionDataToTaskQueues(ctx, versionData)
}

// syncVersionDataToTaskQueues is a helper that syncs the provided version data to all task queues.
// This function does NOT acquire the workflow lock - the caller is responsible for that.
func (d *VersionWorkflowRunner) syncVersionDataToTaskQueues(ctx workflow.Context, versionData *deploymentspb.DeploymentVersionData) error {
	state := d.GetVersionState()

	// sync to task queues
	syncReq := &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version: state.GetVersion(),
	}

	// send in the task-queue families in batches of syncBatchSize
	batches := make([][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)
	for _, tqName := range workflow.DeterministicKeys(state.TaskQueueFamilies) {
		byType := state.TaskQueueFamilies[tqName]
		var types []enumspb.TaskQueueType
		for _, tqType := range workflow.DeterministicKeys(byType.TaskQueues) {
			types = append(types, enumspb.TaskQueueType(tqType))
		}

		syncReq.Sync = append(syncReq.Sync, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			Name:  tqName,
			Types: types,
			Data:  versionData,
		})

		if len(syncReq.Sync) == int(d.VersionState.SyncBatchSize) {
			batches = append(batches, syncReq.Sync)
			syncReq.Sync = make([]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0) // reset the syncReq.Sync slice for the next batch
		}
	}
	if len(syncReq.Sync) > 0 {
		batches = append(batches, syncReq.Sync)
	}

	// calling SyncDeploymentVersionUserData for each batch
	for _, batch := range batches {
		activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
		var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse

		err := workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
			Version: state.GetVersion(),
			Sync:    batch,
		}).Get(ctx, &syncRes)
		if err != nil {
			return err
		}
		if len(syncRes.TaskQueueMaxVersions) > 0 {
			// wait for propagation
			err = workflow.ExecuteActivity(
				activityCtx,
				d.a.CheckWorkerDeploymentUserDataPropagation,
				&deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
					TaskQueueMaxVersions: syncRes.TaskQueueMaxVersions,
				}).Get(ctx, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// syncTaskQueuesAsync performs async propagation of routing config
func (d *VersionWorkflowRunner) syncTaskQueuesAsync(
	ctx workflow.Context,
	routingConfig *deploymentpb.RoutingConfig,
	newStatus enumspb.WorkerDeploymentVersionStatus,
) error {
	state := d.GetVersionState()

	// Build WorkerDeploymentVersionData for this version from current state
	versionData := &deploymentspb.WorkerDeploymentVersionData{
		Status: newStatus,
	}

	// Build sync request batches with routing config (async mode)
	batches := make([][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)
	var currentBatch []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData

	for _, tqName := range workflow.DeterministicKeys(state.TaskQueueFamilies) {
		byType := state.TaskQueueFamilies[tqName]
		var types []enumspb.TaskQueueType
		for _, tqType := range workflow.DeterministicKeys(byType.TaskQueues) {
			types = append(types, enumspb.TaskQueueType(tqType))
		}

		currentBatch = append(currentBatch, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			Name:  tqName,
			Types: types,
		})

		if len(currentBatch) == int(d.VersionState.SyncBatchSize) {
			batches = append(batches, currentBatch)
			currentBatch = make([]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)
		}
	}
	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	// Increment counter to prevent CaN while propagation is in progress
	d.asyncPropagationsInProgress++

	// Start async propagation - DON'T WAIT
	workflow.Go(ctx, func(gCtx workflow.Context) {
		d.executeAndTrackAsyncPropagation(gCtx, batches, routingConfig, versionData)
		// Decrement counter when propagation completes
		d.asyncPropagationsInProgress--
	})

	return nil
}

// executeAndTrackAsyncPropagation monitors propagation and signals completion
func (d *VersionWorkflowRunner) executeAndTrackAsyncPropagation(
	ctx workflow.Context,
	batches [][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData,
	routingConfig *deploymentpb.RoutingConfig,
	versionData *deploymentspb.WorkerDeploymentVersionData,
) {
	taskQueueMaxVersionsToCheck := make(map[string]int64)

	for _, batch := range batches {
		if d.cancelPropagations {
			// Version is deleting. no need to continue propagation. Also can skip sending signal to deployment workflow.
			return
		}
		result := d.executePropagationBatch(ctx, batch, routingConfig, versionData)
		// Merge results into taskQueueMaxVersionsToCheck
		for _, tqName := range workflow.DeterministicKeys(result) {
			taskQueueMaxVersionsToCheck[tqName] = result[tqName]
		}
	}
	if d.cancelPropagations {
		// Version is deleting. no need to continue propagation. Also can skip sending signal to deployment workflow.
		return
	}

	// Wait for propagation to complete only for task queues where config changed
	if len(taskQueueMaxVersionsToCheck) > 0 {
		activityCtx := workflow.WithActivityOptions(ctx, propagationActivityOptions)
		err := workflow.ExecuteActivity(
			activityCtx,
			d.a.CheckWorkerDeploymentUserDataPropagation,
			&deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
				TaskQueueMaxVersions: taskQueueMaxVersionsToCheck,
			}).Get(ctx, nil)

		if err != nil {
			d.logger.Error("async propagation check failed", "error", err)
			return
		}
	}

	if routingConfig != nil {
		d.syncSummary(ctx)
		// Signal deployment workflow that routing config propagation completed
		d.signalPropagationComplete(ctx, routingConfig.GetRevisionNumber())
	}
}

// executePropagationBatch executes a single batch of propagation and returns task queue max versions to check
func (d *VersionWorkflowRunner) executePropagationBatch(
	ctx workflow.Context,
	batch []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData,
	routingConfig *deploymentpb.RoutingConfig,
	versionData *deploymentspb.WorkerDeploymentVersionData,
) map[string]int64 {
	state := d.GetVersionState()
	activityCtx := workflow.WithActivityOptions(ctx, propagationActivityOptions)
	var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse

	err := workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version:             state.GetVersion(),
		UpdateRoutingConfig: routingConfig,
		UpsertVersionData:   versionData,
		Sync:                batch,
	}).Get(ctx, &syncRes)

	if err != nil {
		d.logger.Error("async propagation batch failed", "error", err)
		// Return empty map on error
		return make(map[string]int64)
	}

	return syncRes.TaskQueueMaxVersions
}

// signalPropagationComplete sends a signal to the deployment workflow when async propagation completes
func (d *VersionWorkflowRunner) signalPropagationComplete(ctx workflow.Context, revisionNumber int64) {
	err := workflow.SignalExternalWorkflow(
		ctx,
		worker_versioning.GenerateDeploymentWorkflowID(d.VersionState.Version.DeploymentName),
		"",
		PropagationCompleteSignal,
		&deploymentspb.PropagationCompletionInfo{
			RevisionNumber: revisionNumber,
			BuildId:        d.VersionState.GetVersion().GetBuildId(),
		},
	).Get(ctx, nil)

	if err != nil {
		d.logger.Error("could not signal propagation completion", "error", err)
	}
}

func (d *VersionWorkflowRunner) hasMinVersion(ctx workflow.Context, version DeploymentWorkflowVersion) bool {
	return getWorkflowVersion(ctx, d.unsafeWorkflowVersionGetter) >= version
}
