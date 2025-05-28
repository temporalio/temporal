package workerdeployment

import (
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
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
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
		stateChanged  bool
		signalHandler *SignalHandler
		forceCAN      bool
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

		// If the version is now drained, the drainage workflow has completed execution as well.
		// Update the future to be nil to indicate that the drainage workflow is no longer running.
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

	if err := workflow.SetUpdateHandler(
		ctx,
		UpdateVersionMetadata,
		d.handleUpdateVersionMetadata,
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
		return d.deleteVersion || // version is deleted -> it's ok to drop all signals and updates.
			// There is no pending signal or update, but the state is dirty or forceCaN is requested:
			(!d.signalHandler.signalSelector.HasPending() && d.signalHandler.processingSignals == 0 && workflow.AllHandlersFinished(ctx) &&
				(d.forceCAN || d.stateChanged))
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

func (d *VersionWorkflowRunner) handleUpdateVersionMetadata(ctx workflow.Context, args *deploymentspb.UpdateVersionMetadataArgs) (*deploymentspb.UpdateVersionMetadataResponse, error) {
	if d.VersionState.Metadata == nil && args.UpsertEntries != nil {
		d.VersionState.Metadata = &deploymentpb.VersionMetadata{}
		d.VersionState.Metadata.Entries = make(map[string]*commonpb.Payload)
	}

	for _, key := range workflow.DeterministicKeys(args.UpsertEntries) {
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

func (d *VersionWorkflowRunner) startDrainage(ctx workflow.Context, isCan bool) {
	if d.VersionState.GetDrainageInfo().GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED {
		now := timestamppb.New(workflow.Now(ctx))
		d.VersionState.DrainageInfo = &deploymentpb.VersionDrainageInfo{
			Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
			LastChangedTime: now,
			LastCheckedTime: now,
		}
		d.syncSummary(ctx)
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

	state := d.GetVersionState()
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	// Manual deletion of versions is only possible when:
	// 1. The version is not current or ramping (checked in the deployment wf)
	// 2. The version is not draining. (check skipped when `skip-drainage=true` )
	// 3. The version has no active pollers.

	// 2. Check if the version is draining.
	if !args.SkipDrainage {
		if state.GetDrainageInfo().GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_DRAINING {
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
	err = workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, syncReq).Get(ctx, &syncRes)
	if err != nil {
		// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
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
			// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
			return err
		}
	}

	d.deleteVersion = true
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

	v := workflow.GetVersion(ctx, "RefactorRegisterWorker", workflow.DefaultVersion, 1)
	if v == workflow.DefaultVersion {
		// wait until deployment workflow started
		err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
		if err != nil {
			d.logger.Error("Update canceled before deployment workflow started")
			// TODO (Carly): This is likely due to too many deployments, but make sure we excluded other possible errors here and send a proper error message all the time.
			// TODO (Carly): mention the limit in here or make sure matching does in the error returned to the poller
			return temporal.NewApplicationError("failed to create deployment version, likely you are exceeding the limit of allowed deployments in a namespace", errTooManyDeployments)
		}
	}

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
	data := &deploymentspb.DeploymentVersionData{
		Version:           d.VersionState.Version,
		RoutingUpdateTime: d.VersionState.RoutingUpdateTime,
		CurrentSinceTime:  d.VersionState.CurrentSinceTime,
		RampingSinceTime:  d.VersionState.RampingSinceTime,
		RampPercentage:    d.VersionState.RampPercentage,
	}

	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	if v == workflow.DefaultVersion {
		// First try to add version to worker-deployment workflow so it rejects in case we hit the limit
		err = workflow.ExecuteActivity(activityCtx, d.a.AddVersionToWorkerDeployment, &deploymentspb.AddVersionToWorkerDeploymentRequest{
			DeploymentName: d.VersionState.Version.GetDeploymentName(),
			UpdateArgs: &deploymentspb.AddVersionUpdateArgs{
				Version:    worker_versioning.WorkerDeploymentVersionToString(d.VersionState.Version),
				CreateTime: d.VersionState.CreateTime,
			},
			RequestId: d.newUUID(ctx),
		}).Get(ctx, nil)
		if err != nil {
			// TODO (carly): make sure the error message that goes to the user is informative and has the limit mentioned
			return temporal.NewApplicationError("too many versions in this deployment", errTooManyVersions)
		}
	}

	// sync to user data
	var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse
	err = workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version: d.VersionState.Version,
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

// If routing update time has changed then we want to let the update through.
func (d *VersionWorkflowRunner) validateSyncState(args *deploymentspb.SyncVersionStateUpdateArgs) error {
	res := &deploymentspb.SyncVersionStateResponse{VersionState: d.VersionState}
	if args.GetRoutingUpdateTime().AsTime().Equal(d.GetVersionState().GetRoutingUpdateTime().AsTime()) {
		return temporal.NewApplicationError("no change", errNoChangeType, res)
	}
	return nil
}

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

	// wait until deployment workflow started
	err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before worker deployment workflow started")
		return nil, serviceerror.NewDeadlineExceeded("Update canceled before worker deployment workflow started")
	}

	state := d.GetVersionState()

	// sync to task queues
	syncReq := &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version: state.GetVersion(),
	}

	v := workflow.GetVersion(ctx, "SyncStateWithBatching", workflow.DefaultVersion, 1)
	if v == workflow.DefaultVersion {
		for _, tqName := range workflow.DeterministicKeys(state.TaskQueueFamilies) {
			byType := state.TaskQueueFamilies[tqName]
			data := &deploymentspb.DeploymentVersionData{
				Version:           d.VersionState.Version,
				RoutingUpdateTime: args.RoutingUpdateTime,
				CurrentSinceTime:  args.CurrentSinceTime,
				RampingSinceTime:  args.RampingSinceTime,
				RampPercentage:    args.RampPercentage,
			}
			var types []enumspb.TaskQueueType
			for _, tqType := range workflow.DeterministicKeys(byType.TaskQueues) {
				types = append(types, enumspb.TaskQueueType(tqType))
			}

			syncReq.Sync = append(syncReq.Sync, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
				Name:  tqName,
				Types: types,
				Data:  data,
			})
		}
		activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
		var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse
		err = workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, syncReq).Get(ctx, &syncRes)
		if err != nil {
			// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
			return nil, err
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
				// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
				return nil, err
			}
		}
	} else {

		// send in the task-queue families in batches of syncBatchSize
		batches := make([][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData, 0)

		for _, tqName := range workflow.DeterministicKeys(state.TaskQueueFamilies) {
			byType := state.TaskQueueFamilies[tqName]
			data := &deploymentspb.DeploymentVersionData{
				Version:           d.VersionState.Version,
				RoutingUpdateTime: args.RoutingUpdateTime,
				CurrentSinceTime:  args.CurrentSinceTime,
				RampingSinceTime:  args.RampingSinceTime,
				RampPercentage:    args.RampPercentage,
			}
			var types []enumspb.TaskQueueType
			for _, tqType := range workflow.DeterministicKeys(byType.TaskQueues) {
				types = append(types, enumspb.TaskQueueType(tqType))
			}

			syncReq.Sync = append(syncReq.Sync, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
				Name:  tqName,
				Types: types,
				Data:  data,
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

			err = workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
				Version: state.GetVersion(),
				Sync:    batch,
			}).Get(ctx, &syncRes)
			if err != nil {
				// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
				return nil, err
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
					// TODO (Shivam): Compensation functions required to roll back the local state + activity changes.
					return nil, err
				}
			}
		}
	}

	wasAcceptingNewWorkflows := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil

	// apply changes to current and ramping
	state.RoutingUpdateTime = args.RoutingUpdateTime
	state.CurrentSinceTime = args.CurrentSinceTime
	state.RampingSinceTime = args.RampingSinceTime
	state.RampPercentage = args.RampPercentage

	isAcceptingNewWorkflows := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil

	// stopped accepting new workflows --> start drainage tracking
	if wasAcceptingNewWorkflows && !isAcceptingNewWorkflows {
		// Version deactivated from current/ramping
		d.VersionState.LastDeactivationTime = args.RoutingUpdateTime
		d.startDrainage(ctx, false)
		state.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING
	}

	// started accepting new workflows --> stop drainage child wf if it exists
	if !wasAcceptingNewWorkflows && isAcceptingNewWorkflows {
		if d.VersionState.FirstActivationTime == nil {
			// First time this version is activated to current/ramping
			d.VersionState.FirstActivationTime = args.RoutingUpdateTime
		}
	}

	// Set the appropriate status for the version if it is current/ramping.
	if state.CurrentSinceTime != nil {
		state.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT
	} else if state.RampingSinceTime != nil {
		state.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING
	}

	return &deploymentspb.SyncVersionStateResponse{
		VersionState: state,
	}, nil
}

func (d *VersionWorkflowRunner) handleDescribeQuery() (*deploymentspb.QueryDescribeVersionResponse, error) {
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
		&deploymentspb.WorkerDeploymentVersionSummary{
			Version:              worker_versioning.WorkerDeploymentVersionToString(d.VersionState.Version),
			CreateTime:           d.VersionState.CreateTime,
			DrainageStatus:       d.VersionState.DrainageInfo.GetStatus(), // deprecated.
			DrainageInfo:         d.VersionState.DrainageInfo,
			RoutingUpdateTime:    d.VersionState.RoutingUpdateTime,
			CurrentSinceTime:     d.VersionState.CurrentSinceTime,
			RampingSinceTime:     d.VersionState.RampingSinceTime,
			FirstActivationTime:  d.VersionState.FirstActivationTime,
			LastDeactivationTime: d.VersionState.LastDeactivationTime,
			Status:               d.VersionState.Status,
		},
	).Get(ctx, nil)
	if err != nil {
		d.logger.Error("could not sync version summary to deployment workflow", "error", err)
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

	if d.VersionState.DrainageInfo == nil {
		d.VersionState.DrainageInfo = &deploymentpb.VersionDrainageInfo{}
	}

	d.VersionState.DrainageInfo.LastCheckedTime = newInfo.LastCheckedTime
	if d.VersionState.GetDrainageInfo().GetStatus() != newInfo.Status {
		d.VersionState.DrainageInfo.Status = newInfo.Status
		d.VersionState.DrainageInfo.LastChangedTime = newInfo.LastCheckedTime

		// Update the status of the version according to the drainage status
		d.updateVersionStatusAfterDrainageStatusChange(newInfo.Status)
	}
	d.syncSummary(ctx)
}

func (d *VersionWorkflowRunner) setStateChanged() {
	d.stateChanged = true
}

func (d *VersionWorkflowRunner) updateVersionStatusAfterDrainageStatusChange(newStatus enumspb.VersionDrainageStatus) {
	if newStatus == enumspb.VERSION_DRAINAGE_STATUS_DRAINED {
		d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED
	} else if newStatus == enumspb.VERSION_DRAINAGE_STATUS_DRAINING {
		d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING
	} else {
		// This should only happen if we encounter an error while checking the drainage status of the version
		d.VersionState.Status = enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_UNSPECIFIED
	}
}
