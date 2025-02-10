// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2024 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// VersionWorkflowRunner holds the local state for a deployment workflow
	VersionWorkflowRunner struct {
		*deploymentspb.WorkerDeploymentVersionWorkflowArgs
		a                      *VersionActivities
		logger                 sdklog.Logger
		metrics                sdkclient.MetricsHandler
		lock                   workflow.Mutex
		pendingUpdates         int
		signalsCompleted       bool
		drainageWorkflowFuture *workflow.ChildWorkflowFuture
		done                   bool
	}
)

var (
	defaultActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 100 * time.Millisecond,
			MaximumInterval: 60 * time.Second,
		},
	}
)

func VersionWorkflow(ctx workflow.Context, versionWorkflowArgs *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
	versionWorkflowRunner := &VersionWorkflowRunner{
		WorkerDeploymentVersionWorkflowArgs: versionWorkflowArgs,

		a:       nil,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", versionWorkflowArgs.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": versionWorkflowArgs.NamespaceName}),
		lock:    workflow.NewMutex(ctx),
	}

	return versionWorkflowRunner.run(ctx)
}

func (d *VersionWorkflowRunner) listenToSignals(ctx workflow.Context) {
	// Fetch signal channels
	forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
	forceCAN := false
	drainageStatusSignalChannel := workflow.GetSignalChannel(ctx, SyncDrainageSignalName)

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(forceCANSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal
		c.Receive(ctx, nil)
		forceCAN = true
	})
	selector.AddReceive(drainageStatusSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		var newInfo *deploymentpb.VersionDrainageInfo
		c.Receive(ctx, &newInfo)
		if d.VersionState.GetDrainageInfo() == nil {
			d.VersionState.DrainageInfo = &deploymentpb.VersionDrainageInfo{}
		}
		d.VersionState.DrainageInfo.LastCheckedTime = newInfo.LastCheckedTime
		if d.VersionState.GetDrainageInfo().GetStatus() != newInfo.Status {
			d.VersionState.DrainageInfo.Status = newInfo.Status
			d.VersionState.DrainageInfo.LastChangedTime = newInfo.LastCheckedTime
			d.syncSummary(ctx)
		}
	})

	for (!workflow.GetInfo(ctx).GetContinueAsNewSuggested() && !forceCAN) || selector.HasPending() {
		selector.Select(ctx)
	}

	// Done processing signals before CAN
	d.signalsCompleted = true
}

func (d *VersionWorkflowRunner) run(ctx workflow.Context) error {
	if d.GetVersionState().Version == nil {
		return fmt.Errorf("version cannot be nil on start")
	}
	if d.VersionState.GetCreateTime() == nil {
		d.VersionState.CreateTime = timestamppb.New(workflow.Now(ctx))
	}

	// if we were draining and just continued-as-new, restart drainage child wf
	if d.VersionState.GetDrainageInfo().GetStatus() == enumspb.VERSION_DRAINAGE_STATUS_DRAINING {
		d.startDrainage(ctx, true)
	}

	// Set up Query Handlers here:
	if err := workflow.SetQueryHandler(ctx, QueryDescribeVersion, d.handleDescribeQuery); err != nil {
		d.logger.Error("Failed while setting up query handler")
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		RegisterWorkerInDeployment,
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

	// Wait on any pending signals and updates.
	err := workflow.Await(ctx, func() bool { return (d.signalsCompleted && d.pendingUpdates == 0) || d.done })
	if err != nil {
		return err
	}

	// Before continue-as-new or done, stop drainage wf if it exists.
	if d.drainageWorkflowFuture != nil {
		d.logger.Debug("Version terminating drainage workflow before continue-as-new")
		_ = d.stopDrainage(ctx) // child options say terminate-on-close, so if this fails the wf will terminate instead.
	}

	if d.done {
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

	for key, payload := range args.UpsertEntries {
		d.VersionState.Metadata.Entries[key] = payload
	}

	for _, key := range args.RemoveEntries {
		delete(d.VersionState.Metadata.Entries, key)
	}

	return &deploymentspb.UpdateVersionMetadataResponse{
		Metadata: d.VersionState.Metadata,
	}, nil
}

func (d *VersionWorkflowRunner) startDrainage(ctx workflow.Context, isCan bool) {
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	})
	fut := workflow.ExecuteChildWorkflow(childCtx, WorkerDeploymentDrainageWorkflowType, &deploymentspb.DrainageWorkflowArgs{
		Version: d.VersionState.Version,
		IsCan:   isCan,
	})
	d.drainageWorkflowFuture = &fut
}

func (d *VersionWorkflowRunner) stopDrainage(ctx workflow.Context) error {
	var terminated bool
	if d.drainageWorkflowFuture == nil {
		return nil
	}
	fut := *d.drainageWorkflowFuture
	err := fut.SignalChildWorkflow(ctx, TerminateDrainageSignal, nil).Get(ctx, &terminated)
	if err != nil {
		return err
	}
	d.drainageWorkflowFuture = nil
	return nil
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
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
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
	// 1. The version is not current or ramping
	// 2. The version is drained. (check skipped when `skip-drainage=true` )
	// 3. The version has no active pollers.

	// 1. Check if the version is not current or ramping.
	if state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil {
		// activity won't retry on this error since version not eligible for deletion
		return serviceerror.NewFailedPrecondition(errVersionIsCurrentOrRamping)
	}

	// 2. Check if the version is drained.
	if !args.SkipDrainage {
		if state.GetDrainageInfo() == nil || state.GetDrainageInfo().Status != enumspb.VERSION_DRAINAGE_STATUS_DRAINED {
			// activity won't retry on this error since version not eligible for deletion
			return serviceerror.NewFailedPrecondition(errVersionNotDrained)
		}
	}

	// 3. Check if the version has any active pollers.
	hasPollers, err := d.doesVersionHaveActivePollers(ctx)
	if hasPollers {
		// activity won't retry on this error since version not eligible for deletion
		return serviceerror.NewFailedPrecondition(errVersionHasPollers)
	}
	if err != nil {
		// some other error allowing activity retries
		return err
	}

	d.logger.Info("Version is eligible for deletion")

	// sync version removal to task queues
	syncReq := &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version:       state.GetVersion(),
		ForgetVersion: true,
	}

	for tqName, byType := range state.TaskQueueFamilies {
		var types []enumspb.TaskQueueType
		for tqType := range byType.TaskQueues {
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

	d.done = true
	return nil
}

// doesVersionHaveActivePollers returns true if the version has active pollers.
func (d *VersionWorkflowRunner) doesVersionHaveActivePollers(ctx workflow.Context) (bool, error) {

	// describe all task queues in the deployment, if any have pollers, then cannot delete

	tqNameToTypes := make(map[string]*deploymentspb.CheckTaskQueuesHavePollersActivityArgs_TaskQueueTypes)
	for tqName, tqFamilyData := range d.VersionState.TaskQueueFamilies {
		var tqTypes []enumspb.TaskQueueType
		for tqType := range tqFamilyData.TaskQueues {
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
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
		d.lock.Unlock()
	}()

	// wait until deployment workflow started
	err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before deployment workflow started")
		// TODO (Carly): This is likely due to too many deployments, but make sure we excluded other possible errors here and send a proper error message all the time.
		// TODO (Carly): mention the limit in here or make sure matching does in the error returned to the poller
		return temporal.NewApplicationError("failed to create deployment version, likely you are exceeding the limit of allowed deployments in a namespace", errTooManyDeployments)
	}

	// Add the task queue to the local state first. This is the safest because in case the rest of
	// registration flow takes some time, DescribeVersion will return this TQ and other places such
	// as SyncUnversionedRamp will have the most up-to-date version list sooner. Note that
	// registration, once started, has to complete, all activities are indefinitely retried.
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

	// First try to add version to worker-deployment workflow so it rejects in case we hit the limit
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
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
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
		d.lock.Unlock()
	}()

	// wait until deployment workflow started
	err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before worker deployment workflow started")
		return nil, serviceerror.NewDeadlineExceeded("Update canceled before worker deployment workflow started")
	}

	state := d.GetVersionState()

	// TODO (Shivam): __unversioned__

	// sync to task queues
	syncReq := &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version: state.GetVersion(),
	}
	for tqName, byType := range state.TaskQueueFamilies {
		data := &deploymentspb.DeploymentVersionData{
			Version:           d.VersionState.Version,
			RoutingUpdateTime: args.RoutingUpdateTime,
			CurrentSinceTime:  args.CurrentSinceTime,
			RampingSinceTime:  args.RampingSinceTime,
			RampPercentage:    args.RampPercentage,
		}
		var types []enumspb.TaskQueueType
		for tqType := range byType.TaskQueues {
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

	wasAcceptingNewWorkflows := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil

	// apply changes to current and ramping
	state.RoutingUpdateTime = args.RoutingUpdateTime
	state.CurrentSinceTime = args.CurrentSinceTime
	state.RampingSinceTime = args.RampingSinceTime
	state.RampPercentage = args.RampPercentage

	isAcceptingNewWorkflows := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil

	// stopped accepting new workflows --> start drainage child wf
	if wasAcceptingNewWorkflows && !isAcceptingNewWorkflows {
		d.startDrainage(ctx, false)
	}

	// started accepting new workflows --> stop drainage child wf if it exists
	if !wasAcceptingNewWorkflows && isAcceptingNewWorkflows && d.drainageWorkflowFuture != nil {
		err = d.stopDrainage(ctx)
		if err != nil {
			// TODO: compensate
			return nil, err
		}
		d.VersionState.DrainageInfo = nil
	}

	return &deploymentspb.SyncVersionStateResponse{
		VersionState: state,
	}, nil
}

func (d *VersionWorkflowRunner) dataWithTime(data *deploymentspb.DeploymentVersionData, routingUpdateTime *timestamppb.Timestamp) *deploymentspb.DeploymentVersionData {
	data = common.CloneProto(data)
	data.RoutingUpdateTime = routingUpdateTime

	return data
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
			Version:        worker_versioning.WorkerDeploymentVersionToString(d.VersionState.Version),
			CreateTime:     d.VersionState.CreateTime,
			DrainageStatus: d.VersionState.DrainageInfo.GetStatus(),
		},
	).Get(ctx, nil)
	d.logger.Error("could not sync version summary to deployment workflow", "error", err)
}
