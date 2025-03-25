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
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// The actual limit is set in dynamic configs, this is only used in case we cannot read the DC.
	defaultMaxVersions = 100
)

type (
	// WorkflowRunner holds the local state while running a deployment-series workflow
	WorkflowRunner struct {
		*deploymentspb.WorkerDeploymentWorkflowArgs
		a                *Activities
		logger           sdklog.Logger
		metrics          sdkclient.MetricsHandler
		lock             workflow.Mutex
		pendingUpdates   int
		conflictToken    []byte
		done             bool
		signalsCompleted bool
		unsafeMaxVersion func() int
	}
)

func Workflow(ctx workflow.Context, unsafeMaxVersion func() int, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
	workflowRunner := &WorkflowRunner{
		WorkerDeploymentWorkflowArgs: args,

		a:                nil,
		logger:           sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.NamespaceName),
		metrics:          workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.NamespaceName}),
		lock:             workflow.NewMutex(ctx),
		unsafeMaxVersion: unsafeMaxVersion,
	}

	return workflowRunner.run(ctx)
}

func (d *WorkflowRunner) listenToSignals(ctx workflow.Context) {
	forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
	syncVersionSummaryChannel := workflow.GetSignalChannel(ctx, SyncVersionSummarySignal)
	forceCAN := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(forceCANSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, nil)
		forceCAN = true
	})
	selector.AddReceive(syncVersionSummaryChannel, func(c workflow.ReceiveChannel, more bool) {
		var summary *deploymentspb.WorkerDeploymentVersionSummary
		c.Receive(ctx, &summary)
		if _, ok := d.State.Versions[summary.GetVersion()]; !ok {
			d.logger.Error("received summary for a non-existing version, ignoring it", "version", summary.GetVersion())
		}
		d.State.Versions[summary.GetVersion()] = summary
	})

	for (!workflow.GetInfo(ctx).GetContinueAsNewSuggested() && !forceCAN) || selector.HasPending() {
		selector.Select(ctx)
	}

	// Done processing signals before CAN
	d.signalsCompleted = true
}

func (d *WorkflowRunner) run(ctx workflow.Context) error {
	if d.State == nil {
		d.State = &deploymentspb.WorkerDeploymentLocalState{}
		d.State.CreateTime = timestamppb.New(workflow.Now(ctx))
		d.State.RoutingConfig = &deploymentpb.RoutingConfig{CurrentVersion: worker_versioning.UnversionedVersionId}
		d.State.ConflictToken, _ = workflow.Now(ctx).MarshalBinary()

		// updating the memo since the RoutingConfig is updated
		if err := d.updateMemo(ctx); err != nil {
			return err
		}
	}
	if d.State.Versions == nil {
		d.State.Versions = make(map[string]*deploymentspb.WorkerDeploymentVersionSummary)
	}

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

	// Wait until we can continue as new or are cancelled.
	err = workflow.Await(ctx, func() bool {
		return (d.signalsCompleted && d.pendingUpdates == 0) || d.done
	})
	if err != nil {
		return err
	}

	if d.done {
		return nil
	}

	// Continue as new when there are no pending updates and history size is greater than requestsBeforeContinueAsNew.
	// Note, if update requests come in faster than they
	// are handled, there will not be a moment where the workflow has
	// nothing pending which means this will run forever.
	return workflow.NewContinueAsNewError(ctx, WorkerDeploymentWorkflowType, d.WorkerDeploymentWorkflowArgs)
}

func (d *WorkflowRunner) addVersionToWorkerDeployment(ctx workflow.Context, args *deploymentspb.AddVersionUpdateArgs) error {
	if d.State.Versions == nil {
		return nil
	}

	for _, v := range d.State.Versions {
		if v.Version == args.Version {
			return nil
		}
	}

	maxVersions := d.getMaxVersions(ctx)

	if len(d.State.Versions) >= maxVersions {
		err := d.tryDeleteVersion(ctx)
		if err != nil {
			return temporal.NewApplicationError(fmt.Sprintf("cannot add version, already at max versions %d", maxVersions), errTooManyVersions)
		}
	}

	d.State.Versions[args.Version] = &deploymentspb.WorkerDeploymentVersionSummary{
		Version:    args.Version,
		CreateTime: args.CreateTime,
	}
	return nil
}

func (d *WorkflowRunner) handleRegisterWorker(ctx workflow.Context, args *deploymentspb.RegisterWorkerInWorkerDeploymentArgs) error {
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

	// Add version to local state of the workflow, if not already present.
	err = d.addVersionToWorkerDeployment(ctx, &deploymentspb.AddVersionUpdateArgs{
		Version:    worker_versioning.WorkerDeploymentVersionToString(args.Version),
		CreateTime: timestamppb.New(workflow.Now(ctx)),
	})
	if err != nil {
		return err
	}

	// Register task-queue worker in version workflow.
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	err = workflow.ExecuteActivity(activityCtx, d.a.RegisterWorkerInVersion, &deploymentspb.RegisterWorkerInVersionArgs{
		TaskQueueName: args.TaskQueueName,
		TaskQueueType: args.TaskQueueType,
		MaxTaskQueues: args.MaxTaskQueues,
		Version:       worker_versioning.WorkerDeploymentVersionToString(args.Version),
	}).Get(ctx, nil)
	if err != nil {
		var appError *temporal.ApplicationError
		if errors.As(err, &appError) {
			if appError.Type() == errMaxTaskQueuesInVersionType {
				return temporal.NewApplicationError(
					fmt.Sprintf("maximum number of task queues (%d) have been registered in deployment", args.MaxTaskQueues),
					errMaxTaskQueuesInVersionType,
				)
			}
		}
		return err
	}

	return nil
}

func (d *WorkflowRunner) validateDeleteDeployment() error {
	if len(d.State.Versions) > 0 {
		return serviceerror.NewFailedPrecondition("deployment has versions, can't be deleted")
	}
	return nil
}

func (d *WorkflowRunner) handleDeleteDeployment(ctx workflow.Context) error {
	if len(d.State.Versions) == 0 {
		d.done = true
	}
	return nil
}

func (d *WorkflowRunner) validateStateBeforeAcceptingRampingUpdate(args *deploymentspb.SetRampingVersionArgs) error {
	if args.Version == d.State.RoutingConfig.RampingVersion && args.Percentage == d.State.RoutingConfig.RampingVersionPercentage && args.Identity == d.State.LastModifierIdentity {
		return temporal.NewApplicationError("version already ramping, no change", errNoChangeType, d.State.ConflictToken)
	}

	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.ConflictToken) {
		return temporal.NewApplicationError("conflict token mismatch", errConflictTokenMismatchType)
	}
	if args.Version == d.State.RoutingConfig.CurrentVersion {
		d.logger.Info("version can't be set to ramping since it is already current")
		return temporal.NewApplicationError("version can't be set to ramping since it is already current", errVersionAlreadyCurrentType)
	}

	if _, ok := d.State.Versions[args.Version]; !ok && args.Version != "" && args.Version != worker_versioning.UnversionedVersionId {
		d.logger.Info("version not found in deployment")
		return temporal.NewApplicationError("version not found in deployment", errVersionNotFound)
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
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
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

	var rampingSinceTime *timestamppb.Timestamp
	var rampingVersionUpdateTime *timestamppb.Timestamp

	// unsetting ramp
	if newRampingVersion == "" {

		unsetRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: routingUpdateTime,
			RampingSinceTime:  nil, // remove ramp
			RampPercentage:    0,   // remove ramp
		}

		if prevRampingVersion != worker_versioning.UnversionedVersionId {
			if _, err := d.syncVersion(ctx, prevRampingVersion, unsetRampUpdateArgs); err != nil {
				return nil, err
			}
		} else {
			if err := d.syncUnversionedRamp(ctx, unsetRampUpdateArgs); err != nil {
				return nil, err
			}
		}

		rampingVersionUpdateTime = routingUpdateTime // ramp was updated to ""

		// Set summary drainage status immediately to draining.
		// We know prevRampingVersion cannot have been current, so it must now be draining
		d.setDrainageStatus(prevRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING)
	} else {
		// setting ramp

		if prevRampingVersion == newRampingVersion { // the version was already ramping, user changing ramp %
			rampingSinceTime = d.State.RoutingConfig.RampingVersionChangedTime
			rampingVersionUpdateTime = d.State.RoutingConfig.RampingVersionChangedTime
		} else {
			// version ramping for the first time

			currentVersion := d.State.RoutingConfig.CurrentVersion
			if !args.IgnoreMissingTaskQueues &&
				currentVersion != worker_versioning.UnversionedVersionId &&
				newRampingVersion != worker_versioning.UnversionedVersionId {
				isMissingTaskQueues, err := d.isVersionMissingTaskQueues(ctx, currentVersion, newRampingVersion)
				if err != nil {
					d.logger.Info("Error verifying poller presence in version", "error", err)
					return nil, err
				}
				if isMissingTaskQueues {
					return nil, serviceerror.NewFailedPrecondition("New ramping version does not have all the task queues from the previous current version and some missing task queues are active and would become unversioned after this operation")
				}
			}
			rampingSinceTime = routingUpdateTime
			rampingVersionUpdateTime = routingUpdateTime

			// Erase summary drainage status immediately, so it is not draining/drained.
			d.setDrainageStatus(newRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED)
		}

		setRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: routingUpdateTime,
			RampingSinceTime:  rampingSinceTime,
			RampPercentage:    args.Percentage,
		}
		if newRampingVersion != worker_versioning.UnversionedVersionId {
			if _, err := d.syncVersion(ctx, newRampingVersion, setRampUpdateArgs); err != nil {
				return nil, err
			}
		} else {
			if err := d.syncUnversionedRamp(ctx, setRampUpdateArgs); err != nil {
				return nil, err
			}
		}

		// tell previous ramping version, if present, that it's no longer ramping
		if prevRampingVersion != "" && prevRampingVersion != newRampingVersion {
			unsetRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
				RoutingUpdateTime: routingUpdateTime,
				RampingSinceTime:  nil, // remove ramp
				RampPercentage:    0,   // remove ramp
			}
			if prevRampingVersion != worker_versioning.UnversionedVersionId {
				if _, err := d.syncVersion(ctx, prevRampingVersion, unsetRampUpdateArgs); err != nil {
					return nil, err
				}
			} else {
				if err := d.syncUnversionedRamp(ctx, unsetRampUpdateArgs); err != nil {
					return nil, err
				}
			}
			// Set summary drainage status immediately to draining.
			// We know prevRampingVersion cannot have been current, so it must now be draining
			d.setDrainageStatus(prevRampingVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING)
		}
	}

	// update local state
	d.State.RoutingConfig.RampingVersion = newRampingVersion
	d.State.RoutingConfig.RampingVersionPercentage = args.Percentage
	d.State.RoutingConfig.RampingVersionChangedTime = rampingVersionUpdateTime
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

func (d *WorkflowRunner) setDrainageStatus(version string, status enumspb.VersionDrainageStatus) {
	if summary := d.State.GetVersions()[version]; summary != nil {
		summary.DrainageStatus = status
	}
}

func (d *WorkflowRunner) validateStateBeforeAcceptingDeleteVersion(args *deploymentspb.DeleteVersionArgs) error {
	if _, ok := d.State.Versions[args.Version]; !ok {
		return temporal.NewApplicationError("version not found in deployment", errVersionNotFound)
	}
	return nil
}

func (d *WorkflowRunner) validateDeleteVersion(args *deploymentspb.DeleteVersionArgs) error {
	return d.validateStateBeforeAcceptingDeleteVersion(args)
}

func (d *WorkflowRunner) deleteVersion(ctx workflow.Context, args *deploymentspb.DeleteVersionArgs) error {
	// ask version to delete itself
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.SyncVersionStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.DeleteWorkerDeploymentVersion, &deploymentspb.DeleteVersionActivityArgs{
		Identity:       args.Identity,
		DeploymentName: d.DeploymentName,
		Version:        args.Version,
		RequestId:      uuid.New(),
		SkipDrainage:   args.SkipDrainage,
	}).Get(ctx, &res)
	if err != nil {
		return err
	}
	// update local state
	delete(d.State.Versions, args.Version)
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
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
		d.lock.Unlock()
	}()

	// Validating the state before starting the DeleteVersion operation. This is required due to the following reason:
	// The validator accepts/rejects updates based on the state of the deployment workflow. Theoretically, two concurrent delete version requests
	// might be accepted by the validator since the local state of the workflow contains the version which is requested to be deleted. Since this update handler
	// enforces sequential updates, after the first update completes, the version will be removed from the local state of the deployment workflow. The second update,
	// now already accepted by the validator, should now not be allowed to run since the initial workflow state is different.
	err = d.validateStateBeforeAcceptingDeleteVersion(args)
	if err != nil {
		return err
	}

	return d.deleteVersion(ctx, args)
}

func (d *WorkflowRunner) validateStateBeforeAcceptingSetCurrent(args *deploymentspb.SetCurrentVersionArgs) error {
	if d.State.RoutingConfig.CurrentVersion == args.Version && d.State.LastModifierIdentity == args.Identity {
		return temporal.NewApplicationError("no change", errNoChangeType, d.State.ConflictToken)
	}
	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.ConflictToken) {
		return temporal.NewApplicationError("conflict token mismatch", errConflictTokenMismatchType)
	}
	if _, ok := d.State.Versions[args.Version]; !ok && args.Version != worker_versioning.UnversionedVersionId {
		d.logger.Info("version not found in deployment")
		return temporal.NewApplicationError("version not found in deployment", errVersionNotFound)
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
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
		d.lock.Unlock()
	}()

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

	if !args.IgnoreMissingTaskQueues &&
		prevCurrentVersion != worker_versioning.UnversionedVersionId &&
		newCurrentVersion != worker_versioning.UnversionedVersionId {
		isMissingTaskQueues, err := d.isVersionMissingTaskQueues(ctx, prevCurrentVersion, newCurrentVersion)
		if err != nil {
			d.logger.Info("Error verifying poller presence in version", "error", err)
			return nil, err
		}
		if isMissingTaskQueues {
			return nil, serviceerror.NewFailedPrecondition("New current version does not have all the task queues from the previous current version and some missing task queues are active and would become unversioned after this operation")
		}
	}

	if newCurrentVersion != worker_versioning.UnversionedVersionId {
		// Tell new current version that it's current
		currUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: updateTime,
			CurrentSinceTime:  updateTime,
			RampingSinceTime:  nil, // remove ramp for that version if it was ramping
			RampPercentage:    0,   // remove ramp for that version if it was ramping
		}
		if _, err := d.syncVersion(ctx, newCurrentVersion, currUpdateArgs); err != nil {
			return nil, err
		}
		// Erase summary drainage status immediately (in case it was previously drained/draining)
		d.setDrainageStatus(newCurrentVersion, enumspb.VERSION_DRAINAGE_STATUS_UNSPECIFIED)
	} else if d.State.RoutingConfig.RampingVersion == worker_versioning.UnversionedVersionId {
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
	// If the new current version is unversioned and there was no unversioned ramp, all we need to
	// do is tell the previous current version that it is not current. Then, the task queues in the
	// previous current version will have no current version and will become unversioned implicitly.

	if prevCurrentVersion != worker_versioning.UnversionedVersionId {
		// Tell previous current that it's no longer current
		prevUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: updateTime,
			CurrentSinceTime:  nil, // remove current
			RampingSinceTime:  nil, // no change, the prev current was not ramping
			RampPercentage:    0,   // no change, the prev current was not ramping
		}
		if _, err := d.syncVersion(ctx, prevCurrentVersion, prevUpdateArgs); err != nil {
			return nil, err
		}
		// Set summary drainage status immediately to draining.
		// We know prevCurrentVersion cannot have been ramping, so it must now be draining
		d.setDrainageStatus(prevCurrentVersion, enumspb.VERSION_DRAINAGE_STATUS_DRAINING)
	}
	// If the previous current version was unversioned, there is nothing in the task queues
	// to remove, because they were implicitly unversioned. We don't have to remove any
	// unversioned ramps, because current and ramping cannot both be unversioned.

	// update local state
	d.State.RoutingConfig.CurrentVersion = args.Version
	d.State.RoutingConfig.CurrentVersionChangedTime = updateTime
	d.State.ConflictToken, _ = updateTime.AsTime().MarshalBinary()
	d.State.LastModifierIdentity = args.Identity

	// unset ramping version if it was set to current version
	if d.State.RoutingConfig.CurrentVersion == d.State.RoutingConfig.RampingVersion {
		d.State.RoutingConfig.RampingVersion = ""
		d.State.RoutingConfig.RampingVersionPercentage = 0
		d.State.RoutingConfig.RampingVersionChangedTime = updateTime // since ramp was removed
	}

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

// to-be-deprecated
func (d *WorkflowRunner) handleAddVersionToWorkerDeployment(ctx workflow.Context, args *deploymentspb.AddVersionUpdateArgs) error {
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
	}()

	maxVersions := d.getMaxVersions(ctx)

	if len(d.State.Versions) >= maxVersions {
		err := d.tryDeleteVersion(ctx)
		if err != nil {
			return temporal.NewApplicationError(fmt.Sprintf("cannot add version, already at max versions %d", maxVersions), errTooManyVersions)
		}
	}

	d.State.Versions[args.Version] = &deploymentspb.WorkerDeploymentVersionSummary{
		Version:    args.Version,
		CreateTime: args.CreateTime,
	}
	return nil
}

func (d *WorkflowRunner) tryDeleteVersion(ctx workflow.Context) error {
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
	for _, v := range sortedSummaries {
		// this might hang on the lock
		err := d.deleteVersion(ctx, &deploymentspb.DeleteVersionArgs{
			Identity: "try-delete-for-add-version",
			Version:  v.Version,
		})
		if err == nil {
			return nil
		}
	}
	return serviceerror.NewFailedPrecondition("could not add version: too many versions in deployment and none are eligible for deletion")
}

func (d *WorkflowRunner) syncVersion(ctx workflow.Context, targetVersion string, versionUpdateArgs *deploymentspb.SyncVersionStateUpdateArgs) (*deploymentspb.VersionLocalState, error) {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.SyncVersionStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.SyncWorkerDeploymentVersion, &deploymentspb.SyncVersionStateActivityArgs{
		DeploymentName: d.DeploymentName,
		Version:        targetVersion,
		UpdateArgs:     versionUpdateArgs,
		RequestId:      d.newUUID(ctx),
	}).Get(ctx, &res)
	return res.VersionState, err
}

func (d *WorkflowRunner) syncUnversionedRamp(ctx workflow.Context, versionUpdateArgs *deploymentspb.SyncVersionStateUpdateArgs) error {
	var err error
	v := workflow.GetVersion(ctx, "syncUnversionedRamp", workflow.DefaultVersion, 1)
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	if v == workflow.DefaultVersion {
		var res deploymentspb.SyncUnversionedRampActivityResponse
		err := workflow.ExecuteActivity(
			activityCtx,
			d.a.SyncUnversionedRamp,
			&deploymentspb.SyncUnversionedRampActivityArgs{
				CurrentVersion: d.State.RoutingConfig.CurrentVersion,
				UpdateArgs:     versionUpdateArgs,
			}).Get(ctx, &res)
		if err != nil {
			return err
		}
		// check propagation
		err = workflow.ExecuteActivity(
			activityCtx,
			d.a.CheckUnversionedRampUserDataPropagation,
			&deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
				TaskQueueMaxVersions: res.TaskQueueMaxVersions,
			}).Get(ctx, nil)
		if err != nil {
			return err
		}
	} else {

		// DescribeVersion activity to get all the task queues in the current version
		var res deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult
		err := workflow.ExecuteActivity(
			activityCtx,
			d.a.DescribeVersionFromWorkerDeployment,
			&deploymentspb.DescribeVersionFromWorkerDeploymentActivityArgs{
				Version: d.State.RoutingConfig.CurrentVersion,
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

			if len(syncReqs) == syncBatchSize {
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
				Version:       nil,
				ForgetVersion: false,
				Sync:          batch,
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

func (d *WorkflowRunner) newUUID(ctx workflow.Context) string {
	var val string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New()
	}).Get(&val)
	return val
}

func (d *WorkflowRunner) updateMemo(ctx workflow.Context) error {
	return workflow.UpsertMemo(ctx, map[string]any{
		WorkerDeploymentMemoField: &deploymentspb.WorkerDeploymentWorkflowMemo{
			DeploymentName: d.DeploymentName,
			CreateTime:     d.State.CreateTime,
			RoutingConfig:  d.State.RoutingConfig,
		},
	})
}
