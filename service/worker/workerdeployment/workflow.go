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
	"slices"
	"time"

	"github.com/pborman/uuid"
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
	// WorkflowRunner holds the local state while running a deployment-series workflow
	WorkflowRunner struct {
		*deploymentspb.WorkerDeploymentWorkflowArgs
		a              *Activities
		logger         sdklog.Logger
		metrics        sdkclient.MetricsHandler
		lock           workflow.Mutex
		pendingUpdates int
		conflictToken  []byte
	}
)

func Workflow(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
	workflowRunner := &WorkflowRunner{
		WorkerDeploymentWorkflowArgs: args,

		a:       nil,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.NamespaceName}),
		lock:    workflow.NewMutex(ctx),
	}
	return workflowRunner.run(ctx)
}

func (d *WorkflowRunner) run(ctx workflow.Context) error {
	if d.State == nil {
		d.State = &deploymentspb.WorkerDeploymentLocalState{}
		d.State.CreateTime = timestamppb.New(time.Now())
		d.State.RoutingConfig = &deploymentpb.RoutingConfig{}
		d.State.ConflictToken, _ = workflow.Now(ctx).MarshalBinary()
	}

	var pendingUpdates int

	err := workflow.SetQueryHandler(ctx, QueryDescribeDeployment, func() (*deploymentspb.QueryDescribeWorkerDeploymentResponse, error) {
		return &deploymentspb.QueryDescribeWorkerDeploymentResponse{
			State: d.State,
		}, nil
	})
	if err != nil {
		d.logger.Info("SetQueryHandler failed for WorkerDeployment workflow with error: " + err.Error())
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

	// Wait until we can continue as new or are cancelled.
	err = workflow.Await(ctx, func() bool { return workflow.GetInfo(ctx).GetContinueAsNewSuggested() && pendingUpdates == 0 })
	if err != nil {
		return err
	}

	// Continue as new when there are no pending updates and history size is greater than requestsBeforeContinueAsNew.
	// Note, if update requests come in faster than they
	// are handled, there will not be a moment where the workflow has
	// nothing pending which means this will run forever.
	return workflow.NewContinueAsNewError(ctx, Workflow, d.WorkerDeploymentWorkflowArgs)
}

func (d *WorkflowRunner) validateSetRampingVersion(args *deploymentspb.SetRampingVersionArgs) error {
	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.ConflictToken) {
		return temporal.NewApplicationError("conflict token mismatch", errConflictTokenMismatchType)
	}
	if args.Version == d.State.RoutingConfig.RampingVersion && args.Percentage == d.State.RoutingConfig.RampingVersionPercentage {
		d.logger.Info("version already ramping, no change")
		return temporal.NewApplicationError("version already ramping, no change", errNoChangeType)
	}
	// todo: this will only work when "__unversioned__" is the default current-version of a deployment.
	if args.Version == d.State.RoutingConfig.CurrentVersion {
		d.logger.Info("version can't be set to ramping since it is already current")
		return temporal.NewApplicationError("version can't be set to ramping since it is already current", errVersionAlreadyCurrentType)
	}

	return nil
}

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

	prevRampingVersion := d.State.RoutingConfig.RampingVersion
	prevRampingVersionPercentage := d.State.RoutingConfig.RampingVersionPercentage

	newRampingVersion := args.Version
	routingUpdateTime := timestamppb.New(workflow.Now(ctx))

	var rampingSinceTime *timestamppb.Timestamp
	var rampingVersionUpdateTime *timestamppb.Timestamp

	// unsetting ramp
	if newRampingVersion == "" {

		rampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: routingUpdateTime,
			RampingSinceTime:  nil, // remove ramp
			RampPercentage:    0,   // remove ramp
		}

		if _, err := d.syncVersion(ctx, prevRampingVersion, rampUpdateArgs); err != nil {
			return nil, err
		}

		rampingVersionUpdateTime = routingUpdateTime // ramp was updated to ""
	} else {
		// setting ramp

		if prevRampingVersion == newRampingVersion { // the version was already ramping, user changing ramp %
			rampingSinceTime = d.State.RoutingConfig.RampingVersionChangedTime
			rampingVersionUpdateTime = d.State.RoutingConfig.RampingVersionChangedTime
		} else {
			rampingSinceTime = routingUpdateTime // version ramping for the first time
			rampingVersionUpdateTime = routingUpdateTime
		}

		rampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: routingUpdateTime,
			RampingSinceTime:  rampingSinceTime,
			RampPercentage:    args.Percentage,
		}
		if _, err := d.syncVersion(ctx, newRampingVersion, rampUpdateArgs); err != nil {
			return nil, err
		}

		// tell previous ramping version, if present, that it's no longer ramping
		if prevRampingVersion != "" && prevRampingVersion != newRampingVersion {
			prevRampUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
				RoutingUpdateTime: routingUpdateTime,
				RampingSinceTime:  nil, // remove ramp
				RampPercentage:    0,   // remove ramp
			}
			if _, err := d.syncVersion(ctx, prevRampingVersion, prevRampUpdateArgs); err != nil {
				return nil, err
			}
		}
	}

	// update local state
	d.State.RoutingConfig.RampingVersion = newRampingVersion
	d.State.RoutingConfig.RampingVersionPercentage = args.Percentage
	d.State.RoutingConfig.RampingVersionChangedTime = rampingVersionUpdateTime
	d.State.ConflictToken, _ = routingUpdateTime.AsTime().MarshalBinary()

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

func (d *WorkflowRunner) validateDeleteVersion(args *deploymentspb.DeleteVersionArgs) error {
	if !slices.Contains(d.State.Versions, args.Version) {
		return serviceerror.NewNotFound("version not found in deployment")
	}
	return nil
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

	// ask version to delete itself
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var res deploymentspb.SyncVersionStateActivityResult
	err = workflow.ExecuteActivity(activityCtx, d.a.DeleteWorkerDeploymentVersion, &deploymentspb.DeleteVersionActivityArgs{
		Identity:       args.Identity,
		DeploymentName: d.DeploymentName,
		Version:        args.Version,
		RequestId:      uuid.New(),
	}).Get(ctx, &res)
	if err != nil {
		return err
	}

	// update local state
	d.State.Versions = slices.DeleteFunc(d.State.Versions, func(v string) bool { return v == args.Version })

	// update memo
	if err = d.updateMemo(ctx); err != nil {
		return err
	}
	return nil
}

func (d *WorkflowRunner) validateSetCurrent(args *deploymentspb.SetCurrentVersionArgs) error {
	if args.ConflictToken != nil && !bytes.Equal(args.ConflictToken, d.State.ConflictToken) {
		return temporal.NewApplicationError("conflict token mismatch", errConflictTokenMismatchType)
	}
	if d.State.RoutingConfig.CurrentVersion == args.Version {
		return temporal.NewApplicationError("no change", errNoChangeType)
	}
	return nil
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

	prevCurrentVersion := d.State.RoutingConfig.CurrentVersion
	newCurrentVersion := args.Version
	updateTime := timestamppb.New(workflow.Now(ctx))

	// tell new current that it's current
	currUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
		RoutingUpdateTime: updateTime,
		CurrentSinceTime:  updateTime,
		RampingSinceTime:  nil, // remove ramp if it existed
		RampPercentage:    0,   // remove ramp if it existed
	}
	if _, err := d.syncVersion(ctx, newCurrentVersion, currUpdateArgs); err != nil {
		return nil, err
	}

	if prevCurrentVersion != "" {
		// tell previous current that it's no longer current
		prevUpdateArgs := &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: updateTime,
			CurrentSinceTime:  nil, // remove current
			RampingSinceTime:  nil, // no change, the prev current was not ramping
			RampPercentage:    0,   // no change, the prev current was not ramping
		}
		if _, err := d.syncVersion(ctx, prevCurrentVersion, prevUpdateArgs); err != nil {
			return nil, err
		}
	}

	// update local state
	d.State.RoutingConfig.CurrentVersion = args.Version
	d.State.RoutingConfig.CurrentVersionChangedTime = updateTime
	d.State.ConflictToken, _ = updateTime.AsTime().MarshalBinary()

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

func (d *WorkflowRunner) validateAddVersionToWorkerDeployment(version string) error {
	if d.State.Versions == nil {
		return nil
	}

	for _, v := range d.State.Versions {
		if v == version {
			return temporal.NewApplicationError("deployment version already registered", errVersionAlreadyExistsType)
		}
	}

	return nil
}

func (d *WorkflowRunner) handleAddVersionToWorkerDeployment(ctx workflow.Context, version string) error {
	d.pendingUpdates++
	defer func() {
		d.pendingUpdates--
	}()

	// Add version to local state
	if d.State.Versions == nil {
		d.State.Versions = make([]string, 0)
	}
	d.State.Versions = append(d.State.Versions, version)
	return nil
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
