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
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common"
)

type (
	// VersionWorkflowRunner holds the local state for a deployment workflow
	VersionWorkflowRunner struct {
		*deploymentspb.WorkerDeploymentVersionWorkflowArgs
		a                *VersionActivities
		logger           sdklog.Logger
		metrics          sdkclient.MetricsHandler
		lock             workflow.Mutex
		pendingUpdates   int
		signalsCompleted bool
	}
)

var (
	defaultActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
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
		forceCAN = true
	})
	selector.AddReceive(drainageStatusSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		var status enumspb.VersionDrainageStatus
		c.Receive(ctx, &status)
		now := timestamppb.Now()
		info := d.VersionState.DrainageInfo
		if info == nil {
			d.VersionState.DrainageInfo = &deploymentpb.VersionDrainageInfo{
				Status:          status,
				LastChangedTime: now,
				LastCheckedTime: now,
			}
		} else {
			info.LastCheckedTime = now
			if info.Status != status {
				info.LastChangedTime = now
				info.Status = status
			}
		}
		d.updateMemo(ctx)
	})

	for (!workflow.GetInfo(ctx).GetContinueAsNewSuggested() && !forceCAN) || selector.HasPending() {
		selector.Select(ctx)
	}

	// Done processing signals before CAN
	d.signalsCompleted = true
}

func (d *VersionWorkflowRunner) run(ctx workflow.Context) error {
	if d.VersionState == nil {
		d.VersionState = &deploymentspb.VersionLocalState{}
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

	// First ensure series workflow is running
	if !d.VersionState.StartedDeploymentWorkflow {
		activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
		err := workflow.ExecuteActivity(activityCtx, d.a.StartWorkerDeploymentWorkflow, &deploymentspb.StartWorkerDeploymentRequest{
			DeploymentName: d.VersionState.DeploymentName,
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
	err := workflow.Await(ctx, func() bool { return d.pendingUpdates == 0 && d.signalsCompleted })
	if err != nil {
		return err
	}

	d.logger.Debug("Version doing continue-as-new")
	return workflow.NewContinueAsNewError(ctx, VersionWorkflow, d.WorkerDeploymentVersionWorkflowArgs)
}

func (d *VersionWorkflowRunner) validateRegisterWorker(args *deploymentspb.RegisterWorkerInVersionArgs) error {
	if _, ok := d.VersionState.TaskQueueFamilies[args.TaskQueueName].GetTaskQueues()[int32(args.TaskQueueType)]; ok {
		return temporal.NewApplicationError("task queue already exists in deployment", errNoChangeType)
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

	// wait until series workflow started
	err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before series workflow started")
		return err
	}

	// initial data
	data := &deploymentspb.DeploymentVersionTaskQueueData{}

	// sync to user data
	// activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	// var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse
	// err = workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
	// 	DeploymentName: d.VersionState.DeploymentName,
	// 	Sync: []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
	// 		&deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
	// 			Name: args.TaskQueueName,
	// 			Type: args.TaskQueueType,
	// 			Data: d.dataWithTime(data),
	// 		},
	// 	},
	// }).Get(ctx, &syncRes)
	// if err != nil {
	// 	return err
	// }

	// if len(syncRes.TaskQueueMaxVersions) > 0 {
	// 	// wait for propagation
	// 	err = workflow.ExecuteActivity(
	// 		activityCtx,
	// 		d.a.CheckWorkerDeploymentUserDataPropagation,
	// 		&deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
	// 			TaskQueueMaxVersions: syncRes.TaskQueueMaxVersions,
	// 		}).Get(ctx, nil)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// add version to worker-deployment workflow
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	err = workflow.ExecuteActivity(activityCtx, d.a.AddVersionToWorkerDeployment, &deploymentspb.AddVersionToWorkerDeploymentRequest{
		DeploymentName: d.VersionState.DeploymentName,
		Version:        d.VersionState.Version,
		RequestId:      d.newUUID(ctx),
	}).Get(ctx, nil)
	if err != nil {
		return err
	}

	// if successful, add the task queue to the local state
	if d.VersionState.TaskQueueFamilies == nil {
		d.VersionState.TaskQueueFamilies = make(map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData)
	}
	if d.VersionState.TaskQueueFamilies[args.TaskQueueName] == nil {
		d.VersionState.TaskQueueFamilies[args.TaskQueueName] = &deploymentspb.VersionLocalState_TaskQueueFamilyData{}
	}
	if d.VersionState.TaskQueueFamilies[args.TaskQueueName].TaskQueues == nil {
		d.VersionState.TaskQueueFamilies[args.TaskQueueName].TaskQueues = make(map[int32]*deploymentspb.DeploymentVersionTaskQueueData)
	}
	d.VersionState.TaskQueueFamilies[args.TaskQueueName].TaskQueues[int32(args.TaskQueueType)] = data

	return nil
}

func (d *VersionWorkflowRunner) validateSyncState(args *deploymentspb.SyncVersionStateArgs) error {
	if args.GetCurrentSinceTime().AsTime().Equal(d.GetVersionState().GetCurrentSinceTime().AsTime()) &&
		args.GetRampingSinceTime().AsTime().Equal(d.GetVersionState().GetRampingSinceTime().AsTime()) {
		res := &deploymentspb.SyncVersionStateResponse{VersionState: d.VersionState}
		return temporal.NewApplicationError("no change", errNoChangeType, res)
	}
	return nil
}

func (d *VersionWorkflowRunner) handleSyncState(ctx workflow.Context, args *deploymentspb.SyncVersionStateArgs) (*deploymentspb.SyncVersionStateResponse, error) {
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

	// wait until series workflow started
	err = workflow.Await(ctx, func() bool { return d.VersionState.StartedDeploymentWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before series workflow started")
		return nil, serviceerror.NewDeadlineExceeded("Update canceled before series workflow started")
	}

	state := d.GetVersionState()
	wasAcceptingNewWorkflows := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil
	if args.GetCurrentSinceTime() != nil {
		state.CurrentSinceTime = args.GetCurrentSinceTime()
	}
	if args.GetRampingSinceTime() != nil {
		state.RampingSinceTime = args.GetRampingSinceTime()
	}
	isAcceptingNewWorkflows := state.GetCurrentSinceTime() != nil || state.GetRampingSinceTime() != nil

	if wasAcceptingNewWorkflows && !isAcceptingNewWorkflows {
		workflow.ExecuteChildWorkflow(ctx, WorkerDeploymentCheckDrainageWorkflowType)
	}

	if err = d.updateMemo(ctx); err != nil {
		return nil, err
	}

	return &deploymentspb.SyncVersionStateResponse{
		VersionState: state,
	}, nil
}

func (d *VersionWorkflowRunner) dataWithTime(data *deploymentspb.DeploymentVersionTaskQueueData) *deploymentspb.DeploymentVersionTaskQueueData {
	data = common.CloneProto(data)
	return data
}

func (d *VersionWorkflowRunner) handleDescribeQuery() (*deploymentspb.QueryDescribeVersionResponse, error) {
	return &deploymentspb.QueryDescribeVersionResponse{
		VersionState: d.VersionState,
	}, nil
}

// updateMemo should be called whenever the workflow updates its local state
func (d *VersionWorkflowRunner) updateMemo(ctx workflow.Context) error {
	return workflow.UpsertMemo(ctx, map[string]any{
		WorkerDeploymentVersionMemoField: &deploymentspb.VersionWorkflowMemo{
			DeploymentName:   d.VersionState.DeploymentName,
			Version:          d.VersionState.Version,
			CurrentSinceTime: d.VersionState.CurrentSinceTime,
			RampingSinceTime: d.VersionState.RampingSinceTime,
			CreateTime:       d.VersionState.CreateTime,
			DrainageInfo:     d.VersionState.DrainageInfo,
		},
	})
}

func (d *VersionWorkflowRunner) newUUID(ctx workflow.Context) string {
	var val string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New()
	}).Get(&val)
	return val
}
