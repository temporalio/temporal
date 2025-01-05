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

package deployment

import (
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// DeploymentWorkflowRunner holds the local state for a deployment workflow
	DeploymentWorkflowRunner struct {
		*deploymentspb.DeploymentWorkflowArgs
		a                *DeploymentActivities
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

func DeploymentWorkflow(ctx workflow.Context, deploymentWorkflowArgs *deploymentspb.DeploymentWorkflowArgs) error {
	deploymentWorkflowRunner := &DeploymentWorkflowRunner{
		DeploymentWorkflowArgs: deploymentWorkflowArgs,

		a:       nil,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", deploymentWorkflowArgs.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": deploymentWorkflowArgs.NamespaceName}),
		lock:    workflow.NewMutex(ctx),
	}
	return deploymentWorkflowRunner.run(ctx)
}

func (d *DeploymentWorkflowRunner) listenToSignals(ctx workflow.Context) {
	// Fetch signal channels
	forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
	forceCAN := false

	timerScheduled := false

	selector := workflow.NewSelector(ctx)
	selector.AddReceive(forceCANSignalChannel, func(c workflow.ReceiveChannel, more bool) {
		// Process Signal
		forceCAN = true
	})

	for {
		if !timerScheduled && d.State.DrainedStateInfo != nil {
			timerFuture, cancelHandler := d.createScavengerTimer(ctx)
			selector.AddFuture(timerFuture, func(f workflow.Future) {
				if d.isDeploymentScavengeable(ctx) {
					timerScheduled = true
					forceCAN = true
					cancelHandler()
				}
			})
		}

		// Break iff we have no pending signals AND if CAN is suggested or forced.
		if (workflow.GetInfo(ctx).GetContinueAsNewSuggested() || forceCAN) && !selector.HasPending() {
			break
		}
		selector.Select(ctx)
	}

	// Done processing signals before CAN
	d.signalsCompleted = true
}

func (d *DeploymentWorkflowRunner) createScavengerTimer(ctx workflow.Context) (workflow.Future, workflow.CancelFunc) {
	var timerDuration time.Duration

	if d.State.DrainedStateInfo.ScavengerDurationTime != nil {
		// Use configured duration
		if duration, err := d.a.getDefaultDurationBeforeScavenging(); err == nil {
			timerDuration = duration
		}
	} else {
		// Calculate remaining time based on retention
		if retention, err := d.a.GetNamespaceRetention(); err == nil {
			timerDuration = retention - workflow.Now(ctx).Sub(d.State.DrainedStateInfo.CreateTime.AsTime())
		}
	}

	if timerDuration <= 0 {
		d.logger.Error("Could not create scavenger timer")
		return nil, nil
	}

	timerCtx, cancelHandler := workflow.WithCancel(ctx)
	timerFuture := workflow.NewTimer(timerCtx, timerDuration)
	return timerFuture, cancelHandler
}

// isDeploymentScavengeable returns true if the deployment is in a state where it can be scavenged.
// This is possible when a deployment is:
// 1. Is Unreachable (has no open or closed workflows)
// 2. No pending pollers (not sure how to get this info)
// 3. No pending updates and signals (will be gracefully handled by the listenToSignals function so no need to check here)
func (d *DeploymentWorkflowRunner) isDeploymentScavengeable(ctx workflow.Context) bool {
	if d.State.DrainedStateInfo == nil {
		return false
	}
	if !d.IsDeploymentDrainedOfAllWorkflows(ctx) {
		return false
	}
	return false
}

func (d *DeploymentWorkflowRunner) run(ctx workflow.Context) error {
	if d.State == nil {
		d.State = &deploymentspb.DeploymentLocalState{}
	}

	// Set up Query Handlers here:
	if err := workflow.SetQueryHandler(ctx, QueryDescribeDeployment, d.handleDescribeQuery); err != nil {
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
		SyncDeploymentState,
		d.handleSyncState,
		workflow.UpdateHandlerOptions{
			Validator: d.validateSyncState,
		},
	); err != nil {
		return err
	}

	// First ensure series workflow is running
	if !d.State.StartedSeriesWorkflow {
		activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
		err := workflow.ExecuteActivity(activityCtx, d.a.StartDeploymentSeriesWorkflow, &deploymentspb.StartDeploymentSeriesRequest{
			SeriesName: d.State.Deployment.SeriesName,
			RequestId:  d.newUUID(ctx),
		}).Get(ctx, nil)
		if err != nil {
			return err
		}
		d.State.StartedSeriesWorkflow = true
	}

	// Listen to signals in a different goroutine to make business logic clearer
	workflow.Go(ctx, d.listenToSignals)

	// TODO (shivam): - Until deployment states are finalized, this go-routine runs regardless of the current state of the deployment.
	// After deployment states are implemented, the go-routine will only run when the deployment is in "Draining State".
	workflow.Go(ctx, func(ctx workflow.Context) {
		d.retryWithBackoff(ctx, func(ctx workflow.Context) bool {
			return d.IsDeploymentDrainedOfOpenWorkflows(ctx)
		})
	})

	// Wait on any pending signals and updates.
	err := workflow.Await(ctx, func() bool { return d.pendingUpdates == 0 && d.signalsCompleted })
	if err != nil {
		return err
	}

	/*

		 	Posting this as a reminder to limit the number of signals coming through since we use CAN:

			Workflows cannot have infinitely-sized history and when the event count grows too large, `ContinueAsNew` can be returned
			to start a new one atomically. However, in order not to lose any data, signals must be drained and any other futures
			that need to be reacted to must be completed first. This means there must be a period where there are no signals to
			drain and no futures to wait on. If signals come in faster than processed or futures wait so long there is no idle
			period, `ContinueAsNew` will not happen in a timely manner and history will grow.

	*/

	d.logger.Debug("Deployment doing continue-as-new")
	return workflow.NewContinueAsNewError(ctx, DeploymentWorkflow, d.DeploymentWorkflowArgs)
}

// TODO (Shivam) - should only run when deployment enters "Draining State"
func (d *DeploymentWorkflowRunner) retryWithBackoff(ctx workflow.Context, fn func(workflow.Context) bool) {
	backoff := 1 * time.Millisecond
	maxBackoff := 20 * time.Minute

	// todo (Shivam) -if this function is sleeping and the calling main workflow thread CAN's, this gets cancelled abruptly.

	for {
		if shouldStop := fn(ctx); shouldStop {
			return // Stop retrying if function signals completion
		}

		if err := workflow.Sleep(ctx, backoff); err != nil {
			return // Context canceled
		}

		// Exponential backoff with max cap
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (d *DeploymentWorkflowRunner) IsDeploymentDrainedOfOpenWorkflows(ctx workflow.Context) bool {
	var resp *deploymentspb.IsDeploymentDrainedOfOpenWorkflowsResponse

	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	err := workflow.ExecuteActivity(activityCtx, d.a.IsDeploymentDrainedOfOpenWorkflows, &deploymentspb.IsDeploymentDrainedOfOpenWorkflowsRequest{
		Deployment: d.State.Deployment,
	}).Get(ctx, &resp)
	if err != nil {
		d.logger.Error("error when checking if deployment is drained", "error", err)
		return false
	}

	if resp.IsDrained {
		d.State.DrainedStateInfo = &deploymentspb.DeploymentLocalState_StateInfo{
			CreateTime: timestamppb.New(workflow.Now(ctx)),
		}
		return true
	}

	return false
}

func (d *DeploymentWorkflowRunner) IsDeploymentDrainedOfAllWorkflows(ctx workflow.Context) bool {
	var resp *deploymentspb.IsDeploymentDrainedOfAllWorkflowsResponse

	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	err := workflow.ExecuteActivity(activityCtx, d.a.IsDeploymentDrainedOfAllWorkflows, &deploymentspb.IsDeploymentDrainedOfAllWorkflowsRequest{
		Deployment: d.State.Deployment,
	}).Get(ctx, &resp)
	if err != nil {
		d.logger.Error("error when checking if deployment is drained of all workflows", "error", err)
		return false
	}

	return resp.IsDrained
}

func (d *DeploymentWorkflowRunner) validateRegisterWorker(args *deploymentspb.RegisterWorkerInDeploymentArgs) error {
	if _, ok := d.State.TaskQueueFamilies[args.TaskQueueName].GetTaskQueues()[int32(args.TaskQueueType)]; ok {
		return temporal.NewApplicationError("task queue already exists in deployment", errNoChangeType)
	}
	if len(d.State.TaskQueueFamilies) >= int(args.MaxTaskQueues) {
		return temporal.NewApplicationError(
			fmt.Sprintf("maximum number of task queues (%d) have been registered in deployment", args.MaxTaskQueues),
			errMaxTaskQueuesInDeploymentType,
		)
	}
	return nil
}

func (d *DeploymentWorkflowRunner) handleRegisterWorker(ctx workflow.Context, args *deploymentspb.RegisterWorkerInDeploymentArgs) error {
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
	err = workflow.Await(ctx, func() bool { return d.State.StartedSeriesWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before series workflow started")
		return err
	}

	// initial data
	data := &deploymentspb.TaskQueueData{
		FirstPollerTime: args.FirstPollerTime,
	}

	// sync to user data
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
	var syncRes deploymentspb.SyncUserDataResponse
	err = workflow.ExecuteActivity(activityCtx, d.a.SyncUserData, &deploymentspb.SyncUserDataRequest{
		Deployment: d.State.Deployment,
		Sync: []*deploymentspb.SyncUserDataRequest_SyncUserData{
			&deploymentspb.SyncUserDataRequest_SyncUserData{
				Name: args.TaskQueueName,
				Type: args.TaskQueueType,
				Data: d.dataWithTime(data),
			},
		},
	}).Get(ctx, &syncRes)
	if err != nil {
		return err
	}

	if len(syncRes.TaskQueueMaxVersions) > 0 {
		// wait for propagation
		err = workflow.ExecuteActivity(activityCtx, d.a.CheckUserDataPropagation, &deploymentspb.CheckUserDataPropagationRequest{
			TaskQueueMaxVersions: syncRes.TaskQueueMaxVersions,
		}).Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	// if successful, add the task queue to the local state
	if d.State.TaskQueueFamilies == nil {
		d.State.TaskQueueFamilies = make(map[string]*deploymentspb.DeploymentLocalState_TaskQueueFamilyData)
	}
	if d.State.TaskQueueFamilies[args.TaskQueueName] == nil {
		d.State.TaskQueueFamilies[args.TaskQueueName] = &deploymentspb.DeploymentLocalState_TaskQueueFamilyData{}
	}
	if d.State.TaskQueueFamilies[args.TaskQueueName].TaskQueues == nil {
		d.State.TaskQueueFamilies[args.TaskQueueName].TaskQueues = make(map[int32]*deploymentspb.TaskQueueData)
	}
	d.State.TaskQueueFamilies[args.TaskQueueName].TaskQueues[int32(args.TaskQueueType)] = data

	return nil
}

func (d *DeploymentWorkflowRunner) validateSyncState(args *deploymentspb.SyncDeploymentStateArgs) error {
	if set := args.SetCurrent; set != nil {
		if set.LastBecameCurrentTime == nil {
			if d.State.IsCurrent {
				return nil
			}
		} else {
			if !d.State.IsCurrent ||
				!d.State.LastBecameCurrentTime.AsTime().Equal(set.LastBecameCurrentTime.AsTime()) {
				return nil
			}
		}
	}
	if args.UpdateMetadata != nil {
		// can't compare payloads, just assume it changes something
		return nil
	}
	// return current state along with "no change"
	res := &deploymentspb.SyncDeploymentStateResponse{DeploymentLocalState: d.State}
	return temporal.NewApplicationError("no change", errNoChangeType, res)
}

func (d *DeploymentWorkflowRunner) handleSyncState(ctx workflow.Context, args *deploymentspb.SyncDeploymentStateArgs) (*deploymentspb.SyncDeploymentStateResponse, error) {
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
	err = workflow.Await(ctx, func() bool { return d.State.StartedSeriesWorkflow })
	if err != nil {
		d.logger.Error("Update canceled before series workflow started")
		return nil, serviceerror.NewDeadlineExceeded("Update canceled before series workflow started")
	}

	// apply changes to "current"
	if set := args.SetCurrent; set != nil {
		if set.LastBecameCurrentTime == nil {
			d.State.IsCurrent = false

			// Deployment workflow enters the "Draining State"
		} else {
			d.State.IsCurrent = true
			d.State.LastBecameCurrentTime = set.LastBecameCurrentTime
		}
		if err = d.updateMemo(ctx); err != nil {
			return nil, err
		}

		// sync to task queues
		syncReq := &deploymentspb.SyncUserDataRequest{
			Deployment: d.State.Deployment,
		}
		for tqName, byType := range d.State.TaskQueueFamilies {
			for tqType, data := range byType.TaskQueues {
				syncReq.Sync = append(syncReq.Sync, &deploymentspb.SyncUserDataRequest_SyncUserData{
					Name: tqName,
					Type: enumspb.TaskQueueType(tqType),
					Data: d.dataWithTime(data),
				})
			}
		}
		activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
		var syncRes deploymentspb.SyncUserDataResponse
		err = workflow.ExecuteActivity(activityCtx, d.a.SyncUserData, syncReq).Get(ctx, &syncRes)
		if err != nil {
			// TODO: if this fails, should we roll back anything?
			return nil, err
		}
		if len(syncRes.TaskQueueMaxVersions) > 0 {
			// wait for propagation
			err = workflow.ExecuteActivity(activityCtx, d.a.CheckUserDataPropagation, &deploymentspb.CheckUserDataPropagationRequest{
				TaskQueueMaxVersions: syncRes.TaskQueueMaxVersions,
			}).Get(ctx, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	// apply changes to metadata
	if d.State.Metadata == nil && args.UpdateMetadata != nil {
		d.State.Metadata = make(map[string]*commonpb.Payload)
	}
	for key, payload := range args.UpdateMetadata.GetUpsertEntries() {
		d.State.Metadata[key] = payload
	}
	for _, key := range args.UpdateMetadata.GetRemoveEntries() {
		delete(d.State.Metadata, key)
	}

	return &deploymentspb.SyncDeploymentStateResponse{
		DeploymentLocalState: d.State,
	}, nil
}

func (d *DeploymentWorkflowRunner) dataWithTime(data *deploymentspb.TaskQueueData) *deploymentspb.TaskQueueData {
	data = common.CloneProto(data)
	data.LastBecameCurrentTime = d.State.LastBecameCurrentTime
	return data
}

func (d *DeploymentWorkflowRunner) handleDescribeQuery() (*deploymentspb.QueryDescribeDeploymentResponse, error) {
	return &deploymentspb.QueryDescribeDeploymentResponse{
		DeploymentLocalState: d.State,
	}, nil
}

// updateMemo should be called whenever the workflow updates its local state
func (d *DeploymentWorkflowRunner) updateMemo(ctx workflow.Context) error {
	return workflow.UpsertMemo(ctx, map[string]any{
		DeploymentMemoField: &deploymentspb.DeploymentWorkflowMemo{
			Deployment:          d.State.Deployment,
			CreateTime:          d.State.CreateTime,
			IsCurrentDeployment: d.State.IsCurrent,
		},
	})
}

func (d *DeploymentWorkflowRunner) newUUID(ctx workflow.Context) string {
	var val string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New()
	}).Get(&val)
	return val
}
