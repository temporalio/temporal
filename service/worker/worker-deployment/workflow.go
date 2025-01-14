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
	"github.com/pborman/uuid"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	worker_deploymentspb "go.temporal.io/server/api/worker_deployment/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// WorkflowRunner holds the local state while running a deployment-series workflow
	WorkflowRunner struct {
		*worker_deploymentspb.WorkflowArgs
		a              *Activities
		logger         sdklog.Logger
		metrics        sdkclient.MetricsHandler
		lock           workflow.Mutex
		pendingUpdates int
	}
)

func Workflow(ctx workflow.Context, args *worker_deploymentspb.WorkflowArgs) error {
	workflowRunner := &WorkflowRunner{
		WorkflowArgs: args,

		a:       nil,
		logger:  sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.NamespaceName),
		metrics: workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.NamespaceName}),
		lock:    workflow.NewMutex(ctx),
	}
	return workflowRunner.run(ctx)
}

func (d *WorkflowRunner) run(ctx workflow.Context) error {
	if d.State == nil {
		d.State = &worker_deploymentspb.LocalState{}
	}

	var pendingUpdates int

	err := workflow.SetQueryHandler(ctx, QueryCurrentVersion, func() (string, error) {
		return d.State.CurrentBuildId, nil
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

	// Wait until we can continue as new or are cancelled.
	err = workflow.Await(ctx, func() bool { return workflow.GetInfo(ctx).GetContinueAsNewSuggested() && pendingUpdates == 0 })
	if err != nil {
		return err
	}

	// Continue as new when there are no pending updates and history size is greater than requestsBeforeContinueAsNew.
	// Note, if update requests come in faster than they
	// are handled, there will not be a moment where the workflow has
	// nothing pending which means this will run forever.
	return workflow.NewContinueAsNewError(ctx, Workflow, d.WorkflowArgs)
}

func (d *WorkflowRunner) validateSetCurrent(args *worker_deploymentspb.SetCurrentVersionArgs) error {
	if d.State.CurrentBuildId != args.BuildId {
		return nil
	}

	return temporal.NewApplicationError("no change", errNoChangeType)
}

func (d *WorkflowRunner) handleSetCurrent(ctx workflow.Context, args *worker_deploymentspb.SetCurrentVersionArgs) (*worker_deploymentspb.SetCurrentVersionResponse, error) {
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

	var curState, prevState *worker_deploymentspb.VersionLocalState
	prevCurrent := d.State.CurrentBuildId

	// update local state
	d.State.CurrentBuildId = args.BuildId
	d.State.CurrentChangedTime = timestamppb.New(workflow.Now(ctx))

	// update memo
	if err = d.updateMemo(ctx); err != nil {
		return nil, err
	}

	// tell new current that it's current
	if curState, err = d.syncVersion(ctx, d.State.CurrentBuildId, args.UpdateMetadata); err != nil {
		return nil, err
	}

	if prevCurrent != "" {
		// tell previous current that it's no longer current
		if prevState, err = d.syncVersion(ctx, prevCurrent, nil); err != nil {
			return nil, err
		}
	}

	return &worker_deploymentspb.SetCurrentVersionResponse{
		CurrentVersionState:  curState,
		PreviousVersionState: prevState,
	}, nil
}

func (d *WorkflowRunner) syncVersion(ctx workflow.Context, buildId string, updateMetadata *deploymentpb.UpdateDeploymentMetadata) (*worker_deploymentspb.VersionLocalState, error) {
	activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)

	setCur := &worker_deploymentspb.SyncVersionStateArgs_SetCurrent{}
	if d.State.CurrentBuildId == buildId {
		setCur.LastBecameCurrentTime = d.State.CurrentChangedTime
	}

	var res worker_deploymentspb.SyncVersionStateActivityResult
	err := workflow.ExecuteActivity(activityCtx, d.a.SyncWorkerDeploymentVersion, &worker_deploymentspb.SyncVersionStateActivityArgs{
		Deployment: &deploymentpb.Deployment{
			SeriesName: d.SeriesName,
			BuildId:    buildId,
		},
		Args: &worker_deploymentspb.SyncVersionStateArgs{
			SetCurrent:     setCur,
			UpdateMetadata: updateMetadata,
		},
		RequestId: d.newUUID(ctx),
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
		WorkerDeploymentMemoField: &worker_deploymentspb.WorkflowMemo{
			SeriesName:         d.SeriesName,
			CurrentBuildId:     d.State.CurrentBuildId,
			CurrentChangedTime: d.State.CurrentChangedTime,
		},
	})
}
