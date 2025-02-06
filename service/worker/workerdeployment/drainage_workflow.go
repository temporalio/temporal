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
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

const (
	defaultVisibilityRefresh = 5 * time.Minute
	defaultVisibilityGrace   = 3 * time.Minute
)

func DrainageWorkflowWithDC(dc *dynamicconfig.Collection, ns string) func(ctx workflow.Context, version *deploymentspb.WorkerDeploymentVersion, first bool) error {
	return func(ctx workflow.Context, version *deploymentspb.WorkerDeploymentVersion, first bool) error {
		activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
		var a *DrainageActivities

		// listen for done signal sent by parent if started accepting new executions or continued-as-new
		done := false
		workflow.Go(ctx, func(ctx workflow.Context) {
			terminateChan := workflow.GetSignalChannel(ctx, TerminateDrainageSignal)
			terminateChan.Receive(ctx, nil)
			done = true
		})

		// Set status = DRAINING and then sleep for visibilityGracePeriod (to let recently-started workflows arrive in visibility)
		if first { // skip if resuming after the parent continued-as-new
			parentWf := workflow.GetInfo(ctx).ParentWorkflowExecution
			now := timestamppb.Now()
			drainingInfo := &deploymentpb.VersionDrainageInfo{
				Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
				LastChangedTime: now,
				LastCheckedTime: now,
			}
			err := workflow.SignalExternalWorkflow(ctx, parentWf.ID, parentWf.RunID, SyncDrainageSignalName, drainingInfo).Get(ctx, nil)
			if err != nil {
				return err
			}
			grace, err := getVisibilityGracePeriod(ctx, dc, ns)
			if err != nil {
				return err
			}
			_ = workflow.Sleep(ctx, grace)
		}

		for {
			if done {
				return nil
			}
			var info *deploymentpb.VersionDrainageInfo
			err := workflow.ExecuteActivity(
				activityCtx,
				a.GetVersionDrainageStatus,
				version,
			).Get(ctx, &info)
			if err != nil {
				return err
			}

			parentWf := workflow.GetInfo(ctx).ParentWorkflowExecution
			err = workflow.SignalExternalWorkflow(ctx, parentWf.ID, parentWf.RunID, SyncDrainageSignalName, info).Get(ctx, nil)
			if err != nil {
				return err
			}
			if info.Status == enumspb.VERSION_DRAINAGE_STATUS_DRAINED {
				return nil
			}
			refresh, err := getRefreshInterval(ctx, dc, ns)
			if err != nil {
				return err
			}
			_ = workflow.Sleep(ctx, refresh)
		}
	}
}

func getRefreshInterval(ctx workflow.Context, dc *dynamicconfig.Collection, ns string) (time.Duration, error) {
	get := func(ctx workflow.Context) interface{} {
		return dynamicconfig.VersionDrainageStatusRefreshInterval.Get(dc)(ns)
	}
	var refresh time.Duration
	if err := workflow.MutableSideEffect(ctx, "getDrainageRefreshInterval", get, durationEq).Get(&refresh); err != nil {
		return defaultVisibilityRefresh, err
	}
	return refresh, nil
}

func getVisibilityGracePeriod(ctx workflow.Context, dc *dynamicconfig.Collection, ns string) (time.Duration, error) {
	get := func(ctx workflow.Context) interface{} {
		return dynamicconfig.VersionDrainageStatusVisibilityGracePeriod.Get(dc)(ns)
	}
	var gracePeriod time.Duration
	if err := workflow.MutableSideEffect(ctx, "getVisibilityGracePeriod", get, durationEq).Get(&gracePeriod); err != nil {
		return defaultVisibilityGrace, err
	}
	return gracePeriod, nil
}

func durationEq(a, b interface{}) bool {
	aDur, ok := a.(time.Duration)
	if !ok {
		return false
	}
	bDur, ok := b.(time.Duration)
	if !ok {
		return false
	}
	return aDur == bDur
}
