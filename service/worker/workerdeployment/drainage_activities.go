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
	"context"
	"go.temporal.io/server/common/worker_versioning"

	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	DrainageActivities struct {
		namespace        *namespace.Namespace
		deploymentClient Client
	}
)

func (a *DrainageActivities) GetVersionDrainageStatus(ctx context.Context, version *deploymentspb.WorkerDeploymentVersion) (*deploymentpb.VersionDrainageInfo, error) {
	logger := activity.GetLogger(ctx)
	response, err := a.deploymentClient.GetVersionDrainageStatus(ctx, a.namespace, worker_versioning.WorkerDeploymentVersionToString(version))
	if err != nil {
		logger.Error("error counting workflows for drainage status", "error", err)
		return nil, err
	}
	return &deploymentpb.VersionDrainageInfo{
		Status:          response,
		LastChangedTime: nil, // ignored; whether Status changed will be evaluated by the receiver
		LastCheckedTime: timestamppb.Now(),
	}, nil
}
