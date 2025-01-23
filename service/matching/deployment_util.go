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

package matching

import (
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/worker_versioning"
)

var (
	errDeploymentsNotAllowed = serviceerror.NewPermissionDenied("deployments are disabled on this namespace", "")
	// [cleanup-wv-pre-release]
	errMissingDeployment = serviceerror.NewInvalidArgument("missing deployment")

	errMissingDeploymentVersion = serviceerror.NewInvalidArgument("missing deployment version")
)

// [cleanup-wv-pre-release]
func findDeployment(deployments *persistencespb.DeploymentData, deployment *deploymentpb.Deployment) int {
	for i, d := range deployments.GetDeployments() {
		if d.Deployment.Equal(deployment) {
			return i
		}
	}
	return -1
}

func findDeploymentVersion(deployments *persistencespb.DeploymentData, dv *deploymentpb.WorkerDeploymentVersion) int {
	for i, v := range deployments.GetVersions() {
		if v.Version.Equal(dv) {
			return i
		}
	}
	return -1
}

func hasDeploymentVersion(deployments *persistencespb.DeploymentData, v *deploymentpb.WorkerDeploymentVersion) bool {
	for _, d := range deployments.GetDeployments() {
		if d.Deployment.Equal(worker_versioning.DeploymentFromDeploymentVersion(v)) {
			return true
		}
	}

	for _, vd := range deployments.GetVersions() {
		if v.Equal(vd.GetVersion()) {
			return true
		}
	}

	return false
}
