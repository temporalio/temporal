// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package worker_versioning

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	deploymentpb "go.temporal.io/api/deployment/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

var (
	v1 = &deploymentpb.WorkerDeploymentVersion{
		Version:        "v1",
		DeploymentName: "foo",
	}
	v2 = &deploymentpb.WorkerDeploymentVersion{
		Version:        "v2",
		DeploymentName: "foo",
	}
	v3 = &deploymentpb.WorkerDeploymentVersion{
		Version:        "v3",
		DeploymentName: "foo",
	}
)

func TestCalculateTaskQueueVersioningInfo(t *testing.T) {
	t1 := timestamp.TimePtr(time.Now().Add(-2 * time.Hour))
	t2 := timestamp.TimePtr(time.Now().Add(-time.Hour))
	t3 := timestamp.TimePtr(time.Now())

	tests := []struct {
		name string
		want *taskqueuepb.TaskQueueVersioningInfo
		data *persistencespb.DeploymentData
	}{
		{name: "nil data", want: nil, data: nil},
		{name: "empty data", want: nil, data: &persistencespb.DeploymentData{}},
		{name: "old data", want: &taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: v1, UpdateTime: t1},
			data: &persistencespb.DeploymentData{
				Deployments: []*persistencespb.DeploymentData_DeploymentDataItem{
					{Deployment: DeploymentFromDeploymentVersion(v1), Data: &deploymentspb.TaskQueueData{LastBecameCurrentTime: t1}},
				}},
		},
		{name: "old and new data", want: &taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: v2, UpdateTime: t2},
			data: &persistencespb.DeploymentData{
				Deployments: []*persistencespb.DeploymentData_DeploymentDataItem{
					{Deployment: DeploymentFromDeploymentVersion(v1), Data: &deploymentspb.TaskQueueData{LastBecameCurrentTime: t1}},
				},
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v2, IsCurrent: true, RoutingUpdateTime: t2},
				},
			},
		},
		{name: "two current + two ramping", want: &taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: v2, UpdateTime: t3, RampingVersion: v3, RampingVersionPercentage: 20},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, IsCurrent: true, RoutingUpdateTime: t1},
					{Version: v2, IsCurrent: true, RoutingUpdateTime: t2},
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3},
				},
			},
		},
		{name: "ramp without current", want: &taskqueuepb.TaskQueueVersioningInfo{UpdateTime: t3, RampingVersion: v3, RampingVersionPercentage: 20},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3},
				},
			},
		},
		{name: "ramp to unversioned", want: &taskqueuepb.TaskQueueVersioningInfo{UpdateTime: t2, RampingVersionPercentage: 20},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t1},
					// Passing only deployment name without version
					{Version: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo"}, RampPercentage: 20, RoutingUpdateTime: t2},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateTaskQueueVersioningInfo(tt.data); !got.Equal(tt.want) {
				t.Errorf("CalculateTaskQueueVersioningInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindDeploymentVersionForWorkflowID(t *testing.T) {
	tests := []struct {
		name           string
		versioningInfo *taskqueuepb.TaskQueueVersioningInfo
		want           *deploymentpb.WorkerDeploymentVersion
	}{
		{name: "nil versioning info", versioningInfo: nil, want: nil},
		{name: "empty versioning info", versioningInfo: &taskqueuepb.TaskQueueVersioningInfo{}, want: nil},
		{name: "with current version", versioningInfo: &taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: v1}, want: v1},
		{name: "with full ramp", versioningInfo: &taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: v1, RampingVersion: v2, RampingVersionPercentage: 100}, want: v2},
		{name: "with full ramp to unversioned", versioningInfo: &taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: v1, RampingVersionPercentage: 100}, want: nil},
		{name: "with full ramp from unversioned", versioningInfo: &taskqueuepb.TaskQueueVersioningInfo{RampingVersion: v1, RampingVersionPercentage: 100}, want: v1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindDeploymentVersionForWorkflowID(tt.versioningInfo, "my-wf-id"); !got.Equal(tt.want) {
				t.Errorf("FindDeploymentVersionForWorkflowID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindDeploymentVersionForWorkflowID_PartialRamp(t *testing.T) {
	tests := []struct {
		name string
		from *deploymentpb.WorkerDeploymentVersion
		to   *deploymentpb.WorkerDeploymentVersion
	}{
		{name: "from v1 to v2", from: v1, to: v2},
		{name: "from v1 to unversioned", from: v1},
		{name: "from unversioned to v2", to: v2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			versioningInfo := &taskqueuepb.TaskQueueVersioningInfo{
				CurrentVersion:           tt.from,
				RampingVersion:           tt.to,
				RampingVersionPercentage: 30,
			}
			histogram := make(map[string]int)
			runs := 1000000
			for i := 0; i < runs; i++ {
				v := FindDeploymentVersionForWorkflowID(versioningInfo, "wf-"+strconv.Itoa(i))
				histogram[v.GetVersion()]++
			}

			assert.InEpsilon(t, .7*float64(runs), histogram[tt.from.GetVersion()], .02)
			assert.InEpsilon(t, .3*float64(runs), histogram[tt.to.GetVersion()], .02)
		})
	}
}
