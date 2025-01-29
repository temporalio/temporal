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

package tests

import (
	"context"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/tests/testcore"
)

const (
	maxConcurrentBatchOperations             = 3
	testVersionDrainageRefreshInterval       = 3 * time.Second
	testVersionDrainageVisibilityGracePeriod = 3 * time.Second
)

type (
	DeploymentVersionSuite struct {
		testcore.FunctionalTestSuite
		sdkClient sdkclient.Client
	}
)

func TestDeploymentVersionSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DeploymentVersionSuite))
}

func (s *DeploymentVersionSuite) SetupSuite() {
	s.FunctionalTestSuite.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
		dynamicconfig.EnableDeploymentVersions.Key():                   true,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():     true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key(): true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs.Key():     true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableExecuteMultiOperation.Key():        true,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,

		// Reduce the chance of hitting max batch job limit in tests
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): maxConcurrentBatchOperations,

		dynamicconfig.VersionDrainageStatusRefreshInterval.Key():       testVersionDrainageRefreshInterval,
		dynamicconfig.VersionDrainageStatusVisibilityGracePeriod.Key(): testVersionDrainageVisibilityGracePeriod,
	}))
}

func (s *DeploymentVersionSuite) SetupTest() {
	s.FunctionalTestSuite.SetupTest()

	var err error
	s.sdkClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
}

func (s *DeploymentVersionSuite) TearDownTest() {
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

// pollFromDeployment calls PollWorkflowTaskQueue to start deployment related workflows
func (s *DeploymentVersionSuite) pollFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *DeploymentVersionSuite) pollActivityFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *DeploymentVersionSuite) TestDescribeVersion_RegisterTaskQueue() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	numberOfDeployments := 1

	// Starting a deployment workflow
	go s.pollFromDeployment(ctx, tv)

	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
		})
		a.NoError(err)

		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetDeploymentName())
		a.Equal(tv.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetBuildId())

		a.Equal(numberOfDeployments, len(resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()))
		if len(resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()) < numberOfDeployments {
			return
		}
		a.Equal(tv.TaskQueue().GetName(), resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()[0].Name)
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) TestDescribeVersion_RegisterTaskQueue_ConcurrentPollers() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	root, err := tqid.PartitionFromProto(tv.TaskQueue(), s.Namespace().String(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.NoError(err)
	// Making concurrent polls to 4 partitions, 3 polls to each
	for p := 0; p < 4; p++ {
		tv2 := tv.WithTaskQueue(root.TaskQueue().NormalPartition(p).RpcName())
		for i := 0; i < 3; i++ {
			go s.pollFromDeployment(ctx, tv2)
			go s.pollActivityFromDeployment(ctx, tv2)
		}
	}

	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
		})
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetDeploymentName())
		a.Equal(tv.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetBuildId())

		if !a.Equal(2, len(resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos())) {
			return
		}
		a.Equal(tv.TaskQueue().GetName(), resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()[0].Name)
	}, time.Second*10, time.Millisecond*1000)
}

// Name is used by testvars. We use a shorten test name in variables so that physical task queue IDs
// do not grow larger that DB column limit (currently as low as 272 chars).
func (s *DeploymentVersionSuite) Name() string {
	fullName := s.T().Name()
	if len(fullName) <= 30 {
		return fullName
	}
	return fmt.Sprintf("%s-%08x",
		fullName[len(fullName)-21:],
		farm.Fingerprint32([]byte(fullName)),
	)
}

func (s *DeploymentVersionSuite) TestDrainageStatus_SetCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := testvars.New(s).WithBuildIDNumber(2)

	// Start deployment workflow 1 and wait for the deployment version to exist
	go s.pollFromDeployment(ctx, tv1)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: tv1.DeploymentSeries(),
				BuildId:        tv1.BuildID(),
			},
		})
		a.NoError(err)
		a.Equal(tv1.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetDeploymentName())
		a.Equal(tv1.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetBuildId())
	}, time.Second*5, time.Millisecond*200)

	// Start deployment workflow 2 and wait for the deployment version to exist
	go s.pollFromDeployment(ctx, tv2)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: tv2.DeploymentSeries(),
				BuildId:        tv2.BuildID(),
			},
		})
		a.NoError(err)
		a.Equal(tv2.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetDeploymentName())
		a.Equal(tv2.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetBuildId())
	}, time.Second*5, time.Millisecond*200)

	// non-current deployments have never been used and have no drainage info
	s.checkVersionDrainage(ctx, tv1, nil)
	s.checkVersionDrainage(ctx, tv2, nil)

	// SetCurrent tv1
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		BuildId:        tv1.BuildID(),
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)
	s.checkVersionIsCurrent(ctx, tv1)

	// both still nil since neither are draining
	s.checkVersionDrainage(ctx, tv1, nil)
	s.checkVersionDrainage(ctx, tv2, nil)

	// SetCurrent tv2 --> tv1 starts the child drainage workflow
	_, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv2.DeploymentSeries(),
		BuildId:        tv2.BuildID(),
		Identity:       tv2.ClientIdentity(),
	})
	s.Nil(err)

	// tv1 should now be "draining" for visibilityGracePeriod duration
	s.checkVersionDrainage(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
		LastChangedTime: nil, // don't test this now
		LastCheckedTime: nil, // don't test this now
	})

	time.Sleep(testVersionDrainageVisibilityGracePeriod)

	// tv1 should now be "drained"
	s.checkVersionDrainage(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		LastChangedTime: nil, // don't test this now
		LastCheckedTime: nil, // don't test this now
	})
}

func (s *DeploymentVersionSuite) checkVersionDrainage(
	ctx context.Context,
	tv *testvars.TestVars,
	expectedDrainageInfo *deploymentpb.VersionDrainageInfo,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
		})
		a.NoError(err)

		dInfo := resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo()

		if expectedDrainageInfo == nil {
			a.Nil(dInfo)
			return
		}

		a.Equal(expectedDrainageInfo.Status, dInfo.GetStatus())
		if expectedDrainageInfo.LastCheckedTime != nil {
			a.Equal(expectedDrainageInfo.LastCheckedTime, dInfo.GetLastCheckedTime())
		}
		if expectedDrainageInfo.LastChangedTime != nil {
			a.Equal(expectedDrainageInfo.LastChangedTime, dInfo.GetLastChangedTime())
		}
	}, 5*time.Second, time.Millisecond*100)
}

func (s *DeploymentVersionSuite) checkVersionIsCurrent(ctx context.Context, tv *testvars.TestVars) {
	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
		})
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetDeploymentName())
		a.Equal(tv.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion().GetBuildId())

		a.NotNil(resp.GetWorkerDeploymentVersionInfo().GetCurrentSinceTime())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *DeploymentVersionSuite) checkDescribeWorkflowAfterOverride(
	ctx context.Context,
	wf *commonpb.WorkflowExecution,
	expectedOverride *workflowpb.VersioningOverride,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: wf,
		})
		a.NoError(err)
		a.NotNil(resp)
		a.NotNil(resp.GetWorkflowExecutionInfo())
		a.Equal(expectedOverride.GetBehavior(), resp.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride().GetBehavior())
		a.True(proto.Equal(expectedOverride.GetPinnedVersion(), resp.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride().GetPinnedVersion()))
	}, 5*time.Second, 50*time.Millisecond)
}
