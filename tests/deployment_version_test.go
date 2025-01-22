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
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/tests/testcore"
)

const (
	maxConcurrentBatchOperations = 3
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
func (s *DeploymentVersionSuite) pollFromDeployment(ctx context.Context, taskQueue *taskqueuepb.TaskQueue,
	deploymentName string, version string) {
	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  "random",
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			UseVersioning:        true,
			BuildId:              version,
			DeploymentSeriesName: deploymentName,
		},
	})
}

func (s *DeploymentVersionSuite) pollActivityFromDeployment(ctx context.Context, taskQueue *taskqueuepb.TaskQueue,
	deploymentName string, version string) {
	_, _ = s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  "random",
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			UseVersioning:        true,
			BuildId:              version,
			DeploymentSeriesName: deploymentName,
		},
	})
}

func (s *DeploymentVersionSuite) TestDescribeVersion_RegisterTaskQueue() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	numberOfDeployments := 1

	// Starting a deployment workflow
	go s.pollFromDeployment(ctx, tv.TaskQueue(), tv.DeploymentName(), tv.DeploymentVersion())

	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersion(),
		})
		a.NoError(err)

		a.Equal(tv.DeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv.DeploymentVersion(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

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
		tq := &taskqueuepb.TaskQueue{Name: root.TaskQueue().NormalPartition(p).RpcName(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
		for i := 0; i < 3; i++ {
			go s.pollFromDeployment(ctx, tq, tv.DeploymentName(), tv.DeploymentVersion())
			go s.pollActivityFromDeployment(ctx, tq, tv.DeploymentName(), tv.DeploymentVersion())
		}
	}

	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersion(),
		})
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv.DeploymentVersion(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

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
