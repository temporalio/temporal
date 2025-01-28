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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
			Version:   tv.BuildID(),
		})
		a.NoError(err)

		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

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
			Version:   tv.BuildID(),
		})
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

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

func (s *DeploymentVersionSuite) TestDrainageStatusNil_OverrideUnversioned() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// Starting a deployment workflow
	go s.pollFromDeployment(ctx, tv)

	// Wait for the deployment version to exist
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.BuildID(),
		})
		a.NoError(err)
		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)

	// non-current deployment has never been used and has no drainage info
	s.checkVersionDrainage(ctx, tv, nil)

	// start an unversioned workflow, set pinned deployment override --> deployment should be reachable
	unversionedTQ := "unversioned-test-tq"
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: unversionedTQ}, "wf")
	s.NoError(err)
	unversionedWFExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}

	// set override on our new unversioned workflow
	updateOpts := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: &deploymentpb.WorkerDeploymentVersion{
				Version:        tv.BuildID(),
				DeploymentName: tv.DeploymentSeries(),
			},
		},
	}
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace().String(),
		WorkflowExecution:        unversionedWFExec,
		WorkflowExecutionOptions: updateOpts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), updateOpts))

	// describe workflow and check that the versioning info has the override
	s.checkDescribeWorkflowAfterOverride(ctx, unversionedWFExec, updateOpts.GetVersioningOverride())
	// check that the version is still not draining, since an open workflow is using it via override
	s.checkVersionDrainage(ctx, tv, nil)

	// TODO (carly): test this with SetCurrent one deployment, then SetCurrent another
}

func (s *DeploymentVersionSuite) TestDrainageStatus_SetCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s)
	tv2 := testvars.New(s)

	// Start deployment workflows 1 and wait for the deployment version to exist
	go s.pollFromDeployment(ctx, tv1)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv1.BuildID(),
		})
		a.NoError(err)
		a.Equal(tv1.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv1.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)

	// Start deployment workflows 2 and wait for the deployment version to exist
	go s.pollFromDeployment(ctx, tv2)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv2.BuildID(),
		})
		a.NoError(err)
		a.Equal(tv2.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv2.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)

	// non-current deployment has never been used and has no drainage info
	s.checkVersionDrainage(ctx, tv1, nil)
	s.checkVersionDrainage(ctx, tv2, nil)

	// SetCurrent tv1
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        wrapperspb.String(tv1.BuildID()),
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	s.checkVersionIsCurrent(ctx, tv1)

	// both still nil since neither are draining
	//s.checkVersionDrainage(ctx, tv1, nil)
	//s.checkVersionDrainage(ctx, tv2, nil)

	// SetCurrent tv2
	_, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv2.DeploymentSeries(),
		Version:        wrapperspb.String(tv2.BuildID()),
		Identity:       tv2.ClientIdentity(),
	})
	s.Nil(err)

	// tv1 should now be "drained", tv2 still nil
	s.checkVersionDrainage(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		LastChangedTime: nil, // don't test this now
		LastCheckedTime: nil, // don't test this now
	})
	//s.checkVersionDrainage(ctx, tv1, nil)

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
			Version:   tv.BuildID(),
		})
		a.NoError(err)

		dInfo := resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo()

		if expectedDrainageInfo == nil {
			a.Nil(dInfo)
			return
		}

		// todo carly: remove // try seeing if a drainage workflow started at all:
		//listResp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		//	Namespace:     s.Namespace().String(),
		//	PageSize:      0,
		//	NextPageToken: nil,
		//	Query:         fmt.Sprintf("WorkflowType = 'temporal-sys-worker-deployment-drainage-workflow'"),
		//})
		//a.NoError(err)
		//a.Equal(1, len(listResp.GetExecutions()))
		//if len(listResp.GetExecutions()) > 0 {
		//	exec := listResp.GetExecutions()[0]
		//	fmt.Printf("%+v", exec)
		//
		//}

		a.Equal(expectedDrainageInfo.Status, dInfo.GetStatus())
		if expectedDrainageInfo.LastCheckedTime != nil {
			a.Equal(expectedDrainageInfo.LastCheckedTime, dInfo.GetLastCheckedTime())
		}
		if expectedDrainageInfo.LastChangedTime != nil {
			a.Equal(expectedDrainageInfo.LastChangedTime, dInfo.GetLastChangedTime())
		}
	}, time.Second*10, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) checkVersionIsCurrent(ctx context.Context, tv *testvars.TestVars) {
	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.BuildID(),
		})
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentName())
		a.Equal(tv.BuildID(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

		a.True(resp.GetWorkerDeploymentVersionInfo().GetIsCurrent())
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
