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
	"strings"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
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

var (
	testRandomMetadataValue = []byte("random metadata value")
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

func (s *DeploymentVersionSuite) startVersionWorkflow(ctx context.Context, tv *testvars.TestVars) {
	go s.pollFromDeployment(ctx, tv)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

		newResp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)

		var versionSummaryNames []string
		for _, versionSummary := range newResp.GetWorkerDeploymentInfo().GetVersionSummaries() {
			versionSummaryNames = append(versionSummaryNames, versionSummary.GetVersion())
		}
		a.Contains(versionSummaryNames, tv.DeploymentVersionString())

	}, time.Second*5, time.Millisecond*200)
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
			Version:   tv.DeploymentVersionString(),
		})
		a.NoError(err)

		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

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
			Version:   tv.DeploymentVersionString(),
		})
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

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
	short := fmt.Sprintf("%s-%08x",
		fullName[len(fullName)-21:],
		farm.Fingerprint32([]byte(fullName)),
	)
	return strings.Replace(short, ".", "|", -1)
}

//nolint:forbidigo
func (s *DeploymentVersionSuite) TestDrainageStatus_SetCurrentVersion_NoOpenWFs() {
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
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv1.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)

	// Start deployment workflow 2 and wait for the deployment version to exist
	go s.pollFromDeployment(ctx, tv2)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv2.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv2.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)

	// non-current deployments have never been used and have no drainage info
	s.checkVersionDrainage(ctx, tv1, &deploymentpb.VersionDrainageInfo{})
	s.checkVersionDrainage(ctx, tv2, &deploymentpb.VersionDrainageInfo{})

	// SetCurrent tv1
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv1.DeploymentSeries(),
		Version:                 tv1.DeploymentVersionString(),
		Identity:                tv1.ClientIdentity(),
		IgnoreMissingTaskQueues: true,
	})
	s.Nil(err)
	s.checkVersionIsCurrent(ctx, tv1)

	// both still nil since neither are draining
	s.checkVersionDrainage(ctx, tv1, &deploymentpb.VersionDrainageInfo{})
	s.checkVersionDrainage(ctx, tv2, &deploymentpb.VersionDrainageInfo{})

	// SetCurrent tv2 --> tv1 starts the child drainage workflow
	_, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv2.DeploymentSeries(),
		Version:                 tv2.DeploymentVersionString(),
		Identity:                tv2.ClientIdentity(),
		IgnoreMissingTaskQueues: true,
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

	// todo carly: not really sure how to check lastChangedTime / lastCheckedTime
}

func (s *DeploymentVersionSuite) TestDrainageStatus_SetCurrentVersion_YesOpenWFs() {
	// todo carly: test with open workflows on the draining version that then complete
}

func (s *DeploymentVersionSuite) TestDrainageStatus_SetRampingVersion_NoOpenWFs() {
	// todo carly: test with open workflows on the draining version that then complete
}

func (s *DeploymentVersionSuite) TestDrainageStatus_SetRampingVersion_YesOpenWFs() {
	// todo carly: test with open workflows on the draining version that then complete
}

func (s *DeploymentVersionSuite) TestDeleteVersion_DeleteCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Create a deployment version
	s.startVersionWorkflow(ctx, tv1)

	// set version as current
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        tv1.DeploymentVersionString(),
		ConflictToken:  nil,
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// deleting this version should fail since the version is current
	s.tryDeleteVersion(ctx, tv1, false)

}

func (s *DeploymentVersionSuite) TestDeleteVersion_DeleteRampedVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Create a deployment version
	s.startVersionWorkflow(ctx, tv1)

	// set version as ramping
	_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        tv1.DeploymentVersionString(),
		ConflictToken:  nil,
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// deleting this version should fail since the version is ramping
	s.tryDeleteVersion(ctx, tv1, false)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_NotDrained() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Version is not "drained" so delete should fail
	s.tryDeleteVersion(ctx, tv1, false)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_Drained_But_Pollers_Exist() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Make the version current
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        tv1.DeploymentVersionString(),
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// Start another version workflow
	tv2 := testvars.New(s).WithBuildIDNumber(2)
	s.startVersionWorkflow(ctx, tv2)

	// Setting this version to current should start the drainage workflow for version1
	_, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv2.DeploymentSeries(),
		Version:                 tv2.DeploymentVersionString(),
		Identity:                tv2.ClientIdentity(),
		IgnoreMissingTaskQueues: true,
	})
	s.Nil(err)

	// Signal the first version to be drained. Only do this in tests.
	versionWorkflowID := worker_versioning.GenerateVersionWorkflowID(tv1.DeploymentSeries(), tv1.BuildID())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: versionWorkflowID,
	}
	input := &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		LastChangedTime: timestamppb.New(time.Now()),
		LastCheckedTime: timestamppb.New(time.Now()),
	}
	marshaledData, err := input.Marshal()
	s.NoError(err)
	signalPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("binary/protobuf"),
				},
				Data: marshaledData,
			},
		},
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.SyncDrainageSignalName, signalPayload, tv1.ClientIdentity())
	s.Nil(err)

	// Version will bypass "drained" check but delete should still fail since we have active pollers.
	s.tryDeleteVersion(ctx, tv1, false)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_ValidDelete() {
	s.T().Skip("skipping this test for now until I make TTL of pollerHistoryTTL configurable by dynamic config.")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Signal the first version to be drained. Only do this in tests.
	versionWorkflowID := worker_versioning.GenerateVersionWorkflowID(tv1.DeploymentSeries(), tv1.BuildID())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: versionWorkflowID,
	}
	input := &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		LastChangedTime: timestamppb.New(time.Now()),
		LastCheckedTime: timestamppb.New(time.Now()),
	}
	marshaledData, err := input.Marshal()
	s.NoError(err)
	signalPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("binary/protobuf"),
				},
				Data: marshaledData,
			},
		},
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.SyncDrainageSignalName, signalPayload, tv1.ClientIdentity())
	s.Nil(err)

	// Wait for pollers going away
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv1.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		assert.NoError(t, err)
		assert.Empty(t, resp.Pollers)
	}, 10*time.Second, time.Second)

	// delete succeeds
	s.tryDeleteVersion(ctx, tv1, true)

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		if resp != nil {
			for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
				a.NotEqual(tv1.DeploymentVersionString(), vs.Version)
			}
		}
	}, time.Second*5, time.Millisecond*200)

	// idempotency check: deleting the same version again should succeed
	s.tryDeleteVersion(ctx, tv1, true)
}

// VersionMissingTaskQueues
func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_InvalidSetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())

	// Start deployment workflow 1 and wait for the deployment version to exist
	pollerCtx1, pollerCancel1 := context.WithCancel(ctx)
	s.startVersionWorkflow(pollerCtx1, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        tv1.DeploymentVersionString(),
		ConflictToken:  nil,
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := testvars.New(s).WithBuildIDNumber(2).WithTaskQueue(testvars.New(s.T()).Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// Cancel pollers on task_queue_1 to increase the backlog of tasks
	pollerCancel1()

	// Start a workflow on task_queue_1 to increase the add rate
	s.startWorkflow(tv1, tv1.VersioningOverridePinned())

	// SetCurrent tv2
	_, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv2.DeploymentSeries(),
		Version:                 tv2.DeploymentVersionString(),
		ConflictToken:           nil,
		Identity:                tv2.ClientIdentity(),
		IgnoreMissingTaskQueues: false,
	})

	// SetCurrent should fail since task_queue_1 does not have a current version than the deployment's existing current version
	// and it either has a backlog of tasks being present or an add rate > 0.
	s.Error(err)

}

func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_ValidSetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)

	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        tv1.DeploymentVersionString(),
		ConflictToken:  nil,
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := tv.WithBuildIDNumber(2).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// SetCurrent tv2
	_, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv2.DeploymentSeries(),
		Version:                 tv2.DeploymentVersionString(),
		ConflictToken:           nil,
		Identity:                tv2.ClientIdentity(),
		IgnoreMissingTaskQueues: false,
	})

	// SetCurrent tv2 should succeed as task_queue_1, despite missing from the new current version, has no backlogged tasks/add-rate > 0
	s.Nil(err)
}

func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_InvalidSetRampingVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())

	// Start deployment workflow 1 and wait for the deployment version to exist
	pollerCtx1, pollerCancel1 := context.WithCancel(ctx)
	s.startVersionWorkflow(pollerCtx1, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        tv1.DeploymentVersionString(),
		ConflictToken:  nil,
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := tv.WithBuildIDNumber(2).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// Cancel pollers on task_queue_1 to increase the backlog of tasks
	pollerCancel1()

	// Start a workflow on task_queue_1 to increase the add rate
	s.startWorkflow(tv1, tv1.VersioningOverridePinned())

	// SetRampingVersion to tv2
	_, err = s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv2.DeploymentSeries(),
		Version:                 tv2.DeploymentVersionString(),
		ConflictToken:           nil,
		Identity:                tv2.ClientIdentity(),
		IgnoreMissingTaskQueues: false,
	})

	// SetRampingVersion should fail since task_queue_1 does not have a current version than the deployment's existing current version
	// and it either has a backlog of tasks being present or an add rate > 0.
	s.Error(err)
}

func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_ValidSetRampingVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Version:        tv1.DeploymentVersionString(),
		ConflictToken:  nil,
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := tv.WithBuildIDNumber(2).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// SetRampingVersion to tv2
	_, err = s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv2.DeploymentSeries(),
		Version:                 tv2.DeploymentVersionString(),
		ConflictToken:           nil,
		Identity:                tv2.ClientIdentity(),
		IgnoreMissingTaskQueues: false,
	})

	// SetRampingVersion to tv2 should succeed as task_queue_1, despite missing from the new current version, has no backlogged tasks/add-rate > 0
	s.Nil(err)
}

func (s *DeploymentVersionSuite) startWorkflow(
	tv *testvars.TestVars,
	override *workflowpb.VersioningOverride,
) string {
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.Any().String(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.WorkerIdentity(),
		VersioningOverride: override,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	return we.GetRunId()
}

func (s *DeploymentVersionSuite) tryDeleteVersion(
	ctx context.Context,
	tv *testvars.TestVars,
	expectSuccess bool,
) {
	_, err := s.FrontendClient().DeleteWorkerDeploymentVersion(ctx, &workflowservice.DeleteWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		Version:   tv.DeploymentVersionString(),
	})
	if expectSuccess {
		s.Nil(err)
	} else {
		s.Error(err)
	}
}

func (s *DeploymentVersionSuite) TestUpdateVersionMetadata() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	metadata := map[string]*commonpb.Payload{
		"key1": {Data: testRandomMetadataValue},
		"key2": {Data: testRandomMetadataValue},
	}

	_, err := s.FrontendClient().UpdateWorkerDeploymentVersionMetadata(ctx, &workflowservice.UpdateWorkerDeploymentVersionMetadataRequest{
		Namespace:     s.Namespace().String(),
		Version:       tv1.DeploymentVersionString(),
		UpsertEntries: metadata,
		RemoveEntries: nil,
	})
	s.NoError(err)

	resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		Version:   tv1.DeploymentVersionString(),
	})
	s.NoError(err)

	// validating the metadata
	entries := resp.GetWorkerDeploymentVersionInfo().GetMetadata().GetEntries()
	s.Equal(2, len(entries))
	s.Equal(testRandomMetadataValue, entries["key1"].Data)
	s.Equal(testRandomMetadataValue, entries["key2"].Data)

	// Remove all the entries
	_, err = s.FrontendClient().UpdateWorkerDeploymentVersionMetadata(ctx, &workflowservice.UpdateWorkerDeploymentVersionMetadataRequest{
		Namespace:     s.Namespace().String(),
		Version:       tv1.DeploymentVersionString(),
		UpsertEntries: nil,
		RemoveEntries: []string{"key1", "key2"},
	})
	s.NoError(err)

	resp, err = s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		Version:   tv1.DeploymentVersionString(),
	})
	s.NoError(err)
	entries = resp.GetWorkerDeploymentVersionInfo().GetMetadata().GetEntries()
	s.Equal(0, len(entries))
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
			Version:   tv.DeploymentVersionString(),
		})
		a.NoError(err)

		dInfo := resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo()

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
			Version:   tv.DeploymentVersionString(),
		})
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())

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
		a.Equal(expectedOverride.GetPinnedVersion(), resp.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride().GetPinnedVersion())
	}, 5*time.Second, 50*time.Millisecond)
}
