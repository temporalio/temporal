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

//go:build !race

// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sw "go.temporal.io/server/service/worker"
	"go.temporal.io/server/service/worker/migration"
	"go.temporal.io/server/service/worker/scanner/build_ids"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/tests"
)

type (
	userDataReplicationTestSuite struct {
		xdcBaseSuite
	}
)

func TestUserDataReplicationTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(userDataReplicationTestSuite))
}

func (s *userDataReplicationTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs: true,
		dynamicconfig.BuildIdScavengerEnabled:                true,
	}
	s.setupSuite([]string{"task_queue_repl_active", "task_queue_repl_standby"})
}

func (s *userDataReplicationTestSuite) SetupTest() {
	s.setupTest()
}

func (s *userDataReplicationTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *userDataReplicationTestSuite) TestUserDataIsReplicatedFromActiveToPassive() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	activeFrontendClient := s.cluster1.GetFrontendClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)
	description, err := activeFrontendClient.DescribeNamespace(tests.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	standbyMatchingClient := s.cluster2.GetMatchingClient()

	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.1"},
	})
	s.Require().NoError(err)

	s.Eventually(func() bool {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		response, err := standbyMatchingClient.GetWorkerBuildIdCompatibility(tests.NewContext(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
			NamespaceId: description.GetNamespaceInfo().Id,
			Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
				Namespace: namespace,
				TaskQueue: taskQueue,
			},
		})
		if err != nil {
			return false
		}
		return len(response.GetResponse().GetMajorVersionSets()) == 1
	}, 15*time.Second, 500*time.Millisecond)
}

func (s *userDataReplicationTestSuite) TestUserDataIsReplicatedFromPassiveToActive() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	activeFrontendClient := s.cluster1.GetFrontendClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	standbyFrontendClient := s.cluster2.GetFrontendClient()

	s.Eventually(func() bool {
		_, err = standbyFrontendClient.UpdateWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.1"},
		})
		return err == nil
	}, 15*time.Second, 500*time.Millisecond)

	s.Eventually(func() bool {
		response, err := activeFrontendClient.GetWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		if err != nil {
			return false
		}
		return len(response.GetMajorVersionSets()) == 1
	}, 15*time.Second, 500*time.Millisecond)
}

func (s *userDataReplicationTestSuite) TestUserDataEntriesAreReplicatedOnDemand() {
	ctx := tests.NewContext()
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	activeFrontendClient := s.cluster1.GetFrontendClient()
	numTaskQueues := 20
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)
	description, err := activeFrontendClient.DescribeNamespace(tests.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	exectedReplicatedTaskQueues := make(map[string]struct{}, numTaskQueues)
	for i := 0; i < numTaskQueues; i++ {
		taskQueue := fmt.Sprintf("q%v", i)
		res, err := activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
				AddNewBuildIdInNewDefaultSet: "v0.1",
			},
		})
		exectedReplicatedTaskQueues[taskQueue] = struct{}{}
		s.NoError(err)
		s.NotNil(res)
	}
	adminClient := s.cluster1.GetAdminClient()

	// start force-replicate wf
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	workflowID4 := "force-replication-wf-4"
	run, err := sysClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID4,
		TaskQueue:          sw.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Second * 30,
	}, "force-replication", migration.ForceReplicationParams{
		Namespace:  namespace,
		OverallRps: 10,
	})
	s.NoError(err)
	err = run.Get(ctx, nil)
	s.NoError(err)

	replicationResponse, err := adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	numReplicationTasks := len(replicationResponse.GetMessages().ReplicationTasks)
	s.Equal(numReplicationTasks, numTaskQueues*2+1)

	lastTasks := replicationResponse.GetMessages().ReplicationTasks[numReplicationTasks-numTaskQueues:]
	s.Equal(numTaskQueues, len(lastTasks))
	seenTaskQueues := make(map[string]struct{}, numTaskQueues)
	// Check the seeded messages
	for _, task := range lastTasks {
		s.Equal(enums.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA, task.TaskType)
		attrs := task.GetTaskQueueUserDataAttributes()
		s.Equal(description.GetNamespaceInfo().Id, attrs.NamespaceId)
		seenTaskQueues[attrs.TaskQueueName] = struct{}{}
	}

	s.Equal(exectedReplicatedTaskQueues, seenTaskQueues)
}

func (s *userDataReplicationTestSuite) TestUserDataTombstonesAreReplicated() {
	// Advanced visibility only enabled by default in SQLite
	if tests.TestFlags.PersistenceDriver != sqlite.PluginName {
		s.T().Skip("Test requires advanced visibility")
	}
	ctx := tests.NewContext()
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	activeFrontendClient := s.cluster1.GetFrontendClient()
	taskQueue := "test-task-queue"
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)
	description, err := activeFrontendClient.DescribeNamespace(tests.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	for i := 0; i < 3; i++ {
		buildId := fmt.Sprintf("v%d", i)
		_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
				AddNewBuildIdInNewDefaultSet: buildId,
			},
		})
		s.NoError(err)
	}
	activeAdminClient := s.cluster1.GetAdminClient()

	// start build id scavenger workflow
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	workflowID := s.T().Name() + "-workflow"
	run, err := sysClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          build_ids.BuildIdScavengerTaskQueueName,
		WorkflowRunTimeout: time.Second * 30,
	}, build_ids.BuildIdScavangerWorkflowName)
	s.NoError(err)
	err = run.Get(ctx, nil)
	s.NoError(err)

	replicationResponse, err := activeAdminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	numReplicationTasks := len(replicationResponse.GetMessages().ReplicationTasks)
	task := replicationResponse.GetMessages().ReplicationTasks[numReplicationTasks-1]

	s.Equal(enums.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA, task.TaskType)
	attrs := task.GetTaskQueueUserDataAttributes()
	s.Equal(description.GetNamespaceInfo().Id, attrs.NamespaceId)
	s.Equal(taskQueue, attrs.TaskQueueName)
	s.Equal(3, len(attrs.UserData.VersioningData.VersionSets))
	s.Equal("v0", attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_DELETED, attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].State)
	s.Equal("v1", attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_DELETED, attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].State)
	s.Equal("v2", attrs.UserData.VersioningData.VersionSets[2].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[2].BuildIds[0].State)

	// Add another build id to verify that tombstones were deleted after the first scavenger run
	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "v3",
		},
	})
	s.NoError(err)

	replicationResponse, err = activeAdminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	numReplicationTasks = len(replicationResponse.GetMessages().ReplicationTasks)
	task = replicationResponse.GetMessages().ReplicationTasks[numReplicationTasks-1]

	s.Equal(enums.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA, task.TaskType)
	attrs = task.GetTaskQueueUserDataAttributes()
	s.Equal(description.GetNamespaceInfo().Id, attrs.NamespaceId)
	s.Equal(taskQueue, attrs.TaskQueueName)
	s.Equal(2, len(attrs.UserData.VersioningData.VersionSets))
	s.Equal("v2", attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].State)
	s.Equal("v3", attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].State)

	// Add a new build id in standby cluster to verify it did not persist the replicated tombstones
	standbyFrontendClient := s.cluster2.GetFrontendClient()

	s.Eventually(func() bool {
		// Wait for propagation
		response, err := standbyFrontendClient.GetWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		if err != nil {
			return false
		}
		return len(response.GetMajorVersionSets()) == 2 && response.MajorVersionSets[1].BuildIds[0] == "v3"
	}, 15*time.Second, 500*time.Millisecond)

	_, err = standbyFrontendClient.UpdateWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "v4",
		},
	})
	s.Require().NoError(err)

	standbyAdminClient := s.cluster2.GetAdminClient()
	replicationResponse, err = standbyAdminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	numReplicationTasks = len(replicationResponse.GetMessages().ReplicationTasks)
	task = replicationResponse.GetMessages().ReplicationTasks[numReplicationTasks-1]

	s.Equal(enums.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA, task.TaskType)
	attrs = task.GetTaskQueueUserDataAttributes()
	s.Equal(description.GetNamespaceInfo().Id, attrs.NamespaceId)
	s.Equal(taskQueue, attrs.TaskQueueName)
	s.Equal(3, len(attrs.UserData.VersioningData.VersionSets))
	s.Equal("v2", attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].State)
	s.Equal("v3", attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].State)
	s.Equal("v4", attrs.UserData.VersioningData.VersionSets[2].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[2].BuildIds[0].State)
}
