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

package xdc

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/migration"
	"go.temporal.io/server/service/worker/scanner/build_ids"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	UserDataReplicationTestSuite struct {
		xdcBaseSuite
	}
)

func TestUserDataReplicationTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(UserDataReplicationTestSuite))
}

func (s *UserDataReplicationTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():                                    true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key():                                true,
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs.Key():                                    true,
		dynamicconfig.BuildIdScavengerEnabled.Key():                                                   true,
		// Ensure the scavenger can immediately delete build ids that are not in use.
		dynamicconfig.RemovableBuildIdDurationSinceDefault.Key(): time.Microsecond,
	}
	s.setupSuite([]string{"task_queue_repl_active", "task_queue_repl_standby"})
}

func (s *UserDataReplicationTestSuite) SetupTest() {
	s.setupTest()
}

func (s *UserDataReplicationTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *UserDataReplicationTestSuite) TestUserDataIsReplicatedFromActiveToPassive() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	activeFrontendClient := s.cluster1.FrontendClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(testcore.NewContext(), regReq)
	s.NoError(err)

	description, err := activeFrontendClient.DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	standbyMatchingClient := s.cluster2.MatchingClient()

	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.1"},
	})
	s.Require().NoError(err)

	s.Eventually(func() bool {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		response, err := standbyMatchingClient.GetWorkerBuildIdCompatibility(testcore.NewContext(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
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

	// make another change to test that merging works

	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.2"},
	})
	s.Require().NoError(err)

	s.Eventually(func() bool {
		response, err := standbyMatchingClient.GetWorkerBuildIdCompatibility(testcore.NewContext(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
			NamespaceId: description.GetNamespaceInfo().Id,
			Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
				Namespace: namespace,
				TaskQueue: taskQueue,
			},
		})
		if err != nil {
			return false
		}
		return len(response.GetResponse().GetMajorVersionSets()) == 2
	}, 15*time.Second, 500*time.Millisecond)
}

func (s *UserDataReplicationTestSuite) TestUserDataIsReplicatedFromActiveToPassiveV2() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	ctx := testcore.NewContext()
	activeFrontendClient := s.cluster1.FrontendClient()
	standbyMatchingClient := s.cluster2.MatchingClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(ctx, regReq)
	s.NoError(err)

	description, err := activeFrontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	rules, err := activeFrontendClient.GetWorkerVersioningRules(ctx, &workflowservice.GetWorkerVersioningRulesRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
	})
	s.NoError(err)
	s.NotNil(rules)

	_, err = activeFrontendClient.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     namespace,
		TaskQueue:     taskQueue,
		ConflictToken: rules.ConflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertAssignmentRule{
			InsertAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
				Rule: &taskqueuepb.BuildIdAssignmentRule{
					TargetBuildId: "asdf",
				},
			},
		},
	})
	s.NoError(err)

	s.Eventually(func() bool {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		response, err := standbyMatchingClient.GetWorkerVersioningRules(ctx, &matchingservice.GetWorkerVersioningRulesRequest{
			NamespaceId: description.GetNamespaceInfo().Id,
			TaskQueue:   taskQueue,
			Command: &matchingservice.GetWorkerVersioningRulesRequest_Request{
				Request: &workflowservice.GetWorkerVersioningRulesRequest{
					Namespace: namespace,
					TaskQueue: taskQueue,
				},
			},
		})
		if err != nil {
			return false
		}
		return len(response.GetResponse().GetAssignmentRules()) == 1
	}, 15*time.Second, 500*time.Millisecond)

	// make another change to test that merging works

	rules, err = activeFrontendClient.GetWorkerVersioningRules(ctx, &workflowservice.GetWorkerVersioningRulesRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
	})
	s.NoError(err)
	s.NotNil(rules)

	_, err = activeFrontendClient.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     namespace,
		TaskQueue:     taskQueue,
		ConflictToken: rules.ConflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleRedirectRule{
			AddCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: "asdf",
					TargetBuildId: "uiop",
				},
			},
		},
	})
	s.NoError(err)

	s.Eventually(func() bool {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		response, err := standbyMatchingClient.GetWorkerVersioningRules(ctx, &matchingservice.GetWorkerVersioningRulesRequest{
			NamespaceId: description.GetNamespaceInfo().Id,
			TaskQueue:   taskQueue,
			Command: &matchingservice.GetWorkerVersioningRulesRequest_Request{
				Request: &workflowservice.GetWorkerVersioningRulesRequest{
					Namespace: namespace,
					TaskQueue: taskQueue,
				},
			},
		})
		if err != nil {
			return false
		}
		return len(response.GetResponse().GetAssignmentRules()) == 1 &&
			len(response.GetResponse().GetCompatibleRedirectRules()) == 1
	}, 15*time.Second, 500*time.Millisecond)
}

func (s *UserDataReplicationTestSuite) TestUserDataIsReplicatedFromPassiveToActive() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	activeFrontendClient := s.cluster1.FrontendClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(testcore.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	standbyFrontendClient := s.cluster2.FrontendClient()

	s.Eventually(func() bool {
		_, err = standbyFrontendClient.UpdateWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.1"},
		})
		return err == nil
	}, 15*time.Second, 500*time.Millisecond)

	s.Eventually(func() bool {
		response, err := activeFrontendClient.GetWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		if err != nil {
			return false
		}
		return len(response.GetMajorVersionSets()) == 1
	}, 15*time.Second, 500*time.Millisecond)
}

func (s *UserDataReplicationTestSuite) TestUserDataEntriesAreReplicatedOnDemand() {
	ctx := testcore.NewContext()
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	activeFrontendClient := s.cluster1.FrontendClient()
	adminClient := s.cluster1.AdminClient()
	numTaskQueues := 10

	replicationResponse, err := adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	lastMessageId := replicationResponse.GetMessages().GetLastRetrievedMessageId()

	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:         namespace,
		IsGlobalNamespace: true,
		Clusters: []*replicationpb.ClusterReplicationConfig{
			&replicationpb.ClusterReplicationConfig{ClusterName: s.clusterNames[0]},
		},
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err = activeFrontendClient.RegisterNamespace(testcore.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)
	description, err := activeFrontendClient.DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	expectedReplicatedTaskQueues := make(map[string]struct{}, numTaskQueues)
	for i := 0; i < numTaskQueues; i++ {
		taskQueue := fmt.Sprintf("v1q%v", i)
		res, err := activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
				AddNewBuildIdInNewDefaultSet: "v0.1",
			},
		})
		s.NoError(err)
		s.NotNil(res)
		expectedReplicatedTaskQueues[taskQueue] = struct{}{}

		taskQueue2 := fmt.Sprintf("v2q%v", i)
		rules, err := activeFrontendClient.GetWorkerVersioningRules(ctx, &workflowservice.GetWorkerVersioningRulesRequest{
			Namespace: namespace,
			TaskQueue: taskQueue2,
		})
		s.NoError(err)
		s.NotNil(rules)

		rulesRes, err := activeFrontendClient.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
			Namespace:     namespace,
			TaskQueue:     taskQueue2,
			ConflictToken: rules.ConflictToken,
			Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertAssignmentRule{
				InsertAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
					Rule: &taskqueuepb.BuildIdAssignmentRule{
						TargetBuildId: "asdf",
					},
				},
			},
		})
		s.NoError(err)
		s.NotNil(rulesRes)
		expectedReplicatedTaskQueues[taskQueue2] = struct{}{}
	}

	// update namespace to cross clusters
	_, err = activeFrontendClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: s.clusterReplicationConfig(),
		},
	})
	s.NoError(err)
	time.Sleep(cacheRefreshInterval) // nolint:forbidigo

	// we should see one new namespace task in the replication queue
	replicationResponse, err = adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: lastMessageId,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	lastMessageId = replicationResponse.GetMessages().GetLastRetrievedMessageId()
	s.Equal(1, len(replicationResponse.GetMessages().ReplicationTasks))
	task := replicationResponse.GetMessages().ReplicationTasks[0]
	s.Equal(namespace, task.GetNamespaceTaskAttributes().GetInfo().GetName())

	// start force-replicate wf
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.Host().FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	run, err := sysClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 "force-replication-wf",
		TaskQueue:          primitives.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Second * 30,
	}, "force-replication", migration.ForceReplicationParams{
		Namespace:  namespace,
		OverallRps: 10,
	})
	s.NoError(err)
	err = run.Get(ctx, nil)
	s.NoError(err)

	replicationResponse, err = adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: lastMessageId,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)

	// we should see a user data task for all task queues
	seenTaskQueues := make(map[string]struct{}, numTaskQueues)
	for _, task := range replicationResponse.GetMessages().ReplicationTasks {
		if attrs := task.GetTaskQueueUserDataAttributes(); attrs.GetNamespaceId() == description.GetNamespaceInfo().Id {
			seenTaskQueues[attrs.GetTaskQueueName()] = struct{}{}
		}
	}
	s.Equal(expectedReplicatedTaskQueues, seenTaskQueues)

	// failover and check on the other side
	s.failover(namespace, s.clusterNames[1], int64(2), activeFrontendClient)

	activeFrontendClient = s.cluster2.FrontendClient()
	for i := 0; i < numTaskQueues; i++ {
		taskQueue := fmt.Sprintf("v1q%v", i)

		get, err := activeFrontendClient.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		s.NoError(err)
		s.NotNil(get)

		s.NotEmpty(get.MajorVersionSets)

		taskQueue2 := fmt.Sprintf("v2q%v", i)
		rules, err := activeFrontendClient.GetWorkerVersioningRules(ctx, &workflowservice.GetWorkerVersioningRulesRequest{
			Namespace: namespace,
			TaskQueue: taskQueue2,
		})
		s.NoError(err)
		s.NotNil(rules)
		s.NotEmpty(rules.AssignmentRules)
	}
}

func (s *UserDataReplicationTestSuite) TestUserDataTombstonesAreReplicated() {
	s.T().SkipNow() // flaky test
	ctx := testcore.NewContext()
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	activeFrontendClient := s.cluster1.FrontendClient()
	activeAdminClient := s.cluster1.AdminClient()
	standbyAdminClient := s.cluster2.AdminClient()
	taskQueue := "test-task-queue"

	replicationResponse, err := activeAdminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	lastMessageIdActive := replicationResponse.GetMessages().GetLastRetrievedMessageId()

	replicationResponse, err = standbyAdminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	lastMessageIdStandby := replicationResponse.GetMessages().GetLastRetrievedMessageId()

	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err = activeFrontendClient.RegisterNamespace(testcore.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)
	description, err := activeFrontendClient.DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
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

	// start build ID scavenger workflow
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.Host().FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	workflowID := s.T().Name() + "-workflow"
	run, err := sysClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          build_ids.BuildIdScavengerTaskQueueName,
		WorkflowRunTimeout: time.Second * 30,
	}, build_ids.BuildIdScavangerWorkflowName, build_ids.BuildIdScavangerInput{
		IgnoreRetentionTime: true,
	})
	s.NoError(err)
	err = run.Get(ctx, nil)
	s.NoError(err)

	replicationResponse, err = activeAdminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: lastMessageIdActive,
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

	// Add another build ID to verify that tombstones were deleted after the first scavenger run
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
		LastRetrievedMessageId: lastMessageIdActive,
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

	// Add a new build ID in standby cluster to verify it did not persist the replicated tombstones
	standbyFrontendClient := s.cluster2.FrontendClient()

	s.Eventually(func() bool {
		// Wait for propagation
		response, err := standbyFrontendClient.GetWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		if err != nil {
			return false
		}
		return len(response.GetMajorVersionSets()) == 2 && response.MajorVersionSets[1].BuildIds[0] == "v3"
	}, 15*time.Second, 500*time.Millisecond)

	_, err = standbyFrontendClient.UpdateWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "v4",
		},
	})
	s.Require().NoError(err)

	replicationResponse, err = standbyAdminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: lastMessageIdStandby,
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

func (s *UserDataReplicationTestSuite) TestApplyReplicationEventRevivesInUseTombstones() {
	ctx := testcore.NewContext()
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "test-task-queue"
	activeFrontendClient := s.cluster1.FrontendClient()

	_, err := activeFrontendClient.RegisterNamespace(testcore.NewContext(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	})
	s.Require().NoError(err)

	// Create v0 and process a workflow with it - should be revived
	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "v0",
		},
	})
	s.Require().NoError(err)

	_, err = activeFrontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    namespace,
		WorkflowId:   "test",
		RequestId:    uuid.NewString(),
		WorkflowType: &commonpb.WorkflowType{Name: "workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.Require().NoError(err)
	task, err := activeFrontendClient.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId:       "v0",
			UseVersioning: true,
		},
	})
	s.Require().NoError(err)
	_, err = activeFrontendClient.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{
			BuildId:       "v0",
			UseVersioning: true,
		},
	})
	s.Require().NoError(err)

	// Create v0.1 as compatible with v0 - should not be revived
	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                "v0.1",
				ExistingCompatibleBuildId: "v0",
			},
		},
	})
	s.Require().NoError(err)

	// Create v0.2 as compatible with v0 - check later that it is revived
	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                "v0.2",
				ExistingCompatibleBuildId: "v0",
			},
		},
	})
	s.Require().NoError(err)

	// Create v1 as queue default - it never gets deleted
	_, err = activeFrontendClient.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "v1",
		},
	})
	s.Require().NoError(err)

	// Wait for visibility propagation
	s.Eventually(func() bool {
		resp, err := activeFrontendClient.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: namespace,
			Query:     fmt.Sprintf("TaskQueue = %q AND BuildIds = %q", taskQueue, worker_versioning.VersionedBuildIdSearchAttribute("v0")),
		})
		s.Require().NoError(err)
		return resp.Count == 1
	}, time.Second*15, time.Millisecond*150)

	adminClient := s.cluster1.AdminClient()
	replicationResponse, err := adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.Require().NoError(err)
	attrsPreApply := replicationResponse.Messages.ReplicationTasks[len(replicationResponse.Messages.ReplicationTasks)-1].GetTaskQueueUserDataAttributes()
	preApplyClock := common.CloneProto(attrsPreApply.UserData.Clock)

	attrsPreApply.UserData.Clock.WallClock = time.Now().UnixMilli()
	attrsPreApply.UserData.Clock.Version++
	// Delete all v0 buildIds
	attrsPreApply.UserData.VersioningData.VersionSets[0].BuildIds[0].State = persistencespb.STATE_DELETED
	attrsPreApply.UserData.VersioningData.VersionSets[0].BuildIds[0].StateUpdateTimestamp = attrsPreApply.UserData.Clock
	attrsPreApply.UserData.VersioningData.VersionSets[0].BuildIds[1].State = persistencespb.STATE_DELETED
	attrsPreApply.UserData.VersioningData.VersionSets[0].BuildIds[1].StateUpdateTimestamp = attrsPreApply.UserData.Clock
	attrsPreApply.UserData.VersioningData.VersionSets[0].BuildIds[2].State = persistencespb.STATE_DELETED
	attrsPreApply.UserData.VersioningData.VersionSets[0].BuildIds[2].StateUpdateTimestamp = attrsPreApply.UserData.Clock

	matchingClient := s.cluster1.MatchingClient()
	_, err = matchingClient.ApplyTaskQueueUserDataReplicationEvent(ctx, &matchingservice.ApplyTaskQueueUserDataReplicationEventRequest{
		NamespaceId: attrsPreApply.NamespaceId,
		TaskQueue:   taskQueue,
		UserData:    attrsPreApply.UserData,
	})
	s.Require().NoError(err)

	// Check that v0 and v2 were revived
	compat, err := activeFrontendClient.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
	})
	s.Require().NoError(err)
	s.Require().Equal([]*taskqueuepb.CompatibleVersionSet{
		{BuildIds: []string{"v0", "v0.2"}},
		{BuildIds: []string{"v1"}},
	}, compat.GetMajorVersionSets())

	// Check that v0 and v2 were revived in the replication event
	replicationResponse, err = adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.Require().NoError(err)

	attrsPostApply := replicationResponse.Messages.ReplicationTasks[len(replicationResponse.Messages.ReplicationTasks)-1].GetTaskQueueUserDataAttributes()

	s.Require().True(hybrid_logical_clock.Greater(attrsPostApply.UserData.Clock, preApplyClock))

	s.Require().Equal("v0", attrsPostApply.UserData.VersioningData.VersionSets[0].BuildIds[0].Id)
	s.Require().Equal(persistencespb.STATE_ACTIVE, attrsPostApply.UserData.VersioningData.VersionSets[0].BuildIds[0].State)
	s.Require().True(hybrid_logical_clock.Greater(attrsPostApply.UserData.VersioningData.VersionSets[0].BuildIds[0].StateUpdateTimestamp, preApplyClock))

	s.Require().Equal("v0.2", attrsPostApply.UserData.VersioningData.VersionSets[0].BuildIds[1].Id)
	s.Require().Equal(persistencespb.STATE_ACTIVE, attrsPostApply.UserData.VersioningData.VersionSets[0].BuildIds[1].State)
	s.Require().True(hybrid_logical_clock.Greater(attrsPostApply.UserData.VersioningData.VersionSets[0].BuildIds[1].StateUpdateTimestamp, preApplyClock))

	s.Require().Equal("v1", attrsPostApply.UserData.VersioningData.VersionSets[1].BuildIds[0].Id)
	s.Require().Equal(persistencespb.STATE_ACTIVE, attrsPostApply.UserData.VersioningData.VersionSets[1].BuildIds[0].State)
	s.Require().True(hybrid_logical_clock.Equal(attrsPostApply.UserData.VersioningData.VersionSets[1].BuildIds[0].StateUpdateTimestamp, preApplyClock))
}
