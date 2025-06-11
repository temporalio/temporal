package xdc

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
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
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
	}{
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &UserDataReplicationTestSuite{}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
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
	s.setupSuite()
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
	activeFrontendClient := s.clusters[0].FrontendClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusters[0].ClusterName(),
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(testcore.NewContext(), regReq)
	s.NoError(err)

	description, err := activeFrontendClient.DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	standbyMatchingClient := s.clusters[1].MatchingClient()

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
	}, replicationWaitTime, replicationCheckInterval)

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
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *UserDataReplicationTestSuite) TestUserDataIsReplicatedFromActiveToPassiveV2() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	ctx := testcore.NewContext()
	activeFrontendClient := s.clusters[0].FrontendClient()
	standbyMatchingClient := s.clusters[1].MatchingClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusters[0].ClusterName(),
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
	}, replicationWaitTime, replicationCheckInterval)

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
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *UserDataReplicationTestSuite) TestUserDataIsReplicatedFromActiveToPassiveV3() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	ctx := testcore.NewContext()
	activeFrontendClient := s.clusters[0].FrontendClient()
	activeMatchingClient := s.clusters[0].MatchingClient()
	standbyMatchingClient := s.clusters[1].MatchingClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusters[0].ClusterName(),
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(ctx, regReq)
	s.NoError(err)

	description, err := activeFrontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.Require().NoError(err)

	expectedVersionData := &deploymentspb.DeploymentVersionData{
		Version: &deploymentspb.WorkerDeploymentVersion{
			BuildId:        "v1",
			DeploymentName: "d1",
		},
		RampingSinceTime: timestamp.TimePtr(time.Now()),
		RampPercentage:   10,
	}

	_, err = activeMatchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    description.GetNamespaceInfo().GetId(),
		TaskQueue:      taskQueue,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY, enumspb.TASK_QUEUE_TYPE_NEXUS},
		Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
			UpdateVersionData: expectedVersionData,
		},
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		response, err := standbyMatchingClient.GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   description.GetNamespaceInfo().Id,
			TaskQueue:     taskQueue,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		a := assert.New(t)
		a.NoError(err)
		if perType := response.GetUserData().GetData().GetPerType(); a.NotNil(perType) {
			for tqType := 1; tqType <= 3; tqType++ {
				data := perType[int32(tqType)].GetDeploymentData()
				if a.Equal(1, len(data.GetVersions())) {
					a.True(data.GetVersions()[0].Equal(expectedVersionData))
				}
			}
		}
	}, replicationWaitTime, replicationCheckInterval)

	// make another change to test that merging works

	expectedVersionData.RampPercentage = 20
	_, err = activeMatchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    description.GetNamespaceInfo().GetId(),
		TaskQueue:      taskQueue,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY, enumspb.TASK_QUEUE_TYPE_NEXUS},
		Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
			UpdateVersionData: expectedVersionData,
		},
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		response, err := standbyMatchingClient.GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   description.GetNamespaceInfo().Id,
			TaskQueue:     taskQueue,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		a := assert.New(t)
		a.NoError(err)
		if perType := response.GetUserData().GetData().GetPerType(); a.NotNil(perType) {
			for tqType := 1; tqType <= 3; tqType++ {
				data := perType[int32(tqType)].GetDeploymentData()
				if a.Equal(1, len(data.GetVersions())) {
					a.True(data.GetVersions()[0].Equal(expectedVersionData))
				}
			}
		}
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *UserDataReplicationTestSuite) TestUserDataIsReplicatedFromPassiveToActive() {
	namespace := s.createGlobalNamespace()
	taskQueue := "versioned"
	activeFrontendClient := s.clusters[0].FrontendClient()
	standbyFrontendClient := s.clusters[1].FrontendClient()

	_, err := standbyFrontendClient.UpdateWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.1"},
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		response, err := activeFrontendClient.GetWorkerBuildIdCompatibility(testcore.NewContext(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		require.NoError(t, err)
		require.Len(t, response.GetMajorVersionSets(), 1)
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *UserDataReplicationTestSuite) TestUserDataEntriesAreReplicatedOnDemand() {
	ctx := testcore.NewContext()
	activeFrontendClient := s.clusters[0].FrontendClient()
	adminClient := s.clusters[0].AdminClient()
	numTaskQueues := 10

	replicationResponse, err := adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
		ClusterName:            "follower",
		LastRetrievedMessageId: -1,
		LastProcessedMessageId: -1,
	})
	s.NoError(err)
	lastMessageId := replicationResponse.GetMessages().GetLastRetrievedMessageId()

	namespace := s.createNamespaceInCluster0(true)
	description, err := activeFrontendClient.DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.NoError(err)

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
	s.updateNamespaceClusters(namespace, 0, s.clusters)

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
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
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
	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	activeFrontendClient = s.clusters[1].FrontendClient()
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
	activeFrontendClient := s.clusters[0].FrontendClient()
	activeAdminClient := s.clusters[0].AdminClient()
	standbyAdminClient := s.clusters[1].AdminClient()
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

	namespace := s.createGlobalNamespace()
	description, err := activeFrontendClient.DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	s.NoError(err)

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
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
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

	s.Equal(enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA, task.TaskType)
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

	s.Equal(enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA, task.TaskType)
	attrs = task.GetTaskQueueUserDataAttributes()
	s.Equal(description.GetNamespaceInfo().Id, attrs.NamespaceId)
	s.Equal(taskQueue, attrs.TaskQueueName)
	s.Equal(2, len(attrs.UserData.VersioningData.VersionSets))
	s.Equal("v2", attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[0].BuildIds[0].State)
	s.Equal("v3", attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].Id)
	s.Equal(persistencespb.STATE_ACTIVE, attrs.UserData.VersioningData.VersionSets[1].BuildIds[0].State)

	// Add a new build ID in standby cluster to verify it did not persist the replicated tombstones
	standbyFrontendClient := s.clusters[1].FrontendClient()

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
	}, replicationWaitTime, replicationCheckInterval)

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

	s.Equal(enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA, task.TaskType)
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
	activeFrontendClient := s.clusters[0].FrontendClient()

	_, err := activeFrontendClient.RegisterNamespace(testcore.NewContext(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusters[0].ClusterName(),
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

	adminClient := s.clusters[0].AdminClient()
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

	matchingClient := s.clusters[0].MatchingClient()
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
