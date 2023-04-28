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
	"errors"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
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
	s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	s.dynamicConfigOverrides[dynamicconfig.EnableWorkerVersioning] = true
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

	s.retryReasonably(func() error {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		response, err := standbyMatchingClient.GetWorkerBuildIdCompatibility(tests.NewContext(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
			NamespaceId: description.GetNamespaceInfo().Id,
			Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
				Namespace: namespace,
				TaskQueue: taskQueue,
			},
		})
		if err != nil {
			return err
		}
		if len(response.GetResponse().GetMajorVersionSets()) != 1 {
			return errors.New("passive has no versioning data")
		}
		return nil
	})
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
	s.cluster1.GetExecutionManager()

	s.retryReasonably(func() error {
		// Call matching directly in case frontend is configured to redirect API calls to the active cluster
		_, err = standbyFrontendClient.UpdateWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.1"},
		})
		return err
	})

	s.retryReasonably(func() error {
		response, err := activeFrontendClient.GetWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		if err != nil {
			return err
		}
		if len(response.GetMajorVersionSets()) != 1 {
			return errors.New("active has no versioning data")
		}
		return nil
	})
}

func (s *userDataReplicationTestSuite) TestUserDataEntriesAreReplicatedOnDemand() {
	ctx := tests.NewContext()
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	activeFrontendClient := s.cluster1.GetFrontendClient()
	numTaskQueues := 20
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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
	activeMatchingClient := s.cluster1.GetMatchingClient()
	adminClient := s.cluster1.GetAdminClient()
	var nextPageToken []byte
	pageSize := 10

	for i := 0; i < 2; i++ {
		response, err := activeMatchingClient.SeedReplicationQueueWithUserDataEntries(ctx, &matchingservice.SeedReplicationQueueWithUserDataEntriesRequest{
			NamespaceId:   description.GetNamespaceInfo().Id,
			PageSize:      int32(pageSize),
			NextPageToken: nextPageToken,
		})
		s.NoError(err)
		if i == 0 {
			s.True(len(response.NextPageToken) > 0)
		} else {
			s.True(len(response.NextPageToken) == 0)
		}
		nextPageToken = response.NextPageToken

		replicationResponse, err := adminClient.GetNamespaceReplicationMessages(ctx, &adminservice.GetNamespaceReplicationMessagesRequest{
			ClusterName:            "follower",
			LastRetrievedMessageId: -1,
			LastProcessedMessageId: -1,
		})
		s.NoError(err)
		numReplicationTasks := len(replicationResponse.GetMessages().ReplicationTasks)
		s.Equal(numReplicationTasks, numTaskQueues+1+(i+1)*pageSize)

		if i == 1 {
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
	}

}
