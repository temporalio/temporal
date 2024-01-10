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
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/tests"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	streamBasedReplicationTestSuite struct {
		xdcBaseSuite
		controller    *gomock.Controller
		namespaceName string
		namespaceID   string
		serializer    serialization.Serializer
		generator     test.Generator
	}
)

func TestStreamBasedReplicationTestSuite(t *testing.T) {
	suite.Run(t, new(streamBasedReplicationTestSuite))
}

func (s *streamBasedReplicationTestSuite) SetupSuite() {
	s.controller = gomock.NewController(s.T())
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.EnableReplicationStream:       true,
		dynamicconfig.EnableEagerNamespaceRefresher: true,
		dynamicconfig.EnableReplicationTaskBatching: true,
	}
	s.logger = log.NewNoopLogger()
	s.serializer = serialization.NewSerializer()
	s.setupSuite(
		[]string{
			"active",
			"standby",
		},
		tests.WithFxOptionsForService(primitives.AllServices,
			fx.Decorate(
				func() config.DCRedirectionPolicy {
					return config.DCRedirectionPolicy{Policy: "noop"}
				},
			),
		),
	)
	ctx := context.Background()
	s.namespaceName = "replication-test"
	_, err := s.cluster1.GetFrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: s.namespaceName,
		Clusters:  s.clusterReplicationConfig(),
		// The first cluster is the active cluster.
		ActiveClusterName: s.clusterNames[0],
		// Needed so that the namespace is replicated.
		IsGlobalNamespace: true,
		// This is a required parameter.
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.Require().NoError(err)
	err = s.waitUntilNamespaceReplicated(ctx, s.namespaceName)
	s.Require().NoError(err)

	nsRes, _ := s.cluster1.GetFrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: s.namespaceName,
	})

	s.namespaceID = nsRes.NamespaceInfo.GetId()
	s.generator = test.InitializeHistoryEventGenerator("namespace", "ns-id", 1)
}

func (s *streamBasedReplicationTestSuite) TearDownSuite() {
	if s.generator != nil {
		s.generator.Reset()
	}
	s.controller.Finish()
	s.tearDownSuite()
}

func (s *streamBasedReplicationTestSuite) SetupTest() {
	s.setupTest()
}

func (s *streamBasedReplicationTestSuite) TestReplicateHistoryEvents_ForceReplicationScenario() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	// let's import some events into cluster 1
	historyClient1 := s.cluster1.GetHistoryClient()
	executions := s.importTestEvents(historyClient1, namespace.Name(s.namespaceName), namespace.ID(s.namespaceID), []int64{3, 13, 2, 202, 302, 402, 602, 502, 802, 1002, 902, 702, 1102})

	// let's trigger replication by calling GenerateLastHistoryReplicationTasks. This is also used by force replication logic
	for _, execution := range executions {
		_, err := historyClient1.GenerateLastHistoryReplicationTasks(ctx, &historyservice.GenerateLastHistoryReplicationTasksRequest{
			NamespaceId: s.namespaceID,
			Execution:   execution,
		})
		s.NoError(err)
	}

	time.Sleep(10 * time.Second)
	for _, execution := range executions {
		err := s.assertHistoryEvents(ctx, s.namespaceID, execution.GetWorkflowId(), execution.GetRunId())
		s.NoError(err)
	}
}

func (s *streamBasedReplicationTestSuite) importTestEvents(
	historyClient tests.HistoryClient,
	namespaceName namespace.Name,
	namespaceId namespace.ID,
	versions []int64,
) []*commonpb.WorkflowExecution {
	executions := []*commonpb.WorkflowExecution{}
	s.generator.Reset()
	var runID string
	for _, version := range versions {
		workflowID := "xdc-stream-replication-test" + uuid.New()
		runID = uuid.New()

		var historyBatch []*historypb.History
		s.generator = test.InitializeHistoryEventGenerator(namespaceName, namespaceId, version)
		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()

			historyEvents := &historypb.History{}
			for _, event := range events {
				historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
			}
			historyBatch = append(historyBatch, historyEvents)
		}

		versionHistory, err := tests.EventBatchesToVersionHistory(nil, historyBatch)
		s.NoError(err)
		s.importEvents(
			workflowID,
			runID,
			versionHistory,
			historyBatch,
			historyClient,
			true,
		)

		executions = append(executions, &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID})
	}
	return executions
}

func (s *streamBasedReplicationTestSuite) waitUntilNamespaceReplicated(
	ctx context.Context,
	namespaceName string,
) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := s.cluster2.GetFrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
				Namespace: namespaceName,
			})
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func (s *streamBasedReplicationTestSuite) assertHistoryEvents(
	ctx context.Context,
	namespaceId string,
	workflowId string,
	runId string,
) error {
	mockClientBean := client.NewMockBean(s.controller)
	mockClientBean.
		EXPECT().
		GetRemoteAdminClient("cluster1").
		Return(s.cluster1.GetAdminClient(), nil).
		AnyTimes()
	mockClientBean.EXPECT().GetRemoteAdminClient("cluster2").Return(s.cluster2.GetAdminClient(), nil).AnyTimes()

	serializer := serialization.NewSerializer()
	cluster1Fetcher := eventhandler.NewHistoryPaginatedFetcher(
		nil,
		mockClientBean,
		serializer,
		nil,
		s.logger,
	)
	cluster2Fetcher := eventhandler.NewHistoryPaginatedFetcher(
		nil,
		mockClientBean,
		serializer,
		nil,
		s.logger,
	)
	iterator1 := cluster1Fetcher.GetSingleWorkflowHistoryPaginatedIterator(
		ctx, "cluster1", namespace.ID(namespaceId), workflowId, runId, 0, 1, 0, 0)
	iterator2 := cluster2Fetcher.GetSingleWorkflowHistoryPaginatedIterator(
		ctx, "cluster2", namespace.ID(namespaceId), workflowId, runId, 0, 1, 0, 0)
	for iterator1.HasNext() {
		s.True(iterator2.HasNext())
		batch1, err := iterator1.Next()
		s.NoError(err)
		batch2, err := iterator2.Next()
		s.NoError(err)
		s.Equal(batch1.VersionHistory.Items, batch2.VersionHistory.Items)
		s.Equal(batch1.RawEventBatch, batch2.RawEventBatch)
	}
	s.False(iterator2.HasNext())
	return nil
}

func (s *streamBasedReplicationTestSuite) importEvents(
	workflowID string,
	runID string,
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
	historyClient tests.HistoryClient,
	verifyWorkflowNotExists bool,
) {
	if len(eventBatches) == 0 {
		return
	}

	historyClient = history.NewRetryableClient(
		historyClient,
		common.CreateHistoryClientRetryPolicy(),
		common.IsResourceExhausted,
	)
	var token []byte
	for _, batch := range eventBatches {
		blob, err := s.serializer.SerializeEvents(batch.Events, enumspb.ENCODING_TYPE_PROTO3)
		s.NoError(err)
		req := &historyservice.ImportWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			VersionHistory: versionHistory,
			HistoryBatches: []*commonpb.DataBlob{blob},
			Token:          token,
		}
		resp, err := historyClient.ImportWorkflowExecution(context.Background(), req)
		s.NoError(err, "Failed to import history event")
		token = resp.Token
	}

	if verifyWorkflowNotExists {
		_, err := historyClient.GetMutableState(context.Background(), &historyservice.GetMutableStateRequest{
			NamespaceId: s.namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		})
		s.IsType(&serviceerror.NotFound{}, err)
	}

	req := &historyservice.ImportWorkflowExecutionRequest{
		NamespaceId: s.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistory: versionHistory,
		HistoryBatches: []*commonpb.DataBlob{},
		Token:          token,
	}
	resp, err := historyClient.ImportWorkflowExecution(context.Background(), req)
	s.NoError(err, "Failed to import history event")
	s.Nil(resp.Token)
}
