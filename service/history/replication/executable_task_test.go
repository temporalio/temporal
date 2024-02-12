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

package replication

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	executableTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		clientBean              *client.MockBean
		shardController         *shard.MockController
		namespaceCache          *namespace.MockRegistry
		ndcHistoryResender      *xdc.MockNDCHistoryResender
		remoteHistoryFetcher    *MockHistoryPaginatedFetcher
		metricsHandler          metrics.Handler
		logger                  log.Logger
		sourceCluster           string
		eagerNamespaceRefresher *MockEagerNamespaceRefresher
		config                  *configs.Config

		task *ExecutableTaskImpl
	}
)

func TestExecutableTaskSuite(t *testing.T) {
	s := new(executableTaskSuite)
	suite.Run(t, s)
}

func (s *executableTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableTaskSuite) TearDownSuite() {

}

func (s *executableTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.sourceCluster = "some cluster"
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.config = tests.NewDynamicConfig()
	s.remoteHistoryFetcher = NewMockHistoryPaginatedFetcher(s.controller)

	creationTime := time.Unix(0, rand.Int63())
	receivedTime := creationTime.Add(time.Duration(rand.Int63()))
	s.task = NewExecutableTask(
		ProcessToolBox{
			Config:                  s.config,
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			NDCHistoryResender:      s.ndcHistoryResender,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,

			DLQWriter:               NoopDLQWriter{},
			HistoryPaginatedFetcher: s.remoteHistoryFetcher,
		},
		rand.Int63(),
		"metrics-tag",
		creationTime,
		receivedTime,
		s.sourceCluster,
	)
}

func (s *executableTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableTaskSuite) TestTaskID() {
	s.Equal(s.task.taskID, s.task.TaskID())
}

func (s *executableTaskSuite) TestCreationTime() {
	s.Equal(s.task.taskCreationTime, s.task.TaskCreationTime())
}

func (s *executableTaskSuite) TestAckStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Ack()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Abort()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestNackStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Abort()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestAbortStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Abort()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestCancelStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Cancel()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Abort()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestRescheduleStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Reschedule()
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.Equal(2, s.task.Attempt())

	s.False(s.task.TerminalState())
}

func (s *executableTaskSuite) TestIsRetryableError() {
	err := errors.New("OwO")
	s.True(s.task.IsRetryableError(err))

	err = serviceerror.NewInternal("OwO")
	s.True(s.task.IsRetryableError(err))

	err = serviceerror.NewUnavailable("OwO")
	s.True(s.task.IsRetryableError(err))

	err = serviceerror.NewInvalidArgument("OwO")
	s.False(s.task.IsRetryableError(err))
}

func (s *executableTaskSuite) TestResend_Success() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(nil)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.NoError(err)
	s.True(doContinue)
}

func (s *executableTaskSuite) TestResend_NotFound() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(serviceerror.NewNotFound(""))
	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().DeleteWorkflowExecution(gomock.Any(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: resendErr.NamespaceId,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: resendErr.WorkflowId,
			RunId:      resendErr.RunId,
		},
		WorkflowVersion:    common.EmptyVersion,
		ClosedWorkflowOnly: false,
	}).Return(&historyservice.DeleteWorkflowExecutionResponse{}, nil)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.NoError(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestResend_ResendError_Success() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	gomock.InOrder(
		s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(anotherResendErr),
		s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
			gomock.Any(),
			remoteCluster,
			namespace.ID(anotherResendErr.NamespaceId),
			anotherResendErr.WorkflowId,
			anotherResendErr.RunId,
			anotherResendErr.StartEventId,
			anotherResendErr.StartEventVersion,
			anotherResendErr.EndEventId,
			anotherResendErr.EndEventVersion,
		).Return(nil),
		s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(nil),
	)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.NoError(err)
	s.True(doContinue)
}

func (s *executableTaskSuite) TestResend_ResendError_Error() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	gomock.InOrder(
		s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(anotherResendErr),
		s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
			gomock.Any(),
			remoteCluster,
			namespace.ID(anotherResendErr.NamespaceId),
			anotherResendErr.WorkflowId,
			anotherResendErr.RunId,
			anotherResendErr.StartEventId,
			anotherResendErr.StartEventVersion,
			anotherResendErr.EndEventId,
			anotherResendErr.EndEventVersion,
		).Return(&serviceerrors.RetryReplication{}),
	)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestResend_SecondResendError_SameWorkflowRun() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             resendErr.RunId,
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(anotherResendErr)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	var dataLossErr *serviceerror.DataLoss
	s.ErrorAs(err, &dataLossErr)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestResend_Error() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	s.ndcHistoryResender.EXPECT().SendSingleWorkflowHistory(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(serviceerror.NewUnavailable(""))

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestImport_Success() {
	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	startEventId := rand.Int63()
	startEventVersion := rand.Int63()
	endEventId := rand.Int63()
	endEventVersion := rand.Int63()

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 0, 1},
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 3, Version: 0},
			{EventId: 5, Version: 4},
			{EventId: 7, Version: 6},
			{EventId: 9, Version: 10},
		},
	}
	dataBlob1 := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte{1, 0, 1},
	}
	dataBlob2 := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte{1, 1, 0},
	}
	historyBatch1 := historyBatch{
		RawEventBatch:  dataBlob1,
		VersionHistory: versionHistory,
	}
	historyBatch2 := historyBatch{
		RawEventBatch:  dataBlob2,
		VersionHistory: versionHistory,
	}

	// in total, historyImportBlobSize + 1 batches returned. So the expected call to ImportWorkflowExecution API is:
	// 1. ImportWorkflowExecution with event[0, historyImportBlobSize), inside the iteration
	// 2. ImportWorkflowExecution with event[historyImportBlobSize], outside the loop
	// 3. ImportWorkflowExecution to commit the import
	times := 0
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]historyBatch, []byte, error) {
		if times < historyImportBlobSize {
			times++
			return []historyBatch{historyBatch1}, []byte{1, 1, 0}, nil
		}
		return []historyBatch{historyBatch2}, nil, nil
	})

	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIterator(
		gomock.Any(),
		remoteCluster,
		namespace.ID(namespaceId),
		workflowId,
		runId,
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
	).Return(fetcher)

	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	returnToken1 := []byte{1, 0, 0, 1}
	returnToken2 := []byte{1, 0, 0, 1, 1, 1, 1, 0}

	expectedBlob1 := []*commonpb.DataBlob{}
	for i := 0; i < historyImportBlobSize; i++ {
		expectedBlob1 = append(expectedBlob1, dataBlob1)
	}
	gomock.InOrder(
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &historyservice.ImportWorkflowExecutionRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
			HistoryBatches: expectedBlob1,
			VersionHistory: versionHistory,
			Token:          nil,
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token: returnToken1,
		}, nil).Times(1),
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &historyservice.ImportWorkflowExecutionRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
			HistoryBatches: []*commonpb.DataBlob{dataBlob2},
			VersionHistory: versionHistory,
			Token:          returnToken1,
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token: returnToken2,
		}, nil).Times(1),
		engine.EXPECT().ImportWorkflowExecution(gomock.Any(), &historyservice.ImportWorkflowExecutionRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
			HistoryBatches: []*commonpb.DataBlob{},
			VersionHistory: versionHistory,
			Token:          returnToken2,
		}).Return(&historyservice.ImportWorkflowExecutionResponse{
			Token: nil,
		}, nil).Times(1),
	)

	err := s.task.Import(
		context.Background(),
		remoteCluster,
		definition.WorkflowKey{
			NamespaceID: namespaceId,
			WorkflowID:  workflowId,
			RunID:       runId,
		},
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
	)
	s.Nil(err)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Process() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	namespaceEntry := namespace.FromPersistentState(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespaceID,
				Name: namespaceName,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
		},
	})
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID)
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.True(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Skip() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	namespaceEntry := namespace.FromPersistentState(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespaceID,
				Name: namespaceName,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestAlternativeClusterName,
				},
			},
		},
	})
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID)
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.False(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Error() {
	namespaceID := uuid.NewString()
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, errors.New("OwO")).AnyTimes()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	_, _, err := s.task.GetNamespaceInfo(context.Background(), namespaceID)
	s.Error(err)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_NotFoundOnCurrentCluster_SyncFromRemoteSuccess() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	namespaceEntry := namespace.FromPersistentState(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespaceID,
				Name: namespaceName,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
		},
	})
	// enable feature flag
	s.config.EnableReplicationEagerRefreshNamespace = dynamicconfig.GetBoolPropertyFn(true)

	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).AnyTimes()
	s.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		namespaceEntry, nil)
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID)
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.True(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_NotFoundOnCurrentCluster_SyncFromRemoteFailed() {
	namespaceID := uuid.NewString()

	// Enable feature flag
	s.config.EnableReplicationEagerRefreshNamespace = dynamicconfig.GetBoolPropertyFn(true)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).AnyTimes()
	s.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		nil, errors.New("some error"))
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID)
	s.Nil(err)
	s.False(toProcess)
}
