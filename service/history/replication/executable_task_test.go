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
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
)

type (
	executableTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		clusterMetadata    *cluster.MockMetadata
		clientBean         *client.MockBean
		shardController    *shard.MockController
		namespaceCache     *namespace.MockRegistry
		ndcHistoryResender *xdc.MockNDCHistoryResender
		metricsHandler     metrics.Handler
		logger             log.Logger

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

	creationTime := time.Unix(0, rand.Int63())
	receivedTime := creationTime.Add(time.Duration(rand.Int63()))
	s.task = NewExecutableTask(
		ProcessToolBox{
			ClusterMetadata:    s.clusterMetadata,
			ClientBean:         s.clientBean,
			ShardController:    s.shardController,
			NamespaceCache:     s.namespaceCache,
			NDCHistoryResender: s.ndcHistoryResender,
			MetricsHandler:     s.metricsHandler,
			Logger:             s.logger,
		},
		rand.Int63(),
		"metrics-tag",
		creationTime,
		receivedTime,
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

	s.task.Ack()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
}

func (s *executableTaskSuite) TestNackStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())

	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
}

func (s *executableTaskSuite) TestCancelStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())

	s.task.Cancel()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
}

func (s *executableTaskSuite) TestRescheduleStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())

	s.task.Reschedule()
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.Equal(2, s.task.Attempt())
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

	err := s.task.Resend(context.Background(), remoteCluster, resendErr)
	s.NoError(err)
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

	err := s.task.Resend(context.Background(), remoteCluster, resendErr)
	s.NoError(err)
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

	err := s.task.Resend(context.Background(), remoteCluster, resendErr)
	s.Error(err)
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

	name, toProcess, err := s.task.GetNamespaceInfo(namespaceID)
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

	name, toProcess, err := s.task.GetNamespaceInfo(namespaceID)
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.False(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Error() {
	namespaceID := uuid.NewString()
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, errors.New("OwO")).AnyTimes()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	_, _, err := s.task.GetNamespaceInfo(namespaceID)
	s.Error(err)
}
