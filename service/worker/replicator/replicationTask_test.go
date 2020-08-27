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

package replicator

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/zap"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	messageMocks "go.temporal.io/server/common/messaging/mocks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/common/task"
	"go.temporal.io/server/common/xdc"
)

type (
	activityReplicationTaskSuite struct {
		suite.Suite
		config        *Config
		logger        log.Logger
		metricsClient metrics.Client

		mockTimeSource    *clock.EventTimeSource
		mockMsg           *messageMocks.Message
		mockHistoryClient *historyservicemock.MockHistoryServiceClient
		mockNDCResender   *xdc.MockNDCHistoryResender

		controller *gomock.Controller
	}

	historyReplicationTaskSuite struct {
		suite.Suite
		config        *Config
		logger        log.Logger
		metricsClient metrics.Client
		sourceCluster string

		mockTimeSource    *clock.EventTimeSource
		mockMsg           *messageMocks.Message
		mockHistoryClient *historyservicemock.MockHistoryServiceClient
		mockNDCResender   *xdc.MockNDCHistoryResender

		controller *gomock.Controller
	}

	historyMetadataReplicationTaskSuite struct {
		suite.Suite
		config        *Config
		logger        log.Logger
		metricsClient metrics.Client
		sourceCluster string

		mockTimeSource    *clock.EventTimeSource
		mockMsg           *messageMocks.Message
		mockHistoryClient *historyservicemock.MockHistoryServiceClient
		mockNDCResender   *xdc.MockNDCHistoryResender

		controller *gomock.Controller
	}
)

func TestActivityReplicationTaskSuite(t *testing.T) {
	s := new(activityReplicationTaskSuite)
	suite.Run(t, s)
}

func TestHistoryReplicationTaskSuite(t *testing.T) {
	s := new(historyReplicationTaskSuite)
	suite.Run(t, s)
}

func TestHistoryMetadataReplicationTaskSuite(t *testing.T) {
	s := new(historyMetadataReplicationTaskSuite)
	suite.Run(t, s)
}

func (s *activityReplicationTaskSuite) SetupSuite() {
}

func (s *activityReplicationTaskSuite) TearDownSuite() {

}

func (s *activityReplicationTaskSuite) SetupTest() {
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.config = &Config{
		ReplicatorActivityBufferRetryCount: dynamicconfig.GetIntPropertyFn(2),
		ReplicationTaskMaxRetryCount:       dynamicconfig.GetIntPropertyFn(10),
		ReplicationTaskMaxRetryDuration:    dynamicconfig.GetDurationPropertyFn(time.Minute),
		ReplicationTaskContextTimeout:      dynamicconfig.GetDurationPropertyFn(30 * time.Second),
	}
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Worker)

	s.mockTimeSource = clock.NewEventTimeSource()
	s.mockMsg = &messageMocks.Message{}
	s.controller = gomock.NewController(s.T())
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockNDCResender = &xdc.MockNDCHistoryResender{}
}

func (s *activityReplicationTaskSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *historyReplicationTaskSuite) SetupSuite() {
}

func (s *historyReplicationTaskSuite) TearDownSuite() {

}

func (s *historyReplicationTaskSuite) SetupTest() {
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.config = &Config{
		ReplicatorHistoryBufferRetryCount: dynamicconfig.GetIntPropertyFn(2),
		ReplicationTaskMaxRetryCount:      dynamicconfig.GetIntPropertyFn(10),
		ReplicationTaskMaxRetryDuration:   dynamicconfig.GetDurationPropertyFn(time.Minute),
		ReplicationTaskContextTimeout:     dynamicconfig.GetDurationPropertyFn(30 * time.Second),
	}
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Worker)
	s.sourceCluster = cluster.TestAlternativeClusterName

	s.mockTimeSource = clock.NewEventTimeSource()
	s.mockMsg = &messageMocks.Message{}
	s.controller = gomock.NewController(s.T())
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockNDCResender = &xdc.MockNDCHistoryResender{}
}

func (s *historyReplicationTaskSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *historyMetadataReplicationTaskSuite) SetupSuite() {
}

func (s *historyMetadataReplicationTaskSuite) TearDownSuite() {

}

func (s *historyMetadataReplicationTaskSuite) SetupTest() {
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.config = &Config{
		ReplicationTaskMaxRetryCount:    dynamicconfig.GetIntPropertyFn(10),
		ReplicationTaskMaxRetryDuration: dynamicconfig.GetDurationPropertyFn(time.Minute),
		ReplicationTaskContextTimeout:   dynamicconfig.GetDurationPropertyFn(30 * time.Second),
	}
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Worker)
	s.sourceCluster = cluster.TestAlternativeClusterName

	s.mockTimeSource = clock.NewEventTimeSource()
	s.mockMsg = &messageMocks.Message{}
	s.controller = gomock.NewController(s.T())
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockNDCResender = &xdc.MockNDCHistoryResender{}
}

func (s *historyMetadataReplicationTaskSuite) TearDownTest() {

	s.mockMsg.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *activityReplicationTaskSuite) TestNewActivityReplicationTask() {
	replicationTask := s.getActivityReplicationTask()
	replicationAttr := replicationTask.GetSyncActivityTaskAttributes()

	activityTask := newActivityReplicationTask(
		replicationTask,
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)
	// overwrite the logger for easy comparison
	activityTask.logger = s.logger

	s.Equal(
		&activityReplicationTask{
			workflowReplicationTask: workflowReplicationTask{
				metricsScope: metrics.SyncActivityTaskScope,
				startTime:    activityTask.startTime,
				queueID: definition.NewWorkflowIdentifier(
					replicationAttr.GetNamespaceId(),
					replicationAttr.GetWorkflowId(),
					replicationAttr.GetRunId(),
				),
				taskID:        replicationAttr.GetScheduledId(),
				state:         task.TaskStatePending,
				attempt:       1,
				kafkaMsg:      s.mockMsg,
				logger:        s.logger,
				timeSource:    s.mockTimeSource,
				config:        s.config,
				historyClient: s.mockHistoryClient,
				metricsClient: s.metricsClient,
			},
			req: &historyservice.SyncActivityRequest{
				NamespaceId:        replicationAttr.NamespaceId,
				WorkflowId:         replicationAttr.WorkflowId,
				RunId:              replicationAttr.RunId,
				Version:            replicationAttr.Version,
				ScheduledId:        replicationAttr.ScheduledId,
				ScheduledTime:      replicationAttr.ScheduledTime,
				StartedId:          replicationAttr.StartedId,
				StartedTime:        replicationAttr.StartedTime,
				LastHeartbeatTime:  replicationAttr.LastHeartbeatTime,
				Details:            replicationAttr.Details,
				Attempt:            replicationAttr.Attempt,
				LastFailure:        replicationAttr.LastFailure,
				LastWorkerIdentity: replicationAttr.LastWorkerIdentity,
			},
			nDCHistoryResender: s.mockNDCResender,
		},
		activityTask,
	)
}

func (s *activityReplicationTaskSuite) TestExecute() {
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)

	randomErr := errors.New("some random error")
	s.mockHistoryClient.EXPECT().SyncActivity(gomock.Any(), task.req).Return(nil, randomErr).Times(1)
	err := task.Execute()
	s.Equal(randomErr, err)
}

func (s *activityReplicationTaskSuite) TestHandleErr_NotEnoughAttempt() {
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *activityReplicationTaskSuite) TestHandleErr_EnoughAttempt_NotRetryErr() {
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)
	task.attempt = s.config.ReplicatorActivityBufferRetryCount() + 1
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *activityReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := serviceerror.NewInvalidArgument("")
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)
	s.False(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable() {
	err := serviceerror.NewInternal("")
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)
	task.attempt = 1
	s.True(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := serviceerror.NewInternal("")
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)
	task.attempt = s.config.ReplicationTaskMaxRetryCount() + 100
	s.False(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable_ExceedDuration() {
	err := serviceerror.NewInternal("")
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)
	task.startTime = s.mockTimeSource.Now().Add(-2 * s.config.ReplicationTaskMaxRetryDuration())
	s.False(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestAck() {
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)

	s.mockMsg.On("Ack").Return(nil).Once()
	task.Ack()
}

func (s *activityReplicationTaskSuite) TestNack() {
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender)

	s.mockMsg.On("Nack").Return(nil).Once()
	task.Nack()
}

func (s *historyMetadataReplicationTaskSuite) TestNewHistoryMetadataReplicationTask() {
	replicationTask := s.getHistoryMetadataReplicationTask()
	replicationAttr := replicationTask.GetHistoryMetadataTaskAttributes()

	metadataTask := newHistoryMetadataReplicationTask(
		replicationTask,
		s.mockMsg,
		s.sourceCluster,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockNDCResender,
	)
	// overwrite the logger for easy comparison
	metadataTask.logger = s.logger

	s.Equal(
		&historyMetadataReplicationTask{
			workflowReplicationTask: workflowReplicationTask{
				metricsScope: metrics.HistoryMetadataReplicationTaskScope,
				startTime:    metadataTask.startTime,
				queueID: definition.NewWorkflowIdentifier(
					replicationAttr.GetNamespaceId(),
					replicationAttr.GetWorkflowId(),
					replicationAttr.GetRunId(),
				),
				taskID:        replicationAttr.GetFirstEventId(),
				state:         task.TaskStatePending,
				attempt:       1,
				kafkaMsg:      s.mockMsg,
				logger:        s.logger,
				timeSource:    s.mockTimeSource,
				config:        s.config,
				historyClient: s.mockHistoryClient,
				metricsClient: s.metricsClient,
			},
			sourceCluster:      s.sourceCluster,
			firstEventID:       replicationAttr.GetFirstEventId(),
			nextEventID:        replicationAttr.GetNextEventId(),
			nDCHistoryResender: s.mockNDCResender,
			version:            replicationAttr.GetVersion(),
		},
		metadataTask,
	)
}

func (s *historyMetadataReplicationTaskSuite) TestHandleErr_NotRetryErr() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockNDCResender)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := serviceerror.NewInvalidArgument("")
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockNDCResender)
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable() {
	err := serviceerror.NewInternal("")
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockNDCResender)
	task.attempt = 1
	s.True(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := serviceerror.NewInternal("")
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockNDCResender)
	task.attempt = s.config.ReplicationTaskMaxRetryCount() + 100
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable_ExceedDuration() {
	err := serviceerror.NewInternal("")
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockNDCResender)
	task.startTime = s.mockTimeSource.Now().Add(-2 * s.config.ReplicationTaskMaxRetryDuration())
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestAck() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockNDCResender)

	s.mockMsg.On("Ack").Return(nil).Once()
	task.Ack()
}

func (s *historyMetadataReplicationTaskSuite) TestNack() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockNDCResender)

	s.mockMsg.On("Nack").Return(nil).Once()
	task.Nack()
}

func (s *activityReplicationTaskSuite) getActivityReplicationTask() *replicationspb.ReplicationTask {
	replicationAttr := &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:        "some random namespace ID",
		WorkflowId:         "some random workflow ID",
		RunId:              "some random run ID",
		Version:            1394,
		ScheduledId:        728,
		ScheduledTime:      timestamp.TimeNowPtrUtc(),
		StartedId:          1015,
		StartedTime:        timestamp.TimeNowPtrUtc(),
		LastHeartbeatTime:  timestamp.TimeNowPtrUtc(),
		Details:            payloads.EncodeString("some random detail"),
		Attempt:            59,
		LastFailure:        failure.NewServerFailure("some random failure reason", false),
		LastWorkerIdentity: "some random worker identity",
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	return replicationTask
}

func (s *historyReplicationTaskSuite) getHistoryReplicationTask() *replicationspb.ReplicationTask {
	replicationAttr := &replicationspb.HistoryTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:    "some random namespace ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		Version:        1394,
		FirstEventId:   728,
		NextEventId:    1015,
		History: &historypb.History{
			Events: []*historypb.HistoryEvent{{EventId: 1}},
		},
		NewRunHistory: &historypb.History{
			Events: []*historypb.HistoryEvent{{EventId: 2}},
		},
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: replicationAttr},
	}
	return replicationTask
}

func (s *historyMetadataReplicationTaskSuite) getHistoryMetadataReplicationTask() *replicationspb.ReplicationTask {
	replicationAttr := &replicationspb.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:    "some random namespace ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
		Version:        common.EmptyVersion,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	return replicationTask
}
