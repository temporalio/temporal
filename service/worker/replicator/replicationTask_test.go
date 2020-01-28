// Copyright (c) 2017 Uber Technologies, Inc.
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
	"go.uber.org/zap"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	messageMocks "github.com/uber/cadence/common/messaging/mocks"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/task"
	"github.com/uber/cadence/common/xdc"
)

type (
	activityReplicationTaskSuite struct {
		suite.Suite
		config        *Config
		logger        log.Logger
		metricsClient metrics.Client

		mockTimeSource    *clock.EventTimeSource
		mockMsg           *messageMocks.Message
		mockHistoryClient *historyservicetest.MockClient
		mockRereplicator  *xdc.MockHistoryRereplicator
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
		mockHistoryClient *historyservicetest.MockClient
		mockRereplicator  *xdc.MockHistoryRereplicator
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
		mockHistoryClient *historyservicetest.MockClient
		mockRereplicator  *xdc.MockHistoryRereplicator

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
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
	s.mockNDCResender = &xdc.MockNDCHistoryResender{}
}

func (s *activityReplicationTaskSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
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
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
	s.mockNDCResender = &xdc.MockNDCHistoryResender{}
}

func (s *historyReplicationTaskSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
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
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
}

func (s *historyMetadataReplicationTaskSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *activityReplicationTaskSuite) TestNewActivityReplicationTask() {
	replicationTask := s.getActivityReplicationTask()
	replicationAttr := replicationTask.SyncActivityTaskAttributes

	activityTask := newActivityReplicationTask(
		replicationTask,
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
		s.mockNDCResender)
	// overwrite the logger for easy comparison
	activityTask.logger = s.logger

	s.Equal(
		&activityReplicationTask{
			workflowReplicationTask: workflowReplicationTask{
				metricsScope: metrics.SyncActivityTaskScope,
				startTime:    activityTask.startTime,
				queueID: definition.NewWorkflowIdentifier(
					replicationAttr.GetDomainId(),
					replicationAttr.GetWorkflowId(),
					replicationAttr.GetRunId(),
				),
				taskID:        replicationAttr.GetScheduledId(),
				state:         task.TaskStatePending,
				attempt:       0,
				kafkaMsg:      s.mockMsg,
				logger:        s.logger,
				timeSource:    s.mockTimeSource,
				config:        s.config,
				historyClient: s.mockHistoryClient,
				metricsClient: s.metricsClient,
			},
			req: &h.SyncActivityRequest{
				DomainId:           replicationAttr.DomainId,
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
				LastFailureReason:  replicationAttr.LastFailureReason,
				LastWorkerIdentity: replicationAttr.LastWorkerIdentity,
				LastFailureDetails: replicationAttr.LastFailureDetails,
			},
			historyRereplicator: s.mockRereplicator,
			nDCHistoryResender:  s.mockNDCResender,
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
		s.mockRereplicator,
		s.mockNDCResender)

	randomErr := errors.New("some random error")
	s.mockHistoryClient.EXPECT().SyncActivity(gomock.Any(), task.req).Return(randomErr).Times(1)
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
		s.mockRereplicator,
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
		s.mockRereplicator,
		s.mockNDCResender)
	task.attempt = s.config.ReplicatorActivityBufferRetryCount() + 1
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *activityReplicationTaskSuite) TestHandleErr_EnoughAttempt_RetryErr() {
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
		s.mockNDCResender)
	task.attempt = s.config.ReplicatorActivityBufferRetryCount() + 1
	retryErr := &shared.RetryTaskError{
		DomainId:    common.StringPtr(task.queueID.DomainID),
		WorkflowId:  common.StringPtr(task.queueID.WorkflowID),
		RunId:       common.StringPtr("other random run ID"),
		NextEventId: common.Int64Ptr(447),
	}

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.queueID.RunID, task.taskID+1,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.queueID.RunID, task.taskID+1,
	).Return(nil).Once()
	s.mockHistoryClient.EXPECT().SyncActivity(gomock.Any(), task.req).Return(nil).Times(1)
	err = task.HandleErr(retryErr)
	s.Nil(err)
}

func (s *activityReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := &shared.BadRequestError{}
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
		s.mockNDCResender)
	s.False(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable() {
	err := &shared.InternalServiceError{}
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
		s.mockNDCResender)
	task.attempt = 0
	s.True(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := &shared.InternalServiceError{}
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
		s.mockNDCResender)
	task.attempt = s.config.ReplicationTaskMaxRetryCount() + 100
	s.False(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable_ExceedDuration() {
	err := &shared.InternalServiceError{}
	task := newActivityReplicationTask(
		s.getActivityReplicationTask(),
		s.mockMsg,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
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
		s.mockRereplicator,
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
		s.mockRereplicator,
		s.mockNDCResender)

	s.mockMsg.On("Nack").Return(nil).Once()
	task.Nack()
}

func (s *historyReplicationTaskSuite) TestNewHistoryReplicationTask() {
	replicationTask := s.getHistoryReplicationTask()
	replicationAttr := replicationTask.HistoryTaskAttributes

	historyTask := newHistoryReplicationTask(
		replicationTask,
		s.mockMsg,
		s.sourceCluster,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
	)
	// overwrite the logger for easy comparison
	historyTask.logger = s.logger

	s.Equal(
		&historyReplicationTask{
			workflowReplicationTask: workflowReplicationTask{
				metricsScope: metrics.HistoryReplicationTaskScope,
				startTime:    historyTask.startTime,
				queueID: definition.NewWorkflowIdentifier(
					replicationAttr.GetDomainId(),
					replicationAttr.GetWorkflowId(),
					replicationAttr.GetRunId(),
				),
				taskID:        replicationAttr.GetFirstEventId(),
				state:         task.TaskStatePending,
				attempt:       0,
				kafkaMsg:      s.mockMsg,
				logger:        s.logger,
				timeSource:    s.mockTimeSource,
				config:        s.config,
				historyClient: s.mockHistoryClient,
				metricsClient: s.metricsClient,
			},
			req: &h.ReplicateEventsRequest{
				SourceCluster: common.StringPtr(s.sourceCluster),
				DomainUUID:    replicationAttr.DomainId,
				WorkflowExecution: &shared.WorkflowExecution{
					WorkflowId: replicationAttr.WorkflowId,
					RunId:      replicationAttr.RunId,
				},
				FirstEventId:      replicationAttr.FirstEventId,
				NextEventId:       replicationAttr.NextEventId,
				Version:           replicationAttr.Version,
				ReplicationInfo:   replicationAttr.ReplicationInfo,
				History:           replicationAttr.History,
				NewRunHistory:     replicationAttr.NewRunHistory,
				ForceBufferEvents: common.BoolPtr(false),
				ResetWorkflow:     replicationAttr.ResetWorkflow,
				NewRunNDC:         replicationAttr.NewRunNDC,
			},
			historyRereplicator: s.mockRereplicator,
		},
		historyTask,
	)
}

func (s *historyReplicationTaskSuite) TestExecute() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	randomErr := errors.New("some random error")
	s.mockHistoryClient.EXPECT().ReplicateEvents(gomock.Any(), task.req).Return(randomErr).Times(1)
	err := task.Execute()
	s.Equal(randomErr, err)
}

func (s *historyReplicationTaskSuite) TestHandleErr_NotEnoughAttempt() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *historyReplicationTaskSuite) TestHandleErr_EnoughAttempt_NotRetryErr() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicatorHistoryBufferRetryCount() + 1
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *historyReplicationTaskSuite) TestHandleErr_EnoughAttempt_RetryErr() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicatorHistoryBufferRetryCount() + 1
	retryErr := &shared.RetryTaskError{
		DomainId:    common.StringPtr(task.queueID.DomainID),
		WorkflowId:  common.StringPtr(task.queueID.WorkflowID),
		RunId:       common.StringPtr("other random run ID"),
		NextEventId: common.Int64Ptr(447),
	}

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.queueID.RunID, task.taskID,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.queueID.RunID, task.taskID,
	).Return(nil).Once()
	s.mockHistoryClient.EXPECT().ReplicateEvents(gomock.Any(), task.req).Return(nil).Times(1)
	err = task.HandleErr(retryErr)
	s.Nil(err)
}

func (s *historyReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := &shared.BadRequestError{}
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	s.False(task.RetryErr(err))
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable() {
	err := &shared.InternalServiceError{}
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = 0
	s.True(task.RetryErr(err))
	s.False(task.req.GetForceBufferEvents())
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := &shared.InternalServiceError{}
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicationTaskMaxRetryCount() + 100
	s.False(task.RetryErr(err))
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable_ExceedDuration() {
	err := &shared.InternalServiceError{}
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.startTime = s.mockTimeSource.Now().Add(-2 * s.config.ReplicationTaskMaxRetryDuration())
	s.False(task.RetryErr(err))
}

func (s *historyReplicationTaskSuite) TestAck() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Ack").Return(nil).Once()
	task.Ack()
}

func (s *historyReplicationTaskSuite) TestNack() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Nack").Return(nil).Once()
	task.Nack()
}

func (s *historyMetadataReplicationTaskSuite) TestNewHistoryMetadataReplicationTask() {
	replicationTask := s.getHistoryMetadataReplicationTask()
	replicationAttr := replicationTask.HistoryMetadataTaskAttributes

	metadataTask := newHistoryMetadataReplicationTask(
		replicationTask,
		s.mockMsg,
		s.sourceCluster,
		s.logger,
		s.config,
		s.mockTimeSource,
		s.mockHistoryClient,
		s.metricsClient,
		s.mockRereplicator,
	)
	// overwrite the logger for easy comparison
	metadataTask.logger = s.logger

	s.Equal(
		&historyMetadataReplicationTask{
			workflowReplicationTask: workflowReplicationTask{
				metricsScope: metrics.HistoryMetadataReplicationTaskScope,
				startTime:    metadataTask.startTime,
				queueID: definition.NewWorkflowIdentifier(
					replicationAttr.GetDomainId(),
					replicationAttr.GetWorkflowId(),
					replicationAttr.GetRunId(),
				),
				taskID:        replicationAttr.GetFirstEventId(),
				state:         task.TaskStatePending,
				attempt:       0,
				kafkaMsg:      s.mockMsg,
				logger:        s.logger,
				timeSource:    s.mockTimeSource,
				config:        s.config,
				historyClient: s.mockHistoryClient,
				metricsClient: s.metricsClient,
			},
			sourceCluster:       s.sourceCluster,
			firstEventID:        replicationAttr.GetFirstEventId(),
			nextEventID:         replicationAttr.GetNextEventId(),
			historyRereplicator: s.mockRereplicator,
		},
		metadataTask,
	)
}

func (s *historyMetadataReplicationTaskSuite) TestExecute() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	randomErr := errors.New("some random error")
	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		task.queueID.RunID, task.firstEventID,
		task.queueID.RunID, task.nextEventID,
	).Return(randomErr).Once()

	err := task.Execute()
	s.Equal(randomErr, err)
}

func (s *historyMetadataReplicationTaskSuite) TestHandleErr_NotRetryErr() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *historyMetadataReplicationTaskSuite) TestHandleErr_RetryErr() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	retryErr := &shared.RetryTaskError{
		DomainId:    common.StringPtr(task.queueID.DomainID),
		WorkflowId:  common.StringPtr(task.queueID.WorkflowID),
		RunId:       common.StringPtr("other random run ID"),
		NextEventId: common.Int64Ptr(447),
	}

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.queueID.RunID, task.taskID,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.queueID.RunID, task.taskID,
	).Return(nil).Once()
	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		task.queueID.RunID, task.firstEventID,
		task.queueID.RunID, task.nextEventID,
	).Return(nil).Once()
	err = task.HandleErr(retryErr)
	s.Nil(err)
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := &shared.BadRequestError{}
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable() {
	err := &shared.InternalServiceError{}
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = 0
	s.True(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := &shared.InternalServiceError{}
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicationTaskMaxRetryCount() + 100
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable_ExceedDuration() {
	err := &shared.InternalServiceError{}
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.startTime = s.mockTimeSource.Now().Add(-2 * s.config.ReplicationTaskMaxRetryDuration())
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestAck() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Ack").Return(nil).Once()
	task.Ack()
}

func (s *historyMetadataReplicationTaskSuite) TestNack() {
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Nack").Return(nil).Once()
	task.Nack()
}

func (s *activityReplicationTaskSuite) getActivityReplicationTask() *replicator.ReplicationTask {
	replicationAttr := &replicator.SyncActivityTaskAttributes{
		DomainId:           common.StringPtr("some random domain ID"),
		WorkflowId:         common.StringPtr("some random workflow ID"),
		RunId:              common.StringPtr("some random run ID"),
		Version:            common.Int64Ptr(1394),
		ScheduledId:        common.Int64Ptr(728),
		ScheduledTime:      common.Int64Ptr(time.Now().UnixNano()),
		StartedId:          common.Int64Ptr(1015),
		StartedTime:        common.Int64Ptr(time.Now().UnixNano()),
		LastHeartbeatTime:  common.Int64Ptr(time.Now().UnixNano()),
		Details:            []byte("some random detail"),
		Attempt:            common.Int32Ptr(59),
		LastFailureReason:  common.StringPtr("some random failure reason"),
		LastWorkerIdentity: common.StringPtr("some random worker identity"),
		LastFailureDetails: []byte("some random failure details"),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                   replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActivityTaskAttributes: replicationAttr,
	}
	return replicationTask
}

func (s *historyReplicationTaskSuite) getHistoryReplicationTask() *replicator.ReplicationTask {
	replicationAttr := &replicator.HistoryTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       common.StringPtr("some random domain ID"),
		WorkflowId:     common.StringPtr("some random workflow ID"),
		RunId:          common.StringPtr("some random run ID"),
		Version:        common.Int64Ptr(1394),
		FirstEventId:   common.Int64Ptr(728),
		NextEventId:    common.Int64Ptr(1015),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			cluster.TestCurrentClusterName: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(0644),
				LastEventId: common.Int64Ptr(0755),
			},
			cluster.TestAlternativeClusterName: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(0755),
				LastEventId: common.Int64Ptr(0644),
			},
		},
		History: &shared.History{
			Events: []*shared.HistoryEvent{&shared.HistoryEvent{EventId: common.Int64Ptr(1)}},
		},
		NewRunHistory: &shared.History{
			Events: []*shared.HistoryEvent{&shared.HistoryEvent{EventId: common.Int64Ptr(2)}},
		},
		ResetWorkflow: common.BoolPtr(true),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:              replicator.ReplicationTaskTypeHistory.Ptr(),
		HistoryTaskAttributes: replicationAttr,
	}
	return replicationTask
}

func (s *historyMetadataReplicationTaskSuite) getHistoryMetadataReplicationTask() *replicator.ReplicationTask {
	replicationAttr := &replicator.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       common.StringPtr("some random domain ID"),
		WorkflowId:     common.StringPtr("some random workflow ID"),
		RunId:          common.StringPtr("some random run ID"),
		FirstEventId:   common.Int64Ptr(728),
		NextEventId:    common.Int64Ptr(1015),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                      replicator.ReplicationTaskTypeHistoryMetadata.Ptr(),
		HistoryMetadataTaskAttributes: replicationAttr,
	}
	return replicationTask
}
