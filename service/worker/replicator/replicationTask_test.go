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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	messageMocks "github.com/temporalio/temporal/common/messaging/mocks"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/common/task"
	"github.com/temporalio/temporal/common/xdc"
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
		mockHistoryClient *historyservicemock.MockHistoryServiceClient
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
		mockHistoryClient *historyservicemock.MockHistoryServiceClient
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
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
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
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
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
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
}

func (s *historyMetadataReplicationTaskSuite) TearDownTest() {

	s.mockMsg.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
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
			req: &historyservice.SyncActivityRequest{
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
	retryErr := serviceerror.NewRetryTask(
		"",
		task.queueID.DomainID,
		task.queueID.WorkflowID,
		"other random run ID",
		447,
	)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.RunId, retryErr.NextEventId,
		task.queueID.RunID, task.taskID+1,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.RunId, retryErr.NextEventId,
		task.queueID.RunID, task.taskID+1,
	).Return(nil).Once()
	s.mockHistoryClient.EXPECT().SyncActivity(gomock.Any(), task.req).Return(nil, nil).Times(1)
	err = task.HandleErr(retryErr)
	s.Nil(err)
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
		s.mockRereplicator,
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
		s.mockRereplicator,
		s.mockNDCResender)
	task.attempt = 0
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
		s.mockRereplicator,
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
	replicationAttr := replicationTask.GetHistoryTaskAttributes()

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
			req: &historyservice.ReplicateEventsRequest{
				SourceCluster: s.sourceCluster,
				DomainUUID:    replicationAttr.DomainId,
				WorkflowExecution: &commonproto.WorkflowExecution{
					WorkflowId: replicationAttr.WorkflowId,
					RunId:      replicationAttr.RunId,
				},
				FirstEventId:      replicationAttr.FirstEventId,
				NextEventId:       replicationAttr.NextEventId,
				Version:           replicationAttr.Version,
				ReplicationInfo:   replicationAttr.ReplicationInfo,
				History:           replicationAttr.History,
				NewRunHistory:     replicationAttr.NewRunHistory,
				ForceBufferEvents: false,
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
	s.mockHistoryClient.EXPECT().ReplicateEvents(gomock.Any(), task.req).Return(nil, randomErr).Times(1)
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
	retryErr := serviceerror.NewRetryTask(
		"",
		task.queueID.DomainID,
		task.queueID.WorkflowID,
		"other random run ID",
		447,
	)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.RunId, retryErr.NextEventId,
		task.queueID.RunID, task.taskID,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.RunId, retryErr.NextEventId,
		task.queueID.RunID, task.taskID,
	).Return(nil).Once()
	s.mockHistoryClient.EXPECT().ReplicateEvents(gomock.Any(), task.req).Return(nil, nil).Times(1)
	err = task.HandleErr(retryErr)
	s.Nil(err)
}

func (s *historyReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := serviceerror.NewInvalidArgument("")
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	s.False(task.RetryErr(err))
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable() {
	err := serviceerror.NewInternal("")
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = 0
	s.True(task.RetryErr(err))
	s.False(task.req.GetForceBufferEvents())
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := serviceerror.NewInternal("")
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicationTaskMaxRetryCount() + 100
	s.False(task.RetryErr(err))
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable_ExceedDuration() {
	err := serviceerror.NewInternal("")
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
	retryErr := serviceerror.NewRetryTask(
		"",
		task.queueID.DomainID,
		task.queueID.WorkflowID,
		"other random run ID",
		447,
	)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.RunId, retryErr.NextEventId,
		task.queueID.RunID, task.taskID,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.queueID.DomainID, task.queueID.WorkflowID,
		retryErr.RunId, retryErr.NextEventId,
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
	err := serviceerror.NewInvalidArgument("")
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable() {
	err := serviceerror.NewInternal("")
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = 0
	s.True(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := serviceerror.NewInternal("")
	task := newHistoryMetadataReplicationTask(s.getHistoryMetadataReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockTimeSource, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicationTaskMaxRetryCount() + 100
	s.False(task.RetryErr(err))
}

func (s *historyMetadataReplicationTaskSuite) TestRetryErr_Retryable_ExceedDuration() {
	err := serviceerror.NewInternal("")
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

func (s *activityReplicationTaskSuite) getActivityReplicationTask() *replication.ReplicationTask {
	replicationAttr := &replication.SyncActivityTaskAttributes{
		DomainId:           "some random domain ID",
		WorkflowId:         "some random workflow ID",
		RunId:              "some random run ID",
		Version:            1394,
		ScheduledId:        728,
		ScheduledTime:      time.Now().UnixNano(),
		StartedId:          1015,
		StartedTime:        time.Now().UnixNano(),
		LastHeartbeatTime:  time.Now().UnixNano(),
		Details:            []byte("some random detail"),
		Attempt:            59,
		LastFailureReason:  "some random failure reason",
		LastWorkerIdentity: "some random worker identity",
		LastFailureDetails: []byte("some random failure details"),
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeSyncActivity,
		Attributes: &replication.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	return replicationTask
}

func (s *historyReplicationTaskSuite) getHistoryReplicationTask() *replication.ReplicationTask {
	replicationAttr := &replication.HistoryTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       "some random domain ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		Version:        1394,
		FirstEventId:   728,
		NextEventId:    1015,
		ReplicationInfo: map[string]*replication.ReplicationInfo{
			cluster.TestCurrentClusterName: {
				Version:     0644,
				LastEventId: 0755,
			},
			cluster.TestAlternativeClusterName: {
				Version:     0755,
				LastEventId: 0644,
			},
		},
		History: &commonproto.History{
			Events: []*commonproto.HistoryEvent{{EventId: 1}},
		},
		NewRunHistory: &commonproto.History{
			Events: []*commonproto.HistoryEvent{{EventId: 2}},
		},
		ResetWorkflow: true,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeHistory,
		Attributes: &replication.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: replicationAttr},
	}
	return replicationTask
}

func (s *historyMetadataReplicationTaskSuite) getHistoryMetadataReplicationTask() *replication.ReplicationTask {
	replicationAttr := &replication.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       "some random domain ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeHistoryMetadata,
		Attributes: &replication.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	return replicationTask
}
