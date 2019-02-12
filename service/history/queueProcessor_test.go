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

package history

import (
	"errors"
	"os"
	"testing"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	queueProcessorSuite struct {
		clusterName         string
		logger              bark.Logger
		mockService         service.Service
		mockShard           ShardContext
		mockMetadataMgr     *mocks.MetadataManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockMessagingClient messaging.Client
		mockProcessor       *MockProcessor
		mockQueueAckMgr     *MockQueueAckMgr
		mockClientBean      *client.MockClientBean

		scope            int
		notificationChan chan struct{}

		suite.Suite
		queueProcessor *queueProcessorBase
	}
)

func TestQueueProcessorSuite(t *testing.T) {
	s := new(queueProcessorSuite)
	suite.Run(t, s)
}

func (s *queueProcessorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *queueProcessorSuite) TearDownSuite() {

}

func (s *queueProcessorSuite) SetupTest() {
	shardID := 0
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.clusterName = cluster.TestAlternativeClusterName
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.mockProcessor = &MockProcessor{}
	s.mockQueueAckMgr = &MockQueueAckMgr{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, nil, metricsClient, s.mockClientBean, s.logger)
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		metricsClient:             metricsClient,
		standbyClusterCurrentTime: make(map[string]time.Time),
	}

	s.scope = 0
	s.notificationChan = make(chan struct{})

	s.queueProcessor = newQueueProcessorBase(
		s.clusterName,
		s.mockShard,
		&QueueProcessorOptions{
			MaxPollRPS:    dynamicconfig.GetIntPropertyFn(10),
			WorkerCount:   dynamicconfig.GetIntPropertyFn(1),
			MaxRetryCount: dynamicconfig.GetIntPropertyFn(1),
		},
		s.mockProcessor,
		s.mockQueueAckMgr,
		s.logger,
	)
}

func (s *queueProcessorSuite) TearDownTest() {
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockProcessor.AssertExpectations(s.T())
	s.mockQueueAckMgr.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
}

func (s *queueProcessorSuite) TestProcessTaskAndAck_ShutDown() {
	close(s.queueProcessor.shutdownCh)
	s.queueProcessor.processTaskAndAck(s.notificationChan, &persistence.TransferTaskInfo{})
}

func (s *queueProcessorSuite) TestProcessTaskAndAck_DomainErrRetry_ProcessNoErr() {
	task := &persistence.TransferTaskInfo{TaskID: 12345}
	var taskFilterErr queueTaskFilter = func(qTask queueTaskInfo) (bool, error) {
		return false, errors.New("some random error")
	}
	var taskFilter queueTaskFilter = func(qTask queueTaskInfo) (bool, error) {
		return true, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilterErr).Once()
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task, true).Return(s.scope, nil).Once()
	s.mockQueueAckMgr.On("completeQueueTask", task.GetTaskID()).Once()
	s.queueProcessor.processTaskAndAck(s.notificationChan, task)
}

func (s *queueProcessorSuite) TestProcessTaskAndAck_DomainFalse_ProcessNoErr() {
	task := &persistence.TransferTaskInfo{TaskID: 12345}
	var taskFilter queueTaskFilter = func(qTask queueTaskInfo) (bool, error) {
		return false, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task, false).Return(s.scope, nil).Once()
	s.mockQueueAckMgr.On("completeQueueTask", task.GetTaskID()).Once()
	s.queueProcessor.processTaskAndAck(s.notificationChan, task)
}

func (s *queueProcessorSuite) TestProcessTaskAndAck_DomainTrue_ProcessNoErr() {
	task := &persistence.TransferTaskInfo{TaskID: 12345}
	var taskFilter queueTaskFilter = func(qTask queueTaskInfo) (bool, error) {
		return true, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task, true).Return(s.scope, nil).Once()
	s.mockQueueAckMgr.On("completeQueueTask", task.GetTaskID()).Once()
	s.queueProcessor.processTaskAndAck(s.notificationChan, task)
}

func (s *queueProcessorSuite) TestProcessTaskAndAck_DomainTrue_ProcessErrNoErr() {
	err := errors.New("some random err")
	task := &persistence.TransferTaskInfo{TaskID: 12345}
	var taskFilter queueTaskFilter = func(qTask queueTaskInfo) (bool, error) {
		return true, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task, true).Return(s.scope, err).Once()
	s.mockProcessor.On("process", task, true).Return(s.scope, nil).Once()
	s.mockQueueAckMgr.On("completeQueueTask", task.GetTaskID()).Once()
	s.queueProcessor.processTaskAndAck(s.notificationChan, task)
}

func (s *queueProcessorSuite) TestHandleTaskError_EntiryNotExists() {
	err := &workflow.EntityNotExistsError{}
	s.Nil(s.queueProcessor.handleTaskError(s.scope, time.Now(), s.notificationChan, err, s.logger))
}

func (s *queueProcessorSuite) TestHandleTaskError_ErrTaskRetry() {
	err := ErrTaskRetry
	delay := time.Second

	startTime := time.Now()
	go func() {
		time.Sleep(delay)
		s.notificationChan <- struct{}{}
	}()

	err = s.queueProcessor.handleTaskError(s.scope, time.Now(), s.notificationChan, err, s.logger)
	duration := time.Since(startTime)
	s.True(duration >= delay)
	s.Equal(ErrTaskRetry, err)
}

func (s *queueProcessorSuite) TestHandleTaskError_ErrTaskDiscarded() {
	err := ErrTaskDiscarded
	s.Nil(s.queueProcessor.handleTaskError(s.scope, time.Now(), s.notificationChan, err, s.logger))
}

func (s *queueProcessorSuite) TestHandleTaskError_DomainNotActiveError() {
	err := &workflow.DomainNotActiveError{}

	startTime := time.Now().Add(-cache.DomainCacheRefreshInterval * time.Duration(2))
	s.Nil(s.queueProcessor.handleTaskError(s.scope, startTime, s.notificationChan, err, s.logger))

	startTime = time.Now()
	s.Equal(err, s.queueProcessor.handleTaskError(s.scope, startTime, s.notificationChan, err, s.logger))
}

func (s *queueProcessorSuite) TestHandleTaskError_CurrentWorkflowConditionFailedError() {
	err := &persistence.CurrentWorkflowConditionFailedError{}
	s.Nil(s.queueProcessor.handleTaskError(s.scope, time.Now(), s.notificationChan, err, s.logger))
}

func (s *queueProcessorSuite) TestHandleTaskError_LimitExceededError() {
	err := &workflow.LimitExceededError{}
	s.Equal(err, s.queueProcessor.handleTaskError(s.scope, time.Now(), s.notificationChan, err, s.logger))
}

func (s *queueProcessorSuite) TestHandleTaskError_RandomErr() {
	err := errors.New("random error")
	s.Equal(err, s.queueProcessor.handleTaskError(s.scope, time.Now(), s.notificationChan, err, s.logger))
}
