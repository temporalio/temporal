// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queue

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/constants"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

type (
	transferQueueProcessorBaseSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.TestContext
		mockTaskProcessor    *task.MockProcessor
		mockQueueSplitPolicy *MockProcessingQueueSplitPolicy

		logger        log.Logger
		metricsClient metrics.Client
		metricsScope  metrics.Scope
	}
)

func TestTransferQueueProcessorBaseSuite(t *testing.T) {
	s := new(transferQueueProcessorBaseSuite)
	suite.Run(t, s)
}

func (s *transferQueueProcessorBaseSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:          10,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		config.NewForTest(),
	)
	s.mockShard.Resource.DomainCache.EXPECT().GetDomainName(gomock.Any()).Return(constants.TestDomainName, nil).AnyTimes()
	s.mockQueueSplitPolicy = NewMockProcessingQueueSplitPolicy(s.controller)
	s.mockTaskProcessor = task.NewMockProcessor(s.controller)

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.metricsScope = s.metricsClient.Scope(metrics.TransferQueueProcessorScope)
}

func (s *transferQueueProcessorBaseSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *transferQueueProcessorBaseSuite) TestProcessQueueCollections_NoNextPage_FullRead() {
	queueLevel := 0
	ackLevel := newTransferTaskKey(0)
	maxLevel := newTransferTaskKey(1000)
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			queueLevel,
			ackLevel,
			maxLevel,
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			0,
			newTransferTaskKey(1000),
			newTransferTaskKey(10000),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
	}
	updateMaxReadLevel := func() task.Key {
		return newTransferTaskKey(10000)
	}
	taskInfos := []*persistence.TransferTaskInfo{
		{
			TaskID:   1,
			DomainID: "testDomain1",
		},
		{
			TaskID:   10,
			DomainID: "testDomain2",
		},
		{
			TaskID:   100,
			DomainID: "testDomain1",
		},
	}
	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	mockExecutionManager.On("GetTransferTasks", &persistence.GetTransferTasksRequest{
		ReadLevel:    ackLevel.(transferTaskKey).taskID,
		MaxReadLevel: maxLevel.(transferTaskKey).taskID,
		BatchSize:    s.mockShard.GetConfig().TransferTaskBatchSize(),
	}).Return(&persistence.GetTransferTasksResponse{
		Tasks:         taskInfos,
		NextPageToken: nil,
	}, nil).Once()

	s.mockTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).AnyTimes()

	processorBase := s.newTestTransferQueueProcessorBase(
		processingQueueStates,
		updateMaxReadLevel,
		nil,
		nil,
	)

	processorBase.processQueueCollections(map[int]struct{}{0: {}})

	queueCollection := processorBase.processingQueueCollections[0]
	s.NotNil(queueCollection.ActiveQueue())
	s.True(taskKeyEquals(maxLevel, queueCollection.Queues()[0].State().ReadLevel()))

	s.True(processorBase.nextPollTime[queueLevel].Before(processorBase.shard.GetTimeSource().Now()))
	time.Sleep(time.Millisecond * 100)
	select {
	case <-processorBase.nextPollTimer.FireChan():
	default:
		s.Fail("poll timer should fire")
	}
}

func (s *transferQueueProcessorBaseSuite) TestProcessQueueCollections_NoNextPage_PartialRead() {
	queueLevel := 1 // non default queue
	ackLevel := newTransferTaskKey(0)
	maxLevel := newTransferTaskKey(1000)
	shardMaxLevel := newTransferTaskKey(500)
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			queueLevel,
			ackLevel,
			maxLevel,
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
	}
	updateMaxReadLevel := func() task.Key {
		return shardMaxLevel
	}
	taskInfos := []*persistence.TransferTaskInfo{
		{
			TaskID:   1,
			DomainID: "testDomain1",
		},
		{
			TaskID:   10,
			DomainID: "testDomain2",
		},
		{
			TaskID:   100,
			DomainID: "testDomain1",
		},
	}
	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	mockExecutionManager.On("GetTransferTasks", &persistence.GetTransferTasksRequest{
		ReadLevel:    ackLevel.(transferTaskKey).taskID,
		MaxReadLevel: shardMaxLevel.(transferTaskKey).taskID,
		BatchSize:    s.mockShard.GetConfig().TransferTaskBatchSize(),
	}).Return(&persistence.GetTransferTasksResponse{
		Tasks:         taskInfos,
		NextPageToken: nil,
	}, nil).Once()

	s.mockTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).AnyTimes()

	processorBase := s.newTestTransferQueueProcessorBase(
		processingQueueStates,
		updateMaxReadLevel,
		nil,
		nil,
	)

	processorBase.processQueueCollections(map[int]struct{}{1: {}})

	queueCollection := processorBase.processingQueueCollections[0]
	s.NotNil(queueCollection.ActiveQueue())
	s.True(taskKeyEquals(shardMaxLevel, queueCollection.Queues()[0].State().ReadLevel()))

	s.True(processorBase.nextPollTime[queueLevel].Before(processorBase.shard.GetTimeSource().Now().Add(nonDefaultQueueBackoffDuration)))
	time.Sleep(time.Millisecond * 100)
	select {
	case <-processorBase.nextPollTimer.FireChan():
		s.Fail("poll timer should not fire")
	default:
	}
}

func (s *transferQueueProcessorBaseSuite) TestProcessQueueCollections_WithNextPage() {
	queueLevel := 0
	ackLevel := newTransferTaskKey(0)
	maxLevel := newTransferTaskKey(1000)
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			queueLevel,
			ackLevel,
			maxLevel,
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
	}
	updateMaxReadLevel := func() task.Key {
		return newTransferTaskKey(10000)
	}
	taskInfos := []*persistence.TransferTaskInfo{
		{
			TaskID:   1,
			DomainID: "testDomain1",
		},
		{
			TaskID:   10,
			DomainID: "testDomain2",
		},
		{
			TaskID:   100,
			DomainID: "testDomain1",
		},
		{
			TaskID:   500,
			DomainID: "testDomain2",
		},
	}
	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	mockExecutionManager.On("GetTransferTasks", &persistence.GetTransferTasksRequest{
		ReadLevel:    ackLevel.(transferTaskKey).taskID,
		MaxReadLevel: maxLevel.(transferTaskKey).taskID,
		BatchSize:    s.mockShard.GetConfig().TransferTaskBatchSize(),
	}).Return(&persistence.GetTransferTasksResponse{
		Tasks:         taskInfos,
		NextPageToken: []byte{1, 2, 3},
	}, nil).Once()

	s.mockTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).AnyTimes()

	processorBase := s.newTestTransferQueueProcessorBase(
		processingQueueStates,
		updateMaxReadLevel,
		nil,
		nil,
	)

	processorBase.processQueueCollections(map[int]struct{}{0: {}})

	queueCollection := processorBase.processingQueueCollections[0]
	s.NotNil(queueCollection.ActiveQueue())
	s.True(taskKeyEquals(newTransferTaskKey(500), queueCollection.Queues()[0].State().ReadLevel()))

	s.True(processorBase.nextPollTime[queueLevel].Before(processorBase.shard.GetTimeSource().Now()))
	time.Sleep(time.Millisecond * 100)
	select {
	case <-processorBase.nextPollTimer.FireChan():

	default:
		s.Fail("poll timer should fire")
	}
}

func (s *transferQueueProcessorBaseSuite) TestReadTasks_NoNextPage() {
	readLevel := newTransferTaskKey(3)
	maxReadLevel := newTransferTaskKey(100)

	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	getTransferTaskResponse := &persistence.GetTransferTasksResponse{
		Tasks:         []*persistence.TransferTaskInfo{{}, {}, {}},
		NextPageToken: nil,
	}
	mockExecutionManager.On("GetTransferTasks", &persistence.GetTransferTasksRequest{
		ReadLevel:    readLevel.(transferTaskKey).taskID,
		MaxReadLevel: maxReadLevel.(transferTaskKey).taskID,
		BatchSize:    s.mockShard.GetConfig().TransferTaskBatchSize(),
	}).Return(getTransferTaskResponse, nil).Once()

	processorBase := s.newTestTransferQueueProcessorBase(
		nil,
		nil,
		nil,
		nil,
	)

	tasks, more, err := processorBase.readTasks(readLevel, maxReadLevel)
	s.NoError(err)
	s.Len(tasks, len(getTransferTaskResponse.Tasks))
	s.False(more)
}

func (s *transferQueueProcessorBaseSuite) TestReadTasks_WithNextPage() {
	readLevel := newTransferTaskKey(3)
	maxReadLevel := newTransferTaskKey(10)

	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	getTransferTaskResponse := &persistence.GetTransferTasksResponse{
		Tasks:         []*persistence.TransferTaskInfo{{}, {}, {}},
		NextPageToken: []byte{1, 2, 3},
	}
	mockExecutionManager.On("GetTransferTasks", &persistence.GetTransferTasksRequest{
		ReadLevel:    readLevel.(transferTaskKey).taskID,
		MaxReadLevel: maxReadLevel.(transferTaskKey).taskID,
		BatchSize:    s.mockShard.GetConfig().TransferTaskBatchSize(),
	}).Return(getTransferTaskResponse, nil).Once()

	processorBase := s.newTestTransferQueueProcessorBase(
		nil,
		nil,
		nil,
		nil,
	)

	tasks, more, err := processorBase.readTasks(readLevel, maxReadLevel)
	s.NoError(err)
	s.Len(tasks, len(getTransferTaskResponse.Tasks))
	s.True(more)
}

func (s *transferQueueProcessorBaseSuite) newTestTransferQueueProcessorBase(
	processingQueueStates []ProcessingQueueState,
	maxReadLevel updateMaxReadLevelFn,
	updateTransferAckLevel updateClusterAckLevelFn,
	transferQueueShutdown queueShutdownFn,
) *transferQueueProcessorBase {
	return newTransferQueueProcessorBase(
		s.mockShard,
		processingQueueStates,
		s.mockTaskProcessor,
		newTransferQueueProcessorOptions(s.mockShard.GetConfig(), true, false),
		maxReadLevel,
		updateTransferAckLevel,
		transferQueueShutdown,
		nil,
		nil,
		s.logger,
		s.metricsClient,
	)
}
