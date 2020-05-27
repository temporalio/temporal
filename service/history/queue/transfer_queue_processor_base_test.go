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
	"errors"
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
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

		redispatchQueue collection.Queue
		logger          log.Logger
		metricsClient   metrics.Client
		metricsScope    metrics.Scope
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
	s.mockQueueSplitPolicy = NewMockProcessingQueueSplitPolicy(s.controller)
	s.mockTaskProcessor = task.NewMockProcessor(s.controller)

	s.redispatchQueue = collection.NewConcurrentQueue()
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.metricsScope = s.metricsClient.Scope(metrics.TransferQueueProcessorScope)
}

func (s *transferQueueProcessorBaseSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *transferQueueProcessorBaseSuite) TestUpdateAckLevel_ProcessedFinished() {
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			2,
			newTransferTaskKey(100),
			newTransferTaskKey(100),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			0,
			newTransferTaskKey(1000),
			newTransferTaskKey(1000),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}, "testDomain2": {}}, true),
		),
	}
	queueShutdown := false
	queueShutdownFn := func() error {
		queueShutdown = true
		return nil
	}

	processorBase := s.newTestTransferQueueProcessBase(
		processingQueueStates,
		nil,
		nil,
		queueShutdownFn,
		nil,
	)

	processFinished, err := processorBase.updateAckLevel()
	s.NoError(err)
	s.True(processFinished)
	s.True(queueShutdown)
}

func (s *transferQueueProcessorBaseSuite) TestUpdateAckLevel_ProcessNotFinished() {
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			2,
			newTransferTaskKey(5),
			newTransferTaskKey(100),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			1,
			newTransferTaskKey(2),
			newTransferTaskKey(100),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			0,
			newTransferTaskKey(100),
			newTransferTaskKey(1000),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}, "testDomain2": {}}, true),
		),
	}
	updateAckLevel := int64(0)
	updateTransferAckLevelFn := func(ackLevel int64) error {
		updateAckLevel = ackLevel
		return nil
	}

	processorBase := s.newTestTransferQueueProcessBase(
		processingQueueStates,
		nil,
		updateTransferAckLevelFn,
		nil,
		nil,
	)

	processFinished, err := processorBase.updateAckLevel()
	s.NoError(err)
	s.False(processFinished)
	s.Equal(int64(2), updateAckLevel)
}

func (s *transferQueueProcessorBaseSuite) TestSplitQueue() {
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			0,
			newTransferTaskKey(0),
			newTransferTaskKey(100),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, true),
		),
		NewProcessingQueueState(
			1,
			newTransferTaskKey(0),
			newTransferTaskKey(100),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			0,
			newTransferTaskKey(100),
			newTransferTaskKey(1000),
			NewDomainFilter(map[string]struct{}{}, true),
		),
	}
	s.mockQueueSplitPolicy.EXPECT().Evaluate(NewProcessingQueue(processingQueueStates[0], s.logger, s.metricsClient)).Return(nil).Times(1)
	s.mockQueueSplitPolicy.EXPECT().Evaluate(NewProcessingQueue(processingQueueStates[1], s.logger, s.metricsClient)).Return([]ProcessingQueueState{
		NewProcessingQueueState(
			2,
			newTransferTaskKey(0),
			newTransferTaskKey(100),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
	}).Times(1)
	s.mockQueueSplitPolicy.EXPECT().Evaluate(NewProcessingQueue(processingQueueStates[2], s.logger, s.metricsClient)).Return([]ProcessingQueueState{
		NewProcessingQueueState(
			0,
			newTransferTaskKey(100),
			newTransferTaskKey(1000),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}, "testDomain2": {}, "testDomain3": {}}, false),
		),
		NewProcessingQueueState(
			1,
			newTransferTaskKey(100),
			newTransferTaskKey(1000),
			NewDomainFilter(map[string]struct{}{"testDomain2": {}}, false),
		),
		NewProcessingQueueState(
			2,
			newTransferTaskKey(100),
			newTransferTaskKey(1000),
			NewDomainFilter(map[string]struct{}{"testDomain3": {}}, false),
		),
	}).Times(1)

	processorBase := s.newTestTransferQueueProcessBase(
		processingQueueStates,
		nil,
		nil,
		nil,
		nil,
	)

	processorBase.splitQueue()
	s.Len(processorBase.processingQueueCollections, 3)
	s.Len(processorBase.processingQueueCollections[0].Queues(), 2)
	s.Len(processorBase.processingQueueCollections[1].Queues(), 1)
	s.Len(processorBase.processingQueueCollections[2].Queues(), 2)
	for idx := 1; idx != len(processorBase.processingQueueCollections)-1; idx++ {
		s.Less(
			processorBase.processingQueueCollections[idx-1].Level(),
			processorBase.processingQueueCollections[idx].Level(),
		)
	}
}

func (s *transferQueueProcessorBaseSuite) TestReadTasks_PartialRead_NoNextPage() {
	readLevel := newTransferTaskKey(3)
	maxReadLevel := newTransferTaskKey(100)
	shardMaxReadLevel := newTransferTaskKey(10)
	shardMaxReadLevelFn := func() task.Key {
		return shardMaxReadLevel
	}

	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	getTransferTaskResponse := &persistence.GetTransferTasksResponse{
		Tasks:         []*persistence.TransferTaskInfo{{}, {}, {}},
		NextPageToken: nil,
	}
	mockExecutionManager.On("GetTransferTasks", &persistence.GetTransferTasksRequest{
		ReadLevel:    readLevel.(*transferTaskKey).taskID,
		MaxReadLevel: shardMaxReadLevel.(*transferTaskKey).taskID,
		BatchSize:    s.mockShard.GetConfig().TransferTaskBatchSize(),
	}).Return(getTransferTaskResponse, nil).Once()

	processorBase := s.newTestTransferQueueProcessBase(
		nil,
		shardMaxReadLevelFn,
		nil,
		nil,
		nil,
	)

	tasks, more, partialRead, err := processorBase.readTasks(readLevel, maxReadLevel)
	s.NoError(err)
	s.Len(tasks, len(getTransferTaskResponse.Tasks))
	s.False(more)
	s.True(partialRead)
}

func (s *transferQueueProcessorBaseSuite) TestReadTasks_FullRead_WithNextPage() {
	readLevel := newTransferTaskKey(3)
	maxReadLevel := newTransferTaskKey(10)
	shardMaxReadLevel := newTransferTaskKey(100)
	shardMaxReadLevelFn := func() task.Key {
		return shardMaxReadLevel
	}

	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	getTransferTaskResponse := &persistence.GetTransferTasksResponse{
		Tasks:         []*persistence.TransferTaskInfo{{}, {}, {}},
		NextPageToken: []byte{1, 2, 3},
	}
	mockExecutionManager.On("GetTransferTasks", &persistence.GetTransferTasksRequest{
		ReadLevel:    readLevel.(*transferTaskKey).taskID,
		MaxReadLevel: maxReadLevel.(*transferTaskKey).taskID,
		BatchSize:    s.mockShard.GetConfig().TransferTaskBatchSize(),
	}).Return(getTransferTaskResponse, nil).Once()

	processorBase := s.newTestTransferQueueProcessBase(
		nil,
		shardMaxReadLevelFn,
		nil,
		nil,
		nil,
	)

	tasks, more, partialRead, err := processorBase.readTasks(readLevel, maxReadLevel)
	s.NoError(err)
	s.Len(tasks, len(getTransferTaskResponse.Tasks))
	s.True(more)
	s.False(partialRead)
}

func (s *transferQueueProcessorBaseSuite) TestRedispatchTask_ProcessorShutDown() {
	numTasks := 5
	for i := 0; i != numTasks; i++ {
		mockTask := task.NewMockTask(s.controller)
		s.redispatchQueue.Add(mockTask)
	}

	successfullyRedispatched := 3
	var calls []*gomock.Call
	for i := 0; i != successfullyRedispatched; i++ {
		calls = append(calls, s.mockTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(true, nil))
	}
	calls = append(calls, s.mockTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(false, errors.New("processor shutdown")))
	gomock.InOrder(calls...)

	shutDownCh := make(chan struct{})
	RedispatchTasks(
		s.redispatchQueue,
		s.mockTaskProcessor,
		s.logger,
		s.metricsScope,
		shutDownCh,
	)

	s.Equal(numTasks-successfullyRedispatched-1, s.redispatchQueue.Len())
}

func (s *transferQueueProcessorBaseSuite) TestRedispatchTask_Random() {
	numTasks := 10
	dispatched := 0
	var calls []*gomock.Call

	for i := 0; i != numTasks; i++ {
		mockTask := task.NewMockTask(s.controller)
		s.redispatchQueue.Add(mockTask)
		submitted := false
		if rand.Intn(2) == 0 {
			submitted = true
			dispatched++
		}
		calls = append(calls, s.mockTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(submitted, nil))
	}

	shutDownCh := make(chan struct{})
	RedispatchTasks(
		s.redispatchQueue,
		s.mockTaskProcessor,
		s.logger,
		s.metricsScope,
		shutDownCh,
	)

	s.Equal(numTasks-dispatched, s.redispatchQueue.Len())
}

func (s *transferQueueProcessorBaseSuite) newTestTransferQueueProcessBase(
	processingQueueStates []ProcessingQueueState,
	maxReadLevel maxReadLevel,
	updateTransferAckLevel updateTransferAckLevel,
	transferQueueShutdown transferQueueShutdown,
	taskInitializer task.Initializer,
) *transferQueueProcessorBase {
	testConfig := s.mockShard.GetConfig()
	testQueueProcessorOptions := &queueProcessorOptions{
		BatchSize:                           testConfig.TransferTaskBatchSize,
		MaxPollRPS:                          testConfig.TransferProcessorMaxPollRPS,
		MaxPollInterval:                     testConfig.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    testConfig.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   testConfig.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  testConfig.TransferProcessorUpdateAckIntervalJitterCoefficient,
		SplitQueueInterval:                  testConfig.TransferProcessorSplitQueueInterval,
		SplitQueueIntervalJitterCoefficient: testConfig.TransferProcessorSplitQueueIntervalJitterCoefficient,
		QueueSplitPolicy:                    s.mockQueueSplitPolicy,
		RedispatchInterval:                  testConfig.TransferProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: testConfig.TransferProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              testConfig.TransferProcessorMaxRedispatchQueueSize,
		MetricScope:                         metrics.TransferQueueProcessorScope,
	}
	return newTransferQueueProcessorBase(
		s.mockShard,
		processingQueueStates,
		s.mockTaskProcessor,
		s.redispatchQueue,
		testQueueProcessorOptions,
		maxReadLevel,
		updateTransferAckLevel,
		transferQueueShutdown,
		taskInitializer,
		s.logger,
		s.metricsClient,
	)
}
