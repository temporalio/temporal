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
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

type (
	processorBaseSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		mockShard         *shard.TestContext
		mockTaskProcessor *task.MockProcessor

		redispatchQueue collection.Queue
		logger          log.Logger
		metricsClient   metrics.Client
		metricsScope    metrics.Scope
	}
)

func TestProcessorBaseSuite(t *testing.T) {
	s := new(processorBaseSuite)
	suite.Run(t, s)
}

func (s *processorBaseSuite) SetupTest() {
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
	s.mockTaskProcessor = task.NewMockProcessor(s.controller)

	s.redispatchQueue = collection.NewConcurrentQueue()
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.metricsScope = s.metricsClient.Scope(metrics.TransferQueueProcessorScope)
}

func (s *processorBaseSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *processorBaseSuite) TestSplitQueue() {
	mockQueueSplitPolicy := NewMockProcessingQueueSplitPolicy(s.controller)

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
	mockQueueSplitPolicy.EXPECT().Evaluate(NewProcessingQueue(processingQueueStates[0], s.logger, s.metricsClient)).Return(nil).Times(1)
	mockQueueSplitPolicy.EXPECT().Evaluate(NewProcessingQueue(processingQueueStates[1], s.logger, s.metricsClient)).Return([]ProcessingQueueState{
		NewProcessingQueueState(
			2,
			newTransferTaskKey(0),
			newTransferTaskKey(100),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
	}).Times(1)
	mockQueueSplitPolicy.EXPECT().Evaluate(NewProcessingQueue(processingQueueStates[2], s.logger, s.metricsClient)).Return([]ProcessingQueueState{
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

	processorBase := s.newTestProcessorBase(
		processingQueueStates,
		nil,
		nil,
		nil,
		nil,
	)

	nextPollTime := make(map[int]time.Time)
	processorBase.splitProcessingQueueCollection(
		mockQueueSplitPolicy,
		func(level int, pollTime time.Time) {
			nextPollTime[level] = pollTime
		},
	)

	processingQueueCollections := processorBase.processingQueueCollections
	sort.Slice(processingQueueCollections, func(i, j int) bool {
		return processingQueueCollections[i].Level() < processingQueueCollections[j].Level()
	})
	s.Len(processingQueueCollections, 3)
	s.Len(processingQueueCollections[0].Queues(), 2)
	s.Len(processingQueueCollections[1].Queues(), 1)
	s.Len(processingQueueCollections[2].Queues(), 2)
	for idx := 1; idx != len(processingQueueCollections)-1; idx++ {
		s.Less(
			processingQueueCollections[idx-1].Level(),
			processingQueueCollections[idx].Level(),
		)
	}
	s.Len(nextPollTime, 3)
	for _, nextPollTime := range nextPollTime {
		s.Zero(nextPollTime)
	}
}

func (s *processorBaseSuite) TestUpdateAckLevel_Transfer_ProcessedFinished() {
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

	processorBase := s.newTestProcessorBase(
		processingQueueStates,
		nil,
		nil,
		nil,
		queueShutdownFn,
	)

	processFinished, err := processorBase.updateAckLevel()
	s.NoError(err)
	s.True(processFinished)
	s.True(queueShutdown)
}

func (s *processorBaseSuite) TestUpdateAckLevel_Tranfer_ProcessNotFinished() {
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
	updateTransferAckLevelFn := func(ackLevel task.Key) error {
		updateAckLevel = ackLevel.(transferTaskKey).taskID
		return nil
	}

	processorBase := s.newTestProcessorBase(
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

func (s *processorBaseSuite) TestUpdateAckLevel_Timer_UpdateAckLevel() {
	now := time.Now()
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			2,
			newTimerTaskKey(now.Add(-5*time.Second), 0),
			newTimerTaskKey(now, 0),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			1,
			newTimerTaskKey(now.Add(-3*time.Second), 0),
			newTimerTaskKey(now.Add(5*time.Second), 0),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			0,
			newTimerTaskKey(now.Add(-1*time.Second), 0),
			newTimerTaskKey(now.Add(100*time.Second), 0),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}, "testDomain2": {}}, true),
		),
	}
	updateAckLevel := time.Time{}
	updateTransferAckLevelFn := func(ackLevel task.Key) error {
		updateAckLevel = ackLevel.(timerTaskKey).visibilityTimestamp
		return nil
	}

	timerQueueProcessBase := s.newTestProcessorBase(processingQueueStates, nil, updateTransferAckLevelFn, nil, nil)
	timerQueueProcessBase.options.EnablePersistQueueStates = dynamicconfig.GetBoolPropertyFn(true)
	processFinished, err := timerQueueProcessBase.updateAckLevel()
	s.NoError(err)
	s.False(processFinished)
	s.Equal(now.Add(-5*time.Second), updateAckLevel)
}

func (s *processorBaseSuite) TestUpdateAckLevel_Timer_UpdateQueueStates() {
	now := time.Now()
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			2,
			newTimerTaskKey(now.Add(-5*time.Second), 0),
			newTimerTaskKey(now, 0),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			1,
			newTimerTaskKey(now.Add(-3*time.Second), 0),
			newTimerTaskKey(now.Add(5*time.Second), 0),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}}, false),
		),
		NewProcessingQueueState(
			0,
			newTimerTaskKey(now.Add(-1*time.Second), 0),
			newTimerTaskKey(now.Add(100*time.Second), 0),
			NewDomainFilter(map[string]struct{}{"testDomain1": {}, "testDomain2": {}}, true),
		),
	}

	var pState []*h.ProcessingQueueState
	updateProcessingQueueStates := func(states []ProcessingQueueState) error {
		pState = convertToPersistenceTimerProcessingQueueStates(states)
		return nil
	}

	timerQueueProcessBase := s.newTestProcessorBase(processingQueueStates, nil, nil, updateProcessingQueueStates, nil)
	timerQueueProcessBase.options.EnablePersistQueueStates = dynamicconfig.GetBoolPropertyFn(true)
	processFinished, err := timerQueueProcessBase.updateAckLevel()
	s.NoError(err)
	s.False(processFinished)
	s.Equal(len(processingQueueStates), len(pState))
}

func (s *processorBaseSuite) newTestProcessorBase(
	processingQueueStates []ProcessingQueueState,
	updateMaxReadLevel updateMaxReadLevelFn,
	updateClusterAckLevel updateClusterAckLevelFn,
	updateProcessingQueueStates updateProcessingQueueStatesFn,
	queueShutdown queueShutdownFn,
) *processorBase {
	return newProcessorBase(
		s.mockShard,
		processingQueueStates,
		s.mockTaskProcessor,
		newTransferQueueProcessorOptions(s.mockShard.GetConfig(), true, false),
		updateMaxReadLevel,
		updateClusterAckLevel,
		updateProcessingQueueStates,
		queueShutdown,
		s.logger,
		s.metricsClient,
	)
}
