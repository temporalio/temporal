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

package history

import (
	"math/rand"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	queueProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller             *gomock.Controller
		mockQueueTaskProcessor *MockqueueTaskProcessor

		redispatchQueue collection.Queue
		logger          log.Logger
		metricsScope    metrics.Scope
	}
)

func TestQueueProcessorSuite(t *testing.T) {
	s := new(queueProcessorSuite)
	suite.Run(t, s)
}

func (s *queueProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockQueueTaskProcessor = NewMockqueueTaskProcessor(s.controller)

	s.redispatchQueue = collection.NewConcurrentQueue()
	s.logger = log.NewTestLogger()
	s.metricsScope = metrics.NewClient(tally.NoopScope, metrics.History).Scope(metrics.TransferQueueProcessorScope)
}

func (s *queueProcessorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *queueProcessorSuite) TestRedispatchTask_ProcessorShutDown() {
	numTasks := 5
	for i := 0; i != numTasks; i++ {
		mockTask := NewMockqueueTask(s.controller)
		s.redispatchQueue.Add(mockTask)
	}

	successfullyRedispatched := 3
	var calls []*gomock.Call
	for i := 0; i != successfullyRedispatched; i++ {
		calls = append(calls, s.mockQueueTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(true, nil))
	}
	calls = append(calls, s.mockQueueTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(false, errTaskProcessorNotRunning))
	gomock.InOrder(calls...)

	shutDownCh := make(chan struct{})
	redispatchQueueTasks(
		s.redispatchQueue,
		s.mockQueueTaskProcessor,
		s.logger,
		s.metricsScope,
		shutDownCh,
	)

	s.Equal(numTasks-successfullyRedispatched-1, s.redispatchQueue.Len())
}

func (s *queueProcessorSuite) TestRedispatchTask_Random() {
	numTasks := 10
	dispatched := 0
	var calls []*gomock.Call

	for i := 0; i != numTasks; i++ {
		mockTask := NewMockqueueTask(s.controller)
		s.redispatchQueue.Add(mockTask)
		submitted := false
		if rand.Intn(2) == 0 {
			submitted = true
			dispatched++
		}
		calls = append(calls, s.mockQueueTaskProcessor.EXPECT().TrySubmit(gomock.Any()).Return(submitted, nil))
	}

	shutDownCh := make(chan struct{})
	redispatchQueueTasks(
		s.redispatchQueue,
		s.mockQueueTaskProcessor,
		s.logger,
		s.metricsScope,
		shutDownCh,
	)

	s.Equal(numTasks-dispatched, s.redispatchQueue.Len())
}
