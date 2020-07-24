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

package task

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	redispatcherSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		mockProcessor *MockProcessor

		metricsScope metrics.Scope
		logger       log.Logger

		redispatcher *redispatcherImpl
	}
)

func TestRedispatcherSuite(t *testing.T) {
	s := new(redispatcherSuite)
	suite.Run(t, s)
}

func (s *redispatcherSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockProcessor = NewMockProcessor(s.controller)

	s.metricsScope = metrics.NewClient(tally.NoopScope, metrics.History).Scope(0)
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)

	s.redispatcher = s.newTestRedispatcher()
	s.redispatcher.Start()
}

func (s *redispatcherSuite) TearDownTest() {
	s.redispatcher.Stop()
	s.Nil(s.redispatcher.redispatchTimer)
	s.controller.Finish()
}

func (s *redispatcherSuite) TestRedispatch_ProcessorShutDown() {
	numTasks := 5

	successfullyRedispatched := 3
	stoppedCh := make(chan struct{})
	for i := 0; i != successfullyRedispatched-1; i++ {
		s.mockProcessor.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).Times(1)
	}
	s.mockProcessor.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ interface{}) (bool, error) {
		go func() {
			s.redispatcher.Stop()
			close(stoppedCh)
		}()
		return true, nil
	}).Times(1)
	s.mockProcessor.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ interface{}) (bool, error) {
		<-s.redispatcher.shutdownCh
		return false, errors.New("processor shutdown")
	}).Times(1)

	for i := 0; i != numTasks; i++ {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().Priority().Return(rand.Intn(5)).AnyTimes()
		s.redispatcher.AddTask(mockTask)
	}

	s.Equal(numTasks, s.redispatcher.Size())

	// redispatch will be triggered by the background redispatch loop
	<-stoppedCh

	s.Equal(numTasks-successfullyRedispatched-1, s.redispatcher.Size())
}

func (s *redispatcherSuite) TestRedispatch_WithTargetSize() {
	numTasks := defaultBufferSize + 20
	targetSize := defaultBufferSize + 10

	for i := 0; i != numTasks; i++ {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().Priority().Return(rand.Intn(5)).AnyTimes()
		s.redispatcher.AddTask(mockTask)
		s.mockProcessor.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).MaxTimes(1)
	}

	s.redispatcher.Redispatch(targetSize)

	// implementation can choose to redispatch more tasks than needed
	s.True(s.redispatcher.Size() <= targetSize)
	s.True(s.redispatcher.Size() > 0)
}

func (s *redispatcherSuite) TestRedispatch_Random() {
	numTasks := 10
	dispatched := 0

	for i := 0; i != numTasks; i++ {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().Priority().Return(rand.Intn(5)).AnyTimes()
		s.redispatcher.AddTask(mockTask)
		submitted := false
		if rand.Intn(2) == 0 {
			submitted = true
			dispatched++
		}
		s.mockProcessor.EXPECT().TrySubmit(NewMockTaskMatcher(mockTask)).Return(submitted, nil).MaxTimes(1)
	}

	s.redispatcher.Redispatch(0)

	// implementation can choose to stop redispatch for a certain priority when previous submit has failed
	s.True(s.redispatcher.Size() >= numTasks-dispatched)
}

func (s *redispatcherSuite) newTestRedispatcher() *redispatcherImpl {
	return NewRedispatcher(
		s.mockProcessor,
		&RedispatcherOptions{
			TaskRedispatchInterval:                  dynamicconfig.GetDurationPropertyFn(time.Millisecond * 50),
			TaskRedispatchIntervalJitterCoefficient: dynamicconfig.GetFloatPropertyFn(0.15),
		},
		s.logger,
		s.metricsScope,
	).(*redispatcherImpl)
}
