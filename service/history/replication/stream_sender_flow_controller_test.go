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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/enums/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/observability/log"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/mock/gomock"
)

type (
	senderFlowControllerSuite struct {
		suite.Suite
		controller         *gomock.Controller
		mockRateLimiter    *quotas.MockRateLimiter
		senderFlowCtrlImpl *SenderFlowControllerImpl
		logger             log.Logger
		config             *configs.Config
	}
)

func TestSenderFlowControllerSuite(t *testing.T) {
	suite.Run(t, new(senderFlowControllerSuite))
}

func (s *senderFlowControllerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockRateLimiter = quotas.NewMockRateLimiter(s.controller)
	s.logger = log.NewTestLogger()
	s.config = &configs.Config{
		ReplicationStreamSenderHighPriorityQPS: func() int { return 10 },
		ReplicationStreamSenderLowPriorityQPS:  func() int { return 5 },
	}
	s.senderFlowCtrlImpl = NewSenderFlowController(s.config, s.logger)
}

func (s *senderFlowControllerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *senderFlowControllerSuite) TestWait_HighPriority() {
	state := s.senderFlowCtrlImpl.flowControlStates[enums.TASK_PRIORITY_HIGH]
	state.rateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s.senderFlowCtrlImpl.Wait(enums.TASK_PRIORITY_HIGH)
	}()

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestWait_LowPriority() {
	state := s.senderFlowCtrlImpl.flowControlStates[enums.TASK_PRIORITY_LOW]
	state.rateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s.senderFlowCtrlImpl.Wait(enums.TASK_PRIORITY_LOW)
	}()

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestWait_DefaultPriority() {
	s.senderFlowCtrlImpl.defaultRateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s.senderFlowCtrlImpl.Wait(enums.TASK_PRIORITY_UNSPECIFIED)
	}()

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestRefreshReceiverFlowControlInfo() {
	senderFlowCtrlImpl := NewSenderFlowController(s.config, s.logger)
	state := &replicationpb.SyncReplicationState{
		HighPriorityState: &replicationpb.ReplicationState{
			FlowControlCommand: enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
		},
		LowPriorityState: &replicationpb.ReplicationState{
			FlowControlCommand: enums.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
		},
	}

	senderFlowCtrlImpl.RefreshReceiverFlowControlInfo(state)

	s.True(senderFlowCtrlImpl.flowControlStates[enums.TASK_PRIORITY_HIGH].resume)
	s.False(senderFlowCtrlImpl.flowControlStates[enums.TASK_PRIORITY_LOW].resume)
}

func (s *senderFlowControllerSuite) TestPauseToResume() {
	state := s.senderFlowCtrlImpl.flowControlStates[enums.TASK_PRIORITY_HIGH]
	state.rateLimiter = s.mockRateLimiter

	// Set initial state to paused
	state.mu.Lock()
	state.resume = false
	state.mu.Unlock()
	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s.senderFlowCtrlImpl.Wait(enums.TASK_PRIORITY_HIGH)
	}()

	// Ensure the goroutine has time to start and block
	assert.Eventually(s.T(), func() bool {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.waiters == 1
	}, 1*time.Second, 100*time.Millisecond)

	s.Equal(1, state.waiters)

	// Transition from paused to resumed
	s.senderFlowCtrlImpl.setState(state, enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)
	wg.Wait()

	s.Equal(0, state.waiters)
	s.True(state.resume)
}
