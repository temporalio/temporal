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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination stream_sender_flow_controller_mock.go

package replication

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/server/api/enums/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
)

type (
	flowControlState struct {
		mu          sync.Mutex
		cond        *sync.Cond
		waiters     int
		resume      bool
		rateLimiter quotas.RateLimiter // todo: consider using a shared rate limiter across shard for better resource allocation
	}
	SenderFlowController interface {
		// Wait will block go routine until the sender is allowed to send a task
		Wait(priority enums.TaskPriority)
		RefreshReceiverFlowControlInfo(syncState *replicationpb.SyncReplicationState)
	}
	SenderFlowControllerImpl struct {
		flowControlStates  map[enums.TaskPriority]*flowControlState
		defaultRateLimiter quotas.RateLimiter
		logger             log.Logger
	}
)

func NewSenderFlowController(config *configs.Config, logger log.Logger) *SenderFlowControllerImpl {
	flowControlStates := make(map[enums.TaskPriority]*flowControlState)
	highPriorityState := &flowControlState{
		resume: true,
	}
	highPriorityState.cond = sync.NewCond(&highPriorityState.mu)
	highPriorityState.rateLimiter = quotas.NewDynamicRateLimiter(quotas.NewDefaultOutgoingRateLimiter(func() float64 {
		return float64(config.ReplicationStreamSenderHighPriorityQPS())
	}), 1*time.Minute)

	lowPriorityState := &flowControlState{
		resume: true,
	}
	lowPriorityState.cond = sync.NewCond(&lowPriorityState.mu)
	lowPriorityState.rateLimiter = quotas.NewDynamicRateLimiter(quotas.NewDefaultOutgoingRateLimiter(func() float64 {
		return float64(config.ReplicationStreamSenderLowPriorityQPS())
	}), 1*time.Minute)
	flowControlStates[enums.TASK_PRIORITY_HIGH] = highPriorityState
	flowControlStates[enums.TASK_PRIORITY_LOW] = lowPriorityState
	return &SenderFlowControllerImpl{
		flowControlStates:  flowControlStates,
		defaultRateLimiter: quotas.NewRateLimiter(float64(config.ReplicationStreamSenderHighPriorityQPS()), config.ReplicationStreamSenderHighPriorityQPS()),
		logger:             logger,
	}
}

func (s *SenderFlowControllerImpl) RefreshReceiverFlowControlInfo(syncState *replicationpb.SyncReplicationState) {
	if syncState.GetHighPriorityState() != nil {
		s.setState(s.flowControlStates[enums.TASK_PRIORITY_HIGH], syncState.GetHighPriorityState().GetFlowControlCommand())
	}
	if syncState.GetLowPriorityState() != nil {
		s.setState(s.flowControlStates[enums.TASK_PRIORITY_LOW], syncState.GetLowPriorityState().GetFlowControlCommand())
	}
}

func (s *SenderFlowControllerImpl) setState(state *flowControlState, flowControlCommand enums.ReplicationFlowControlCommand) {
	switch flowControlCommand {
	case enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME, enums.REPLICATION_FLOW_CONTROL_COMMAND_UNSPECIFIED:
		state.mu.Lock()
		defer state.mu.Unlock()
		state.resume = true
		if state.waiters > 0 {
			state.cond.Broadcast()
		}
	case enums.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE:
		state.mu.Lock()
		defer state.mu.Unlock()
		state.resume = false
	}
}

func (s *SenderFlowControllerImpl) Wait(priority enums.TaskPriority) {
	state, ok := s.flowControlStates[priority]
	waitForRateLimiter := func(rateLimiter quotas.RateLimiter) {
		err := rateLimiter.Wait(context.Background())
		if err != nil {
			s.logger.Error("error waiting for rate limiter", tag.Error(err))
		}
		return
	}
	if !ok {
		waitForRateLimiter(s.defaultRateLimiter)
		return
	}

	state.mu.Lock()
	if !state.resume {
		state.waiters++
		state.cond.Wait()
		state.waiters--
	}
	state.mu.Unlock()
	waitForRateLimiter(state.rateLimiter)
}
