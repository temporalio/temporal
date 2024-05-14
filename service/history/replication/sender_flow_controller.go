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
	"context"

	"go.temporal.io/server/api/enums/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/quotas"
)

const DefaultSenderRps = 1000

type (
	SenderFlowController interface {
		// Wait will block go routine until the sender is allowed to send a task
		Wait(priority enums.TaskPriority) error
		RefreshReceiverFlowControlInfo(syncState *replicationpb.SyncReplicationState)
	}
	senderFlowControllerImpl struct {
		rateLimiters       map[enums.TaskPriority]*quotas.RateLimiterImpl
		defaultRateLimiter *quotas.RateLimiterImpl
	}
)

func NewSenderFlowController(priorities ...enums.TaskPriority) *senderFlowControllerImpl {
	rateLimiters := make(map[enums.TaskPriority]*quotas.RateLimiterImpl)
	for _, priority := range priorities {
		rateLimiters[priority] = quotas.NewRateLimiter(DefaultSenderRps, 0)
	}
	return &senderFlowControllerImpl{
		rateLimiters:       rateLimiters,
		defaultRateLimiter: quotas.NewRateLimiter(DefaultSenderRps, 0),
	}
}

func (s *senderFlowControllerImpl) RefreshReceiverFlowControlInfo(syncState *replicationpb.SyncReplicationState) {
	if syncState.GetHighPriorityState() != nil {
		s.setRate(s.rateLimiters[enums.TASK_PRIORITY_HIGH], syncState.GetHighPriorityState().GetFlowControlCommand())
	}
	if syncState.GetLowPriorityState() != nil {
		s.setRate(s.rateLimiters[enums.TASK_PRIORITY_LOW], syncState.GetLowPriorityState().GetFlowControlCommand())
	}
}

func (s *senderFlowControllerImpl) setRate(rateLimiter *quotas.RateLimiterImpl, flowControlCommand enums.ReplicationFlowControlCommand) {
	if flowControlCommand != enums.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE {
		rateLimiter.SetRPS(DefaultSenderRps)
	} else {
		rateLimiter.SetRPS(0)
	}
}

func (s *senderFlowControllerImpl) Wait(priority enums.TaskPriority) error {
	rateLimiter, ok := s.rateLimiters[priority]
	if !ok {
		rateLimiter = s.defaultRateLimiter
	}
	return rateLimiter.Wait(context.Background()) // Use background context here to block the go routine until receiver resume sending
}
