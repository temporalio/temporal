package flowcontrol

import (
	"context"

	"go.temporal.io/server/api/enums/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/quotas"
)

const DefaultSenderRps = 1000

type (
	SenderFlowController interface {
		Wait(priority enums.TaskPriority) error
		RefreshReceiverFlowControlInfo(syncState *replicationpb.SyncReplicationState)
	}
	senderFlowControllerImpl struct {
		rateLimiters       map[enums.TaskPriority]*quotas.RateLimiterImpl
		defaultRateLimiter *quotas.RateLimiterImpl
	}
)

func NewSenderFlowController(priorities []enums.TaskPriority) *senderFlowControllerImpl {
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
