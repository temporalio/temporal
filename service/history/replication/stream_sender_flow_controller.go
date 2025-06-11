//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_sender_flow_controller_mock.go

package replication

import (
	"context"
	"sync"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
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
		Wait(ctx context.Context, priority enumsspb.TaskPriority) error
		RefreshReceiverFlowControlInfo(syncState *replicationspb.SyncReplicationState)
	}
	SenderFlowControllerImpl struct {
		flowControlStates  map[enumsspb.TaskPriority]*flowControlState
		defaultRateLimiter quotas.RateLimiter
		logger             log.Logger
	}
)

func NewSenderFlowController(config *configs.Config, logger log.Logger) *SenderFlowControllerImpl {
	flowControlStates := make(map[enumsspb.TaskPriority]*flowControlState)
	highPriorityState := &flowControlState{
		resume: true,
	}
	highPriorityState.cond = sync.NewCond(&highPriorityState.mu)
	highPriorityState.rateLimiter = quotas.NewDefaultOutgoingRateLimiter(func() float64 {
		return float64(config.ReplicationStreamSenderHighPriorityQPS())
	})

	lowPriorityState := &flowControlState{
		resume: true,
	}
	lowPriorityState.cond = sync.NewCond(&lowPriorityState.mu)
	lowPriorityState.rateLimiter = quotas.NewDefaultOutgoingRateLimiter(func() float64 {
		return float64(config.ReplicationStreamSenderLowPriorityQPS())
	})
	flowControlStates[enumsspb.TASK_PRIORITY_HIGH] = highPriorityState
	flowControlStates[enumsspb.TASK_PRIORITY_LOW] = lowPriorityState
	return &SenderFlowControllerImpl{
		flowControlStates:  flowControlStates,
		defaultRateLimiter: quotas.NewRateLimiter(float64(config.ReplicationStreamSenderHighPriorityQPS()), config.ReplicationStreamSenderHighPriorityQPS()),
		logger:             logger,
	}
}

func (s *SenderFlowControllerImpl) RefreshReceiverFlowControlInfo(syncState *replicationspb.SyncReplicationState) {
	if syncState.GetHighPriorityState() != nil {
		s.setState(s.flowControlStates[enumsspb.TASK_PRIORITY_HIGH], syncState.GetHighPriorityState().GetFlowControlCommand())
	}
	if syncState.GetLowPriorityState() != nil {
		s.setState(s.flowControlStates[enumsspb.TASK_PRIORITY_LOW], syncState.GetLowPriorityState().GetFlowControlCommand())
	}
}

func (s *SenderFlowControllerImpl) setState(state *flowControlState, flowControlCommand enumsspb.ReplicationFlowControlCommand) {
	switch flowControlCommand {
	case enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_UNSPECIFIED:
		state.mu.Lock()
		defer state.mu.Unlock()
		state.resume = true
		if state.waiters > 0 {
			state.cond.Broadcast()
		}
	case enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE:
		state.mu.Lock()
		defer state.mu.Unlock()
		state.resume = false
	}
}

func (s *SenderFlowControllerImpl) Wait(ctx context.Context, priority enumsspb.TaskPriority) error {
	state, ok := s.flowControlStates[priority]
	waitForRateLimiter := func(rateLimiter quotas.RateLimiter) error {
		childCtx, cancel := context.WithTimeout(ctx, 2*time.Minute) // to avoid infinite wait
		defer cancel()
		err := rateLimiter.Wait(childCtx)
		if err != nil {
			s.logger.Error("error waiting for rate limiter", tag.Error(err))
			return err
		}
		return nil
	}
	if !ok {
		return waitForRateLimiter(s.defaultRateLimiter)
	}

	state.mu.Lock()
	if !state.resume {
		state.waiters++
		s.logger.Info("sender is paused", tag.TaskPriority(priority.String()))
		state.cond.Wait()
		s.logger.Info("sender is resumed", tag.TaskPriority(priority.String()))
		state.waiters--
	}
	state.mu.Unlock()
	return waitForRateLimiter(state.rateLimiter)
}
