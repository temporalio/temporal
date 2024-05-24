package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/enums/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
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
		ReplicationStreamSenderHighPriorityMaxQPS: func() int { return 10 },
		ReplicationStreamSenderLowPriorityMaxQPS:  func() int { return 5 },
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

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s.senderFlowCtrlImpl.Wait(enums.TASK_PRIORITY_HIGH)
	}()

	// Ensure the goroutine has time to start and block
	time.Sleep(100 * time.Millisecond)

	s.Equal(1, state.waiters)

	// Transition from paused to resumed
	s.senderFlowCtrlImpl.setState(state, enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)

	wg.Wait()

	s.Equal(0, state.waiters)
	s.True(state.resume)
}
