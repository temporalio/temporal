package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
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
	state := s.senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH]
	state.rateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		s.NoError(err)
	}()

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestWait_Error() {
	state := s.senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH]
	state.rateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(context.Canceled)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		s.Error(err)
	}()

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestWait_LowPriority() {
	state := s.senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_LOW]
	state.rateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_LOW)
		s.NoError(err)
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
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_UNSPECIFIED)
		s.NoError(err)
	}()

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestRefreshReceiverFlowControlInfo() {
	senderFlowCtrlImpl := NewSenderFlowController(s.config, s.logger)
	state := &replicationspb.SyncReplicationState{
		HighPriorityState: &replicationspb.ReplicationState{
			FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
		},
		LowPriorityState: &replicationspb.ReplicationState{
			FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
		},
	}

	senderFlowCtrlImpl.RefreshReceiverFlowControlInfo(state)

	s.True(senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH].resume)
	s.False(senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_LOW].resume)
}

func (s *senderFlowControllerSuite) TestPauseToResume() {
	state := s.senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH]
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
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		s.NoError(err)
	}()

	// Ensure the goroutine has time to start and block
	assert.Eventually(s.T(), func() bool {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.waiters == 1
	}, 1*time.Second, 100*time.Millisecond)

	s.Equal(1, state.waiters)

	// Transition from paused to resumed
	s.senderFlowCtrlImpl.setState(state, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)
	wg.Wait()

	s.Equal(0, state.waiters)
	s.True(state.resume)
}
