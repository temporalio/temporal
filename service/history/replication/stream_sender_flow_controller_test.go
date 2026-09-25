package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/await"
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

	wg.Go(func() {
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		s.NoError(err)
	})

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestWait_Error() {
	state := s.senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH]
	state.rateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(context.Canceled)

	var wg sync.WaitGroup

	wg.Go(func() {
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		s.Error(err)
	})

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestWait_LowPriority() {
	state := s.senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_LOW]
	state.rateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup

	wg.Go(func() {
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_LOW)
		s.NoError(err)
	})

	wg.Wait()
}

func (s *senderFlowControllerSuite) TestWait_DefaultPriority() {
	s.senderFlowCtrlImpl.defaultRateLimiter = s.mockRateLimiter

	s.mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup

	wg.Go(func() {
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_UNSPECIFIED)
		s.NoError(err)
	})

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

	wg.Go(func() {
		err := s.senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		s.NoError(err)
	})

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

func TestSenderFlowControllerWaitCancellation(t *testing.T) {
	t.Parallel()
	for _, priority := range []enumsspb.TaskPriority{enumsspb.TASK_PRIORITY_HIGH, enumsspb.TASK_PRIORITY_LOW} {
		for _, mode := range []string{"cancel", "already_canceled", "deadline", "cancel_before_resume"} {
			t.Run(priority.String()+"/"+mode, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					flow := NewSenderFlowController(&configs.Config{
						ReplicationStreamSenderHighPriorityQPS: func() int { return 10 },
						ReplicationStreamSenderLowPriorityQPS:  func() int { return 5 },
					}, log.NewTestLogger())
					state := flow.flowControlStates[priority]
					limiter := quotas.NewMockRateLimiter(gomock.NewController(t))
					state.rateLimiter = limiter
					rateLimitCalls := 0
					limiter.EXPECT().Wait(gomock.Any()).DoAndReturn(func(context.Context) error {
						rateLimitCalls++
						return nil
					}).AnyTimes()
					flow.setState(state, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE)
					defer flow.setState(state, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)

					ctx, cancel := context.WithCancel(t.Context())
					wantErr := context.Canceled
					switch mode {
					case "deadline":
						cancel()
						ctx, cancel = context.WithTimeout(t.Context(), time.Second)
						wantErr = context.DeadlineExceeded
					case "already_canceled":
						cancel()
					default:
					}
					defer cancel()
					result := make(chan error, 1)
					go func() { result <- flow.Wait(ctx, priority) }()
					synctest.Wait()
					if mode != "already_canceled" {
						requireFlowControlState(t, state, 1, false)
					}

					switch mode {
					case "cancel":
						cancel()
					case "deadline":
						await.Rcv(t, ctx.Done())
					case "cancel_before_resume":
						// Make both events visible before the waiter can reacquire the mutex.
						state.mu.Lock()
						cancel()
						state.resume = true
						state.cond.Broadcast()
						state.mu.Unlock()
					default:
					}
					synctest.Wait()
					select {
					case err := <-result:
						require.ErrorIs(t, err, wantErr)
					default:
						t.Fatal("canceled paused waiter requires RESUME to return")
					}
					require.Zero(t, rateLimitCalls)
					requireFlowControlState(t, state, 0, mode == "cancel_before_resume")
				})
			})
		}
	}
}

func TestSenderFlowControllerCancelDoesNotResumeOtherWaiter(t *testing.T) {
	t.Parallel()
	for _, priority := range []enumsspb.TaskPriority{enumsspb.TASK_PRIORITY_HIGH, enumsspb.TASK_PRIORITY_LOW} {
		t.Run(priority.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				flow := NewSenderFlowController(&configs.Config{
					ReplicationStreamSenderHighPriorityQPS: func() int { return 10 },
					ReplicationStreamSenderLowPriorityQPS:  func() int { return 5 },
				}, log.NewTestLogger())
				state := flow.flowControlStates[priority]
				limiter := quotas.NewMockRateLimiter(gomock.NewController(t))
				state.rateLimiter = limiter
				rateErr := errors.New("rate limiter failed after resume")
				limiter.EXPECT().Wait(gomock.Any()).Return(rateErr)
				flow.setState(state, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE)
				defer flow.setState(state, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				canceledResult, liveResult := make(chan error, 1), make(chan error, 1)
				go func() { canceledResult <- flow.Wait(ctx, priority) }()
				go func() { liveResult <- flow.Wait(t.Context(), priority) }()
				synctest.Wait()
				requireFlowControlState(t, state, 2, false)

				cancel()
				synctest.Wait()
				select {
				case err := <-canceledResult:
					require.ErrorIs(t, err, context.Canceled)
				default:
					t.Fatal("canceled waiter did not return")
				}
				select {
				case err := <-liveResult:
					t.Fatalf("live waiter bypassed PAUSE: %v", err)
				default:
				}
				requireFlowControlState(t, state, 1, false)

				flow.setState(state, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)
				synctest.Wait()
				select {
				case err := <-liveResult:
					require.ErrorIs(t, err, rateErr)
				default:
					t.Fatal("live waiter did not return after RESUME")
				}
				requireFlowControlState(t, state, 0, true)
			})
		})
	}
}

func requireFlowControlState(t *testing.T, state *flowControlState, waiters int, resume bool) {
	t.Helper()
	state.mu.Lock()
	defer state.mu.Unlock()
	require.Equal(t, waiters, state.waiters)
	require.Equal(t, resume, state.resume)
}
