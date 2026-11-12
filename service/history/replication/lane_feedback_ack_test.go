package replication

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestLaneFeedbackACKScheduleCoalescesWithoutMovingPeriodicAnchor(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := laneFeedbackACKSchedule{period: time.Second, periodic: now.Add(time.Second)}
	anchor := s.periodic
	s.pending = true
	require.Equal(t, now, s.deadline(now))
	s.complete(now, true)
	s.pending = true
	require.Equal(t, now.Add(100*time.Millisecond), s.deadline(now.Add(50*time.Millisecond)))
	s.complete(now.Add(100*time.Millisecond), true)
	require.Equal(t, anchor, s.periodic)
	require.Equal(t, anchor, s.deadline(now.Add(200*time.Millisecond)))
	s.complete(now.Add(950*time.Millisecond), true)
	require.Equal(t, anchor.Add(time.Second), s.periodic)
	s.complete(now.Add(5*time.Second), false)
	require.Equal(t, anchor.Add(5*time.Second), s.periodic)
}

func TestLaneFeedbackACKScheduleWaitsForValidWatermarks(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s := laneFeedbackACKSchedule{period: time.Second, periodic: now.Add(time.Second), pending: true}
	s.complete(now, false)
	for i := range 20 {
		s.pending = true
		require.Equal(t, now.Add(time.Second), s.deadline(now.Add(time.Duration(i)*time.Millisecond)))
	}
	s.complete(now.Add(time.Second), true)
	s.pending = true
	require.Equal(t, now.Add(1100*time.Millisecond), s.deadline(now.Add(time.Second)))
}

func TestLaneFeedbackACKLoopKeepsNotificationsDuringSend(t *testing.T) {
	t.Parallel()
	source := clock.NewEventTimeSource().Update(time.Now())
	wake, shutdown := make(chan struct{}, 1), make(chan struct{})
	started, release := make(chan struct{}, 2), make(chan bool, 2)
	done := make(chan error, 1)
	go func() {
		done <- runLaneFeedbackACKLoop(source, time.Second, wake, shutdown, func() (bool, error) {
			started <- struct{}{}
			return <-release, nil
		})
	}()
	t.Cleanup(func() {
		close(shutdown)
		close(release)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("feedback ACK loop did not stop")
		}
		require.Zero(t, source.NumTimers())
	})
	wake <- struct{}{}
	await.Rcv(t, started)
	wake <- struct{}{}
	release <- true
	await.RequireTrue(t, func() bool { return source.NumTimers() == 1 }, time.Second, time.Millisecond)
	source.Advance(laneFeedbackACKMinInterval)
	await.Rcv(t, started)
	release <- true
}

func TestLaneFeedbackACKLoopPeriodicFallbackAfterClosedSubscription(t *testing.T) {
	t.Parallel()
	source := clock.NewEventTimeSource().Update(time.Now())
	wake, shutdown := make(chan struct{}), make(chan struct{})
	close(wake)
	sent, done := make(chan struct{}, 1), make(chan error, 1)
	go func() {
		done <- runLaneFeedbackACKLoop(source, time.Second, wake, shutdown, func() (bool, error) {
			sent <- struct{}{}
			return true, nil
		})
	}()
	t.Cleanup(func() {
		close(shutdown)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("periodic ACK fallback did not stop")
		}
		require.Zero(t, source.NumTimers())
	})
	await.RequireTrue(t, func() bool { return source.NumTimers() == 1 }, time.Second, time.Millisecond)
	source.Advance(time.Second)
	await.Rcv(t, sent)
}

type feedbackACKTestProvider struct {
	LaneFeedbackProvider
	subscribe func(int32, func()) func()
}

func (p feedbackACKTestProvider) SubscribeFeedback(shard int32, notify func()) func() {
	return p.subscribe(shard, notify)
}

type feedbackACKTestStream struct {
	Stream
	send func(*adminservice.StreamWorkflowReplicationMessagesRequest) error
}

func (s feedbackACKTestStream) Send(request *adminservice.StreamWorkflowReplicationMessagesRequest) error {
	return s.send(request)
}

func TestLaneReceiverFeedbackNotificationAndUnsubscribe(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		name := "shutdown"
		if fail {
			name = "send failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			high, low := NewMockExecutableTaskTracker(controller), NewMockExecutableTaskTracker(controller)
			high.EXPECT().LowWatermark().Return(&WatermarkInfo{Watermark: 100, Timestamp: time.Now()})
			low.EXPECT().LowWatermark().Return(nil)
			high.EXPECT().Size().Return(0)
			low.EXPECT().Size().Return(0)
			subscribed := make(chan func(), 1)
			subscribedShard := make(chan int32, 1)
			unsubscribed := make(chan struct{}, 1)
			provider := feedbackACKTestProvider{LaneFeedbackProvider: NewNoopLaneFeedbackProvider(),
				subscribe: func(shard int32, notify func()) func() {
					subscribedShard <- shard
					subscribed <- notify
					return func() { unsubscribed <- struct{}{} }
				}}
			config := tests.NewDynamicConfig()
			config.ReplicationStreamSyncStatusDuration = dynamicconfig.GetDurationPropertyFn(time.Hour)
			sent := make(chan *adminservice.StreamWorkflowReplicationMessagesRequest, 1)
			shutdown := channel.NewShutdownOnce()
			r := &laneStreamReceiver{
				ProcessToolBox: ProcessToolBox{Config: config, MetricsHandler: metrics.NoopMetricsHandler},
				laneFeedback:   provider, clientShardKey: ClusterShardKey{ClusterID: 2, ShardID: 17},
				highPriorityTaskTracker: high, lowPriorityTaskTracker: low,
				receiverMode: ReceiverModeSingleStack, shutdownChan: shutdown, logger: log.NewNoopLogger(),
				stream: feedbackACKTestStream{send: func(request *adminservice.StreamWorkflowReplicationMessagesRequest) error {
					sent <- request
					<-shutdown.Channel()
					if fail {
						return errors.New("connection lost")
					}
					return nil
				}},
			}
			done := make(chan error, 1)
			go func() { done <- r.sendEventLoop() }()
			t.Cleanup(r.shutdownChan.Shutdown)
			notify := await.Rcv(t, subscribed)
			require.Equal(t, int32(17), await.Rcv(t, subscribedShard))
			for range 100 {
				notify()
			}
			require.Equal(t, int64(100), await.Rcv(t, sent).GetSyncReplicationState().GetInclusiveLowWatermark())
			r.shutdownChan.Shutdown()
			err := await.Rcv(t, done)
			if fail {
				require.ErrorContains(t, err, "connection lost")
			} else {
				require.NoError(t, err)
			}
			await.Rcv(t, unsubscribed)
		})
	}
}

func TestLaneReceiverPromptFeedbackReadsFreshSnapshot(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	high, low := NewMockExecutableTaskTracker(controller), NewMockExecutableTaskTracker(controller)
	for _, tracker := range []*MockExecutableTaskTracker{high, low} {
		tracker.EXPECT().LowWatermark().Return(&WatermarkInfo{Watermark: 100, Timestamp: time.Now()}).Times(2)
		tracker.EXPECT().Size().Return(0).Times(2)
	}
	flow := NewMockReceiverFlowController(controller)
	flow.EXPECT().GetFlowControlInfo(gomock.Any()).Return(FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME}).Times(4)
	feedback := NewMockLaneFeedbackProvider(controller)
	stream := LaneFeedbackStream{Source: ClusterShardKey{ClusterID: 1, ShardID: 529}, Target: ClusterShardKey{ClusterID: 2, ShardID: 17}}
	gomock.InOrder(
		feedback.EXPECT().ThrottledNamespaceIDs(stream).Return([]string{"noisy"}),
		feedback.EXPECT().ThrottledNamespaceIDs(stream).Return(nil),
	)
	feedback.EXPECT().Feedback(stream, gomock.Any()).Return(nil).Times(2)
	subscribed := make(chan func(), 1)
	unsubscribed := make(chan struct{}, 1)
	provider := feedbackACKTestProvider{LaneFeedbackProvider: feedback, subscribe: func(_ int32, notify func()) func() {
		subscribed <- notify
		return func() { unsubscribed <- struct{}{} }
	}}
	config := tests.NewDynamicConfig()
	config.ReplicationStreamSyncStatusDuration = dynamicconfig.GetDurationPropertyFn(time.Hour)
	sent, release := make(chan *adminservice.StreamWorkflowReplicationMessagesRequest, 2), make(chan struct{})
	shutdown := channel.NewShutdownOnce()
	r := &laneStreamReceiver{
		ProcessToolBox: ProcessToolBox{Config: config, MetricsHandler: metrics.NoopMetricsHandler},
		laneFeedback:   provider, clientShardKey: stream.Target, serverShardKey: stream.Source,
		highPriorityTaskTracker: high, lowPriorityTaskTracker: low, flowController: flow,
		receiverMode: ReceiverModeTieredStack, lanesEnabled: true, maxLanes: 4,
		shutdownChan: shutdown, logger: log.NewNoopLogger(),
		stream: feedbackACKTestStream{send: func(request *adminservice.StreamWorkflowReplicationMessagesRequest) error {
			sent <- request
			select {
			case <-release:
			case <-shutdown.Channel():
			}
			return nil
		}},
	}
	done := make(chan error, 1)
	go func() { done <- r.sendEventLoop() }()
	t.Cleanup(shutdown.Shutdown)
	notify := await.Rcv(t, subscribed)
	notify()
	first := await.Rcv(t, sent)
	require.Equal(t, []string{"noisy"}, first.GetSyncReplicationState().GetThrottleHighNamespaceIds())
	notify()
	release <- struct{}{}
	second := await.Rcv(t, sent)
	require.Empty(t, second.GetSyncReplicationState().GetThrottleHighNamespaceIds())
	shutdown.Shutdown()
	require.NoError(t, await.Rcv(t, done))
	await.Rcv(t, unsubscribed)
}

func TestLaneReceiverDeclinedFeedbackSubscriptionKeepsPeriodicLoop(t *testing.T) {
	t.Parallel()
	subscribed := make(chan struct{}, 1)
	config := tests.NewDynamicConfig()
	config.ReplicationStreamSyncStatusDuration = dynamicconfig.GetDurationPropertyFn(time.Hour)
	r := &laneStreamReceiver{
		ProcessToolBox: ProcessToolBox{Config: config},
		laneFeedback: feedbackACKTestProvider{subscribe: func(int32, func()) func() {
			subscribed <- struct{}{}
			return nil
		}},
		shutdownChan: channel.NewShutdownOnce(), logger: log.NewNoopLogger(),
	}
	done := make(chan error, 1)
	go func() { done <- r.sendEventLoop() }()
	t.Cleanup(r.shutdownChan.Shutdown)
	await.Rcv(t, subscribed)
	r.shutdownChan.Shutdown()
	require.NoError(t, await.Rcv(t, done))
}
