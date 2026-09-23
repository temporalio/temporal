package replication

import (
	"strconv"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

const sharedLaneTag = "default"

type laneSendResult struct {
	laneID   string
	more     bool
	err      error
	released <-chan struct{} // closed after the registry lease is released
}

// senderLaneTurnCoordinator binds asynchronous turns to one class-loop invocation.
type senderLaneTurnCoordinator struct {
	registry   *senderLaneRegistry
	run        func(senderLaneSnapshot, int64, func()) laneSendResult
	wake       func(replicationLaneClass)
	shutdown   <-chan struct{}
	streamDone <-chan struct{}
	results    chan laneSendResult
	stopped    chan struct{}
	stopOnce   sync.Once
}

func newSenderLaneTurnCoordinator(
	registry *senderLaneRegistry,
	run func(senderLaneSnapshot, int64, func()) laneSendResult,
	wake func(replicationLaneClass),
	shutdown <-chan struct{},
	streamDone <-chan struct{},
) *senderLaneTurnCoordinator {
	return &senderLaneTurnCoordinator{
		registry:   registry,
		run:        run,
		wake:       wake,
		shutdown:   shutdown,
		streamDone: streamDone,
		results:    make(chan laneSendResult),
		stopped:    make(chan struct{}),
	}
}

func (c *senderLaneTurnCoordinator) Close() {
	c.stopOnce.Do(func() {
		close(c.stopped)
	})
}

func (c *senderLaneTurnCoordinator) Results() <-chan laneSendResult {
	return c.results
}

func (c *senderLaneTurnCoordinator) Start(
	snapshot senderLaneSnapshot,
	end int64,
) (<-chan struct{}, bool) {
	lane, ok := c.registry.Acquire(snapshot.id)
	if !ok {
		return nil, false
	}
	retrying := make(chan struct{})
	var retryOnce sync.Once
	go func() {
		result := c.run(lane, end, func() {
			retryOnce.Do(func() {
				close(retrying)
			})
		})
		released := make(chan struct{})
		result.released = released
		delivered := false
		// Keep the lease until the receiving loop observes this completion. If the
		// loop has exited, abandon the result so the goroutine and lease cannot leak.
		select {
		case c.results <- result:
			delivered = true
		case <-c.stopped:
		case <-c.shutdown:
		case <-c.streamDone:
		}
		c.registry.Release(lane.id)
		close(released)
		current, ok := c.registry.SnapshotByKey(lane.logicalKey)
		if ok && (!delivered || current.class != lane.class) {
			c.wake(current.class)
		}
	}()
	return retrying, true
}

func newLaneClassWakeChannels(classCount int) []chan struct{} {
	channels := make([]chan struct{}, classCount)
	for i := range channels {
		channels[i] = make(chan struct{}, 1)
	}
	return channels
}

func laneClassTag(class replicationLaneClass) string {
	return "class-" + strconv.Itoa(int(class))
}

func (s *StreamSenderImpl) sendLaneEventLoop(class replicationLaneClass) (retErr error) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			retErr = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()
	defer log.CapturePanic(s.logger, &panicErr)
	if s.laneInitializationError != nil {
		return NewStreamError("StreamSender failed to restore replication lanes", s.laneInitializationError)
	}

	newTaskNotificationChan, subscriberID := s.historyEngine.SubscribeReplicationNotification(s.clientClusterName)
	defer s.historyEngine.UnsubscribeReplicationNotification(subscriberID)
	if err := s.waitForLaneCapability(); err != nil {
		return err
	}
	timer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer timer.Stop()
	turns := newSenderLaneTurnCoordinator(
		s.laneRegistry,
		s.runLaneTurn,
		func(class replicationLaneClass) { s.wakeLaneClasses(class) },
		s.shutdownChan.Channel(),
		s.server.Context().Done(),
	)
	defer turns.Close()
	laneClassWakeChannel := s.laneClassWakeChannel(class)

	continuousRound := false
	for {
		if consumeLaneClassWake(laneClassWakeChannel) {
			continuousRound = false
		}
		more, stopped, err := s.sendLaneRound(class, continuousRound, turns)
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
		if more {
			continuousRound = true
			select {
			case <-s.shutdownChan.Channel():
				return nil
			default:
				continue
			}
		}
		continuousRound, stopped, err = s.waitForLaneWork(
			timer,
			newTaskNotificationChan,
			laneClassWakeChannel,
			turns.Results(),
		)
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
}

func (s *StreamSenderImpl) sendLaneRound(
	class replicationLaneClass,
	continuous bool,
	turns *senderLaneTurnCoordinator,
) (more bool, stopped bool, err error) {
	if s.lanesConfirmed.Load() {
		end := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
		for _, lane := range s.laneRegistry.ClassSnapshots(class) {
			if continuous && lane.cursor >= end {
				continue
			}
			retrying, started := turns.Start(lane, end)
			if !started {
				continue
			}
			turnMore, stopped, err := s.waitForLaneTurn(lane.id, retrying, turns.Results())
			if err != nil || stopped {
				return false, stopped, err
			}
			more = more || turnMore
		}
	}
	drainedMore, err := drainLaneResults(turns.Results())
	return more || drainedMore, false, err
}

func (s *StreamSenderImpl) waitForLaneTurn(
	laneID string,
	retrying <-chan struct{},
	results <-chan laneSendResult,
) (more bool, stopped bool, err error) {
	for {
		select {
		case result := <-results:
			waitForLaneRelease(result)
			if result.err != nil {
				return false, false, result.err
			}
			more = more || result.more
			if result.laneID == laneID {
				return more, false, nil
			}
		case <-retrying:
			return more, false, nil
		case <-s.shutdownChan.Channel():
			return false, true, nil
		}
	}
}

func drainLaneResults(results <-chan laneSendResult) (bool, error) {
	more := false
	for {
		select {
		case result := <-results:
			waitForLaneRelease(result)
			if result.err != nil {
				return false, result.err
			}
			more = more || result.more
		default:
			return more, nil
		}
	}
}

func (s *StreamSenderImpl) waitForLaneWork(
	timer *time.Timer,
	newTaskNotificationChan <-chan struct{},
	laneClassWakeChannel <-chan struct{},
	results <-chan laneSendResult,
) (more bool, stopped bool, err error) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(s.config.ReplicationStreamSendEmptyTaskDuration())
	for {
		select {
		case <-s.shutdownChan.Channel():
			return false, true, nil
		case <-newTaskNotificationChan:
			return false, false, nil
		case <-laneClassWakeChannel:
			return false, false, nil
		case <-timer.C:
			return false, false, nil
		case result := <-results:
			waitForLaneRelease(result)
			if result.err != nil {
				return false, false, result.err
			}
			if result.more {
				return true, false, nil
			}
		}
	}
}

func waitForLaneRelease(result laneSendResult) {
	if result.released != nil {
		<-result.released
	}
}

func consumeLaneClassWake(wakeChannel <-chan struct{}) bool {
	select {
	case <-wakeChannel:
		return true
	default:
		return false
	}
}

func (s *StreamSenderImpl) laneClassWakeChannel(class replicationLaneClass) <-chan struct{} {
	index := int(class) - 1
	if index < 0 || index >= len(s.laneClassWakeChannels) {
		return nil
	}
	return s.laneClassWakeChannels[index]
}

func (s *StreamSenderImpl) wakeLaneClasses(classes ...replicationLaneClass) {
	for _, class := range classes {
		index := int(class) - 1
		if index < 0 || index >= len(s.laneClassWakeChannels) {
			continue
		}
		select {
		case s.laneClassWakeChannels[index] <- struct{}{}:
		default:
		}
	}
}

func (s *StreamSenderImpl) runLaneTurn(
	lane senderLaneSnapshot,
	end int64,
	onRetry func(),
) (result laneSendResult) {
	result.laneID = lane.id
	var panicErr error
	defer func() {
		if panicErr != nil {
			result.err = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()
	defer log.CapturePanic(s.logger, &panicErr)
	result.more, result.err = s.sendAcquiredLane(lane, end, onRetry)
	return result
}
