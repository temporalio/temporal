package replication

import (
	"context"
	"strconv"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/sync/errgroup"
)

const sharedLaneTag = "default"

func laneClassTag(class replicationLaneClass) string {
	return "class-" + strconv.Itoa(int(class))
}

func (s *StreamSenderImpl) sendLaneEventLoop() (retErr error) {
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
	if err := s.waitForInitialLaneState(); err != nil {
		return err
	}

	if !s.lanesConfirmed.Load() {
		select {
		case <-s.ctx.Done():
		case <-s.shutdownChan.Channel():
		}
		return nil
	}

	ctx, cancel := context.WithCancel(s.ctx)
	workers, ctx := errgroup.WithContext(ctx)
	defer func() {
		cancel()
		if err := workers.Wait(); retErr == nil {
			retErr = err
		}
	}()
	active := make(map[string]struct{})
	for {
		// Subscribe before reading snapshots so a concurrent registry change cannot be missed.
		changed := s.laneRegistry.Changed()
		lanes := s.laneRegistry.Snapshots()
		present := make(map[string]struct{}, len(lanes))
		for _, lane := range lanes {
			present[lane.id] = struct{}{}
			if _, running := active[lane.id]; running {
				continue
			}
			active[lane.id] = struct{}{}
			workers.Go(func() error { return s.sendLaneWorker(ctx, lane.id) })
		}
		for laneID := range active {
			if _, exists := present[laneID]; !exists {
				delete(active, laneID)
			}
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
}

func (s *StreamSenderImpl) sendLaneWorker(ctx context.Context, laneID string) (retErr error) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			retErr = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()
	defer log.CapturePanic(s.logger, &panicErr)
	newTaskNotificationChan, subscriberID := s.historyEngine.SubscribeReplicationNotification(s.clientClusterName)
	defer s.historyEngine.UnsubscribeReplicationNotification(subscriberID)
	timer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer timer.Stop()
	for {
		if ctx.Err() != nil || s.shutdownChan.IsShutdown() {
			return nil
		}
		changed := s.laneRegistry.Changed()
		lane, exists := s.laneRegistry.SnapshotByID(laneID)
		if !exists {
			return nil
		}
		end := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
		more, err := s.sendLane(ctx, lane, end)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if more {
			continue
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(s.config.ReplicationStreamSendEmptyTaskDuration())
		select {
		case <-newTaskNotificationChan:
		case <-changed:
		case <-timer.C:
		case <-ctx.Done():
			return nil
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
}
