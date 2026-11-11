package replication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/wideevents"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const laneSchedulingClassCount = 16

type lanePoolPhase int

const (
	lanePoolIdle lanePoolPhase = iota
	lanePoolPreparing
	lanePoolPrepared
	lanePoolFlow
	lanePoolFlowReady
	lanePoolWriting
	lanePoolRetry
)

type lanePoolRaw struct {
	item         tasks.Task
	attempt      int64
	lockPriority locks.Priority
	lowAttempts  int
	retry        backoff.Retrier
	throttle     backoff.Retrier
}

type lanePoolEntry struct {
	lane        senderLaneSnapshot
	phase       lanePoolPhase
	held        bool
	slot        bool
	due         time.Time
	raw         *lanePoolRaw
	payload     *replicationspb.ReplicationTask
	end         int64
	nextCursor  int64
	controlTime time.Time
	rescan      bool
}

type lanePoolJob struct {
	lane        *lanePoolEntry
	snapshot    senderLaneSnapshot
	raw         *lanePoolRaw
	payload     *replicationspb.ReplicationTask
	end         int64
	nextCursor  int64
	controlTime time.Time
}

type lanePoolResult struct {
	lane        *lanePoolEntry
	stage       lanePoolPhase
	raw         *lanePoolRaw
	payload     *replicationspb.ReplicationTask
	nextCursor  int64
	controlTime time.Time
	err         error
	panic       bool
}

type lanePool struct {
	sender      *laneStreamSender
	ctx         context.Context
	cancel      context.CancelFunc
	workers     sync.WaitGroup
	lanes       []*lanePoolEntry
	slots       [laneSchedulingClassCount]int
	nextClass   int
	nextLane    [laneSchedulingClassCount]int
	preparing   int
	flow        *lanePoolEntry
	writer      *lanePoolEntry
	prepareJobs chan lanePoolJob
	flowJobs    chan lanePoolJob
	writeJobs   chan lanePoolJob
	results     chan lanePoolResult
	now         func() time.Time
}

func newLanePool(sender *laneStreamSender) *lanePool {
	ctx, cancel := context.WithCancel(sender.server.Context())
	return &lanePool{
		sender: sender, ctx: ctx, cancel: cancel,
		prepareJobs: make(chan lanePoolJob, 2), flowJobs: make(chan lanePoolJob, 1),
		writeJobs: make(chan lanePoolJob, 1), results: make(chan lanePoolResult, 4), now: time.Now,
	}
}

func (p *lanePool) run() error {
	s := p.sender
	notifications, subscriber := s.historyEngine.SubscribeReplicationNotification(s.clientClusterName)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for range 2 {
		p.workers.Go(func() { p.worker(p.prepareJobs, p.prepare) })
	}
	p.workers.Go(func() { p.worker(p.flowJobs, p.admitFlow) })
	p.workers.Go(func() { p.worker(p.writeJobs, p.write) })
	defer func() {
		// Stop releases the RPC handler before joining a writer blocked in Send.
		s.Stop()
		p.cancel()
		p.workers.Wait()
		timer.Stop()
		s.historyEngine.UnsubscribeReplicationNotification(subscriber)
		for _, lane := range p.lanes {
			p.release(lane)
		}
	}()
	for {
		if p.ctx.Err() != nil || s.shutdownChan.IsShutdown() {
			return nil
		}
		changed := s.laneRegistry.Changed()
		if err := s.sendReadyLaneRetirements(); err != nil {
			return err
		}
		p.reconcile()
		if err := p.dispatch(); err != nil {
			return err
		}
		tick := p.armTimer(timer)
		select {
		case result := <-p.results:
			if err := p.complete(result); err != nil {
				return err
			}
		case <-notifications:
			p.wake()
		case <-changed:
			p.wake()
		case <-tick:
		case <-p.ctx.Done():
			return nil
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
}

func (p *lanePool) worker(jobs <-chan lanePoolJob, operation func(lanePoolJob) lanePoolResult) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case job := <-jobs:
			if p.ctx.Err() != nil {
				return
			}
			result := operation(job)
			select {
			case p.results <- result:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

func (p *lanePool) reconcile() {
	snapshots := p.sender.laneRegistry.Snapshots()
	slices.SortFunc(snapshots, func(a, b senderLaneSnapshot) int { return strings.Compare(a.logicalKey, b.logicalKey) })
	present := make(map[string]senderLaneSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		present[snapshot.id] = snapshot
	}
	kept := p.lanes[:0]
	for _, lane := range p.lanes {
		if current, ok := present[lane.lane.id]; ok {
			active := lane.phase == lanePoolPreparing || lane.phase == lanePoolFlow || lane.phase == lanePoolWriting
			if lane.held && !active && !p.valid(lane) {
				p.release(lane)
				if p.flow == lane {
					p.flow = nil
				}
				lane.phase, lane.due = lanePoolIdle, time.Time{}
			}
			if !lane.held {
				lane.lane = current
			}
			kept = append(kept, lane)
			delete(present, current.id)
		} else if lane.phase == lanePoolPreparing || lane.phase == lanePoolFlow || lane.phase == lanePoolWriting {
			kept = append(kept, lane)
		} else {
			p.release(lane)
		}
	}
	p.lanes = kept
	for _, snapshot := range snapshots {
		if _, exists := present[snapshot.id]; exists {
			p.lanes = append(p.lanes, &lanePoolEntry{lane: snapshot})
		}
	}
}

func (p *lanePool) wake() {
	for _, lane := range p.lanes {
		lane.rescan = true
		if lane.phase == lanePoolIdle || lane.phase == lanePoolPrepared {
			lane.due = time.Time{}
		}
	}
}

func (p *lanePool) choose(eligible func(*lanePoolEntry) bool) *lanePoolEntry {
	for offset := range laneSchedulingClassCount {
		class := (p.nextClass + offset) % laneSchedulingClassCount
		for laneOffset := range len(p.lanes) {
			index := (p.nextLane[class] + laneOffset) % len(p.lanes)
			lane := p.lanes[index]
			if lane.lane.schedulingClass == class && eligible(lane) {
				p.nextClass = (class + 1) % laneSchedulingClassCount
				p.nextLane[class] = (index + 1) % len(p.lanes)
				return lane
			}
		}
	}
	return nil
}

func (p *lanePool) valid(lane *lanePoolEntry) bool {
	current, exists := p.sender.laneRegistry.SnapshotByID(lane.lane.id)
	return exists && !current.paused && current.logicalKey == lane.lane.logicalKey &&
		current.schedulingClass == lane.lane.schedulingClass && p.sender.lanesConfirmed.Load()
}

func (p *lanePool) prepareMore() {
	for p.preparing < 2 {
		lane := p.choose(func(lane *lanePoolEntry) bool {
			return (lane.phase == lanePoolIdle || lane.phase == lanePoolRetry) && !lane.due.After(p.now()) && p.slots[lane.lane.schedulingClass] < 2
		})
		if lane == nil {
			return
		}
		if !lane.held {
			snapshot, acquired := p.sender.laneRegistry.Acquire(lane.lane.id)
			if !acquired {
				lane.due = p.now().Add(p.idleDelay())
				continue
			}
			lane.lane, lane.held = snapshot, true
		}
		if !p.valid(lane) {
			p.release(lane)
			lane.phase, lane.due = lanePoolIdle, p.now().Add(p.idleDelay())
			continue
		}
		lane.end = min(p.sender.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID, lane.lane.scope.Range.ExclusiveMax.TaskID)
		if lane.lane.cursor == lane.end && lane.lane.acked >= lane.end {
			p.release(lane)
			lane.due = p.now().Add(p.idleDelay())
			continue
		}
		p.slots[lane.lane.schedulingClass]++
		lane.slot, lane.phase, lane.rescan = true, lanePoolPreparing, false
		p.preparing++
		p.prepareJobs <- lanePoolJob{lane: lane, snapshot: lane.lane, raw: lane.raw, end: lane.end}
	}
}

func (p *lanePool) dispatch() error {
	p.prepareMore()
	if p.writer == nil && p.flow != nil && p.flow.phase == lanePoolFlowReady {
		lane := p.flow
		p.flow = nil
		if !p.valid(lane) {
			p.release(lane)
			lane.phase = lanePoolIdle
		} else if ready, delay := p.admission(lane, true); ready {
			p.startWrite(lane)
		} else {
			lane.phase, lane.due = lanePoolPrepared, p.now().Add(delay)
		}
	}
	if p.writer == nil && p.flow == nil {
		lane := p.choose(func(lane *lanePoolEntry) bool {
			if lane.phase != lanePoolPrepared || lane.due.After(p.now()) || !p.valid(lane) {
				return false
			}
			if lane.payload == nil {
				return true
			}
			ready, delay := p.admission(lane, false)
			if !ready {
				lane.due = p.now().Add(delay)
			}
			return ready
		})
		if lane != nil {
			if lane.payload == nil {
				p.startWrite(lane)
			} else {
				p.flow, lane.phase = lane, lanePoolFlow
				p.flowJobs <- lanePoolJob{lane: lane}
			}
		}
	}
	return nil
}

func (p *lanePool) admission(lane *lanePoolEntry, debit bool) (bool, time.Duration) {
	s := p.sender
	if s.laneRegistry.Paused(lane.lane.logicalKey) {
		return false, laneMinRetryAfter
	}
	if !s.laneShouldBeRateLimited(lane.lane.logicalKey) {
		return true, 0
	}
	limiter := s.laneLimiter(lane.lane.logicalKey)
	if limiter == nil {
		return true, 0
	}
	now := s.shardContext.GetTimeSource().Now()
	ready, delay := true, time.Duration(0)
	method := "Reserve"
	if !debit {
		method = "Ready"
	}
	if !s.callLanePolicy(method, func() {
		if debit {
			ready, delay = limiter.Reserve(now)
		} else if probe, ok := limiter.(LaneLimiterReadiness); ok {
			ready, delay = probe.Ready(now)
		}
	}) {
		return true, 0
	}
	if !ready {
		s.markLaneRefused(lane.lane.logicalKey, now)
		return false, laneRetryDelay(delay)
	}
	return true, 0
}

func (p *lanePool) startWrite(lane *lanePoolEntry) {
	p.writer, lane.phase = lane, lanePoolWriting
	p.writeJobs <- lanePoolJob{lane: lane, snapshot: lane.lane, raw: lane.raw, payload: lane.payload, nextCursor: lane.nextCursor, controlTime: lane.controlTime}
}

func (p *lanePool) complete(result lanePoolResult) error {
	lane := result.lane
	switch result.stage {
	case lanePoolPreparing:
		p.preparing--
		lane.raw, lane.payload = result.raw, result.payload
		lane.nextCursor, lane.controlTime = result.nextCursor, result.controlTime
	case lanePoolFlow:
		p.flow = nil
	case lanePoolWriting:
		p.writer = nil
	default:
		return fmt.Errorf("invalid lane pool result stage %d", result.stage)
	}
	if result.panic {
		return result.err
	}
	if result.stage == lanePoolWriting && result.err == nil {
		// A committed write belongs to the handoff prefix even if policy or PAUSE
		// changed while Send was in flight. Publish it before releasing the lease.
		p.finishMetrics(lane.raw)
		p.sender.laneRegistry.AdvanceLaneCursor(lane.lane.id, lane.nextCursor)
	}
	if !p.valid(lane) {
		p.release(lane)
		lane.phase = lanePoolIdle
		return nil
	}
	if result.err != nil {
		return p.failed(lane, result.err)
	}
	switch result.stage {
	case lanePoolPreparing:
		lane.phase, lane.due = lanePoolPrepared, time.Time{}
	case lanePoolFlow:
		p.flow, lane.phase = lane, lanePoolFlowReady
	case lanePoolWriting:
		more := lane.nextCursor < lane.end || lane.rescan
		p.release(lane)
		lane.phase, lane.due = lanePoolIdle, time.Time{}
		if !more {
			lane.due = p.now().Add(p.idleDelay())
		}
	default:
		return fmt.Errorf("invalid lane pool result stage %d", result.stage)
	}
	return nil
}

func (p *lanePool) failed(lane *lanePoolEntry, err error) error {
	if lane.raw == nil {
		return err
	}
	raw, s := lane.raw, p.sender
	if errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) && raw.lockPriority == locks.PriorityLow {
		raw.lowAttempts++
		if raw.lowAttempts >= max(1, s.config.ReplicationTaskConverterLowPriorityLockMaxAttempts()) {
			raw.lockPriority = locks.PriorityHigh
		}
	}
	delay := raw.retry.NextBackOff(err)
	if deadline, ok := p.ctx.Deadline(); ok && p.now().Add(delay).After(deadline) {
		delay = -1
	}
	if delay >= 0 && isRetryableError(err) && !errors.Is(err, context.Canceled) {
		if _, exhausted := errors.AsType[*serviceerror.ResourceExhausted](err); exhausted {
			delay = max(delay, raw.throttle.NextBackOff(err))
		}
		p.freeSlot(lane)
		lane.phase, lane.due = lanePoolRetry, p.now().Add(delay)
		return nil
	}
	p.finishMetrics(raw)
	metrics.ReplicationTaskSendError.With(s.metrics).Record(1,
		metrics.FromClusterIDTag(s.serverShardKey.ClusterID), metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
		metrics.OperationTag(TaskOperationTagFromTask(raw.item.GetType())), metrics.ReplicationTaskPriorityTag(enumsspb.TASK_PRIORITY_HIGH))
	if !s.config.ReplicationStreamSenderSkipStuckTask() || !isSkippable(err) {
		return fmt.Errorf("failed to send lane task: %w", err)
	}
	s.recordStuckTaskSkipped(raw.item, raw.attempt, enumsspb.TASK_PRIORITY_HIGH, err)
	metrics.ReplicationTaskSendSkipped.With(s.metrics).Record(1,
		metrics.FromClusterIDTag(s.serverShardKey.ClusterID), metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
		metrics.OperationTag(TaskOperationTagFromTask(raw.item.GetType())), metrics.ReplicationTaskPriorityTag(enumsspb.TASK_PRIORITY_HIGH))
	p.freeSlot(lane)
	lane.raw, lane.phase, lane.due = nil, lanePoolPrepared, time.Time{}
	return nil
}

func (p *lanePool) finishMetrics(raw *lanePoolRaw) {
	if raw == nil {
		return
	}
	s := p.sender
	tags := []metrics.Tag{metrics.FromClusterIDTag(s.serverShardKey.ClusterID), metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
		metrics.OperationTag(TaskOperationTagFromTask(raw.item.GetType())), metrics.ReplicationTaskPriorityTag(enumsspb.TASK_PRIORITY_HIGH)}
	metrics.ReplicationTaskSendAttempt.With(s.metrics).Record(raw.attempt, tags...)
	metrics.ReplicationTaskSendLatency.With(s.metrics).Record(time.Since(raw.item.GetVisibilityTime()), tags...)
}

func (p *lanePool) idleDelay() time.Duration {
	return max(laneMinRetryAfter, p.sender.config.ReplicationStreamSendEmptyTaskDuration())
}

func (p *lanePool) freeSlot(lane *lanePoolEntry) {
	if lane.slot {
		p.slots[lane.lane.schedulingClass]--
		lane.slot = false
	}
	lane.payload = nil
}

func (p *lanePool) release(lane *lanePoolEntry) {
	p.freeSlot(lane)
	if lane.held {
		p.sender.laneRegistry.Release(lane.lane.id)
		lane.held = false
	}
	lane.raw = nil
}

func (p *lanePool) armTimer(timer *time.Timer) <-chan time.Time {
	timer.Stop()
	var deadline time.Time
	for _, lane := range p.lanes {
		if (lane.phase == lanePoolIdle || lane.phase == lanePoolRetry || lane.phase == lanePoolPrepared) &&
			lane.due.After(p.now()) && (deadline.IsZero() || lane.due.Before(deadline)) {
			deadline = lane.due
		}
	}
	if deadline.IsZero() {
		return nil
	}
	timer.Reset(max(time.Nanosecond, deadline.Sub(p.now())))
	return timer.C
}

func (p *lanePool) capture(result *lanePoolResult) {
	if result.err != nil {
		metrics.ReplicationStreamPanic.With(p.sender.metrics).Record(1)
		result.panic = true
	}
}

func (p *lanePool) prepare(job lanePoolJob) (result lanePoolResult) {
	s := p.sender
	result = lanePoolResult{lane: job.lane, stage: lanePoolPreparing, raw: job.raw, nextCursor: job.snapshot.cursor, controlTime: p.now()}
	var panicErr error
	defer func() {
		if panicErr != nil {
			result.err = panicErr
			p.capture(&result)
		}
	}()
	defer log.CapturePanic(s.logger, &panicErr)
	if result.raw == nil {
		p.scan(job, &result)
	} else {
		result.nextCursor, result.controlTime = result.raw.item.GetTaskID()+1, result.raw.item.GetVisibilityTime()
	}
	if result.err != nil || result.raw == nil {
		return result
	}
	raw := result.raw
	raw.attempt++
	started := time.Now()
	task, err := s.taskConverter.Convert(raw.item, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_HIGH, raw.lockPriority)
	metrics.ReplicationTaskGenerationLatency.With(s.metrics).Record(time.Since(started),
		metrics.FromClusterIDTag(s.serverShardKey.ClusterID), metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
		metrics.OperationTag(TaskOperationTagFromTask(raw.item.GetType())), metrics.ReplicationTaskPriorityTag(enumsspb.TASK_PRIORITY_HIGH))
	if err != nil {
		result.err = s.recordRetry(raw.item, enumsspb.REPLICATION_TASK_TYPE_UNSPECIFIED, enumsspb.TASK_PRIORITY_HIGH, raw.attempt,
			wideevents.ReplOperationTaskConversion, &convertError{err: fmt.Errorf("convert: %w", err)})
	} else if task != nil {
		task.Priority = enumsspb.TASK_PRIORITY_HIGH
		result.payload = task
	}
	return result
}

func (p *lanePool) scan(job lanePoolJob, result *lanePoolResult) {
	s := p.sender
	if job.snapshot.cursor > job.end {
		result.err = fmt.Errorf("invalid lane task range [%d, %d)", job.snapshot.cursor, job.end)
		return
	}
	if job.snapshot.cursor == job.end {
		return
	}
	ctx := headers.SetCallerInfo(p.ctx, getReplicaitonCallerInfo(enumsspb.TASK_PRIORITY_HIGH))
	iter, err := s.historyEngine.GetReplicationTasksIter(ctx, string(s.clientShardKey.ClusterID), job.snapshot.cursor, job.end)
	if err != nil {
		result.err = err
		return
	}
	for scanned := 0; scanned < TaskMaxSkipCount && iter.HasNext(); scanned++ {
		if p.ctx.Err() != nil {
			result.err = p.ctx.Err()
			return
		}
		item, err := iter.Next()
		if err != nil {
			result.err = fmt.Errorf("unable to read lane task: %w", err)
			return
		}
		result.nextCursor, result.controlTime = item.GetTaskID()+1, item.GetVisibilityTime()
		if s.getTaskPriority(item) != enumsspb.TASK_PRIORITY_HIGH || !s.shouldProcessTask(item) || !job.snapshot.scope.Contains(item) {
			continue
		}
		metrics.ReplicationTaskLoadLatency.With(s.metrics).Record(time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID), metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())), metrics.ReplicationTaskPriorityTag(enumsspb.TASK_PRIORITY_HIGH))
		config := s.config
		policy := backoff.NewExponentialRetryPolicy(config.ReplicationStreamSenderErrorRetryWait()).
			WithBackoffCoefficient(config.ReplicationStreamSenderErrorRetryBackoffCoefficient()).
			WithMaximumInterval(config.ReplicationStreamSenderErrorRetryMaxInterval()).
			WithMaximumAttempts(config.ReplicationStreamSenderErrorRetryMaxAttempts()).
			WithExpirationInterval(config.ReplicationStreamSenderErrorRetryExpiration())
		source := clock.NewRealTimeSource()
		result.raw = &lanePoolRaw{item: item, lockPriority: locks.PriorityLow, retry: backoff.NewRetrier(policy, source),
			throttle: backoff.NewRetrier(backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(backoff.NoInterval), source)}
		return
	}
	if !iter.HasNext() {
		result.nextCursor, result.controlTime = job.end, p.now()
	}
}

func (p *lanePool) admitFlow(job lanePoolJob) (result lanePoolResult) {
	result = lanePoolResult{lane: job.lane, stage: lanePoolFlow}
	var panicErr error
	defer func() {
		if panicErr != nil {
			result.err = panicErr
			p.capture(&result)
		}
	}()
	defer log.CapturePanic(p.sender.logger, &panicErr)
	if err := p.sender.flowController.Wait(p.ctx, enumsspb.TASK_PRIORITY_HIGH); errors.Is(err, context.Canceled) {
		result.err = err
	}
	return result
}

func (p *lanePool) write(job lanePoolJob) (result lanePoolResult) {
	s := p.sender
	result = lanePoolResult{lane: job.lane, stage: lanePoolWriting}
	var panicErr error
	defer func() {
		if panicErr != nil {
			result.err = panicErr
			p.capture(&result)
		}
	}()
	defer log.CapturePanic(s.logger, &panicErr)
	if err := p.ctx.Err(); err != nil {
		result.err = err
		return result
	}
	var payloads []*replicationspb.ReplicationTask
	if job.payload != nil {
		payloads = []*replicationspb.ReplicationTask{job.payload}
		if s.config.EmitReplicationLifecycleEvents() {
			s.emitReplicationSent(job.payload, job.raw.item)
		}
	}
	result.err = s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks: payloads, ExclusiveHighWatermark: job.nextCursor, ExclusiveHighWatermarkTime: timestamppb.New(job.controlTime),
				Priority: enumsspb.TASK_PRIORITY_HIGH, LaneInfo: &replicationspb.ReplicationLaneInfo{LaneId: job.snapshot.id},
			},
		},
	})
	if job.payload != nil {
		if result.err != nil {
			result.err = s.recordRetry(job.raw.item, job.payload.GetTaskType(), enumsspb.TASK_PRIORITY_HIGH, job.raw.attempt,
				wideevents.ReplOperationStreamSend, fmt.Errorf("send: %w", result.err))
		} else {
			metrics.ReplicationLaneTasksSent.With(s.metrics).Record(1, metrics.ToClusterTag(s.clientClusterName), metrics.ReplicationLaneTag(job.snapshot.logicalKey))
			metrics.ReplicationTasksSend.With(s.metrics).Record(1, metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID), metrics.OperationTag(TaskOperationTag(job.payload)))
		}
	}
	return result
}
