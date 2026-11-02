package workflow

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

const (
	maxPipelineSubmissions = 128
	maxPipelineBytes       = 16 << 20
	maxPipelineBatch       = 32
	maxPipelineBatchBytes  = 4 << 20
)

type submission struct {
	ctx             context.Context
	failoverVersion int64
	request         *persistence.UpdateWorkflowExecutionRequest
	completion      *future.FutureImpl[struct{}]
	historySize     int64
	bytes           int
	taskTargets     map[tasks.Category][]tasks.Task
}

type operation struct {
	pipelined   bool
	submissions []*submission
	dependency  future.Future[struct{}]
	ctx         context.Context
}

type persistencePipeline struct {
	mu                       sync.Mutex
	err                      error
	inFlight                 []*submission
	pending                  []*submission
	committedDBRecordVersion int64
	committedHistorySize     int64
	outstandingHistorySize   int64
	coalescing               bool
	waiters                  []chan struct{}
	changed                  chan struct{}
	acquirePin               func()
	releasePin               func()
	pinned                   bool
	cacheSize                atomic.Int64
}

type pipelineContextKey struct{}

// WithPipelinedPersistence opts an audited handler into speculative state access.
func WithPipelinedPersistence(ctx context.Context) context.Context {
	return context.WithValue(ctx, pipelineContextKey{}, ctx)
}

func PipelinedPersistence(ctx context.Context) bool {
	_, enabled := ctx.Value(pipelineContextKey{}).(context.Context)
	return enabled
}

func pipelineDependencyError() error {
	return serviceerror.NewUnavailable("workflow persistence dependency failed; retry the operation")
}

func readyPersistenceFuture(err error) *future.FutureImpl[struct{}] {
	f := future.NewFuture[struct{}]()
	f.Set(struct{}{}, err)
	return f
}

func (p *persistencePipeline) notify() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *persistencePipeline) tail() *submission {
	if len(p.pending) != 0 {
		return p.pending[len(p.pending)-1]
	}
	if len(p.inFlight) != 0 {
		return p.inFlight[len(p.inFlight)-1]
	}
	return nil
}

func (c *ContextImpl) PersistenceFuture() future.Future[struct{}] {
	if c.pipeline == nil {
		return readyPersistenceFuture(nil)
	}
	p := c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return readyPersistenceFuture(p.err)
	}
	if tail := p.tail(); tail != nil {
		return tail.completion
	}
	return readyPersistenceFuture(nil)
}

// SetPersistencePin keeps the cache entry alive independently of canceled callers.
func (c *ContextImpl) SetPersistencePin(acquire, release func()) {
	if c.pipeline != nil {
		c.pipeline.acquirePin = acquire
		c.pipeline.releasePin = release
	}
}

func (c *ContextImpl) PersistencePipelineEnabled() bool { return c.pipeline != nil }

func (c *ContextImpl) PipelinedOperation() bool { return c.operation != nil && c.operation.pipelined }

func (p *persistencePipeline) releaseIdlePin() func() {
	if p.pinned && p.tail() == nil {
		p.pinned = false
		return p.releasePin
	}
	return nil
}

func (c *ContextImpl) lockCommitted(ctx context.Context, priority locks.Priority) error {
	p := c.pipeline
	waiter := make(chan struct{})
	p.mu.Lock()
	p.waiters = append(p.waiters, waiter)
	p.notify()
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.waiters = slices.DeleteFunc(p.waiters, func(w chan struct{}) bool { return w == waiter })
		p.notify()
		p.mu.Unlock()
	}()
	for {
		p.mu.Lock()
		first := p.waiters[0] == waiter
		changed := p.changed
		p.mu.Unlock()
		if !first {
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := c.lock.Acquire(ctx, priority, 1); err != nil {
			return err
		}
		p.mu.Lock()
		if p.waiters[0] != waiter {
			p.mu.Unlock()
			c.lock.Release(1)
			continue
		}
		tail := p.tail()
		invalid := p.err != nil
		p.mu.Unlock()
		if tail != nil {
			c.lock.Release(1)
			if _, err := tail.completion.Get(ctx); err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		if invalid {
			c.clearMutableState()
		}
		c.operation = &operation{ctx: ctx, dependency: readyPersistenceFuture(nil)}
		return nil
	}
}

func (c *ContextImpl) LockSpeculative(ctx context.Context, priority locks.Priority) error {
	if c.pipeline == nil {
		return c.Lock(ctx, priority)
	}
	p := c.pipeline
	for {
		p.mu.Lock()
		blocked := len(p.waiters) != 0 || len(p.pending)+len(p.inFlight) >= maxPipelineSubmissions
		invalid := p.err != nil
		changed := p.changed
		p.mu.Unlock()
		if invalid {
			return c.lockCommitted(ctx, priority)
		}
		if blocked {
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := c.lock.Acquire(ctx, priority, 1); err != nil {
			return err
		}
		p.mu.Lock()
		blocked = len(p.waiters) != 0 || p.err != nil || len(p.pending)+len(p.inFlight) >= maxPipelineSubmissions
		p.mu.Unlock()
		if blocked {
			c.lock.Release(1)
			continue
		}
		operationCtx := ctx
		if original, ok := ctx.Value(pipelineContextKey{}).(context.Context); ok {
			operationCtx = original
		}
		c.operation = &operation{ctx: operationCtx, pipelined: true, dependency: c.PersistenceFuture()}
		return nil
	}
}

func (p *persistencePipeline) invalidate(start int, err error) {
	p.err = pipelineDependencyError()
	for _, s := range p.pending[start:] {
		result := err
		if canceled := s.ctx.Err(); canceled != nil {
			result = canceled
		}
		s.completion.Set(struct{}{}, result)
		p.outstandingHistorySize -= s.historySize
	}
	p.pending = p.pending[:start]
	p.notify()
}

func (p *persistencePipeline) submit(ctx context.Context, failoverVersion int64, request *persistence.UpdateWorkflowExecutionRequest) (*submission, error) {
	owned, size, historySize, err := clonePipelineRequest(request)
	if err != nil {
		return nil, err
	}
	if size > maxPipelineBatchBytes {
		return nil, persistence.ErrIncompatibleWorkflowMutations
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	queuedBytes := size
	for _, s := range p.pending {
		queuedBytes += s.bytes
	}
	if queuedBytes > maxPipelineBytes {
		return nil, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "workflow persistence pipeline is full")
	}
	s := &submission{ctx: ctx, failoverVersion: failoverVersion, request: owned, completion: future.NewFuture[struct{}](), historySize: historySize, bytes: size, taskTargets: request.UpdateWorkflowMutation.Tasks}
	if !p.pinned && p.acquirePin != nil {
		p.acquirePin()
		p.pinned = true
	}
	p.pending = append(p.pending, s)
	p.outstandingHistorySize += historySize
	p.notify()
	return s, nil
}

func (c *ContextImpl) completeOperation(op *operation) error {
	var result error
	for _, s := range op.submissions {
		if err := c.completeSubmission(s); result == nil && err != nil {
			result = err
		}
	}
	if len(op.submissions) == 0 {
		_, result = op.dependency.Get(op.ctx)
	}
	return result
}

func (c *ContextImpl) completeSubmission(s *submission) error {
	p := c.pipeline
	for {
		p.mu.Lock()
		if s.completion.Ready() {
			p.mu.Unlock()
			_, err := s.completion.Get(context.Background())
			return err
		}
		index := slices.Index(p.pending, s)
		if err := s.ctx.Err(); err != nil {
			if index >= 0 {
				p.invalidate(index, pipelineDependencyError())
			}
			release := p.releaseIdlePin()
			p.mu.Unlock()
			if release != nil {
				release()
			}
			return err
		}
		if index == 0 && len(p.inFlight) == 0 {
			request, err := p.claimHead(s)
			if err != nil {
				p.invalidate(0, err)
				release := p.releaseIdlePin()
				p.mu.Unlock()
				if release != nil {
					release()
				}
				continue
			}
			p.mu.Unlock()
			c.executePersistenceGroup(s, request)
			continue
		}
		predecessor := p.predecessor(index)
		p.mu.Unlock()
		select {
		case <-s.completion.Done():
		case <-predecessor:
		case <-s.ctx.Done():
		}
	}
}

func (p *persistencePipeline) predecessor(index int) <-chan struct{} {
	if index > 0 {
		return p.pending[index-1].completion.Done()
	}
	if index == 0 && len(p.inFlight) != 0 {
		return p.inFlight[len(p.inFlight)-1].completion.Done()
	}
	return nil
}

func (p *persistencePipeline) claimHead(s *submission) (*persistence.UpdateWorkflowExecutionRequest, error) {
	request, _, _, err := clonePipelineRequest(s.request)
	if err != nil {
		return nil, err
	}
	count, size := 1, s.bytes
	for p.coalescing && count < len(p.pending) && count < maxPipelineBatch {
		next := p.pending[count]
		if next.ctx.Err() != nil {
			p.invalidate(count, pipelineDependencyError())
			break
		}
		if next.failoverVersion != s.failoverVersion || size+next.bytes > maxPipelineBatchBytes {
			break
		}
		merged, err := composeRequests(request, next.request)
		if err != nil {
			break
		}
		request = merged
		count++
		size += next.bytes
	}
	request.UpdateWorkflowMutation.DBRecordVersion = p.committedDBRecordVersion + 1
	request.UpdateWorkflowMutation.ExecutionInfo.ExecutionStats.HistorySize = p.committedHistorySize
	p.inFlight = slices.Clone(p.pending[:count])
	p.pending = p.pending[count:]
	p.notify()
	return request, nil
}

func (c *ContextImpl) executePersistenceGroup(first *submission, request *persistence.UpdateWorkflowExecutionRequest) {
	shard := c.pipelineShard
	ctx, cancel := context.WithTimeout(rpc.CopyContextValues(shard.GetLifecycleContext(), first.ctx), c.config.ShardIOTimeout())
	defer cancel()
	var delta int64
	var err error
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		delta, _, err = NewTransaction(shard).UpdateWorkflowExecution(ctx, request.Mode, request.ArchetypeID,
			first.failoverVersion, &request.UpdateWorkflowMutation, request.UpdateWorkflowEvents, nil, nil, nil, false,
			request.ArchetypeID == chasm.WorkflowArchetypeID)
	}()
	if recovered != nil {
		err = serviceerror.NewInternal("workflow persistence panicked")
	}
	// Publication must settle a claimed write even when the shard or caller is canceled.
	_ = c.lock.Acquire(context.Background(), locks.PriorityHigh, 1)
	p := c.pipeline
	p.mu.Lock()
	members := p.inFlight
	if err == nil {
		p.committedDBRecordVersion = request.UpdateWorkflowMutation.DBRecordVersion
		p.committedHistorySize += delta
		assigned := make(map[tasks.Category]int)
		for _, s := range members {
			for category, targets := range s.taskTargets {
				for _, target := range targets {
					written := request.UpdateWorkflowMutation.Tasks[category][assigned[category]]
					target.SetTaskID(written.GetTaskID())
					target.SetVisibilityTime(written.GetVisibilityTime())
					assigned[category]++
				}
			}
		}
	} else {
		p.invalidate(0, pipelineDependencyError())
	}
	for _, s := range members {
		p.outstandingHistorySize -= s.historySize
	}
	if err == nil && c.MutableState != nil {
		next, _ := c.MutableState.GetUpdateCondition()
		c.MutableState.SetUpdateCondition(next, p.committedDBRecordVersion)
		info := c.MutableState.GetExecutionInfo()
		info.ExecutionStats.HistorySize = p.committedHistorySize + p.outstandingHistorySize
		if id := request.UpdateWorkflowMutation.ExecutionInfo.CloseTransferTaskId; id != 0 {
			info.CloseTransferTaskId = id
		}
		if id := request.UpdateWorkflowMutation.ExecutionInfo.CloseVisibilityTaskId; id != 0 {
			info.CloseVisibilityTaskId = id
		}
	}
	if c.config.HistoryCacheLimitSizeBased {
		p.cacheSize.Store(int64(c.cacheSizeLocked()))
	}
	p.inFlight = nil
	for _, s := range members {
		s.completion.Set(struct{}{}, err)
	}
	p.notify()
	release := p.releaseIdlePin()
	p.mu.Unlock()
	metrics.WorkflowPersistencePipelineWrites.With(c.metricsHandler).Record(1)
	metrics.WorkflowPersistencePipelineSubmissions.With(c.metricsHandler).Record(int64(len(members)))
	metrics.WorkflowPersistencePipelineBatchSize.With(c.metricsHandler).Record(int64(len(members)))
	if release != nil {
		release()
	}
	c.lock.Release(1)
	if recovered != nil {
		panic(recovered) //nolint:forbidigo // Preserve the original panic after settling accepted writes.
	}
}

func composeRequests(current, next *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionRequest, error) {
	if err := validatePipelineRequest(current); err != nil {
		return nil, err
	}
	if err := validatePipelineRequest(next); err != nil {
		return nil, err
	}
	if current.Mode != next.Mode || current.ArchetypeID != next.ArchetypeID || current.ShardID != next.ShardID ||
		current.NewWorkflowSnapshot != nil || next.NewWorkflowSnapshot != nil || len(current.NewWorkflowEvents) != 0 || len(next.NewWorkflowEvents) != 0 ||
		current.CheckRunAlreadyExists || next.CheckRunAlreadyExists {
		return nil, persistence.ErrIncompatibleWorkflowMutations
	}
	if len(current.UpdateWorkflowMutation.Tasks[tasks.CategoryReplication]) != 0 || len(next.UpdateWorkflowMutation.Tasks[tasks.CategoryReplication]) != 0 {
		return nil, persistence.ErrIncompatibleWorkflowMutations
	}
	currentHistories := current.UpdateWorkflowMutation.ExecutionInfo.GetVersionHistories()
	nextHistories := next.UpdateWorkflowMutation.ExecutionInfo.GetVersionHistories()
	if len(currentHistories.GetHistories()) != 0 && len(nextHistories.GetHistories()) != 0 &&
		!bytes.Equal(currentHistories.Histories[currentHistories.CurrentVersionHistoryIndex].BranchToken, nextHistories.Histories[nextHistories.CurrentVersionHistoryIndex].BranchToken) {
		return nil, persistence.ErrIncompatibleWorkflowMutations
	}
	if len(current.UpdateWorkflowEvents) != 0 && len(next.UpdateWorkflowEvents) != 0 {
		last := current.UpdateWorkflowEvents[len(current.UpdateWorkflowEvents)-1]
		first := next.UpdateWorkflowEvents[0]
		if !bytes.Equal(last.BranchToken, first.BranchToken) || last.TxnID != first.PrevTxnID {
			return nil, persistence.ErrIncompatibleWorkflowMutations
		}
	}
	mutation, err := current.UpdateWorkflowMutation.Merge(&next.UpdateWorkflowMutation)
	if err != nil {
		return nil, err
	}
	result := *current
	result.UpdateWorkflowMutation = *mutation
	result.UpdateWorkflowEvents = make([]*persistence.WorkflowEvents, 0, len(current.UpdateWorkflowEvents)+len(next.UpdateWorkflowEvents))
	for _, batch := range append(slices.Clone(current.UpdateWorkflowEvents), next.UpdateWorkflowEvents...) {
		owned := *batch
		owned.BranchToken = slices.Clone(batch.BranchToken)
		owned.Events = common.CloneProtoSlice(batch.Events)
		result.UpdateWorkflowEvents = append(result.UpdateWorkflowEvents, &owned)
	}
	return &result, nil
}

func clonePipelineRequest(request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionRequest, int, int64, error) {
	if err := validatePipelineRequest(request); err != nil {
		return nil, 0, 0, err
	}
	if request.NewWorkflowSnapshot != nil || len(request.NewWorkflowEvents) != 0 || request.CheckRunAlreadyExists {
		return nil, 0, 0, persistence.ErrIncompatibleWorkflowMutations
	}
	mutation, err := request.UpdateWorkflowMutation.Clone()
	if err != nil {
		return nil, 0, 0, err
	}
	r := *request
	r.UpdateWorkflowMutation = *mutation
	r.UpdateWorkflowEvents = make([]*persistence.WorkflowEvents, 0, len(request.UpdateWorkflowEvents))
	size := proto.Size(mutation.ExecutionInfo) + proto.Size(mutation.ExecutionState) + proto.Size(mutation.Checksum)
	serializer := serialization.NewSerializer()
	var historySize int64
	for _, batch := range request.UpdateWorkflowEvents {
		owned := *batch
		owned.BranchToken = slices.Clone(batch.BranchToken)
		owned.Events = common.CloneProtoSlice(batch.Events)
		r.UpdateWorkflowEvents = append(r.UpdateWorkflowEvents, &owned)
		blob, err := serializer.SerializeEvents(owned.Events)
		if err != nil {
			return nil, 0, 0, err
		}
		historySize += int64(len(blob.Data))
		size += len(blob.Data) + len(owned.BranchToken) + 64
	}
	mutationSize, err := pipelineMutationSize(mutation)
	if err != nil {
		return nil, 0, 0, err
	}
	return &r, size + mutationSize, historySize, nil
}

func pipelineMutationSize(mutation *persistence.WorkflowMutation) (int, error) {
	size := 0
	serializer := serialization.NewSerializer()
	for _, event := range mutation.NewBufferedEvents {
		size += proto.Size(event)
	}
	for _, value := range mutation.UpsertActivityInfos {
		size += proto.Size(value) + 8
	}
	for key, value := range mutation.UpsertTimerInfos {
		size += proto.Size(value) + len(key)
	}
	for _, value := range mutation.UpsertChildExecutionInfos {
		size += proto.Size(value) + 8
	}
	for _, value := range mutation.UpsertRequestCancelInfos {
		size += proto.Size(value) + 8
	}
	for _, value := range mutation.UpsertSignalInfos {
		size += proto.Size(value) + 8
	}
	for key, value := range mutation.UpsertChasmNodes {
		size += proto.Size(value) + len(key)
	}
	for key := range mutation.UpsertSignalRequestedIDs {
		size += len(key)
	}
	for key := range mutation.DeleteSignalRequestedIDs {
		size += len(key)
	}
	for key := range mutation.DeleteTimerInfos {
		size += len(key)
	}
	for key := range mutation.DeleteChasmNodes {
		size += len(key)
	}
	size += 8 * (len(mutation.DeleteActivityInfos) + len(mutation.DeleteChildExecutionInfos) + len(mutation.DeleteRequestCancelInfos) + len(mutation.DeleteSignalInfos))
	for _, entries := range mutation.Tasks {
		for _, task := range entries {
			blob, err := serializer.SerializeTask(task)
			if err != nil {
				return 0, err
			}
			size += len(blob.Data)
		}
	}
	for _, entries := range mutation.BestEffortDeleteTasks {
		size += len(entries) * 24
	}
	return size, nil
}

func validatePipelineRequest(request *persistence.UpdateWorkflowExecutionRequest) error {
	if request == nil {
		return &persistence.InvalidPersistenceRequestError{Msg: "missing workflow persistence request"}
	}
	m := &request.UpdateWorkflowMutation
	if m.ExecutionInfo == nil || m.ExecutionState == nil || m.ExecutionInfo.ExecutionStats == nil || m.NextEventID < m.Condition {
		return &persistence.InvalidPersistenceRequestError{Msg: "invalid workflow persistence mutation"}
	}
	histories := m.ExecutionInfo.GetVersionHistories()
	if len(histories.GetHistories()) != 0 && (histories.CurrentVersionHistoryIndex < 0 || int(histories.CurrentVersionHistoryIndex) >= len(histories.Histories)) {
		return &persistence.InvalidPersistenceRequestError{Msg: "invalid workflow history branch index"}
	}
	if len(histories.GetHistories()) != 0 && histories.Histories[histories.CurrentVersionHistoryIndex] == nil {
		return &persistence.InvalidPersistenceRequestError{Msg: "missing workflow history branch"}
	}
	var cursor int64
	var previous *persistence.WorkflowEvents
	for _, batch := range request.UpdateWorkflowEvents {
		if batch == nil || len(batch.Events) == 0 || batch.NamespaceID != m.ExecutionInfo.NamespaceId || batch.WorkflowID != m.ExecutionInfo.WorkflowId || batch.RunID != m.ExecutionState.RunId {
			return &persistence.InvalidPersistenceRequestError{Msg: "invalid workflow history batch"}
		}
		if previous != nil && (!bytes.Equal(previous.BranchToken, batch.BranchToken) || previous.TxnID != batch.PrevTxnID) {
			return &persistence.InvalidPersistenceRequestError{Msg: "unordered workflow history batches"}
		}
		for _, event := range batch.Events {
			if event == nil || (cursor != 0 && event.EventId != cursor) {
				return &persistence.InvalidPersistenceRequestError{Msg: "unordered workflow history events"}
			}
			cursor = event.EventId + 1
		}
		previous = batch
	}
	return nil
}

func (c *ContextImpl) makeOperationCommitted(ctx context.Context, discard bool) error {
	if c.operation == nil || !c.operation.pipelined {
		return nil
	}
	p := c.pipeline
	op := c.operation
	c.operation = nil
	waiter := make(chan struct{})
	p.mu.Lock()
	// The current holder was admitted before waiters registered during its preparation.
	p.waiters = append([]chan struct{}{waiter}, p.waiters...)
	tail := p.tail()
	p.notify()
	p.mu.Unlock()
	c.lock.Release(1)
	var err error
	if tail != nil {
		_, err = tail.completion.Get(context.WithoutCancel(ctx))
	}
	for {
		p.mu.Lock()
		first, changed := p.waiters[0] == waiter, p.changed
		p.mu.Unlock()
		if first {
			break
		}
		<-changed
	}
	for {
		_ = c.lock.Acquire(context.Background(), locks.PriorityHigh, 1)
		p.mu.Lock()
		remaining := p.tail()
		p.mu.Unlock()
		if remaining == nil {
			break
		}
		c.lock.Release(1)
		_, dependencyErr := remaining.completion.Get(context.WithoutCancel(ctx))
		if err == nil {
			err = dependencyErr
		}
	}
	p.mu.Lock()
	p.waiters = slices.DeleteFunc(p.waiters, func(w chan struct{}) bool { return w == waiter })
	invalid := p.err != nil
	if discard || ctx.Err() != nil {
		p.err = pipelineDependencyError()
	}
	p.notify()
	p.mu.Unlock()
	c.operation = op
	op.pipelined = false
	op.dependency = readyPersistenceFuture(err)
	if discard || invalid || ctx.Err() != nil {
		c.clearMutableState()
	}
	if ctx.Err() != nil {
		op.dependency = readyPersistenceFuture(ctx.Err())
		return ctx.Err()
	}
	if err != nil || invalid {
		op.dependency = readyPersistenceFuture(pipelineDependencyError())
		return pipelineDependencyError()
	}
	return err
}

func (c *ContextImpl) refreshPersistenceBaseline(reloaded bool) {
	if c.pipeline == nil || c.MutableState == nil {
		return
	}
	p := c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tail() != nil {
		return
	}
	if p.err != nil && !reloaded {
		return
	}
	_, p.committedDBRecordVersion = c.MutableState.GetUpdateCondition()
	p.committedHistorySize = c.MutableState.GetExecutionInfo().GetExecutionStats().GetHistorySize()
	p.outstandingHistorySize = 0
	p.err = nil
	p.notify()
}

func (c *ContextImpl) commitPreparedMutation(ctx context.Context, mutation *persistence.WorkflowMutation) error {
	if err := c.makeOperationCommitted(ctx, false); err != nil {
		return err
	}
	p := c.pipeline
	p.mu.Lock()
	version := p.committedDBRecordVersion + 1
	historySize := p.committedHistorySize
	p.mu.Unlock()
	next, _ := c.MutableState.GetUpdateCondition()
	mutation.DBRecordVersion = version
	mutation.ExecutionInfo.ExecutionStats.HistorySize = historySize
	c.MutableState.SetUpdateCondition(next, version)
	return nil
}
