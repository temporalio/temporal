//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination buffer_event_flusher_mock.go

package ndc

import (
	"context"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	historyi "go.temporal.io/server/service/history/interfaces"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	BufferEventFlusher interface {
		flush(
			ctx context.Context,
		) (historyi.WorkflowContext, historyi.MutableState, error)
	}

	BufferEventFlusherImpl struct {
		shardContext    historyi.ShardContext
		clusterMetadata cluster.Metadata

		wfContext    historyi.WorkflowContext
		mutableState historyi.MutableState
		logger       log.Logger
	}
)

var _ BufferEventFlusher = (*BufferEventFlusherImpl)(nil)

func NewBufferEventFlusher(
	shardContext historyi.ShardContext,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	logger log.Logger,
) *BufferEventFlusherImpl {

	return &BufferEventFlusherImpl{
		shardContext:    shardContext,
		clusterMetadata: shardContext.GetClusterMetadata(),

		wfContext:    wfContext,
		mutableState: mutableState,
		logger:       logger,
	}
}

func (r *BufferEventFlusherImpl) flush(
	ctx context.Context,
) (historyi.WorkflowContext, historyi.MutableState, error) {
	// check whether there are buffered events, if so, flush it
	// NOTE: buffered events does not show in version history or next event id
	if !r.mutableState.HasBufferedEvents() {
		if r.mutableState.HasStartedWorkflowTask() && r.mutableState.IsTransientWorkflowTask() {
			if err := r.mutableState.ClearTransientWorkflowTask(); err != nil {
				return nil, nil, err
			}
			// now transient task is gone
		}
		return r.wfContext, r.mutableState, nil
	}

	targetWorkflow := NewWorkflow(
		r.clusterMetadata,
		r.wfContext,
		r.mutableState,
		wcache.NoopReleaseFn,
	)
	if err := targetWorkflow.FlushBufferedEvents(); err != nil {
		return nil, nil, err
	}

	// the workflow must be updated as active, to send out replication tasks
	if err := targetWorkflow.context.UpdateWorkflowExecutionAsActive(
		ctx,
		r.shardContext,
	); err != nil {
		return nil, nil, err
	}

	r.wfContext = targetWorkflow.GetContext()
	r.mutableState = targetWorkflow.GetMutableState()
	return r.wfContext, r.mutableState, nil
}
