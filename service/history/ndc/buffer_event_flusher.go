// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination buffer_event_flusher_mock.go

package ndc

import (
	"context"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	BufferEventFlusher interface {
		flush(
			ctx context.Context,
		) (workflow.Context, workflow.MutableState, error)
	}

	BufferEventFlusherImpl struct {
		shardContext    shard.Context
		clusterMetadata cluster.Metadata

		wfContext    workflow.Context
		mutableState workflow.MutableState
		logger       log.Logger
	}
)

var _ BufferEventFlusher = (*BufferEventFlusherImpl)(nil)

func NewBufferEventFlusher(
	shardContext shard.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
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
) (workflow.Context, workflow.MutableState, error) {
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
