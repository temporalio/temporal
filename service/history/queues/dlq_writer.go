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

package queues

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// DLQWriter can be used to write tasks to the DLQ.
	DLQWriter struct {
		dlqWriter         QueueWriter
		clusterMetadata   cluster.Metadata
		metricsHandler    metrics.Handler
		logger            log.SnTaggedLogger
		namespaceRegistry namespace.Registry
	}
	// QueueWriter is a subset of persistence.HistoryTaskQueueManager.
	QueueWriter interface {
		CreateQueue(
			ctx context.Context,
			request *persistence.CreateQueueRequest,
		) (*persistence.CreateQueueResponse, error)
		EnqueueTask(
			ctx context.Context,
			request *persistence.EnqueueTaskRequest,
		) (*persistence.EnqueueTaskResponse, error)
	}
)

var (
	ErrSendTaskToDLQ      = errors.New("failed to send task to DLQ")
	ErrCreateDLQ          = errors.New("failed to create DLQ")
	ErrGetClusterMetadata = errors.New("failed to get cluster metadata")
)

// NewDLQWriter returns a DLQ which will write to the given QueueWriter.
func NewDLQWriter(w QueueWriter, m cluster.Metadata, h metrics.Handler, l log.SnTaggedLogger, r namespace.Registry) *DLQWriter {
	return &DLQWriter{
		dlqWriter:         w,
		clusterMetadata:   m,
		metricsHandler:    h,
		logger:            l,
		namespaceRegistry: r,
	}
}

// WriteTaskToDLQ writes a task to the DLQ, creating the underlying queue if it doesn't already exist.
func (q *DLQWriter) WriteTaskToDLQ(ctx context.Context, sourceCluster, targetCluster string, task tasks.Task) error {
	queueKey := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		Category:      task.GetCategory(),
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
	}
	_, err := q.dlqWriter.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	if err != nil {
		if !errors.Is(err, persistence.ErrQueueAlreadyExists) {
			return fmt.Errorf("%w: %v", ErrCreateDLQ, err)
		}
	}
	info, ok := q.clusterMetadata.GetAllClusterInfo()[queueKey.SourceCluster]
	if !ok {
		return fmt.Errorf("%w: %v", ErrGetClusterMetadata, queueKey.SourceCluster)
	}
	numShards := int(info.ShardCount)
	shardID := tasks.GetShardIDForTask(task, numShards)
	resp, err := q.dlqWriter.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     queueKey.QueueType,
		SourceCluster: queueKey.SourceCluster,
		TargetCluster: queueKey.TargetCluster,
		Task:          task,
		SourceShardID: shardID,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSendTaskToDLQ, err)
	}
	metrics.DLQWrites.With(q.metricsHandler).Record(1, metrics.TaskCategoryTag(task.GetCategory().Name()))
	ns, err := q.namespaceRegistry.GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
	var namespaceTag tag.Tag
	if err != nil {
		q.logger.Warn("Failed to get namespace name while trying to write a task to DLQ",
			tag.WorkflowNamespace(task.GetNamespaceID()),
			tag.Error(err),
		)
		namespaceTag = tag.WorkflowNamespaceID(task.GetNamespaceID())
	} else {
		namespaceTag = tag.WorkflowNamespace(string(ns.Name()))
	}
	q.logger.Warn("Task enqueued to DLQ",
		tag.DLQMessageID(resp.Metadata.ID),
		tag.SourceCluster(sourceCluster),
		tag.TargetCluster(targetCluster),
		tag.TaskType(task.GetType()),
		namespaceTag,
	)
	return nil
}
