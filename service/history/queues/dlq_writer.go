package queues

import (
	"context"
	"errors"
	"fmt"

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
	ErrSendTaskToDLQ = errors.New("failed to send task to DLQ")
	ErrCreateDLQ     = errors.New("failed to create DLQ")
)

// NewDLQWriter returns a DLQ which will write to the given QueueWriter.
func NewDLQWriter(
	w QueueWriter,
	h metrics.Handler,
	l log.SnTaggedLogger,
	r namespace.Registry,
) *DLQWriter {
	return &DLQWriter{
		dlqWriter:         w,
		metricsHandler:    h,
		logger:            l,
		namespaceRegistry: r,
	}
}

// WriteTaskToDLQ writes a task to the DLQ, creating the underlying queue if it doesn't already exist.
func (q *DLQWriter) WriteTaskToDLQ(
	ctx context.Context,
	sourceCluster, targetCluster string,
	sourceShardID int,
	task tasks.Task,
	isNamespaceActive bool,
) error {
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

	resp, err := q.dlqWriter.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     queueKey.QueueType,
		SourceCluster: queueKey.SourceCluster,
		TargetCluster: queueKey.TargetCluster,
		Task:          task,
		SourceShardID: sourceShardID,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSendTaskToDLQ, err)
	}
	// "passive" means the namespace is in standby mode and only replicates data
	namespaceState := metrics.PassiveNamespaceStateTagValue
	if isNamespaceActive {
		namespaceState = metrics.ActiveNamespaceStateTagValue
	}
	metrics.DLQWrites.With(q.metricsHandler).Record(
		1,
		metrics.TaskCategoryTag(task.GetCategory().Name()),
		metrics.NamespaceStateTag(namespaceState),
	)
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
