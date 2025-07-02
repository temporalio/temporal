//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination namespace_replication_queue_mock.go

package persistence

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	localNamespaceReplicationCluster = "namespaceReplication"
)

var _ NamespaceReplicationQueue = (*namespaceReplicationQueueImpl)(nil)

// NewNamespaceReplicationQueue creates a new NamespaceReplicationQueue instance
func NewNamespaceReplicationQueue(
	queue Queue,
	serializer serialization.Serializer,
	clusterName string,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (NamespaceReplicationQueue, error) {

	blob, err := serializer.QueueMetadataToBlob(
		&persistencespb.QueueMetadata{
			ClusterAckLevels: make(map[string]int64),
		})
	if err != nil {
		return nil, err
	}
	err = queue.Init(context.TODO(), blob)
	if err != nil {
		return nil, err
	}

	return &namespaceReplicationQueueImpl{
		queue:          queue,
		clusterName:    clusterName,
		metricsHandler: metricsHandler,
		logger:         logger,
		serializer:     serializer,
	}, nil
}

type (
	namespaceReplicationQueueImpl struct {
		queue          Queue
		clusterName    string
		metricsHandler metrics.Handler
		logger         log.Logger
		serializer     serialization.Serializer
	}

	// NamespaceReplicationQueue is used to publish and list namespace replication tasks
	NamespaceReplicationQueue interface {
		Closeable
		Publish(ctx context.Context, task *replicationspb.ReplicationTask) error
		GetReplicationMessages(
			ctx context.Context,
			lastMessageID int64,
			maxCount int,
		) ([]*replicationspb.ReplicationTask, int64, error)
		UpdateAckLevel(ctx context.Context, lastProcessedMessageID int64, clusterName string) error
		GetAckLevels(ctx context.Context) (map[string]int64, error)
		DeleteMessagesBefore(ctx context.Context, exclusiveMessageID int64) error

		PublishToDLQ(ctx context.Context, task *replicationspb.ReplicationTask) error
		GetMessagesFromDLQ(
			ctx context.Context,
			firstMessageID int64,
			lastMessageID int64,
			pageSize int,
			pageToken []byte,
		) ([]*replicationspb.ReplicationTask, []byte, error)
		UpdateDLQAckLevel(ctx context.Context, lastProcessedMessageID int64) error
		GetDLQAckLevel(ctx context.Context) (int64, error)

		RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64) error
		DeleteMessageFromDLQ(ctx context.Context, messageID int64) error
	}
)

func (q *namespaceReplicationQueueImpl) Close() {
	q.queue.Close()
}

func (q *namespaceReplicationQueueImpl) Publish(ctx context.Context, task *replicationspb.ReplicationTask) error {
	blob, err := q.serializer.ReplicationTaskToBlob(task)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return q.queue.EnqueueMessage(ctx, blob)
}

func (q *namespaceReplicationQueueImpl) PublishToDLQ(ctx context.Context, task *replicationspb.ReplicationTask) error {
	blob, err := q.serializer.ReplicationTaskToBlob(task)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	messageID, err := q.queue.EnqueueMessageToDLQ(ctx, blob)
	if err != nil {
		return err
	}

	metrics.NamespaceReplicationDLQMaxLevelGauge.With(q.metricsHandler).
		Record(float64(messageID), metrics.OperationTag(metrics.PersistenceNamespaceReplicationQueueScope))
	return nil
}

func (q *namespaceReplicationQueueImpl) GetReplicationMessages(
	ctx context.Context,
	lastMessageID int64,
	pageSize int,
) ([]*replicationspb.ReplicationTask, int64, error) {

	messages, err := q.queue.ReadMessages(ctx, lastMessageID, pageSize)
	if err != nil {
		return nil, lastMessageID, err
	}

	replicationTasks := make([]*replicationspb.ReplicationTask, 0, len(messages))
	for _, message := range messages {
		replicationTask, err := q.serializer.ReplicationTaskFromBlob(NewDataBlob(message.Data, message.Encoding))
		if err != nil {
			return nil, lastMessageID, fmt.Errorf("failed to decode task: %v", err)
		}

		lastMessageID = message.ID
		replicationTasks = append(replicationTasks, replicationTask)
	}

	return replicationTasks, lastMessageID, nil
}

func (q *namespaceReplicationQueueImpl) UpdateAckLevel(
	ctx context.Context,
	lastProcessedMessageID int64,
	clusterName string,
) error {
	return q.updateAckLevelWithRetry(ctx, lastProcessedMessageID, clusterName, false)
}

func (q *namespaceReplicationQueueImpl) DeleteMessagesBefore(
	ctx context.Context,
	exclusiveMessageID int64,
) error {
	err := q.queue.DeleteMessagesBefore(ctx, exclusiveMessageID)
	if err != nil {
		return err
	}
	metrics.NamespaceReplicationTaskAckLevelGauge.With(q.metricsHandler).
		Record(float64(exclusiveMessageID), metrics.OperationTag(metrics.PersistenceNamespaceReplicationQueueScope))
	return nil
}

func (q *namespaceReplicationQueueImpl) updateAckLevelWithRetry(
	ctx context.Context,
	lastProcessedMessageID int64,
	clusterName string,
	isDLQ bool,
) error {
conditionFailedRetry:
	for {
		err := q.updateAckLevel(ctx, lastProcessedMessageID, clusterName, isDLQ)
		switch err.(type) {
		case *ConditionFailedError:
			continue conditionFailedRetry
		}

		return err
	}
}

func (q *namespaceReplicationQueueImpl) updateAckLevel(
	ctx context.Context,
	lastProcessedMessageID int64,
	clusterName string,
	isDLQ bool,
) error {
	var ackLevelErr error
	var internalMetadata *InternalQueueMetadata
	if isDLQ {
		internalMetadata, ackLevelErr = q.queue.GetDLQAckLevels(ctx)
	} else {
		internalMetadata, ackLevelErr = q.queue.GetAckLevels(ctx)
	}

	if ackLevelErr != nil {
		return ackLevelErr
	}

	ackLevels, err := q.ackLevelsFromBlob(internalMetadata.Blob)
	if err != nil {
		return err
	}

	// Ignore possibly delayed message
	if ack, ok := ackLevels[clusterName]; ok && ack >= lastProcessedMessageID {
		return nil
	}

	// update ack level
	ackLevels[clusterName] = lastProcessedMessageID
	blob, err := q.serializer.QueueMetadataToBlob(&persistencespb.QueueMetadata{
		ClusterAckLevels: ackLevels,
	})
	if err != nil {
		return err
	}

	internalMetadata.Blob = blob
	if isDLQ {
		err = q.queue.UpdateDLQAckLevel(ctx, internalMetadata)
	} else {
		err = q.queue.UpdateAckLevel(ctx, internalMetadata)
	}
	if err != nil {
		return fmt.Errorf("failed to update ack level: %v", err)
	}

	return nil
}

func (q *namespaceReplicationQueueImpl) GetAckLevels(
	ctx context.Context,
) (map[string]int64, error) {
	metadata, err := q.queue.GetAckLevels(ctx)
	if err != nil {
		return nil, err
	}
	return q.ackLevelsFromBlob(metadata.Blob)
}

func (q *namespaceReplicationQueueImpl) ackLevelsFromBlob(blob *commonpb.DataBlob) (map[string]int64, error) {
	if blob == nil {
		return make(map[string]int64), nil
	}

	metadata, err := q.serializer.QueueMetadataFromBlob(blob)
	if err != nil {
		return nil, err
	}
	ackLevels := metadata.ClusterAckLevels
	if ackLevels == nil {
		ackLevels = make(map[string]int64)
	}
	return ackLevels, nil
}

func (q *namespaceReplicationQueueImpl) GetMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {

	messages, token, err := q.queue.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	if err != nil {
		return nil, nil, err
	}

	var replicationTasks []*replicationspb.ReplicationTask
	for _, message := range messages {
		replicationTask, err := q.serializer.ReplicationTaskFromBlob(NewDataBlob(message.Data, message.Encoding))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode dlq task: %v", err)
		}

		// Overwrite to local cluster message id
		replicationTask.SourceTaskId = message.ID
		replicationTasks = append(replicationTasks, replicationTask)
	}

	return replicationTasks, token, nil
}

func (q *namespaceReplicationQueueImpl) UpdateDLQAckLevel(
	ctx context.Context,
	lastProcessedMessageID int64,
) error {
	return q.updateAckLevelWithRetry(ctx, lastProcessedMessageID, localNamespaceReplicationCluster, true)
}

func (q *namespaceReplicationQueueImpl) GetDLQAckLevel(
	ctx context.Context,
) (int64, error) {
	metadata, err := q.queue.GetDLQAckLevels(ctx)
	if err != nil {
		return EmptyQueueMessageID, err
	}
	dlqMetadata, err := q.ackLevelsFromBlob(metadata.Blob)
	if err != nil {
		return EmptyQueueMessageID, err
	}

	ackLevel, ok := dlqMetadata[localNamespaceReplicationCluster]
	if !ok {
		return EmptyQueueMessageID, nil
	}
	return ackLevel, nil
}

func (q *namespaceReplicationQueueImpl) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {

	return q.queue.RangeDeleteMessagesFromDLQ(
		ctx,
		firstMessageID,
		lastMessageID,
	)
}

func (q *namespaceReplicationQueueImpl) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {

	return q.queue.DeleteMessageFromDLQ(ctx, messageID)
}
