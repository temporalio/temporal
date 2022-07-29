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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination namespaceReplicationQueue_mock.go

package persistence

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/persistence/v1"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	purgeInterval                    = 5 * time.Minute
	localNamespaceReplicationCluster = "namespaceReplication"
)

var _ NamespaceReplicationQueue = (*namespaceReplicationQueueImpl)(nil)

// NewNamespaceReplicationQueue creates a new NamespaceReplicationQueue instance
func NewNamespaceReplicationQueue(
	queue Queue,
	serializer serialization.Serializer,
	clusterName string,
	metricsClient metrics.Client,
	logger log.Logger,
) (NamespaceReplicationQueue, error) {

	blob, err := serializer.QueueMetadataToBlob(
		&persistence.QueueMetadata{
			ClusterAckLevels: make(map[string]int64),
		}, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	err = queue.Init(context.TODO(), blob)
	if err != nil {
		return nil, err
	}

	return &namespaceReplicationQueueImpl{
		queue:               queue,
		clusterName:         clusterName,
		metricsClient:       metricsClient,
		logger:              logger,
		ackNotificationChan: make(chan bool),
		done:                make(chan bool),
		status:              common.DaemonStatusInitialized,
		serializer:          serializer,
	}, nil
}

type (
	namespaceReplicationQueueImpl struct {
		queue               Queue
		clusterName         string
		metricsClient       metrics.Client
		logger              log.Logger
		ackLevelUpdated     bool
		ackNotificationChan chan bool
		done                chan bool
		status              int32
		serializer          serialization.Serializer
	}

	// NamespaceReplicationQueue is used to publish and list namespace replication tasks
	NamespaceReplicationQueue interface {
		common.Daemon
		Publish(ctx context.Context, task *replicationspb.ReplicationTask) error
		GetReplicationMessages(
			ctx context.Context,
			lastMessageID int64,
			maxCount int,
		) ([]*replicationspb.ReplicationTask, int64, error)
		UpdateAckLevel(ctx context.Context, lastProcessedMessageID int64, clusterName string) error
		GetAckLevels(ctx context.Context) (map[string]int64, error)

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

func (q *namespaceReplicationQueueImpl) Start() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go q.purgeProcessor(context.TODO())
}

func (q *namespaceReplicationQueueImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(q.done)
}

func (q *namespaceReplicationQueueImpl) Publish(ctx context.Context, task *replicationspb.ReplicationTask) error {
	blob, err := q.serializer.ReplicationTaskToBlob(task, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return q.queue.EnqueueMessage(ctx, *blob)
}

func (q *namespaceReplicationQueueImpl) PublishToDLQ(ctx context.Context, task *replicationspb.ReplicationTask) error {
	blob, err := q.serializer.ReplicationTaskToBlob(task, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	messageID, err := q.queue.EnqueueMessageToDLQ(ctx, *blob)
	if err != nil {
		return err
	}

	q.metricsClient.Scope(
		metrics.PersistenceNamespaceReplicationQueueScope,
	).UpdateGauge(
		metrics.NamespaceReplicationDLQMaxLevelGauge,
		float64(messageID),
	)
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
	if ack, ok := ackLevels[clusterName]; ok && ack > lastProcessedMessageID {
		return nil
	}

	// TODO remove this block in 1.12.x
	delete(ackLevels, "")
	// TODO remove this block in 1.12.x

	// update ack level
	ackLevels[clusterName] = lastProcessedMessageID
	blob, err := q.serializer.QueueMetadataToBlob(&persistence.QueueMetadata{
		ClusterAckLevels: ackLevels,
	}, enumspb.ENCODING_TYPE_PROTO3)
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

	select {
	case q.ackNotificationChan <- true:
	default:
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

	if err := q.queue.RangeDeleteMessagesFromDLQ(
		ctx,
		firstMessageID,
		lastMessageID,
	); err != nil {
		return err
	}

	return nil
}

func (q *namespaceReplicationQueueImpl) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {

	return q.queue.DeleteMessageFromDLQ(ctx, messageID)
}

func (q *namespaceReplicationQueueImpl) purgeAckedMessages(
	ctx context.Context,
) error {
	ackLevelByCluster, err := q.GetAckLevels(ctx)
	if err != nil {
		return fmt.Errorf("failed to purge messages: %v", err)
	}

	if len(ackLevelByCluster) == 0 {
		return nil
	}

	var minAckLevel *int64
	for _, ackLevel := range ackLevelByCluster {
		if minAckLevel == nil || ackLevel < *minAckLevel {
			minAckLevel = convert.Int64Ptr(ackLevel)
		}
	}
	if minAckLevel == nil {
		return nil
	}

	err = q.queue.DeleteMessagesBefore(ctx, *minAckLevel)
	if err != nil {
		return fmt.Errorf("failed to purge messages: %v", err)
	}
	q.metricsClient.
		Scope(metrics.PersistenceNamespaceReplicationQueueScope).
		UpdateGauge(metrics.NamespaceReplicationTaskAckLevelGauge, float64(*minAckLevel))
	return nil
}

func (q *namespaceReplicationQueueImpl) purgeProcessor(
	ctx context.Context,
) {
	ticker := time.NewTicker(purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.done:
			return
		case <-ticker.C:
			if q.ackLevelUpdated {
				err := q.purgeAckedMessages(ctx)
				if err != nil {
					q.logger.Warn("Failed to purge acked namespace replication messages.", tag.Error(err))
				} else {
					q.ackLevelUpdated = false
				}
			}
		case <-q.ackNotificationChan:
			q.ackLevelUpdated = true
		}
	}
}
