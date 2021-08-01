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
	"errors"
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
	clusterName string,
	metricsClient metrics.Client,
	logger log.Logger,
) (NamespaceReplicationQueue, error) {
	serializer := serialization.NewSerializer()

	blob, err := serializer.QueueMetadataToBlob(
		&persistence.QueueMetadata{
			ClusterAckLevels: make(map[string]int64),
		}, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	err = queue.Init(blob)
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
		Publish(message interface{}) error
		GetReplicationMessages(lastMessageID int64, maxCount int) ([]*replicationspb.ReplicationTask, int64, error)
		UpdateAckLevel(lastProcessedMessageID int64, clusterName string) error
		GetAckLevels() (map[string]int64, error)

		PublishToDLQ(message interface{}) error
		GetMessagesFromDLQ(firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) ([]*replicationspb.ReplicationTask, []byte, error)
		UpdateDLQAckLevel(lastProcessedMessageID int64) error
		GetDLQAckLevel() (int64, error)

		RangeDeleteMessagesFromDLQ(firstMessageID int64, lastMessageID int64) error
		DeleteMessageFromDLQ(messageID int64) error
	}
)

func (q *namespaceReplicationQueueImpl) Start() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go q.purgeProcessor()
}

func (q *namespaceReplicationQueueImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(q.done)
}

func (q *namespaceReplicationQueueImpl) Publish(message interface{}) error {
	task, ok := message.(*replicationspb.ReplicationTask)
	if !ok {
		return errors.New("wrong message type")
	}

	blob, err := q.serializer.ReplicationTaskToBlob(task, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return q.queue.EnqueueMessage(*blob)
}

func (q *namespaceReplicationQueueImpl) PublishToDLQ(message interface{}) error {
	task, ok := message.(*replicationspb.ReplicationTask)
	if !ok {
		return errors.New("wrong message type")
	}

	blob, err := q.serializer.ReplicationTaskToBlob(task, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	messageID, err := q.queue.EnqueueMessageToDLQ(*blob)
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
	lastMessageID int64,
	pageSize int,
) ([]*replicationspb.ReplicationTask, int64, error) {

	messages, err := q.queue.ReadMessages(lastMessageID, pageSize)
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
	lastProcessedMessageID int64,
	clusterName string,
) error {
	return q.updateAckLevelWithRetry(lastProcessedMessageID, clusterName, false)
}

func (q *namespaceReplicationQueueImpl) updateAckLevelWithRetry(
	lastProcessedMessageID int64,
	clusterName string,
	isDLQ bool,
) error {
conditionFailedRetry:
	for {
		err := q.updateAckLevel(lastProcessedMessageID, clusterName, isDLQ)
		switch err.(type) {
		case *ConditionFailedError:
			continue conditionFailedRetry
		}

		return err
	}
}

func (q *namespaceReplicationQueueImpl) updateAckLevel(
	lastProcessedMessageID int64,
	clusterName string,
	isDLQ bool,
) error {
	var err error
	var internalMetadata *InternalQueueMetadata
	if isDLQ {
		internalMetadata, err = q.queue.GetDLQAckLevels()
	} else {
		internalMetadata, err = q.queue.GetAckLevels()
	}

	if err != nil {
		return err
	}

	ackLevels, err := q.ackLevelsFromBlob(internalMetadata.Blob)

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
		err = q.queue.UpdateDLQAckLevel(internalMetadata)
	} else {
		err = q.queue.UpdateAckLevel(internalMetadata)
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

func (q *namespaceReplicationQueueImpl) GetAckLevels() (map[string]int64, error) {
	metadata, err := q.queue.GetAckLevels()
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
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {

	messages, token, err := q.queue.ReadMessagesFromDLQ(firstMessageID, lastMessageID, pageSize, pageToken)
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
	lastProcessedMessageID int64,
) error {
	return q.updateAckLevelWithRetry(lastProcessedMessageID, localNamespaceReplicationCluster, true)
}

func (q *namespaceReplicationQueueImpl) GetDLQAckLevel() (int64, error) {
	metadata, err := q.queue.GetDLQAckLevels()
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
	firstMessageID int64,
	lastMessageID int64,
) error {

	if err := q.queue.RangeDeleteMessagesFromDLQ(
		firstMessageID,
		lastMessageID,
	); err != nil {
		return err
	}

	return nil
}

func (q *namespaceReplicationQueueImpl) DeleteMessageFromDLQ(
	messageID int64,
) error {

	return q.queue.DeleteMessageFromDLQ(messageID)
}

func (q *namespaceReplicationQueueImpl) purgeAckedMessages() error {
	ackLevelByCluster, err := q.GetAckLevels()
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

	err = q.queue.DeleteMessagesBefore(*minAckLevel)
	if err != nil {
		return fmt.Errorf("failed to purge messages: %v", err)
	}
	q.metricsClient.
		Scope(metrics.PersistenceNamespaceReplicationQueueScope).
		UpdateGauge(metrics.NamespaceReplicationTaskAckLevelGauge, float64(*minAckLevel))
	return nil
}

func (q *namespaceReplicationQueueImpl) purgeProcessor() {
	ticker := time.NewTicker(purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.done:
			return
		case <-ticker.C:
			if q.ackLevelUpdated {
				err := q.purgeAckedMessages()
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
