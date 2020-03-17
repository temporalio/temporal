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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination domainReplicationQueue_mock.go -self_package github.com/temporalio/temporal/common/persistence

package persistence

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
)

const (
	purgeInterval                 = 5 * time.Minute
	emptyMessageID                = -1
	localDomainReplicationCluster = "domainReplication"
)

var _ DomainReplicationQueue = (*domainReplicationQueueImpl)(nil)

// NewDomainReplicationQueue creates a new DomainReplicationQueue instance
func NewDomainReplicationQueue(
	queue Queue,
	clusterName string,
	metricsClient metrics.Client,
	logger log.Logger,
) DomainReplicationQueue {
	return &domainReplicationQueueImpl{
		queue:               queue,
		clusterName:         clusterName,
		metricsClient:       metricsClient,
		logger:              logger,
		ackNotificationChan: make(chan bool),
		done:                make(chan bool),
		status:              common.DaemonStatusInitialized,
	}
}

type (
	domainReplicationQueueImpl struct {
		queue               Queue
		clusterName         string
		metricsClient       metrics.Client
		logger              log.Logger
		ackLevelUpdated     bool
		ackNotificationChan chan bool
		done                chan bool
		status              int32
	}

	// DomainReplicationQueue is used to publish and list domain replication tasks
	DomainReplicationQueue interface {
		common.Daemon
		Publish(message interface{}) error
		PublishToDLQ(message interface{}) error
		GetReplicationMessages(lastMessageID int, maxCount int) ([]*replication.ReplicationTask, int, error)
		UpdateAckLevel(lastProcessedMessageID int, clusterName string) error
		GetAckLevels() (map[string]int, error)
		GetMessagesFromDLQ(firstMessageID int, lastMessageID int, pageSize int, pageToken []byte) ([]*replication.ReplicationTask, []byte, error)
		UpdateDLQAckLevel(lastProcessedMessageID int) error
		GetDLQAckLevel() (int, error)
		RangeDeleteMessagesFromDLQ(firstMessageID int, lastMessageID int) error
		DeleteMessageFromDLQ(messageID int) error
	}
)

func (q *domainReplicationQueueImpl) Start() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go q.purgeProcessor()
}

func (q *domainReplicationQueueImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	close(q.done)
}

func (q *domainReplicationQueueImpl) Publish(message interface{}) error {
	task, ok := message.(*replication.ReplicationTask)
	if !ok {
		return errors.New("wrong message type")
	}

	bytes, err := task.Marshal()
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return q.queue.EnqueueMessage(bytes)
}

func (q *domainReplicationQueueImpl) PublishToDLQ(message interface{}) error {
	task, ok := message.(*replication.ReplicationTask)
	if !ok {
		return errors.New("wrong message type")
	}

	bytes, err := task.Marshal()
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	messageID, err := q.queue.EnqueueMessageToDLQ(bytes)
	if err != nil {
		return err
	}

	q.metricsClient.Scope(
		metrics.PersistenceDomainReplicationQueueScope,
	).UpdateGauge(
		metrics.DomainReplicationDLQMaxLevelGauge,
		float64(messageID),
	)
	return nil
}

func (q *domainReplicationQueueImpl) GetReplicationMessages(
	lastMessageID int,
	maxCount int,
) ([]*replication.ReplicationTask, int, error) {

	messages, err := q.queue.ReadMessages(lastMessageID, maxCount)
	if err != nil {
		return nil, lastMessageID, err
	}

	var replicationTasks []*replication.ReplicationTask
	for _, message := range messages {
		replicationTask := &replication.ReplicationTask{}
		err := replicationTask.Unmarshal(message.Payload)
		if err != nil {
			return nil, lastMessageID, fmt.Errorf("failed to decode task: %v", err)
		}

		lastMessageID = message.ID
		replicationTasks = append(replicationTasks, replicationTask)
	}

	return replicationTasks, lastMessageID, nil
}

func (q *domainReplicationQueueImpl) UpdateAckLevel(
	lastProcessedMessageID int,
	clusterName string,
) error {

	err := q.queue.UpdateAckLevel(lastProcessedMessageID, clusterName)
	if err != nil {
		return fmt.Errorf("failed to update ack level: %v", err)
	}

	select {
	case q.ackNotificationChan <- true:
	default:
	}

	return nil
}

func (q *domainReplicationQueueImpl) GetAckLevels() (map[string]int, error) {
	return q.queue.GetAckLevels()
}

func (q *domainReplicationQueueImpl) GetMessagesFromDLQ(
	firstMessageID int,
	lastMessageID int,
	pageSize int,
	pageToken []byte,
) ([]*replication.ReplicationTask, []byte, error) {

	messages, token, err := q.queue.ReadMessagesFromDLQ(firstMessageID, lastMessageID, pageSize, pageToken)
	if err != nil {
		return nil, nil, err
	}

	var replicationTasks []*replication.ReplicationTask
	for _, message := range messages {
		replicationTask := &replication.ReplicationTask{}
		err := replicationTask.Unmarshal(message.Payload)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode dlq task: %v", err)
		}

		// Overwrite to local cluster message id
		replicationTask.SourceTaskId = int64(message.ID)
		replicationTasks = append(replicationTasks, replicationTask)
	}

	return replicationTasks, token, nil
}

func (q *domainReplicationQueueImpl) UpdateDLQAckLevel(
	lastProcessedMessageID int,
) error {

	if err := q.queue.UpdateDLQAckLevel(
		lastProcessedMessageID,
		localDomainReplicationCluster,
	); err != nil {
		return err
	}

	q.metricsClient.Scope(
		metrics.PersistenceDomainReplicationQueueScope,
	).UpdateGauge(
		metrics.DomainReplicationDLQAckLevelGauge,
		float64(lastProcessedMessageID),
	)
	return nil
}

func (q *domainReplicationQueueImpl) GetDLQAckLevel() (int, error) {
	dlqMetadata, err := q.queue.GetDLQAckLevels()
	if err != nil {
		return emptyMessageID, err
	}

	ackLevel, ok := dlqMetadata[localDomainReplicationCluster]
	if !ok {
		return emptyMessageID, nil
	}
	return ackLevel, nil
}

func (q *domainReplicationQueueImpl) RangeDeleteMessagesFromDLQ(
	firstMessageID int,
	lastMessageID int,
) error {

	if err := q.queue.RangeDeleteMessagesFromDLQ(
		firstMessageID,
		lastMessageID,
	); err != nil {
		return err
	}

	return nil
}

func (q *domainReplicationQueueImpl) DeleteMessageFromDLQ(
	messageID int,
) error {

	return q.queue.DeleteMessageFromDLQ(messageID)
}

func (q *domainReplicationQueueImpl) purgeAckedMessages() error {
	ackLevelByCluster, err := q.GetAckLevels()
	if err != nil {
		return fmt.Errorf("failed to purge messages: %v", err)
	}

	if len(ackLevelByCluster) == 0 {
		return nil
	}

	minAckLevel := math.MaxInt64
	for _, ackLevel := range ackLevelByCluster {
		if ackLevel < minAckLevel {
			minAckLevel = ackLevel
		}
	}

	err = q.queue.DeleteMessagesBefore(minAckLevel)
	if err != nil {
		return fmt.Errorf("failed to purge messages: %v", err)
	}

	q.metricsClient.
		Scope(metrics.PersistenceDomainReplicationQueueScope).
		UpdateGauge(metrics.DomainReplicationTaskAckLevelGauge, float64(minAckLevel))
	return nil
}

func (q *domainReplicationQueueImpl) purgeProcessor() {
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
					q.logger.Warn("Failed to purge acked domain replication messages.", tag.Error(err))
				} else {
					q.ackLevelUpdated = false
				}
			}
		case <-q.ackNotificationChan:
			q.ackLevelUpdated = true
		}
	}
}
