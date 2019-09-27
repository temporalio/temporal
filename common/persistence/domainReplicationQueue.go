// Copyright (c) 2017 Uber Technologies, Inc.
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

package persistence

import (
	"errors"
	"fmt"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/common/codec"
)

var _ DomainReplicationQueue = (*domainReplicationQueueImpl)(nil)

// NewDomainReplicationQueue creates a new DomainReplicationQueue instance
func NewDomainReplicationQueue(queue Queue) DomainReplicationQueue {
	return &domainReplicationQueueImpl{
		queue:   queue,
		encoder: codec.NewThriftRWEncoder(),
	}
}

type (
	domainReplicationQueueImpl struct {
		queue   Queue
		encoder codec.BinaryEncoder
	}
)

func (q *domainReplicationQueueImpl) Publish(message interface{}) error {
	task, ok := message.(*replicator.ReplicationTask)
	if !ok {
		return errors.New("wrong message type")
	}

	bytes, err := q.encoder.Encode(task)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return q.queue.EnqueueMessage(bytes)
}

func (q *domainReplicationQueueImpl) GetReplicationMessages(
	lastMessageID int,
	maxCount int,
) ([]*replicator.ReplicationTask, int, error) {
	messages, err := q.queue.DequeueMessages(lastMessageID, maxCount)
	if err != nil {
		return nil, lastMessageID, err
	}

	var replicationTasks []*replicator.ReplicationTask
	for _, message := range messages {
		var replicationTask replicator.ReplicationTask
		err := q.encoder.Decode(message.Payload, &replicationTask)
		if err != nil {
			return nil, lastMessageID, fmt.Errorf("failed to decode task: %v", err)
		}

		lastMessageID = message.ID
		replicationTasks = append(replicationTasks, &replicationTask)
	}

	return replicationTasks, lastMessageID, nil
}
