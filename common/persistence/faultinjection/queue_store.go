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

package faultinjection

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/common/persistence"
)

type (
	FaultInjectionQueue struct {
		baseStore persistence.Queue
		generator faultGenerator
	}
)

func NewFaultInjectionQueue(
	baseStore persistence.Queue,
	generator faultGenerator,
) *FaultInjectionQueue {
	return &FaultInjectionQueue{
		baseStore: baseStore,
		generator: generator,
	}
}

func (q *FaultInjectionQueue) Close() {
	q.baseStore.Close()
}

func (q *FaultInjectionQueue) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	// potentially Init can return golang errors from blob.go encode/decode.
	return inject0(q.generator.generate(), func() error {
		return q.baseStore.Init(ctx, blob)
	})
}

func (q *FaultInjectionQueue) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	return inject0(q.generator.generate(), func() error {
		return q.baseStore.EnqueueMessage(ctx, blob)
	})
}

func (q *FaultInjectionQueue) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	return inject1(q.generator.generate(), func() ([]*persistence.QueueMessage, error) {
		return q.baseStore.ReadMessages(ctx, lastMessageID, maxCount)
	})
}

func (q *FaultInjectionQueue) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	return inject0(q.generator.generate(), func() error {
		return q.baseStore.DeleteMessagesBefore(ctx, messageID)
	})
}

func (q *FaultInjectionQueue) UpdateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return inject0(q.generator.generate(), func() error {
		return q.baseStore.UpdateAckLevel(ctx, metadata)
	})
}

func (q *FaultInjectionQueue) GetAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	return inject1(q.generator.generate(), func() (*persistence.InternalQueueMetadata, error) {
		return q.baseStore.GetAckLevels(ctx)
	})
}

func (q *FaultInjectionQueue) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (int64, error) {
	return inject1(q.generator.generate(), func() (int64, error) {
		return q.baseStore.EnqueueMessageToDLQ(ctx, blob)
	})
}

func (q *FaultInjectionQueue) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	return inject2(q.generator.generate(), func() ([]*persistence.QueueMessage, []byte, error) {
		return q.baseStore.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	})
}

func (q *FaultInjectionQueue) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	return inject0(q.generator.generate(), func() error {
		return q.baseStore.DeleteMessageFromDLQ(ctx, messageID)
	})
}

func (q *FaultInjectionQueue) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	return inject0(q.generator.generate(), func() error {
		return q.baseStore.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	})
}

func (q *FaultInjectionQueue) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return inject0(q.generator.generate(), func() error {
		return q.baseStore.UpdateDLQAckLevel(ctx, metadata)
	})
}

func (q *FaultInjectionQueue) GetDLQAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	return inject1(q.generator.generate(), func() (*persistence.InternalQueueMetadata, error) {
		return q.baseStore.GetDLQAckLevels(ctx)
	})
}
