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

// Code generated by gowrap. DO NOT EDIT.
// template: gowrap_template
// gowrap: http://github.com/hexdigest/gowrap

package faultinjection

//go:generate gowrap gen -p go.temporal.io/server/common/persistence -i Queue -t gowrap_template -o queue_gen.go -l ""

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	_sourcePersistence "go.temporal.io/server/common/persistence"
)

type (
	// faultInjectionQueue implements Queue interface with fault injection.
	faultInjectionQueue struct {
		_sourcePersistence.Queue
		generator faultGenerator
	}
)

// newFaultInjectionQueue returns faultInjectionQueue.
func newFaultInjectionQueue(
	baseStore _sourcePersistence.Queue,
	generator faultGenerator,
) *faultInjectionQueue {
	return &faultInjectionQueue{
		Queue:     baseStore,
		generator: generator,
	}
}

// DeleteMessageFromDLQ wraps Queue.DeleteMessageFromDLQ.
func (d faultInjectionQueue) DeleteMessageFromDLQ(ctx context.Context, messageID int64) (err error) {
	err = d.generator.generate("DeleteMessageFromDLQ").inject(func() error {
		err = d.Queue.DeleteMessageFromDLQ(ctx, messageID)
		return err
	})
	return
}

// DeleteMessagesBefore wraps Queue.DeleteMessagesBefore.
func (d faultInjectionQueue) DeleteMessagesBefore(ctx context.Context, messageID int64) (err error) {
	err = d.generator.generate("DeleteMessagesBefore").inject(func() error {
		err = d.Queue.DeleteMessagesBefore(ctx, messageID)
		return err
	})
	return
}

// EnqueueMessage wraps Queue.EnqueueMessage.
func (d faultInjectionQueue) EnqueueMessage(ctx context.Context, blob *commonpb.DataBlob) (err error) {
	err = d.generator.generate("EnqueueMessage").inject(func() error {
		err = d.Queue.EnqueueMessage(ctx, blob)
		return err
	})
	return
}

// EnqueueMessageToDLQ wraps Queue.EnqueueMessageToDLQ.
func (d faultInjectionQueue) EnqueueMessageToDLQ(ctx context.Context, blob *commonpb.DataBlob) (i1 int64, err error) {
	err = d.generator.generate("EnqueueMessageToDLQ").inject(func() error {
		i1, err = d.Queue.EnqueueMessageToDLQ(ctx, blob)
		return err
	})
	return
}

// Init wraps Queue.Init.
func (d faultInjectionQueue) Init(ctx context.Context, blob *commonpb.DataBlob) (err error) {
	err = d.generator.generate("Init").inject(func() error {
		err = d.Queue.Init(ctx, blob)
		return err
	})
	return
}

// RangeDeleteMessagesFromDLQ wraps Queue.RangeDeleteMessagesFromDLQ.
func (d faultInjectionQueue) RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64) (err error) {
	err = d.generator.generate("RangeDeleteMessagesFromDLQ").inject(func() error {
		err = d.Queue.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
		return err
	})
	return
}

// ReadMessages wraps Queue.ReadMessages.
func (d faultInjectionQueue) ReadMessages(ctx context.Context, lastMessageID int64, maxCount int) (qpa1 []*_sourcePersistence.QueueMessage, err error) {
	err = d.generator.generate("ReadMessages").inject(func() error {
		qpa1, err = d.Queue.ReadMessages(ctx, lastMessageID, maxCount)
		return err
	})
	return
}

// ReadMessagesFromDLQ wraps Queue.ReadMessagesFromDLQ.
func (d faultInjectionQueue) ReadMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) (qpa1 []*_sourcePersistence.QueueMessage, ba1 []byte, err error) {
	err = d.generator.generate("ReadMessagesFromDLQ").inject(func() error {
		qpa1, ba1, err = d.Queue.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
		return err
	})
	return
}

// UpdateAckLevel wraps Queue.UpdateAckLevel.
func (d faultInjectionQueue) UpdateAckLevel(ctx context.Context, metadata *_sourcePersistence.InternalQueueMetadata) (err error) {
	err = d.generator.generate("UpdateAckLevel").inject(func() error {
		err = d.Queue.UpdateAckLevel(ctx, metadata)
		return err
	})
	return
}

// UpdateDLQAckLevel wraps Queue.UpdateDLQAckLevel.
func (d faultInjectionQueue) UpdateDLQAckLevel(ctx context.Context, metadata *_sourcePersistence.InternalQueueMetadata) (err error) {
	err = d.generator.generate("UpdateDLQAckLevel").inject(func() error {
		err = d.Queue.UpdateDLQAckLevel(ctx, metadata)
		return err
	})
	return
}
