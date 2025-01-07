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

package telemetry

//go:generate gowrap gen -p go.temporal.io/server/common/persistence -i Queue -t gowrap_template -o queue_gen.go -l ""

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	_sourcePersistence "go.temporal.io/server/common/persistence"
)

// telemetryQueue implements Queue interface instrumented with OpenTelemetry.
type telemetryQueue struct {
	_sourcePersistence.Queue
	tracer trace.Tracer
}

// newTelemetryQueue returns telemetryQueue.
func newTelemetryQueue(base _sourcePersistence.Queue, tracer trace.Tracer) telemetryQueue {
	return telemetryQueue{
		Queue:  base,
		tracer: tracer,
	}
}

// DeleteMessageFromDLQ wraps Queue.DeleteMessageFromDLQ.
func (_d telemetryQueue) DeleteMessageFromDLQ(ctx context.Context, messageID int64) (err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/DeleteMessageFromDLQ")
	defer span.End()

	err = _d.Queue.DeleteMessageFromDLQ(ctx, messageID)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// DeleteMessagesBefore wraps Queue.DeleteMessagesBefore.
func (_d telemetryQueue) DeleteMessagesBefore(ctx context.Context, messageID int64) (err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/DeleteMessagesBefore")
	defer span.End()

	err = _d.Queue.DeleteMessagesBefore(ctx, messageID)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// EnqueueMessage wraps Queue.EnqueueMessage.
func (_d telemetryQueue) EnqueueMessage(ctx context.Context, blob *commonpb.DataBlob) (err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/EnqueueMessage")
	defer span.End()

	err = _d.Queue.EnqueueMessage(ctx, blob)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// EnqueueMessageToDLQ wraps Queue.EnqueueMessageToDLQ.
func (_d telemetryQueue) EnqueueMessageToDLQ(ctx context.Context, blob *commonpb.DataBlob) (i1 int64, err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/EnqueueMessageToDLQ")
	defer span.End()

	i1, err = _d.Queue.EnqueueMessageToDLQ(ctx, blob)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// GetAckLevels wraps Queue.GetAckLevels.
func (_d telemetryQueue) GetAckLevels(ctx context.Context) (ip1 *_sourcePersistence.InternalQueueMetadata, err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/GetAckLevels")
	defer span.End()

	ip1, err = _d.Queue.GetAckLevels(ctx)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// GetDLQAckLevels wraps Queue.GetDLQAckLevels.
func (_d telemetryQueue) GetDLQAckLevels(ctx context.Context) (ip1 *_sourcePersistence.InternalQueueMetadata, err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/GetDLQAckLevels")
	defer span.End()

	ip1, err = _d.Queue.GetDLQAckLevels(ctx)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// Init wraps Queue.Init.
func (_d telemetryQueue) Init(ctx context.Context, blob *commonpb.DataBlob) (err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/Init")
	defer span.End()

	err = _d.Queue.Init(ctx, blob)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// RangeDeleteMessagesFromDLQ wraps Queue.RangeDeleteMessagesFromDLQ.
func (_d telemetryQueue) RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64) (err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/RangeDeleteMessagesFromDLQ")
	defer span.End()

	err = _d.Queue.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// ReadMessages wraps Queue.ReadMessages.
func (_d telemetryQueue) ReadMessages(ctx context.Context, lastMessageID int64, maxCount int) (qpa1 []*_sourcePersistence.QueueMessage, err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/ReadMessages")
	defer span.End()

	qpa1, err = _d.Queue.ReadMessages(ctx, lastMessageID, maxCount)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// ReadMessagesFromDLQ wraps Queue.ReadMessagesFromDLQ.
func (_d telemetryQueue) ReadMessagesFromDLQ(ctx context.Context, firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) (qpa1 []*_sourcePersistence.QueueMessage, ba1 []byte, err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/ReadMessagesFromDLQ")
	defer span.End()

	qpa1, ba1, err = _d.Queue.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// UpdateAckLevel wraps Queue.UpdateAckLevel.
func (_d telemetryQueue) UpdateAckLevel(ctx context.Context, metadata *_sourcePersistence.InternalQueueMetadata) (err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/UpdateAckLevel")
	defer span.End()

	err = _d.Queue.UpdateAckLevel(ctx, metadata)
	if err != nil {
		span.RecordError(err)
	}

	return
}

// UpdateDLQAckLevel wraps Queue.UpdateDLQAckLevel.
func (_d telemetryQueue) UpdateDLQAckLevel(ctx context.Context, metadata *_sourcePersistence.InternalQueueMetadata) (err error) {
	ctx, span := _d.tracer.Start(ctx, "persistence.Queue/UpdateDLQAckLevel")
	defer span.End()

	err = _d.Queue.UpdateDLQAckLevel(ctx, metadata)
	if err != nil {
		span.RecordError(err)
	}

	return
}
