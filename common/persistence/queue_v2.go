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

package persistence

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
)

const (
	QueueTypeUnspecified   QueueV2Type = 0
	QueueTypeHistoryNormal QueueV2Type = 1
	QueueTypeHistoryDLQ    QueueV2Type = 2

	// FirstQueueMessageID is the ID of the first message written to a queue partition.
	FirstQueueMessageID = 0
)

var (
	ErrInvalidReadQueueMessagesNextPageToken = &InvalidPersistenceRequestError{
		Msg: "invalid next-page token for reading queue messages",
	}
	ErrInvalidListQueuesNextPageToken = &InvalidPersistenceRequestError{
		Msg: "invalid next-page token for listing queues",
	}
	ErrNonPositiveReadQueueMessagesPageSize = &InvalidPersistenceRequestError{
		Msg: "non-positive page size for reading queue messages",
	}
	ErrInvalidQueueRangeDeleteMaxMessageID = &InvalidPersistenceRequestError{
		Msg: "max message id for queue range delete is invalid",
	}
	ErrNonPositiveListQueuesPageSize = &InvalidPersistenceRequestError{
		Msg: "non-positive page size for listing queues",
	}
	ErrNegativeListQueuesOffset = &InvalidPersistenceRequestError{
		Msg: "negative offset for listing queues",
	}
)

func NewQueueNotFoundError(queueType QueueV2Type, queueName string) error {
	return serviceerror.NewNotFound(fmt.Sprintf(
		"queue not found: type = %v and name = %v",
		queueType,
		queueName,
	))
}
