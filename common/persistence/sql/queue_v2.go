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

package sql

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/common/persistence"
)

type (
	queueV2 struct{}
)

var (
	ErrNotImplemented = errors.New("method is not implemented yet")
)

// NewQueueV2 returns an implementation of [persistence.QueueV2] which always returns an error because it is not
// implemented yet.
func NewQueueV2() persistence.QueueV2 {
	return &queueV2{}
}

func (q queueV2) EnqueueMessage(
	context.Context,
	*persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	return nil, fmt.Errorf("%w: EnqueueMessage", ErrNotImplemented)
}

func (q queueV2) ReadMessages(
	context.Context,
	*persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	return nil, fmt.Errorf("%w: ReadMessages", ErrNotImplemented)
}
