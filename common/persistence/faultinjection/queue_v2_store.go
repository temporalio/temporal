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

	"go.temporal.io/server/common/persistence"
)

type (
	faultInjectionQueueV2 struct {
		baseStore persistence.QueueV2
		generator faultGenerator
	}
)

func newFaultInjectionQueueV2(
	baseQueue persistence.QueueV2,
	generator faultGenerator,
) *faultInjectionQueueV2 {
	return &faultInjectionQueueV2{
		baseStore: baseQueue,
		generator: generator,
	}
}

func (f *faultInjectionQueueV2) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	return inject1(f.generator.generate(), func() (*persistence.InternalEnqueueMessageResponse, error) {
		return f.baseStore.EnqueueMessage(ctx, request)
	})
}

func (f *faultInjectionQueueV2) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	return inject1(f.generator.generate(), func() (*persistence.InternalReadMessagesResponse, error) {
		return f.baseStore.ReadMessages(ctx, request)
	})
}

func (f *faultInjectionQueueV2) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	return inject1(f.generator.generate(), func() (*persistence.InternalCreateQueueResponse, error) {
		return f.baseStore.CreateQueue(ctx, request)
	})
}

func (f *faultInjectionQueueV2) RangeDeleteMessages(
	ctx context.Context,
	request *persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	return inject1(f.generator.generate(), func() (*persistence.InternalRangeDeleteMessagesResponse, error) {
		return f.baseStore.RangeDeleteMessages(ctx, request)
	})
}

func (f *faultInjectionQueueV2) ListQueues(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	return inject1(f.generator.generate(), func() (*persistence.InternalListQueuesResponse, error) {
		return f.baseStore.ListQueues(ctx, request)
	})
}
