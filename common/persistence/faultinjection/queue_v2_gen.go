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

// Code generated by cmd/tools/genfaultinjection. DO NOT EDIT.

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
	baseStore persistence.QueueV2,
	generator faultGenerator,
) *faultInjectionQueueV2 {
	return &faultInjectionQueueV2{
		baseStore: baseStore,
		generator: generator,
	}
}

func (c *faultInjectionQueueV2) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	return inject1(c.generator.generate("CreateQueue"), func() (*persistence.InternalCreateQueueResponse, error) {
		return c.baseStore.CreateQueue(ctx, request)
	})
}

func (c *faultInjectionQueueV2) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	return inject1(c.generator.generate("EnqueueMessage"), func() (*persistence.InternalEnqueueMessageResponse, error) {
		return c.baseStore.EnqueueMessage(ctx, request)
	})
}

func (c *faultInjectionQueueV2) ListQueues(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	return inject1(c.generator.generate("ListQueues"), func() (*persistence.InternalListQueuesResponse, error) {
		return c.baseStore.ListQueues(ctx, request)
	})
}

func (c *faultInjectionQueueV2) RangeDeleteMessages(
	ctx context.Context,
	request *persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	return inject1(c.generator.generate("RangeDeleteMessages"), func() (*persistence.InternalRangeDeleteMessagesResponse, error) {
		return c.baseStore.RangeDeleteMessages(ctx, request)
	})
}

func (c *faultInjectionQueueV2) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	return inject1(c.generator.generate("ReadMessages"), func() (*persistence.InternalReadMessagesResponse, error) {
		return c.baseStore.ReadMessages(ctx, request)
	})
}
