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

package persistencetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	getQueueKeyParams struct {
		QueueType persistence.QueueV2Type
		Category  tasks.Category
	}
)

func WithQueueType(queueType persistence.QueueV2Type) func(p *getQueueKeyParams) {
	return func(p *getQueueKeyParams) {
		p.QueueType = queueType
	}
}

func WithCategory(category tasks.Category) func(p *getQueueKeyParams) {
	return func(p *getQueueKeyParams) {
		p.Category = category
	}
}

func GetQueueKey(t *testing.T, opts ...func(p *getQueueKeyParams)) persistence.QueueKey {
	params := &getQueueKeyParams{
		QueueType: persistence.QueueTypeHistoryNormal,
		Category:  tasks.CategoryTransfer,
	}
	for _, opt := range opts {
		opt(params)
	}
	// Note that it is important to include the test name in the cluster name to ensure that the generated queue name is
	// unique across tests. That way, we can run many queue tests without any risk of queue name collisions.
	return persistence.QueueKey{
		QueueType:     params.QueueType,
		Category:      params.Category,
		SourceCluster: "test-source-cluster-" + t.Name(),
		TargetCluster: "test-target-cluster-" + t.Name(),
	}
}

type EnqueueParams struct {
	Data         []byte
	EncodingType int
}

func EnqueueMessage(
	ctx context.Context,
	queue persistence.QueueV2,
	queueType persistence.QueueV2Type,
	queueName string,
	opts ...func(p *EnqueueParams),
) (*persistence.InternalEnqueueMessageResponse, error) {
	params := EnqueueParams{
		Data:         []byte("1"),
		EncodingType: int(enums.ENCODING_TYPE_JSON),
	}
	for _, opt := range opts {
		opt(&params)
	}
	return queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: &common.DataBlob{
			EncodingType: enums.EncodingType(params.EncodingType),
			Data:         params.Data,
		},
	})
}

func EnqueueMessagesForDelete(t *testing.T, q persistence.QueueV2, queueName string, queueType persistence.QueueV2Type) {
	for i := 0; i < 2; i++ {
		// We have to actually enqueue 2 messages. Otherwise, there won't be anything to actually delete,
		// since we never delete the last message.
		_, err := EnqueueMessage(context.Background(), q, queueType, queueName)
		require.NoError(t, err)
	}
}
