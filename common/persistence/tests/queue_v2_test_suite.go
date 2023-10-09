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

package tests

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/persistence"
)

// RunQueueV2TestSuite executes interface-level tests for a queue persistence-layer implementation. There should be more
// implementation-specific tests that will not be covered by this suite elsewhere.
func RunQueueV2TestSuite(t *testing.T, queue persistence.QueueV2) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	t.Cleanup(cancel)

	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()

	// TODO: Remove this condition after implementing CreateQueue for SQL.
	if strings.HasPrefix(t.Name(), "TestCassandra") {
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
	}

	t.Run("TestHappyPath", func(t *testing.T) {
		t.Parallel()

		testHappyPath(ctx, t, queue, queueType, queueName)
	})
	t.Run("TestInvalidPageToken", func(t *testing.T) {
		t.Parallel()

		_, err := queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType:     queueType,
			QueueName:     queueName,
			PageSize:      1,
			NextPageToken: []byte("some invalid token"),
		})
		assert.ErrorIs(t, err, persistence.ErrInvalidReadQueueMessagesNextPageToken)
	})
	t.Run("TestNonPositivePageSize", func(t *testing.T) {
		t.Parallel()

		_, err := queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType:     queueType,
			QueueName:     queueName,
			PageSize:      0,
			NextPageToken: nil,
		})
		assert.ErrorIs(t, err, persistence.ErrNonPositiveReadQueueMessagesPageSize)
	})
	t.Run("TestEnqueueMessageToNonExistentQueue", func(t *testing.T) {
		SkipUnimplementedForSQL(t)
		t.Parallel()

		_, err := queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
			QueueType: queueType,
			QueueName: "non-existent-queue",
		})
		assert.ErrorAs(t, err, new(*serviceerror.NotFound))
		assert.ErrorContains(t, err, "non-existent-queue")
		assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	})
	t.Run("TestCreateQueueTwice", func(t *testing.T) {
		SkipUnimplementedForSQL(t)
		t.Parallel()

		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, persistence.ErrQueueAlreadyExists)
		assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
		assert.ErrorContains(t, err, queueName)
	})
	t.Run("HistoryTaskQueueManagerImpl", func(t *testing.T) {
		t.Parallel()
		SkipUnimplementedForSQL(t)
		RunHistoryTaskQueueManagerTestSuite(t, queue)
	})

}

func testHappyPath(
	ctx context.Context,
	t *testing.T,
	queue persistence.QueueV2,
	queueType persistence.QueueV2Type,
	queueName string,
) {
	response, err := queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType:     queueType,
		QueueName:     queueName,
		PageSize:      1,
		NextPageToken: nil,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, len(response.Messages))

	encodingType := enums.ENCODING_TYPE_JSON
	_, err = queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: commonpb.DataBlob{
			EncodingType: encodingType,
			Data:         []byte("1"),
		},
	})
	require.NoError(t, err)

	_, err = queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: commonpb.DataBlob{
			EncodingType: encodingType,
			Data:         []byte("2"),
		},
	})
	require.NoError(t, err)

	response, err = queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType:     queueType,
		QueueName:     queueName,
		PageSize:      1,
		NextPageToken: nil,
	})
	require.NoError(t, err)
	require.Len(t, response.Messages, 1)
	assert.Equal(t, int64(persistence.FirstQueueMessageID), response.Messages[0].MetaData.ID)
	assert.Equal(t, []byte("1"), response.Messages[0].Data.Data)
	assert.Equal(t, encodingType, response.Messages[0].Data.EncodingType)
	assert.NotNil(t, response.NextPageToken)

	response, err = queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType:     queueType,
		QueueName:     queueName,
		PageSize:      1,
		NextPageToken: response.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, response.Messages, 1)
	assert.Equal(t, int64(persistence.FirstQueueMessageID+1), response.Messages[0].MetaData.ID)
	assert.Equal(t, []byte("2"), response.Messages[0].Data.Data)
	assert.Equal(t, encodingType, response.Messages[0].Data.EncodingType)

	response, err = queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType:     queueType,
		QueueName:     queueName,
		PageSize:      1,
		NextPageToken: response.NextPageToken,
	})
	require.NoError(t, err)
	assert.Empty(t, response.Messages)
	assert.Nil(t, response.NextPageToken)
}

// TODO: Remove this function after implementing CreateQueue for SQL.
func SkipUnimplementedForSQL(t *testing.T) {
	if !strings.HasPrefix(t.Name(), "TestCassandra") {
		t.Skip("skipping test which is not implemented for SQL yet")
	}
}
