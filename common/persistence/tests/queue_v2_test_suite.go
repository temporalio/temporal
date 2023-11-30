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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/tests"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
)

// RunQueueV2TestSuite executes interface-level tests for a queue persistence-layer implementation. There should be more
// implementation-specific tests that will not be covered by this suite elsewhere.
func RunQueueV2TestSuite(t *testing.T, q persistence.QueueV2) {
	ctx := context.Background()

	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()

	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	t.Run("TestListQueues", func(t *testing.T) {
		testListQueues(ctx, t, q)
	})
	t.Run("TestHappyPath", func(t *testing.T) {
		t.Parallel()

		testHappyPath(ctx, t, q, queueType, queueName)
	})
	t.Run("TestInvalidPageToken", func(t *testing.T) {
		t.Parallel()

		_, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType:     queueType,
			QueueName:     queueName,
			PageSize:      1,
			NextPageToken: []byte("some invalid token"),
		})
		assert.ErrorIs(t, err, persistence.ErrInvalidReadQueueMessagesNextPageToken)
	})
	t.Run("TestNonPositivePageSize", func(t *testing.T) {
		t.Parallel()

		_, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType:     queueType,
			QueueName:     queueName,
			PageSize:      0,
			NextPageToken: nil,
		})
		assert.ErrorIs(t, err, persistence.ErrNonPositiveReadQueueMessagesPageSize)
	})
	t.Run("TestEnqueueMessageToNonExistentQueue", func(t *testing.T) {
		t.Parallel()

		_, err := q.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
			QueueType: queueType,
			QueueName: "non-existent-queue",
		})
		assert.ErrorAs(t, err, new(*serviceerror.NotFound))
		assert.ErrorContains(t, err, "non-existent-queue")
		assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
	})
	t.Run("TestCreateQueueTwice", func(t *testing.T) {
		t.Parallel()

		_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, persistence.ErrQueueAlreadyExists)
		assert.ErrorContains(t, err, strconv.Itoa(int(queueType)))
		assert.ErrorContains(t, err, queueName)
	})
	t.Run("InvalidEncodingForQueueMessage", func(t *testing.T) {
		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName, func(p *persistencetest.EnqueueParams) {
			p.EncodingType = -1
		})
		require.NoError(t, err)
		_, err = q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			PageSize:  10,
		})
		require.Error(t, err)
		assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	})
	t.Run("InvalidEncodingForQueueMetadata", func(t *testing.T) {
		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName, func(p *persistencetest.EnqueueParams) {
			p.EncodingType = -1
		})
		require.NoError(t, err)
		_, err = q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			PageSize:  10,
		})
		require.Error(t, err)
		assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	})
	t.Run("TestRangeDeleteMessages", func(t *testing.T) {
		t.Parallel()
		testRangeDeleteMessages(ctx, t, q)
	})
	t.Run("HistoryTaskQueueManagerImpl", func(t *testing.T) {
		t.Parallel()
		RunHistoryTaskQueueManagerTestSuite(t, q)
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
	_, err = persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
	require.NoError(t, err)

	_, err = persistencetest.EnqueueMessage(ctx, queue, queueType, queueName, func(p *persistencetest.EnqueueParams) {
		p.Data = []byte("2")
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

func testRangeDeleteMessages(ctx context.Context, t *testing.T, queue persistence.QueueV2) {
	t.Helper()

	t.Run("DeleteBeforeCreate", func(t *testing.T) {
		t.Parallel()

		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		assert.ErrorAs(t, err, new(*serviceerror.NotFound))
	})

	t.Run("InvalidMaxMessageID", func(t *testing.T) {
		t.Parallel()

		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		_, err = queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID - 1,
			},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, persistence.ErrInvalidQueueRangeDeleteMaxMessageID)
		assert.ErrorContains(t, err, strconv.Itoa(persistence.FirstQueueMessageID-1))
		assert.ErrorContains(t, err, strconv.Itoa(persistence.FirstQueueMessageID))
	})

	t.Run("HappyPath", func(t *testing.T) {
		t.Parallel()

		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			_, err := persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
			require.NoError(t, err)
		}
		resp, err := queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID + 1,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(2), resp.MessagesDeleted)
		response, err := queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			PageSize:  10,
		})
		require.NoError(t, err)
		require.Len(t, response.Messages, 1)
		assert.Equal(t, int64(persistence.FirstQueueMessageID+2), response.Messages[0].MetaData.ID)
	})

	t.Run("DeleteAllAndReEnqueue", func(t *testing.T) {
		t.Parallel()

		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		msg, err := persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
		require.NoError(t, err)
		assert.Equal(t, int64(persistence.FirstQueueMessageID), msg.Metadata.ID)
		resp, err := queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.MessagesDeleted)
		msg, err = persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
		require.NoError(t, err)
		assert.Equal(t, int64(persistence.FirstQueueMessageID+1), msg.Metadata.ID, "Even though all"+
			" messages are deleted, the next message ID should still be incremented")
	})

	t.Run("DeleteAndValidateMinId", func(t *testing.T) {
		t.Parallel()

		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			msg, err := persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
			require.NoError(t, err)
			assert.Equal(t, int64(persistence.FirstQueueMessageID+i), msg.Metadata.ID)
		}
		resp, err := queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID + 10,
			},
		})
		require.NoError(t, err)
		require.Equal(t, int64(3), resp.MessagesDeleted)
		_, err = persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
		require.NoError(t, err)
		response, err := queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			PageSize:  10,
		})
		require.NoError(t, err)
		require.Len(t, response.Messages, 1)
		require.Equal(t, response.Messages[0].MetaData.ID, int64(3))
	})

	t.Run("DeleteSameRangeTwice", func(t *testing.T) {
		t.Parallel()

		queueType := persistence.QueueTypeHistoryNormal
		queueName := "test-queue-" + t.Name()
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			_, err := persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
			require.NoError(t, err)
		}

		resp, err := queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID,
			},
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.MessagesDeleted)

		resp, err = queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID,
			},
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), resp.MessagesDeleted)

		response, err := queue.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			PageSize:  10,
		})
		require.NoError(t, err)
		require.Len(t, response.Messages, 1)
		assert.Equal(t, int64(persistence.FirstQueueMessageID+1), response.Messages[0].MetaData.ID)
	})
}

func RunQueueV2TestSuiteForSQL(t *testing.T, factory *sql.Factory) {
	t.Run("Generic", func(t *testing.T) {
		t.Parallel()
		queue, err := factory.NewQueueV2()
		require.NoError(t, err)
		RunQueueV2TestSuite(t, queue)
	})
	t.Run("SQL", func(t *testing.T) {
		t.Parallel()
		db, err := factory.GetDB()
		require.NoError(t, err)
		tests.RunSQLQueueV2TestSuite(t, db)
	})
}

func testListQueues(ctx context.Context, t *testing.T, queue persistence.QueueV2) {
	t.Run("HappyPath", func(t *testing.T) {
		// ListQueues when empty
		queueType := persistence.QueueTypeHistoryDLQ
		response, err := queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      10,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(response.Queues))

		// List of all created queues
		var queueNames []string

		// List one queue.
		queueName := "test-queue-" + t.Name() + "first"
		queueNames = append(queueNames, queueName)
		_, err = queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      10,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(response.Queues))
		require.Equal(t, queueName, response.Queues[0].QueueName)
		require.Equal(t, int64(0), response.Queues[0].MessageCount)

		// List multiple queues.
		queueName = "test-queue-" + t.Name() + "second"
		queueNames = append(queueNames, queueName)
		_, err = queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      10,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		require.Equal(t, 2, len(response.Queues))
		require.Contains(t, []string{response.Queues[0].QueueName, response.Queues[1].QueueName}, queueName)
		require.Equal(t, int64(0), response.Queues[0].MessageCount)
		require.Equal(t, int64(0), response.Queues[1].MessageCount)

		// List multiple queues in pages.
		for i := 0; i < 3; i++ {
			queueNames = append(queueNames, "test-queue-"+t.Name()+strconv.Itoa(i))
		}
		for _, queueName := range queueNames[2:] {
			_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
				QueueType: queueType,
				QueueName: queueName,
			})
			require.NoError(t, err)
		}
		var listedQueueNames []string
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      1,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(response.Queues))
		listedQueueNames = append(listedQueueNames, response.Queues[0].QueueName)
		require.Equal(t, int64(0), response.Queues[0].MessageCount)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      1,
			NextPageToken: response.NextPageToken,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(response.Queues))
		listedQueueNames = append(listedQueueNames, response.Queues[0].QueueName)
		require.Equal(t, int64(0), response.Queues[0].MessageCount)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      3,
			NextPageToken: response.NextPageToken,
		})
		require.NoError(t, err)
		require.Equal(t, 3, len(response.Queues))
		for _, queue := range response.Queues {
			listedQueueNames = append(listedQueueNames, queue.QueueName)
			require.Equal(t, int64(0), queue.MessageCount)
		}
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      1,
			NextPageToken: response.NextPageToken,
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(response.Queues))
		require.Empty(t, response.NextPageToken)
		for _, queueName := range queueNames {
			require.Contains(t, listedQueueNames, queueName)
		}
	})
	t.Run("QueueSize", func(t *testing.T) {
		queueType := persistence.QueueTypeHistoryDLQ
		queueName := "test-queue-" + t.Name()
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
			QueueType: queueType,
			QueueName: queueName,
		})
		require.NoError(t, err)
		response, err := queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      100,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		var queueNames []string
		for _, queue := range response.Queues {
			queueNames = append(queueNames, queue.QueueName)
			if queue.QueueName == queueName {
				assert.Equal(t, int64(0), queue.MessageCount)
			}
		}
		require.Contains(t, queueNames, queueName)

		// Enqueue one message and verify QueueSize is 1
		_, err = persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
		require.NoError(t, err)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      100,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		for _, queue := range response.Queues {
			queueNames = append(queueNames, queue.QueueName)
			if queue.QueueName == queueName {
				assert.Equal(t, int64(1), queue.MessageCount)
			}
		}
		require.Contains(t, queueNames, queueName)

		// Enqueue one more message and verify QueueSize is 2
		_, err = persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
		require.NoError(t, err)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      100,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		for _, queue := range response.Queues {
			queueNames = append(queueNames, queue.QueueName)
			if queue.QueueName == queueName {
				assert.Equal(t, int64(2), queue.MessageCount)
			}
		}
		require.Contains(t, queueNames, queueName)

		// Delete one message and verify QueueSize is 1
		_, err = queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID,
			},
		})
		require.NoError(t, err)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      100,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		for _, queue := range response.Queues {
			queueNames = append(queueNames, queue.QueueName)
			if queue.QueueName == queueName {
				assert.Equal(t, int64(1), queue.MessageCount)
			}
		}
		require.Contains(t, queueNames, queueName)

		// Delete one more message and verify QueueSize is 0
		_, err = queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID + 1,
			},
		})
		require.NoError(t, err)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      100,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		for _, queue := range response.Queues {
			queueNames = append(queueNames, queue.QueueName)
			if queue.QueueName == queueName {
				assert.Equal(t, int64(0), queue.MessageCount)
			}
		}
		require.Contains(t, queueNames, queueName)

		// Enqueue one message once again and verify QueueSize is 1
		_, err = persistencetest.EnqueueMessage(ctx, queue, queueType, queueName)
		require.NoError(t, err)
		response, err = queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      100,
			NextPageToken: nil,
		})
		require.NoError(t, err)
		for _, queue := range response.Queues {
			queueNames = append(queueNames, queue.QueueName)
			if queue.QueueName == queueName {
				assert.Equal(t, int64(1), queue.MessageCount)
			}
		}
		require.Contains(t, queueNames, queueName)

	})
	t.Run("NegativePageSize", func(t *testing.T) {
		t.Parallel()
		queueType := persistence.QueueTypeHistoryDLQ
		_, err := queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      -1,
			NextPageToken: nil,
		})
		require.Error(t, err)
		require.ErrorIs(t, err, persistence.ErrNonPositiveListQueuesPageSize)
	})
	t.Run("InvalidPageToken", func(t *testing.T) {
		t.Parallel()
		queueType := persistence.QueueTypeHistoryDLQ
		_, err := queue.ListQueues(ctx, &persistence.InternalListQueuesRequest{
			QueueType:     queueType,
			PageSize:      1,
			NextPageToken: []byte("some invalid token"),
		})
		assert.Error(t, err)
	})
}
