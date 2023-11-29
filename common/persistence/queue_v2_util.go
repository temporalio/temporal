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

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

const (
	// pageTokenPrefixByte is the first byte of the serialized page token. It's used to ensure that the page token is
	// not empty. Without this, if the last_read_message_id is 0, the serialized page token would be empty, and clients
	// could erroneously assume that there are no more messages beyond the first page. This is purely used to ensure
	// that tokens are non-empty; it is not used to verify that the token is valid like the magic byte in some other
	// protocols.
	pageTokenPrefixByte = 0
)

func GetNextPageTokenForReadMessages(result []QueueV2Message) []byte {
	if len(result) == 0 {
		return nil
	}
	lastReadMessageID := result[len(result)-1].MetaData.ID
	token := &persistencespb.ReadQueueMessagesNextPageToken{
		LastReadMessageId: lastReadMessageID,
	}
	// This can never fail if you inspect the implementation.
	b, _ := token.Marshal()

	// See the comment above pageTokenPrefixByte for why we want to do this.
	return append([]byte{pageTokenPrefixByte}, b...)
}

func GetMinMessageIDToReadForQueueV2(
	queueType QueueV2Type,
	queueName string,
	nextPageToken []byte,
	queue *persistencespb.Queue,
) (int64, error) {
	if len(nextPageToken) == 0 {
		partition, err := GetPartitionForQueueV2(queueType, queueName, queue)
		if err != nil {
			return 0, err
		}
		return partition.MinMessageId, nil
	}
	var token persistencespb.ReadQueueMessagesNextPageToken

	// Skip the first byte. See the comment on pageTokenPrefixByte for more details.
	err := token.Unmarshal(nextPageToken[1:])
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %q: %v",
			ErrInvalidReadQueueMessagesNextPageToken,
			nextPageToken,
			err,
		)
	}
	return token.LastReadMessageId + 1, nil
}

func GetNextPageTokenForListQueues(queueNumber int64) []byte {
	token := &persistencespb.ListQueuesNextPageToken{
		LastReadQueueNumber: queueNumber,
	}
	// This can never fail if you inspect the implementation.
	b, _ := token.Marshal()

	// See the comment above pageTokenPrefixByte for why we want to do this.
	return append([]byte{pageTokenPrefixByte}, b...)
}

func GetOffsetForListQueues(
	nextPageToken []byte,
) (int64, error) {
	if len(nextPageToken) == 0 {
		return 0, nil
	}
	var token persistencespb.ListQueuesNextPageToken

	// Skip the first byte. See the comment on pageTokenPrefixByte for more details.
	err := token.Unmarshal(nextPageToken[1:])
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %q: %v",
			ErrInvalidListQueuesNextPageToken,
			nextPageToken,
			err,
		)
	}
	return token.LastReadQueueNumber, nil
}

func GetPartitionForQueueV2(
	queueType QueueV2Type,
	queueName string,
	queue *persistencespb.Queue,
) (*persistencespb.QueuePartition, error) {
	// Currently, we only have one partition for each queue. However, that might change in the future. If a queue is
	// created with more than 1 partition by a server on a future release, and then that server is downgraded, we
	// will need to handle this case. Since all DLQ tasks are retried infinitely, we just return an error.
	numPartitions := len(queue.Partitions)
	if numPartitions != 1 {
		return nil, serviceerror.NewInternal(
			fmt.Sprintf(
				"queue without single partition detected. queue with type %v and queueName %v has %d partitions, "+
					"but this implementation only supports queues with 1 partition. Did you downgrade your Temporal server?",
				queueType,
				queueName,
				numPartitions,
			),
		)
	}
	partition := queue.Partitions[0]
	return partition, nil
}

type DeleteRequest struct {
	// LastIDToDeleteInclusive represents the maximum message ID that the user wants to delete, inclusive.
	LastIDToDeleteInclusive int64
	// ExistingMessageRange represents an inclusive range of the minimum message ID and the maximum message ID in the queue.
	ExistingMessageRange InclusiveMessageRange
}

type InclusiveMessageRange struct {
	MinMessageID int64
	MaxMessageID int64
}

type DeleteRange struct {
	InclusiveMessageRange
	NewMinMessageID  int64
	MessagesToDelete int64
}

// GetDeleteRange returns the range of messages to delete, and a boolean indicating whether there is any update to be
// made: meaning either we should delete messages, update the min message ID, or both.
func GetDeleteRange(request DeleteRequest) (DeleteRange, bool) {
	if request.LastIDToDeleteInclusive < request.ExistingMessageRange.MinMessageID {
		// Nothing to delete
		return DeleteRange{}, false
	}
	return DeleteRange{
		InclusiveMessageRange: InclusiveMessageRange{
			MinMessageID: request.ExistingMessageRange.MinMessageID,
			// Never actually delete the last message
			MaxMessageID: min(request.LastIDToDeleteInclusive, request.ExistingMessageRange.MaxMessageID-1),
		},
		NewMinMessageID:  min(request.LastIDToDeleteInclusive, request.ExistingMessageRange.MaxMessageID) + 1,
		MessagesToDelete: min(request.LastIDToDeleteInclusive, request.ExistingMessageRange.MaxMessageID) - request.ExistingMessageRange.MinMessageID + 1,
	}, true
}
