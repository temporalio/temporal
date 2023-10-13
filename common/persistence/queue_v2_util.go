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

func GetNextPageTokenForQueueV2(result []QueueV2Message) []byte {
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
				"queue with type %v and queueName %v has %d partitions, but this implementation only supports"+
					" queues with 1 partition. Did you downgrade your Temporal server?",
				queueType,
				queueName,
				numPartitions,
			),
		)
	}
	partition := queue.Partitions[0]
	return partition, nil
}

func ClampLastIDToDeleteForQueueV2(
	lastIDToDelete int64,
	nextMessageID int64,
	minMessageID int64,
) int64 {
	if lastIDToDelete >= nextMessageID {
		// We need to clamp the lastIDToDelete to the last message ID in the queue. This is because we never actually
		// delete the last message (so that we can keep track of the max message ID). If we don't do this, a request to
		// delete messages with a lastIDToDelete that is greater than the last message ID will delete all messages in
		// the queue, and we will lose track of the max message ID, so the next message ID from an enqueue request will
		// be wrong.
		return nextMessageID - 1
	}
	if lastIDToDelete < minMessageID {
		// This is more than just an optimization; we need it for correctness. If the lastIDToDelete is more than one
		// less than the minMessageID, then if we update the minMessageID to be lastIDToDelete + 1, we will have
		// decreased the minMessageID, which doesn't make sense because there would be messages >= minMessageID that
		// have not been deleted. For example, if the minMessageID is 10 and the lastIDToDelete is 7, then we would
		// update the minMessageID to 8, which is incorrect because messages 8 and 9 have been deleted.
		//
		// If lastIDToDelete = minMessageID - 1, then we wouldn't update the minMessageID to something incorrect, but we
		// would waste two queries because we wouldn't delete anything, and the queue metadata would be updated to be
		// the same as it was before. For example, if the minMessageID is 10 and the lastIDToDelete is 9, then we would
		// send a query to delete messages < 9 (there are none), and then we would update the minMessageID to 10, which
		// is the same as it was before.
		//
		// If the lastIDToDelete is 10, then we would send a query to delete messages < 10 (there would be one because
		// we never delete all elements from the queue), and then we would update the minMessageID to be 11, which is
		// necessary because subsequent queries would need to start at 11.
		return -1
	}
	return lastIDToDelete
}
