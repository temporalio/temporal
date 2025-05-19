package persistence

import (
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
	return serviceerror.NewNotFoundf(
		"queue not found: type = %v and name = %v",
		queueType,
		queueName,
	)
}
