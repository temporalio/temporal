package listqueues

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
)

func Invoke(
	ctx context.Context,
	historyTaskQueueManager persistence.HistoryTaskQueueManager,
	req *historyservice.ListQueuesRequest,
) (*historyservice.ListQueuesResponse, error) {
	resp, err := historyTaskQueueManager.ListQueues(ctx, &persistence.ListQueuesRequest{
		QueueType:     persistence.QueueV2Type(req.QueueType),
		PageSize:      int(req.PageSize),
		NextPageToken: req.NextPageToken,
	})
	if err != nil {
		if errors.Is(err, persistence.ErrNonPositiveListQueuesPageSize) {
			return nil, consts.ErrInvalidPageSize
		}
		if errors.Is(err, persistence.ErrNegativeListQueuesOffset) || errors.Is(err, persistence.ErrInvalidListQueuesNextPageToken) {
			return nil, consts.ErrInvalidPaginationToken
		}
		return nil, serviceerror.NewUnavailablef("ListQueues failed. Error: %v", err)
	}
	var queues []*historyservice.ListQueuesResponse_QueueInfo
	for _, queue := range resp.Queues {
		queues = append(queues, &historyservice.ListQueuesResponse_QueueInfo{
			QueueName:     queue.QueueName,
			MessageCount:  queue.MessageCount,
			LastMessageId: queue.LastMessageID,
		})
	}
	return &historyservice.ListQueuesResponse{
		Queues:        queues,
		NextPageToken: resp.NextPageToken,
	}, nil
}
