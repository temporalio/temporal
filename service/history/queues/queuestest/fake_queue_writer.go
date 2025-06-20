package queuestest

import (
	"context"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/queues"
)

// FakeQueueWriter is a [queues.QueueWriter] which records the requests it receives and returns the provided errors.
type FakeQueueWriter struct {
	EnqueueTaskRequests []*persistence.EnqueueTaskRequest
	EnqueueTaskErr      error
	CreateQueueErr      error
}

var _ queues.QueueWriter = (*FakeQueueWriter)(nil)

func (d *FakeQueueWriter) EnqueueTask(
	_ context.Context,
	request *persistence.EnqueueTaskRequest,
) (*persistence.EnqueueTaskResponse, error) {
	d.EnqueueTaskRequests = append(d.EnqueueTaskRequests, request)
	return &persistence.EnqueueTaskResponse{Metadata: persistence.MessageMetadata{ID: 0}}, d.EnqueueTaskErr
}

func (d *FakeQueueWriter) CreateQueue(
	context.Context,
	*persistence.CreateQueueRequest,
) (*persistence.CreateQueueResponse, error) {
	return nil, d.CreateQueueErr
}
