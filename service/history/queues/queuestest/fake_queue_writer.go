package queuestest

import (
	"context"
	"sync"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/queues"
)

// EnqueueTaskFunc is a function type for custom EnqueueTask behavior in tests
type EnqueueTaskFunc func(context.Context, *persistence.EnqueueTaskRequest) (*persistence.EnqueueTaskResponse, error)

// FakeQueueWriter is a [queues.QueueWriter] which records the requests it receives and returns the provided errors.
type FakeQueueWriter struct {
	mu                  sync.Mutex
	EnqueueTaskRequests []*persistence.EnqueueTaskRequest
	EnqueueTaskErr      error
	CreateQueueErr      error
	// EnqueueTaskFunc allows tests to provide custom behavior for EnqueueTask calls.
	// If set, this function is called instead of the default behavior.
	EnqueueTaskFunc EnqueueTaskFunc
}

var _ queues.QueueWriter = (*FakeQueueWriter)(nil)

func (d *FakeQueueWriter) EnqueueTask(
	ctx context.Context,
	request *persistence.EnqueueTaskRequest,
) (*persistence.EnqueueTaskResponse, error) {
	// Protect the slice append from concurrent access
	d.mu.Lock()
	d.EnqueueTaskRequests = append(d.EnqueueTaskRequests, request)
	d.mu.Unlock()

	if d.EnqueueTaskFunc != nil {
		return d.EnqueueTaskFunc(ctx, request)
	}
	return &persistence.EnqueueTaskResponse{Metadata: persistence.MessageMetadata{ID: 0}}, d.EnqueueTaskErr
}

func (d *FakeQueueWriter) CreateQueue(
	context.Context,
	*persistence.CreateQueueRequest,
) (*persistence.CreateQueueResponse, error) {
	return nil, d.CreateQueueErr
}
