package activity

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"google.golang.org/grpc"
)

type testConcurrencyServiceClient struct {
	fcpb.ConcurrencyServiceClient
	batch func(context.Context, *fcpb.ConcurrencyServiceBatchRequest, ...grpc.CallOption) (*fcpb.ConcurrencyServiceBatchResponse, error)
}

func (c *testConcurrencyServiceClient) Batch(
	ctx context.Context,
	request *fcpb.ConcurrencyServiceBatchRequest,
	opts ...grpc.CallOption,
) (*fcpb.ConcurrencyServiceBatchResponse, error) {
	return c.batch(ctx, request, opts...)
}

func TestReleaseLimiterTaskExecute(t *testing.T) {
	releaseErr := errors.New("release failed")
	var lock sync.Mutex
	var requests []*fcpb.ConcurrencyServiceBatchRequest
	handler := newReleaseLimiterTaskHandler(&testConcurrencyServiceClient{
		batch: func(
			_ context.Context,
			request *fcpb.ConcurrencyServiceBatchRequest,
			_ ...grpc.CallOption,
		) (*fcpb.ConcurrencyServiceBatchResponse, error) {
			lock.Lock()
			requests = append(requests, request)
			lock.Unlock()
			if request.GetReleaseSlots()[0] == "bad-slot" {
				return nil, releaseErr
			}
			return &fcpb.ConcurrencyServiceBatchResponse{}, nil
		},
	})
	task := &activitypb.ReleaseLimiterTask{Limiters: []*taskqueuespb.LimiterRef{
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "first-key", SlotId: "bad-slot"},
		{LimiterType: enumsspb.LIMITER_TYPE_UNSPECIFIED, Key: "ignored-key", SlotId: "ignored-slot"},
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "second-key", SlotId: "good-slot"},
	}}

	err := handler.Execute(t.Context(), chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
		NamespaceID: "namespace-id",
	}}, chasm.TaskAttributes{}, task)

	require.ErrorIs(t, err, releaseErr)
	require.ElementsMatch(t, []*fcpb.ConcurrencyServiceBatchRequest{
		{NamespaceId: "namespace-id", Key: "first-key", ReleaseSlots: []string{"bad-slot"}},
		{NamespaceId: "namespace-id", Key: "second-key", ReleaseSlots: []string{"good-slot"}},
	}, requests)
}

func TestReleaseLimiterTaskExecuteConcurrent(t *testing.T) {
	t.Parallel()
	firstErr := errors.New("first release failed")
	secondErr := errors.New("second release failed")
	var started atomic.Int32
	allStarted := make(chan struct{})
	handler := newReleaseLimiterTaskHandler(&testConcurrencyServiceClient{
		batch: func(
			ctx context.Context,
			request *fcpb.ConcurrencyServiceBatchRequest,
			_ ...grpc.CallOption,
		) (*fcpb.ConcurrencyServiceBatchResponse, error) {
			if started.Add(1) == 2 {
				close(allStarted)
			}
			select {
			case <-allStarted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if request.GetKey() == "first-key" {
				return nil, firstErr
			}
			return nil, secondErr
		},
	})
	task := &activitypb.ReleaseLimiterTask{Limiters: []*taskqueuespb.LimiterRef{
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "first-key", SlotId: "first-slot"},
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "second-key", SlotId: "second-slot"},
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := handler.Execute(ctx, chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
		NamespaceID: "namespace-id",
	}}, chasm.TaskAttributes{}, task)

	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
}
