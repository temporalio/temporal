package concurrency

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/stream_batcher"
	"google.golang.org/grpc"
)

type testClient struct {
	fcpb.ConcurrencyServiceClient
	batch func(context.Context, *fcpb.ConcurrencyBatchRequest, ...grpc.CallOption) (*fcpb.ConcurrencyBatchResponse, error)
}

func (c *testClient) Batch(
	ctx context.Context,
	req *fcpb.ConcurrencyBatchRequest,
	opts ...grpc.CallOption,
) (*fcpb.ConcurrencyBatchResponse, error) {
	return c.batch(ctx, req, opts...)
}

func TestBatchingClient(t *testing.T) {
	callCount := 0
	batchedRequests := make(chan *fcpb.ConcurrencyBatchRequest, 1)
	client := NewBatchingClient(
		&testClient{batch: func(
			_ context.Context,
			req *fcpb.ConcurrencyBatchRequest,
			_ ...grpc.CallOption,
		) (*fcpb.ConcurrencyBatchResponse, error) {
			callCount++
			batchedRequests <- req
			res := &fcpb.ConcurrencyBatchResponse{Generation: 42}
			for _, slotID := range req.ReserveSlots {
				res.ReserveSuccess = append(res.ReserveSuccess, slotID == "reserve-1")
			}
			for _, slotID := range req.CommitSlots {
				res.CommitSuccess = append(res.CommitSuccess, slotID == "commit-1")
			}
			return res, nil
		}},
		stream_batcher.BatcherOptions{
			MaxItems: 2,
			MinDelay: time.Second,
			MaxDelay: time.Second,
			IdleTime: time.Minute,
		},
		clock.NewRealTimeSource(),
	)

	type result struct {
		res *fcpb.ConcurrencyBatchResponse
		err error
	}
	requests := []*fcpb.ConcurrencyBatchRequest{
		{
			NamespaceId:            "namespace-id",
			Key:                    "limiter-key",
			ReserveSlots:           []string{"reserve-1"},
			CancelReservationSlots: []string{"cancel-1"},
			ConfigUpdate:           &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 10},
			ConfigUpdateVersion:    1,
		},
		{
			NamespaceId:         "namespace-id",
			Key:                 "limiter-key",
			ReserveSlots:        []string{"reserve-2"},
			CommitSlots:         []string{"commit-1"},
			ReleaseSlots:        []string{"release-1"},
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 20},
			ConfigUpdateVersion: 2,
		},
	}
	results := make([]result, len(requests))

	var wg sync.WaitGroup
	for i, req := range requests {
		wg.Go(func() {
			res, err := client.Batch(t.Context(), req)
			results[i] = result{res: res, err: err}
		})
	}
	wg.Wait()

	require.Equal(t, 1, callCount)
	batchedRequest := <-batchedRequests
	require.Equal(t, "namespace-id", batchedRequest.GetNamespaceId())
	require.Equal(t, "limiter-key", batchedRequest.GetKey())
	require.ElementsMatch(t, []string{"reserve-1", "reserve-2"}, batchedRequest.GetReserveSlots())
	require.Equal(t, []string{"cancel-1"}, batchedRequest.GetCancelReservationSlots())
	require.Equal(t, []string{"commit-1"}, batchedRequest.GetCommitSlots())
	require.Equal(t, []string{"release-1"}, batchedRequest.GetReleaseSlots())
	require.Equal(t, int64(2), batchedRequest.GetConfigUpdateVersion())
	require.Equal(t, int32(20), batchedRequest.GetConfigUpdate().GetConcurrentTasks())
	for _, result := range results {
		require.NoError(t, result.err)
		require.Equal(t, int64(42), result.res.GetGeneration())
	}
	require.Equal(t, []bool{true}, results[0].res.ReserveSuccess)
	require.Empty(t, results[0].res.CommitSuccess)
	require.Equal(t, []bool{false}, results[1].res.ReserveSuccess)
	require.Equal(t, []bool{true}, results[1].res.CommitSuccess)
}

func TestBatchingClientRejectsInvalidBatchResponse(t *testing.T) {
	tests := []struct {
		name string
		res  *fcpb.ConcurrencyBatchResponse
	}{
		{name: "nil response"},
		{name: "missing reserve result", res: &fcpb.ConcurrencyBatchResponse{
			ReserveSuccess: []bool{true},
			CommitSuccess:  []bool{true},
		}},
		{name: "extra reserve result", res: &fcpb.ConcurrencyBatchResponse{
			ReserveSuccess: []bool{true, false, true},
			CommitSuccess:  []bool{true},
		}},
		{name: "missing commit result", res: &fcpb.ConcurrencyBatchResponse{
			ReserveSuccess: []bool{true, false},
		}},
		{name: "extra commit result", res: &fcpb.ConcurrencyBatchResponse{
			ReserveSuccess: []bool{true, false},
			CommitSuccess:  []bool{true, false},
		}},
	}
	items := []clientBatchItem{
		{ctx: t.Context(), req: &fcpb.ConcurrencyBatchRequest{
			ReserveSlots: []string{"first"},
			CommitSlots:  []string{"first"},
		}},
		{ctx: t.Context(), req: &fcpb.ConcurrencyBatchRequest{
			ReserveSlots: []string{"second"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &BatchingClient{ConcurrencyServiceClient: &testClient{batch: func(
				context.Context,
				*fcpb.ConcurrencyBatchRequest,
				...grpc.CallOption,
			) (*fcpb.ConcurrencyBatchResponse, error) {
				return tt.res, nil
			}}}

			results := client.applyBatch(clientBatchKey{}, items)
			require.Len(t, results, len(items))
			for _, result := range results {
				require.Nil(t, result.res)
				var internalErr *serviceerror.Internal
				require.ErrorAs(t, result.err, &internalErr)
			}
		})
	}
}

func TestBatchingClientPropagatesBatchError(t *testing.T) {
	testErr := errors.New("batch failed")
	client := &BatchingClient{ConcurrencyServiceClient: &testClient{batch: func(
		context.Context,
		*fcpb.ConcurrencyBatchRequest,
		...grpc.CallOption,
	) (*fcpb.ConcurrencyBatchResponse, error) {
		return nil, testErr
	}}}

	results := client.applyBatch(clientBatchKey{}, []clientBatchItem{
		{ctx: t.Context(), req: &fcpb.ConcurrencyBatchRequest{}},
		{ctx: t.Context(), req: &fcpb.ConcurrencyBatchRequest{}},
	})
	require.Len(t, results, 2)
	for _, result := range results {
		require.Nil(t, result.res)
		require.ErrorIs(t, result.err, testErr)
	}
}

func TestBatchingClientUsesActiveRequestContext(t *testing.T) {
	type contextKey struct{}
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	activeCtx := context.WithValue(t.Context(), contextKey{}, "active")
	client := &BatchingClient{ConcurrencyServiceClient: &testClient{batch: func(
		ctx context.Context,
		_ *fcpb.ConcurrencyBatchRequest,
		_ ...grpc.CallOption,
	) (*fcpb.ConcurrencyBatchResponse, error) {
		require.Equal(t, "active", ctx.Value(contextKey{}))
		return &fcpb.ConcurrencyBatchResponse{}, nil
	}}}

	results := client.applyBatch(clientBatchKey{}, []clientBatchItem{
		{ctx: canceledCtx, req: &fcpb.ConcurrencyBatchRequest{}},
		{ctx: activeCtx, req: &fcpb.ConcurrencyBatchRequest{}},
	})
	require.NoError(t, results[0].err)
	require.NoError(t, results[1].err)
}

func TestBatchingClientCallOptionsBypassBatching(t *testing.T) {
	callCount := 0
	client := &BatchingClient{ConcurrencyServiceClient: &testClient{batch: func(
		_ context.Context,
		_ *fcpb.ConcurrencyBatchRequest,
		opts ...grpc.CallOption,
	) (*fcpb.ConcurrencyBatchResponse, error) {
		callCount++
		require.Len(t, opts, 1)
		return &fcpb.ConcurrencyBatchResponse{Generation: 42}, nil
	}}}

	res, err := client.Batch(t.Context(), &fcpb.ConcurrencyBatchRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)
	require.Equal(t, int64(42), res.Generation)
	require.Equal(t, 1, callCount)
}

func TestBatchingClientDoesNotCombineDifferentLimiters(t *testing.T) {
	keys := make(chan string, 3)
	client := NewBatchingClient(
		&testClient{batch: func(
			_ context.Context,
			req *fcpb.ConcurrencyBatchRequest,
			_ ...grpc.CallOption,
		) (*fcpb.ConcurrencyBatchResponse, error) {
			keys <- req.NamespaceId + "/" + req.Key
			return &fcpb.ConcurrencyBatchResponse{}, nil
		}},
		stream_batcher.BatcherOptions{
			MaxItems: 2,
			MinDelay: time.Millisecond,
			MaxDelay: time.Millisecond,
			IdleTime: time.Millisecond,
		},
		clock.NewRealTimeSource(),
	)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for _, limiter := range []clientBatchKey{
		{namespaceID: "namespace", key: "first"},
		{namespaceID: "namespace", key: "second"},
		{namespaceID: "other-namespace", key: "first"},
	} {
		wg.Go(func() {
			_, err := client.Batch(t.Context(), &fcpb.ConcurrencyBatchRequest{
				NamespaceId: limiter.namespaceID,
				Key:         limiter.key,
			})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	close(keys)
	var gotKeys []string
	for key := range keys {
		gotKeys = append(gotKeys, key)
	}
	require.ElementsMatch(t, []string{
		"namespace/first",
		"namespace/second",
		"other-namespace/first",
	}, gotKeys)
}
