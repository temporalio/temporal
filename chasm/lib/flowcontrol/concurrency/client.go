package concurrency

import (
	"context"
	"slices"

	"go.temporal.io/api/serviceerror"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/stream_batcher"
	"google.golang.org/grpc"
)

type clientBatchKey struct {
	namespaceID string
	key         string
}

type clientBatchItem struct {
	ctx context.Context
	req *fcpb.ConcurrencyBatchRequest
}

type clientBatchResult struct {
	res *fcpb.ConcurrencyBatchResponse
	err error
}

// BatchingClient batches calls to ConcurrencyService.Batch by limiter.
type BatchingClient struct {
	fcpb.ConcurrencyServiceClient

	batchers *stream_batcher.KeyedBatcher[clientBatchKey, clientBatchItem, clientBatchResult]
}

func NewBatchingClient(
	client fcpb.ConcurrencyServiceClient,
	opts stream_batcher.BatcherOptions,
	timeSource clock.TimeSource,
) *BatchingClient {
	c := &BatchingClient{ConcurrencyServiceClient: client}
	c.batchers = stream_batcher.NewKeyedBatcherWithPerItemResults(c.applyBatch, opts, timeSource)
	return c
}

func (c *BatchingClient) Batch(
	ctx context.Context,
	req *fcpb.ConcurrencyBatchRequest,
	opts ...grpc.CallOption,
) (*fcpb.ConcurrencyBatchResponse, error) {
	if len(opts) > 0 {
		return c.ConcurrencyServiceClient.Batch(ctx, req, opts...)
	}
	res, err := c.batchers.Add(ctx, clientBatchKey{
		namespaceID: req.GetNamespaceId(),
		key:         req.GetKey(),
	}, clientBatchItem{ctx: ctx, req: req})
	if err != nil {
		return nil, err
	}
	return res.res, res.err
}

func (c *BatchingClient) applyBatch(key clientBatchKey, items []clientBatchItem) []clientBatchResult {
	var reserveCount, cancelCount, commitCount, releaseCount int
	var configItem *clientBatchItem
	for i := range items {
		item := &items[i]
		reserveCount += len(item.req.GetReserveSlots())
		cancelCount += len(item.req.GetCancelReservationSlots())
		commitCount += len(item.req.GetCommitSlots())
		releaseCount += len(item.req.GetReleaseSlots())
		if item.req.GetConfigUpdate() != nil &&
			(configItem == nil || item.req.GetConfigUpdateVersion() > configItem.req.GetConfigUpdateVersion()) {
			configItem = item
		}
	}

	req := &fcpb.ConcurrencyBatchRequest{
		NamespaceId: key.namespaceID,
		Key:         key.key,
	}
	if reserveCount > 0 {
		req.ReserveSlots = make([]string, 0, reserveCount)
	}
	if cancelCount > 0 {
		req.CancelReservationSlots = make([]string, 0, cancelCount)
	}
	if commitCount > 0 {
		req.CommitSlots = make([]string, 0, commitCount)
	}
	if releaseCount > 0 {
		req.ReleaseSlots = make([]string, 0, releaseCount)
	}
	if configItem != nil {
		req.ConfigUpdate = configItem.req.GetConfigUpdate()
		req.ConfigUpdateVersion = configItem.req.GetConfigUpdateVersion()
	}
	for _, item := range items {
		req.ReserveSlots = append(req.ReserveSlots, item.req.GetReserveSlots()...)
		req.CancelReservationSlots = append(req.CancelReservationSlots, item.req.GetCancelReservationSlots()...)
		req.CommitSlots = append(req.CommitSlots, item.req.GetCommitSlots()...)
		req.ReleaseSlots = append(req.ReleaseSlots, item.req.GetReleaseSlots()...)
	}

	ctx := items[0].ctx
	for _, item := range items {
		if item.ctx.Err() == nil {
			ctx = item.ctx
			break
		}
	}
	res, err := c.ConcurrencyServiceClient.Batch(ctx, req)
	if err == nil && (res == nil || len(res.ReserveSuccess) != reserveCount || len(res.CommitSuccess) != commitCount) {
		err = serviceerror.NewInternal("invalid concurrency batch response")
	}
	if err != nil {
		return clientBatchResults(len(items), nil, err)
	}

	results := make([]clientBatchResult, len(items))
	reserveOffset := 0
	commitOffset := 0
	for i, item := range items {
		itemReserveCount := len(item.req.GetReserveSlots())
		itemCommitCount := len(item.req.GetCommitSlots())
		results[i].res = &fcpb.ConcurrencyBatchResponse{
			ReserveSuccess: slices.Clone(res.ReserveSuccess[reserveOffset : reserveOffset+itemReserveCount]),
			CommitSuccess:  slices.Clone(res.CommitSuccess[commitOffset : commitOffset+itemCommitCount]),
			Generation:     res.Generation,
		}
		reserveOffset += itemReserveCount
		commitOffset += itemCommitCount
	}
	return results
}

func clientBatchResults(size int, res *fcpb.ConcurrencyBatchResponse, err error) []clientBatchResult {
	results := make([]clientBatchResult, size)
	for i := range results {
		results[i] = clientBatchResult{res: res, err: err}
	}
	return results
}
