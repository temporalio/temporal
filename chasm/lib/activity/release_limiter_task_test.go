package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/streambatcher"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	var requests []*fcpb.ConcurrencyServiceBatchRequest
	handler := newReleaseLimiterTaskHandler(&testConcurrencyServiceClient{
		batch: func(
			_ context.Context,
			request *fcpb.ConcurrencyServiceBatchRequest,
			_ ...grpc.CallOption,
		) (*fcpb.ConcurrencyServiceBatchResponse, error) {
			requests = append(requests, request)
			if request.GetReleaseSlots()[0] == "bad-slot" {
				return nil, releaseErr
			}
			return &fcpb.ConcurrencyServiceBatchResponse{}, nil
		},
	}, &Config{FlowControlClientBatcherOptions: dynamicconfig.GetTypedPropertyFn(streambatcher.BatcherOptions{
		MaxItems: 1,
		IdleTime: time.Minute,
	})}, metrics.NoopMetricsHandler)
	task := &activitypb.ReleaseLimiterTask{Limiters: []*taskqueuespb.LimiterRef{
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "first-key", SlotId: "bad-slot"},
		{LimiterType: enumsspb.LIMITER_TYPE_UNSPECIFIED, Key: "ignored-key", SlotId: "ignored-slot"},
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "second-key", SlotId: "good-slot"},
	}}

	err := handler.Execute(t.Context(), chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
		NamespaceID: "namespace-id",
	}}, chasm.TaskAttributes{}, task)

	require.ErrorIs(t, err, releaseErr)
	require.Equal(t, []*fcpb.ConcurrencyServiceBatchRequest{
		{NamespaceId: "namespace-id", Key: "first-key", ReleaseSlots: []string{"bad-slot"}},
		{NamespaceId: "namespace-id", Key: "second-key", ReleaseSlots: []string{"good-slot"}},
	}, requests)
}

func TestReleaseLatencyPreservesSchedulingTime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		timestamp *timestamppb.Timestamp
		failed    bool
		want      bool
	}{
		{name: "success", timestamp: timestamppb.New(defaultTime), want: true},
		{name: "failed release", timestamp: timestamppb.New(defaultTime), failed: true},
		{name: "legacy task"},
		{name: "future timestamp", timestamp: timestamppb.New(defaultTime.Add(time.Second))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)
			clockSource := clock.NewEventTimeSource().Update(defaultTime.Add(150 * time.Millisecond))
			releaseErr := errors.New("release failed")
			h := newReleaseLimiterTaskHandler(&testConcurrencyServiceClient{batch: func(context.Context, *fcpb.ConcurrencyServiceBatchRequest, ...grpc.CallOption) (*fcpb.ConcurrencyServiceBatchResponse, error) {
				if tc.failed {
					return nil, releaseErr
				}
				return &fcpb.ConcurrencyServiceBatchResponse{}, nil
			}}, &Config{FlowControlClientBatcherOptions: dynamicconfig.GetTypedPropertyFn(streambatcher.BatcherOptions{MaxItems: 1, IdleTime: time.Minute})}, captureHandler)
			h.timeSource = clockSource
			task := &activitypb.ReleaseLimiterTask{ScheduledAt: tc.timestamp, Limiters: []*taskqueuespb.LimiterRef{{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "wholequeue/test/queue/2/0", SlotId: "slot"}}}
			err := h.Execute(t.Context(), chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{NamespaceID: "namespace-id"}}, chasm.TaskAttributes{}, task)
			if tc.failed {
				require.ErrorIs(t, err, releaseErr)
			} else {
				require.NoError(t, err)
			}
			records := capture.Snapshot()["flowcontrol_concurrency_release_latency"]
			if !tc.want {
				require.Empty(t, records)
				return
			}
			require.Len(t, records, 1)
			require.Equal(t, 150*time.Millisecond, records[0].Value)
			require.Equal(t, "namespace-id", records[0].Tags["namespace_id"])
			require.Equal(t, "test/queue", records[0].Tags["taskqueue"])
		})
	}
}

func TestReleaseLimiterTaskSchedulingTimestamp(t *testing.T) {
	t.Parallel()
	cctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return defaultTime }}}
	limiter := &taskqueuespb.LimiterRef{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "wholequeue/queue/2/0", SlotId: "slot"}
	a := &Activity{LastAttempt: chasm.NewDataField(cctx, &activitypb.ActivityAttemptState{Limiters: []*taskqueuespb.LimiterRef{limiter}})}
	a.releaseAttemptLimiters(cctx)
	require.Len(t, cctx.Tasks, 1)
	payload, ok := cctx.Tasks[0].Payload.(*activitypb.ReleaseLimiterTask)
	require.True(t, ok)
	require.Equal(t, defaultTime, payload.GetScheduledAt().AsTime())
	require.Equal(t, []*taskqueuespb.LimiterRef{limiter}, payload.GetLimiters())
}
