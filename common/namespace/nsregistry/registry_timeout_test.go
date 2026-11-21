package nsregistry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

func TestRefreshNamespacesPageDeadline(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		earlierDeadline bool
	}{
		{name: "background context"},
		{name: "earlier caller deadline", earlierDeadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.earlierDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			store := NewMockPersistence(gomock.NewController(t))
			r := &registry{persistence: store, metricsHandler: metrics.NoopMetricsHandler}
			var pageContexts []context.Context
			store.EXPECT().ListNamespaces(gomock.Any(), gomock.Any()).DoAndReturn(
				func(pageCtx context.Context, request *persistence.ListNamespacesRequest) (*persistence.ListNamespacesResponse, error) {
					deadline, ok := pageCtx.Deadline()
					require.True(t, ok, "each page must have a deadline")
					require.False(t, deadline.After(time.Now().Add(refreshPageTimeout)))
					if tc.earlierDeadline {
						parentDeadline, _ := ctx.Deadline()
						require.Equal(t, parentDeadline, deadline)
					}
					pageContexts = append(pageContexts, pageCtx)
					if len(pageContexts) == 1 {
						require.Empty(t, request.NextPageToken)
						return &persistence.ListNamespacesResponse{NextPageToken: []byte("next")}, nil
					}
					require.Equal(t, []byte("next"), request.NextPageToken)
					require.ErrorIs(t, pageContexts[0].Err(), context.Canceled)
					return nil, context.DeadlineExceeded
				},
			).Times(2)
			require.ErrorIs(t, r.refreshNamespaces(ctx), context.DeadlineExceeded)
			require.ErrorIs(t, pageContexts[1].Err(), context.Canceled)
		})
	}
}
