package ringpop

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

func TestUpsertMyMembershipDeadline(t *testing.T) {
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
			store := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
			monitor := &monitor{metadataManager: store}
			request := &persistence.UpsertClusterMembershipRequest{}
			var callCtx context.Context
			store.EXPECT().UpsertClusterMembership(gomock.Any(), request).DoAndReturn(
				func(ctxFromStore context.Context, _ *persistence.UpsertClusterMembershipRequest) error {
					callCtx = ctxFromStore
					deadline, ok := callCtx.Deadline()
					require.True(t, ok, "membership heartbeats must have a deadline")
					require.False(t, deadline.After(time.Now().Add(upsertMembershipTimeout)))
					if tc.earlierDeadline {
						parentDeadline, _ := ctx.Deadline()
						require.Equal(t, parentDeadline, deadline)
					}
					return context.DeadlineExceeded
				},
			)
			require.ErrorIs(t, monitor.upsertMyMembership(ctx, request), context.DeadlineExceeded)
			require.ErrorIs(t, callCtx.Err(), context.Canceled)
		})
	}
}
