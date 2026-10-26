package cassandra

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskQueueMetadataTTL(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	for _, tc := range []struct {
		name     string
		assigned bool
		expiry   *timestamppb.Timestamp
		ttl      int64
		invalid  bool
	}{
		{name: "assigned metadata has no ttl", assigned: true},
		{name: "assignment removes existing ttl", assigned: true, expiry: timestamppb.New(now.Add(time.Hour))},
		{name: "ordinary sticky requires expiry", invalid: true},
		{name: "ordinary sticky keeps ttl", expiry: timestamppb.New(now.Add(time.Hour)), ttl: 3600},
		{name: "fractional ttl rounds up", expiry: timestamppb.New(now.Add(time.Second + time.Millisecond)), ttl: 2},
		{name: "ttl is capped", expiry: timestamppb.New(now.Add(time.Duration(maxCassandraTTL+3600) * time.Second)), ttl: maxCassandraTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ttl, err := taskQueueMetadataTTL(&persistence.InternalUpdateTaskQueueRequest{SingleSessionAssigned: tc.assigned, ExpiryTime: tc.expiry}, now)
			if tc.invalid {
				require.ErrorContains(t, err, "ExpiryTime cannot be nil")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.ttl, ttl)
		})
	}
}
