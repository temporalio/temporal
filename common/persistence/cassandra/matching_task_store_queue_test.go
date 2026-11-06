package cassandra

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

/*
TestTaskQueueMetadataTTL tests how metadata expiry becomes a Cassandra TTL:
1. Verify omitted expiry produces no TTL and a future expiry preserves its remaining seconds.
2. Verify fractional seconds round up and oversized TTL values are capped.
*/
func TestTaskQueueMetadataTTL(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	for _, tc := range []struct {
		name   string
		expiry *timestamppb.Timestamp
		ttl    int64
	}{
		{name: "no expiry has no ttl"},
		{name: "ordinary sticky keeps ttl", expiry: timestamppb.New(now.Add(time.Hour)), ttl: 3600},
		{name: "fractional ttl rounds up", expiry: timestamppb.New(now.Add(time.Second + time.Millisecond)), ttl: 2},
		{name: "ttl is capped", expiry: timestamppb.New(now.Add(time.Duration(maxCassandraTTL+3600) * time.Second)), ttl: maxCassandraTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.ttl, taskQueueMetadataTTL(tc.expiry, now))
		})
	}
}
