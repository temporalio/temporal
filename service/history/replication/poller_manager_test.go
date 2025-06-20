package replication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPollingShardIds(t *testing.T) {
	testCases := []struct {
		shardID          int32
		remoteShardCount int32
		localShardCount  int32
		expectedShardIDs []int32
	}{
		{
			1,
			4,
			4,
			[]int32{1},
		},
		{
			1,
			2,
			4,
			[]int32{1},
		},
		{
			3,
			2,
			4,
			nil,
		},
		{
			1,
			16,
			4,
			[]int32{1, 5, 9, 13},
		},
		{
			4,
			16,
			4,
			[]int32{4, 8, 12, 16},
		},
		{
			4,
			17,
			4,
			[]int32{4, 8, 12, 16},
		},
		{
			1,
			17,
			4,
			[]int32{1, 5, 9, 13, 17},
		},
	}
	for idx, tt := range testCases {
		t.Run(fmt.Sprintf("Testcase %d", idx), func(t *testing.T) {
			shardIDs := generateShardIDs(tt.shardID, tt.localShardCount, tt.remoteShardCount)
			assert.Equal(t, tt.expectedShardIDs, shardIDs)
		})
	}
}
