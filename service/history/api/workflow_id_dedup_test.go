package api

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

func DisableTestResolveJustStartedDuplicatedWorfklows(t *testing.T) {
	timeSource := clock.NewEventTimeSource()
	now := timeSource.Now()

	testCases := []struct {
		gracePeriod          time.Duration
		currentWorfklowStart time.Time
		expectError          bool
	}{
		{
			gracePeriod:          time.Duration(0 * time.Second),
			currentWorfklowStart: now,
			expectError:          false,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorfklowStart: now,
			expectError:          true,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorfklowStart: now.Add(-2 * time.Second),
			expectError:          false,
		},
	}

	config := tests.NewDynamicConfig()
	mockShard := shard.NewTestContextWithTimeSource(
		gomock.NewController(t),
		&persistencespb.ShardInfo{RangeId: 1},
		config,
		timeSource,
	)

	for _, tc := range testCases {
		config.WorkflowDeduplicationGracePeriod = dynamicconfig.GetDurationPropertyFn(tc.gracePeriod)

		_, err := resolveJustStartedDuplicatedWorfklows(
			mockShard, tc.currentWorfklowStart, "newRunID", "workflowID",
		)

		if tc.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
