package api

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestResolveDuplicateWorkflowStart(t *testing.T) {
	timeSource := clock.NewEventTimeSource()
	now := timeSource.Now()

	testCases := []struct {
		gracePeriod          time.Duration
		currentWorkflowStart time.Time
		expectError          bool
	}{
		{
			gracePeriod:          time.Duration(0 * time.Second),
			currentWorkflowStart: now,
			expectError:          false,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorkflowStart: now,
			expectError:          true,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorkflowStart: now.Add(-2 * time.Second),
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

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		&persistencespb.NamespaceConfig{},
		"target_cluster",
	)

	for _, tc := range testCases {
		config.WorkflowIdReuseMinimalInterval = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(tc.gracePeriod)
		workflowKey := definition.WorkflowKey{
			NamespaceID: uuid.New().String(),
			WorkflowID:  "workflowID",
			RunID:       "oldRunID",
		}
		_, err := resolveDuplicateWorkflowStart(mockShard, tc.currentWorkflowStart, workflowKey, namespaceEntry, "newRunID", nil, false)

		if tc.expectError {
			assert.Error(t, err)
			var resourceErr *serviceerror.ResourceExhausted
			assert.ErrorAs(t, err, &resourceErr)

		} else {
			assert.NoError(t, err)
		}
	}
}
