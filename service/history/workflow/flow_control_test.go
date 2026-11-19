package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/definition"
)

func TestNewReleaseLimiterTask(t *testing.T) {
	workflowKey := definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")
	concurrencyLimiter1 := &taskqueuespb.LimiterRef{
		LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY,
		Key:         "concurrency-1",
		SlotId:      "slot-1",
	}
	concurrencyLimiter2 := &taskqueuespb.LimiterRef{
		LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY,
		Key:         "concurrency-2",
		SlotId:      "slot-2",
	}

	task := newReleaseLimiterTask(workflowKey, []*taskqueuespb.LimiterRef{
		nil,
		{Key: "unspecified"},
		concurrencyLimiter1,
		{LimiterType: enumsspb.LimiterType(100), Key: "unknown"},
		concurrencyLimiter2,
	})

	require.NotNil(t, task)
	require.Equal(t, workflowKey, task.WorkflowKey)
	require.Equal(t, []*taskqueuespb.LimiterRef{concurrencyLimiter1, concurrencyLimiter2}, task.Limiters)
}

func TestNewReleaseLimiterTask_NoReleaseNeeded(t *testing.T) {
	task := newReleaseLimiterTask(
		definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"),
		[]*taskqueuespb.LimiterRef{{Key: "unspecified"}},
	)

	require.Nil(t, task)
}
