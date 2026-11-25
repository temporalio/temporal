package migration

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestShardedActivitiesRegisterSeparately(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(&activities{})
	require.NotPanics(t, func() {
		env.RegisterActivityWithOptions(
			&shardedActivities{activities: &activities{}},
			activity.RegisterOptions{Name: shardedActivityPrefix},
		)
	})

	result, err := env.ExecuteActivity(shardedReplicateBatchActivityName, &shardedBatchReq{})
	require.NoError(t, err)
	var batchResult replicateBatchResult
	require.NoError(t, result.Get(&batchResult))

	_, err = env.ExecuteActivity("ReplicateBatch", &shardedBatchReq{})
	require.Error(t, err)
}
