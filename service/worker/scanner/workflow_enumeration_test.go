package scanner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
)

func TestHistoryScannerRetriesEnumerationFromHeartbeat(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	controller := gomock.NewController(t)
	db := persistence.NewMockExecutionManager(controller)
	scannerCtx := scannerContext{
		cfg: &Config{
			Persistence:                   &config.Persistence{NumHistoryShards: 512},
			PersistenceMaxQPS:             dynamicconfig.GetIntPropertyFn(100),
			HistoryScannerDataMinAge:      dynamicconfig.GetDurationPropertyFn(time.Hour),
			HistoryScannerVerifyRetention: dynamicconfig.GetBoolPropertyFn(false),
			ExecutionDataDurationBuffer:   dynamicconfig.GetDurationPropertyFn(time.Second),
		},
		executionManager: db,
		logger:           log.NewNoopLogger(),
		metricsHandler:   metrics.NoopMetricsHandler,
		serializer:       serialization.NewSerializer(),
	}
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: context.WithValue(t.Context(), scannerContextKey, scannerCtx)})
	env.SetTestTimeout(5 * time.Second)
	env.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})

	// Bound the test's retries without changing the scanner's retry policy.
	opts := activityOptions
	retry := activityRetryPolicy
	retry.MaximumAttempts = 2
	retry.InitialInterval = time.Millisecond
	retry.MaximumInterval = time.Millisecond
	opts.RetryPolicy = &retry
	opts.StartToCloseTimeout = 2 * time.Second
	opts.HeartbeatTimeout = time.Second
	testWorkflow := func(ctx workflow.Context) error {
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), historyScavengerActivityName).Get(ctx, nil)
	}
	env.RegisterWorkflow(testWorkflow)
	type observation struct {
		attempt      int32
		token        string
		hasHeartbeat bool
	}
	var mu sync.Mutex
	var observed []observation
	failure := errors.New("next history page unavailable")
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *persistence.GetAllHistoryTreeBranchesRequest) (*persistence.GetAllHistoryTreeBranchesResponse, error) {
			attempt := activity.GetInfo(ctx).Attempt
			mu.Lock()
			observed = append(observed, observation{attempt, string(req.NextPageToken), activity.HasHeartbeatDetails(ctx)})
			mu.Unlock()
			switch string(req.NextPageToken) {
			case "":
				// The loader heartbeats even when cleanup metadata is invalid.
				return &persistence.GetAllHistoryTreeBranchesResponse{
					Branches:      []persistence.HistoryBranchDetail{{Info: "invalid-cleanup-info"}},
					NextPageToken: []byte("next"),
				}, nil
			case "next":
				if attempt == 1 {
					return nil, failure
				}
				return &persistence.GetAllHistoryTreeBranchesResponse{}, nil
			default:
				return nil, failure
			}
		}).AnyTimes()

	env.ExecuteWorkflow(testWorkflow)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	mu.Lock()
	defer mu.Unlock()
	// Retry recovery uses SDK-carried heartbeat details, never the failed result.
	require.Equal(t, []observation{{1, "", false}, {1, "next", false}, {2, "next", true}}, observed)
}
