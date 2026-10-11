package scanner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/worker/scanner/history"
	"go.uber.org/mock/gomock"
)

func TestHistoryScavengerCheckpointReplaysUnfinishedPage(t *testing.T) {
	t.Parallel()
	db := persistence.NewMockExecutionManager(gomock.NewController(t))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env := newCheckpointActivityEnvironment(ctx, db)
	env.SetHeartbeatDetails(history.ScavengerHeartbeatDetails{
		CurrentPage: 7, ErrorCount: 3, NextPageToken: []byte("current"),
	})
	var captured history.ScavengerHeartbeatDetails
	var captureErr error
	var first sync.Once
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, values converter.EncodedValues) {
		first.Do(func() {
			captureErr = values.Get(&captured)
			cancel()
		})
	})
	rows := []persistence.HistoryBranchDetail{{Info: "invalid-first"}, {Info: "invalid-second"}}
	db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
		PageSize: 100, NextPageToken: []byte("current"),
	}).Return(&persistence.GetAllHistoryTreeBranchesResponse{Branches: rows, NextPageToken: []byte("next")}, nil)
	_, _ = env.ExecuteActivity(historyScavengerActivityName)
	require.NoError(t, captureErr)
	require.Equal(t, history.ScavengerHeartbeatDetails{
		CurrentPage: 8, ErrorCount: 3, NextPageToken: []byte("current"),
	}, captured)

	// Recovery uses the SDK heartbeat, not the activity's returned result.
	retryEnv := newCheckpointActivityEnvironment(t.Context(), db)
	retryEnv.SetHeartbeatDetails(captured)
	gomock.InOrder(
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
			PageSize: 100, NextPageToken: []byte("current"),
		}).Return(&persistence.GetAllHistoryTreeBranchesResponse{Branches: rows, NextPageToken: []byte("next")}, nil),
		db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
			PageSize: 100, NextPageToken: []byte("next"),
		}).Return(&persistence.GetAllHistoryTreeBranchesResponse{}, nil),
	)
	result, err := retryEnv.ExecuteActivity(historyScavengerActivityName)
	require.NoError(t, err)
	var final history.ScavengerHeartbeatDetails
	require.NoError(t, result.Get(&final))
	require.Equal(t, history.ScavengerHeartbeatDetails{CurrentPage: 10, ErrorCount: 5}, final)
}

func TestHistoryScavengerCheckpointEmptyPage(t *testing.T) {
	t.Parallel()
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal=%v", terminal), func(t *testing.T) {
			t.Parallel()
			db := persistence.NewMockExecutionManager(gomock.NewController(t))
			env := newCheckpointActivityEnvironment(t.Context(), db)
			env.SetHeartbeatDetails(history.ScavengerHeartbeatDetails{CurrentPage: 7, NextPageToken: []byte("current")})
			var captured history.ScavengerHeartbeatDetails
			var captureErr error
			var first sync.Once
			env.SetOnActivityHeartbeatListener(func(_ *activity.Info, values converter.EncodedValues) {
				first.Do(func() { captureErr = values.Get(&captured) })
			})
			var next []byte
			if !terminal {
				next = []byte("next")
				db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
					PageSize: 100, NextPageToken: next,
				}).Return(&persistence.GetAllHistoryTreeBranchesResponse{}, nil)
			}
			db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
				PageSize: 100, NextPageToken: []byte("current"),
			}).Return(&persistence.GetAllHistoryTreeBranchesResponse{NextPageToken: next}, nil)
			_, err := env.ExecuteActivity(historyScavengerActivityName)
			require.NoError(t, err)
			require.NoError(t, captureErr)
			require.Equal(t, history.ScavengerHeartbeatDetails{CurrentPage: 8, NextPageToken: next}, captured)

			retryEnv := newCheckpointActivityEnvironment(t.Context(), db)
			retryEnv.SetHeartbeatDetails(captured)
			db.EXPECT().GetAllHistoryTreeBranches(gomock.Any(), &persistence.GetAllHistoryTreeBranchesRequest{
				PageSize: 100, NextPageToken: next,
			}).Return(&persistence.GetAllHistoryTreeBranchesResponse{}, nil)
			result, err := retryEnv.ExecuteActivity(historyScavengerActivityName)
			require.NoError(t, err)
			var final history.ScavengerHeartbeatDetails
			require.NoError(t, result.Get(&final))
			require.Equal(t, history.ScavengerHeartbeatDetails{CurrentPage: 9}, final)
		})
	}
}

func newCheckpointActivityEnvironment(ctx context.Context, db persistence.ExecutionManager) *testsuite.TestActivityEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: context.WithValue(ctx, scannerContextKey, scannerContext{
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
	})})
	return env
}
