// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package scanner

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/tests"
	"go.temporal.io/server/common/resolver"
	tests2 "go.temporal.io/server/service/history/tests"
)

type TearDownFn = func(t testing.TB)

func BenchmarkHistoryScavenger(t *testing.B) {
	for _, c := range []struct {
		Name          string
		StartDatabase func() (cfg *config.SQL, tearDown TearDownFn)
		ClusterName   string
	}{
		{
			Name: "PostgreSQL",
			StartDatabase: func() (cfg *config.SQL, tearDown TearDownFn) {
				cfg = tests.NewPostgreSQLConfig()
				tests.SetupPostgreSQLDatabase(cfg)
				tests.SetupPostgreSQLSchema(cfg)
				return cfg, func(_ testing.TB) {
					tests.TearDownPostgreSQLDatabase(cfg)
				}
			},
			ClusterName: "scavenger-postgresql-test",
		},
		{
			Name: "MySQL",
			StartDatabase: func() (cfg *config.SQL, tearDown TearDownFn) {
				cfg = tests.NewMySQLConfig()
				tests.SetupMySQLDatabase(cfg)
				tests.SetupMySQLSchema(cfg)
				return cfg, func(_ testing.TB) {
					tests.TearDownMySQLDatabase(cfg)
				}
			},
			ClusterName: "scavenger-mysql-test",
		},
		{
			Name: "SQLite/FileBased",
			StartDatabase: func() (cfg *config.SQL, tearDown TearDownFn) {
				cfg = tests.NewSQLiteFileConfig()
				tests.SetupSQLiteDatabase(cfg)
				return cfg, func(t testing.TB) {
					require.NoError(t, os.Remove(cfg.DatabaseName))
				}
			},
			ClusterName: "scavenger-sqlite-test",
		},
		{
			Name: "SQLite/MemoryBased",
			StartDatabase: func() (cfg *config.SQL, tearDown TearDownFn) {
				cfg = tests.NewSQLiteMemoryConfig()
				return cfg, func(_ testing.TB) {

				}
			},
			ClusterName: "scavenger-sqlite-test",
		},
	} {
		t.Run(c.Name, func(t *testing.B) {
			s := new(testsuite.WorkflowTestSuite)
			env := s.NewTestActivityEnvironment()
			env.SetTestTimeout(2 * time.Minute)
			env.RegisterActivity(HistoryScavengerActivity)

			controller := gomock.NewController(t)
			defer controller.Finish()

			historyClient := historyservicemock.NewMockHistoryServiceClient(controller)

			cfg, tearDown := c.StartDatabase()
			defer tearDown(t)

			clusterName := uuid.New()
			factory := sql.NewFactory(
				*cfg,
				resolver.NewNoopResolver(),
				clusterName,
				log.NewNoopLogger(),
			)
			defer factory.Close()

			store, err := factory.NewExecutionStore()
			require.NoError(t, err)
			defer store.Close()

			executionManager := p.NewExecutionManager(
				store,
				serialization.NewSerializer(),
				log.NewNoopLogger(),
				dynamicconfig.GetIntPropertyFn(4*1024*1024),
			)
			defer executionManager.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			start := time.Now()
			// dangling branches have no corresponding workflow execution history
			numDanglingBranches := 1000
			danglingBranches := make(map[string]bool, numDanglingBranches)
			numAttachedBranches := 2000
			attachedBranches := make(map[string]bool, numAttachedBranches)
			for i := 0; i < numDanglingBranches+numAttachedBranches; i++ {
				treeID := uuid.New()
				resp, err := executionManager.NewHistoryBranch(ctx, &p.NewHistoryBranchRequest{
					TreeID: treeID,
				})
				require.NoError(t, err)
				branchToken := resp.BranchToken
				namespaceID := tests2.NamespaceID
				workflowID := tests2.WorkflowID
				runID := uuid.New()
				_, err = executionManager.AppendHistoryNodes(ctx, &p.AppendHistoryNodesRequest{
					ShardID:     1,
					IsNewBranch: true,
					Info:        p.BuildHistoryGarbageCleanupInfo(namespaceID.String(), workflowID, runID),
					BranchToken: branchToken,
					Events: []*historypb.HistoryEvent{
						{
							EventId: 1,
						},
					},
				})
				require.NoError(t, err)
				var retErr error
				if i < numDanglingBranches {
					retErr = serviceerror.NewNotFound("history not found")
					danglingBranches[treeID] = true
				} else {
					retErr = nil
					attachedBranches[treeID] = true
				}
				historyClient.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
					NamespaceId: namespaceID.String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: workflowID,
						RunId:      runID,
					},
				}).Return(nil, retErr)
			}
			t.Logf(
				"spent %v creating %d attached branches and %d dangling branches",
				time.Since(start),
				numAttachedBranches,
				numDanglingBranches,
			)

			scannerCtx := scannerContext{
				cfg: &Config{
					PersistenceMaxQPS: func() int {
						return 9999
					},
					MaxConcurrentActivityExecutionSize: func() int {
						return 9999
					},
					MaxConcurrentWorkflowTaskExecutionSize: func() int {
						return 9999
					},
					MaxConcurrentActivityTaskPollers: func() int {
						return 9999
					},
					MaxConcurrentWorkflowTaskPollers: func() int {
						return 9999
					},
					ExecutionsScannerEnabled: func() bool {
						return false
					},
					HistoryScannerEnabled: func() bool {
						return true
					},
					TaskQueueScannerEnabled: func() bool {
						return false
					},
					HistoryScannerDataMinAge: func() time.Duration {
						return 0
					},
					Persistence: &config.Persistence{
						DefaultStore:     store.GetName(),
						NumHistoryShards: 1,
						DataStores: map[string]config.DataStore{
							config.StoreTypeNoSQL: {},
							config.StoreTypeSQL: {
								SQL: cfg,
							},
						},
					},
				},
				logger:           log.NewNoopLogger(),
				metricsClient:    metrics.NoopClient,
				executionManager: executionManager,
				taskManager:      p.NewMockTaskManager(controller),
				historyClient:    historyClient,
			}
			ctx = context.WithValue(ctx, scannerContextKey, scannerCtx)
			env.SetWorkerOptions(worker.Options{
				BackgroundActivityContext: ctx,
			})

			start = time.Now()
			t.ResetTimer()
			t.StartTimer()
			_, err = env.ExecuteActivity(HistoryScavengerActivity)
			require.NoError(t, err)
			t.StopTimer()
			t.Logf("spent %v running the history scavenger", time.Since(start))

			var nextPageToken []byte
			start = time.Now()
			numRemainingBranches := 0
			for {
				resp, err := store.GetAllHistoryTreeBranches(ctx, &p.GetAllHistoryTreeBranchesRequest{
					NextPageToken: nextPageToken,
					PageSize:      100,
				})
				require.NoError(t, err)
				nextPageToken = resp.NextPageToken
				for _, b := range resp.Branches {
					numRemainingBranches++
					treeID := b.TreeID
					if assert.False(t, danglingBranches[treeID], "no dangling branch should remain") {
						delete(attachedBranches, treeID)
					}
				}
				if len(nextPageToken) == 0 {
					break
				}
			}
			t.Logf(
				"spent %v iterating through the remaining %d history branches",
				time.Since(start),
				numRemainingBranches,
			)
			assert.Empty(t, attachedBranches, "all attached branches should remain")
		})
	}
}
