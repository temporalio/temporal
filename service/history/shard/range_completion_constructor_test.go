package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// newContext (the constructor used by the production ContextFactory) enables range-completion
// admission exactly when the default store is Cassandra. It also keeps the existing rule that
// Cassandra always gets a single-token ioSemaphore.
func TestNewContext_RangeCompletionSerializationFollowsDefaultStore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		dataStores    map[string]config.DataStore
		defaultStore  string
		ioConcurrency int
		wantSerialize bool
		wantTokens    int
	}{
		{"cassandra", map[string]config.DataStore{"default": {Cassandra: &config.Cassandra{}}}, "default", 1, true, 1},
		{"cassandra forces one token", map[string]config.DataStore{"default": {Cassandra: &config.Cassandra{}}}, "default", 4, true, 1},
		{"sql", map[string]config.DataStore{"default": {SQL: &config.SQL{}}}, "default", 1, false, 1},
		{"sql with concurrency 4", map[string]config.DataStore{"default": {SQL: &config.SQL{}}}, "default", 4, false, 4},
		{"custom datastore", map[string]config.DataStore{"default": {CustomDataStoreConfig: &config.CustomDatastoreConfig{Name: "custom"}}}, "default", 1, false, 1},
		{"cassandra only as non-default store", map[string]config.DataStore{"default": {SQL: &config.SQL{}}, "visibility": {Cassandra: &config.Cassandra{}}}, "default", 1, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
			hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("127.0.0.1:7234")).AnyTimes()

			historyConfig := tests.NewDynamicConfig()
			historyConfig.ShardIOConcurrency = dynamicconfig.GetIntPropertyFn(tc.ioConcurrency)

			shardContext, err := newContext(
				1,   // shardID
				nil, // engine factory
				historyConfig,
				config.Persistence{DefaultStore: tc.defaultStore, DataStores: tc.dataStores},
				nil, // close callback
				log.NewNoopLogger(),
				log.NewNoopLogger(),
				nil, // execution manager
				nil, // shard manager
				nil, // client bean
				nil, // history client
				metrics.NoopMetricsHandler,
				nil, // event logger
				nil, // payload serializer
				clock.NewRealTimeSource(),
				nil, // namespace registry
				nil, // search attributes provider
				nil, // search attributes mapper provider
				nil, // cluster metadata
				nil, // archival metadata
				hostInfoProvider,
				tasks.NewDefaultTaskCategoryRegistry(),
				nil, // events cache
				nil, // state machine registry
				nil, // chasm registry
				nil, // chasm workflow registry
				nil, // endpoint registry
				nil, // callback validator
				NewDefaultHandoverTrackerFactory(),
			)
			require.NoError(t, err)
			defer shardContext.lifecycleCancel()

			require.Equal(t, tc.wantSerialize, shardContext.serializeRangeCompletion)
			for i := 0; i < tc.wantTokens; i++ {
				require.True(t, shardContext.ioSemaphore.TryAcquire(locks.PriorityHigh, 1), "token %d", i)
			}
			require.False(t, shardContext.ioSemaphore.TryAcquire(locks.PriorityHigh, 1), "more than %d tokens", tc.wantTokens)
		})
	}
}
