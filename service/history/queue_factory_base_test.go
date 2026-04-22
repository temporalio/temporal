package history

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
)

// TestQueueModule_ArchivalQueue tests that the archival queue is created if and only if the task category exists.
func TestQueueModule_ArchivalQueue(t *testing.T) {
	for _, c := range []moduleTestCase{
		{
			Name:                "Archival disabled",
			CategoryExists:      false,
			ExpectArchivalQueue: false,
		},
		{
			Name:                "Archival enabled",
			CategoryExists:      true,
			ExpectArchivalQueue: true,
		},
	} {
		c := c
		t.Run(c.Name, c.Run)
	}
}

// moduleTestCase is a test case for the QueueModule.
type moduleTestCase struct {
	Name                string
	ExpectArchivalQueue bool
	CategoryExists      bool
}

// Run runs the test case.
func (c *moduleTestCase) Run(t *testing.T) {

	controller := gomock.NewController(t)
	dependencies := getModuleDependencies(controller, c)
	var factories []QueueFactory

	app := fx.New(
		dependencies,
		QueueModule,
		fx.Invoke(func(params QueueFactoriesLifetimeHookParams) {
			factories = params.Factories
		}),
	)

	require.NoError(t, app.Err())
	require.NotNil(t, factories)
	var (
		txq QueueFactory
		tiq QueueFactory
		viq QueueFactory
		aq  QueueFactory
	)
	for _, f := range factories {
		switch f.(type) {
		case *transferQueueFactory:
			require.Nil(t, txq)
			txq = f
		case *timerQueueFactory:
			require.Nil(t, tiq)
			tiq = f
		case *visibilityQueueFactory:
			require.Nil(t, viq)
			viq = f
		case *archivalQueueFactory:
			require.Nil(t, aq)
			aq = f
		}
	}
	require.NotNil(t, txq)
	require.NotNil(t, tiq)
	require.NotNil(t, viq)
	if c.ExpectArchivalQueue {
		require.NotNil(t, aq)
	} else {
		require.Nil(t, aq)
	}
}

// getModuleDependencies returns an fx.Option that provides all the dependencies needed for the queue module.
func getModuleDependencies(controller *gomock.Controller, c *moduleTestCase) fx.Option {
	cfg := configs.NewConfig(
		dynamicconfig.NewNoopCollection(),
		1,
	)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("module-test-cluster-name").AnyTimes()
	lazyLoadedOwnershipBasedQuotaScaler := shard.LazyLoadedOwnershipBasedQuotaScaler{
		Value: &atomic.Value{},
	}
	registry := tasks.NewDefaultTaskCategoryRegistry()
	if c.CategoryExists {
		registry.AddCategory(tasks.CategoryArchival)
	}
	serializer := serialization.NewSerializer()
	historyFetcher := eventhandler.NewMockHistoryPaginatedFetcher(controller)
	return fx.Supply(
		unusedDependencies{},
		cfg,
		fx.Annotate(registry, fx.As(new(tasks.TaskCategoryRegistry))),
		fx.Annotate(metrics.NoopMetricsHandler, fx.As(new(metrics.Handler))),
		fx.Annotate(log.NewTestLogger(), fx.As(new(log.SnTaggedLogger))),
		fx.Annotate(clusterMetadata, fx.As(new(cluster.Metadata))),
		lazyLoadedOwnershipBasedQuotaScaler,
		fx.Annotate(serializer, fx.As(new(serialization.Serializer))),
		fx.Annotate(historyFetcher, fx.As(new(eventhandler.HistoryPaginatedFetcher))),
		fx.Annotate(telemetry.NoopTracerProvider, fx.As(new(trace.TracerProvider))),
	)
}

// This is a struct that provides nil implementations of all the dependencies needed for the queue module that are not
// actually used.
type unusedDependencies struct {
	fx.Out

	clock.TimeSource
	membership.ServiceResolver
	namespace.Registry
	client.Bean
	sdk.ClientFactory
	resource.MatchingRawClient
	resource.MatchingClient
	resource.HistoryRawClient
	manager.VisibilityManager
	archival.Archiver
	workflow.RelocatableAttributesFetcher
	persistence.HistoryTaskQueueManager
	cache.Cache
	chasm.Engine
	ChasmRegistry *chasm.Registry
	worker_versioning.VersionMembershipCache
}

func TestNewHostRateLimiterRateFn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		hostRPS        int
		persistenceRPS int
		ratio          float64
		expected       float64
	}{
		{
			name:           "explicit host cap wins",
			hostRPS:        50,
			persistenceRPS: 9000,
			ratio:          0.3,
			expected:       50,
		},
		{
			name:           "falls back to persistence-derived rate",
			hostRPS:        0,
			persistenceRPS: 9000,
			ratio:          0.3,
			expected:       2700,
		},
		{
			name:           "non positive persistence leaves no fallback cap",
			hostRPS:        0,
			persistenceRPS: 0,
			ratio:          0.3,
			expected:       0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hostRPS := tc.hostRPS
			persistenceRPS := tc.persistenceRPS
			rateFn := NewHostRateLimiterRateFn(
				func() int { return hostRPS },
				func() int { return persistenceRPS },
				tc.ratio,
			)

			require.InDelta(t, tc.expected, rateFn(), 0.001)
		})
	}
}

func TestNewHostReaderRateLimiter_NoopsWhenNoHostFallbackCapExists(t *testing.T) {
	t.Parallel()

	limiter := newHostReaderRateLimiter(
		func() int { return 0 },
		func() int { return 0 },
		0.3,
		1,
	)

	request := quotas.NewRequest(
		"",
		1,
		strconv.FormatInt(common.DefaultQueueReaderID, 10),
		"",
		0,
		"",
	)

	require.True(t, limiter.Allow(time.Now(), request))
	require.Equal(t, quotas.NoopReservation, limiter.Reserve(time.Now(), request))
	require.NoError(t, limiter.Wait(context.Background(), request))
}

func TestNewHostReaderRateLimiter_TracksRuntimeTransitions(t *testing.T) {
	t.Parallel()

	persistenceRPS := 0
	limiter := newHostReaderRateLimiter(
		func() int { return 0 },
		func() int { return persistenceRPS },
		0.3,
		1,
	)

	request := quotas.NewRequest(
		"",
		1,
		strconv.FormatInt(common.DefaultQueueReaderID, 10),
		"",
		0,
		"",
	)

	require.Equal(t, quotas.NoopReservation, limiter.Reserve(time.Now(), request))
	require.NoError(t, limiter.Wait(context.Background(), request))

	persistenceRPS = 10
	reservation := limiter.Reserve(time.Now(), request)
	require.NotEqual(t, quotas.NoopReservation, reservation)
	require.True(t, reservation.OK())
	require.NoError(t, limiter.Wait(context.Background(), request))

	persistenceRPS = 0
	require.Equal(t, quotas.NoopReservation, limiter.Reserve(time.Now(), request))
	require.NoError(t, limiter.Wait(context.Background(), request))
}
