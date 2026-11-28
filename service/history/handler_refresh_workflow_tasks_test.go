package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestRefreshWorkflowTasks_MinNamespaceConfigVersion(t *testing.T) {
	t.Parallel()

	const (
		nsID          = namespace.ID("ns-id")
		configVersion = int64(42)
	)

	testCases := []struct {
		name             string
		minConfigVersion int64
		state            enumspb.NamespaceState
		registryErr      error
		wantRefresh      bool
		// wantErrAs is a pointer to the expected error type; nil means no error.
		wantErrAs any
	}{
		{name: "no minimum", minConfigVersion: 0, wantRefresh: true},
		{name: "at minimum", minConfigVersion: configVersion, state: enumspb.NAMESPACE_STATE_REGISTERED, wantRefresh: true},
		{name: "above minimum", minConfigVersion: configVersion - 1, state: enumspb.NAMESPACE_STATE_REGISTERED, wantRefresh: true},
		{name: "registry behind", minConfigVersion: configVersion + 1, state: enumspb.NAMESPACE_STATE_SOFT_DELETED, wantErrAs: new(*serviceerror.Unavailable)},
		{name: "not registered", minConfigVersion: configVersion, state: enumspb.NAMESPACE_STATE_SOFT_DELETED, wantErrAs: new(*serviceerror.FailedPrecondition)},
		{name: "namespace not found", minConfigVersion: configVersion, registryErr: serviceerror.NewNamespaceNotFound("ns"), wantErrAs: new(*serviceerror.NamespaceNotFound)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			controller := shard.NewMockController(ctrl)
			registry := namespace.NewMockRegistry(ctrl)
			engine := historyi.NewMockEngine(ctrl)
			shardContext := shard.NewTestContext(
				ctrl,
				&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
				tests.NewDynamicConfig(),
			)
			shardContext.SetEngineForTesting(engine)
			h := Handler{
				config:            tests.NewDynamicConfig(),
				metricsHandler:    metrics.NoopMetricsHandler,
				logger:            log.NewNoopLogger(),
				controller:        controller,
				namespaceRegistry: registry,
			}

			execution := &commonpb.WorkflowExecution{WorkflowId: "wf-id", RunId: "run-id"}
			controller.EXPECT().GetShardByNamespaceWorkflow(nsID, "wf-id").Return(shardContext, nil)
			if tc.minConfigVersion != 0 {
				if tc.registryErr != nil {
					registry.EXPECT().GetNamespaceByID(nsID).Return(nil, tc.registryErr)
				} else {
					registry.EXPECT().GetNamespaceByID(nsID).Return(namespaceAtVersion(t, nsID, tc.state, configVersion), nil)
				}
			}
			if tc.wantRefresh {
				engine.EXPECT().RefreshWorkflowTasks(gomock.Any(), nsID, execution, chasm.WorkflowArchetypeID).Return(nil)
			}

			_, err := h.RefreshWorkflowTasks(context.Background(), &historyservice.RefreshWorkflowTasksRequest{
				NamespaceId: nsID.String(),
				ArchetypeId: chasm.WorkflowArchetypeID,
				Request: &adminservice.RefreshWorkflowTasksRequest{
					NamespaceId:               nsID.String(),
					Execution:                 execution,
					MinNamespaceConfigVersion: tc.minConfigVersion,
				},
			})
			if tc.wantErrAs == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorAs(t, err, tc.wantErrAs)
		})
	}
}

func TestRefreshWorkflowTasks_RestoreRefreshRateLimit(t *testing.T) {
	t.Parallel()

	const (
		nsID          = namespace.ID("ns-id")
		configVersion = int64(42)
	)

	testCases := []struct {
		name             string
		minConfigVersion int64
		enabled          bool
		shadowMode       bool
		// allow is the limiter's answer; nil means the limiter isn't asked.
		allow       *bool
		wantRefresh bool
	}{
		{name: "admitted", minConfigVersion: configVersion, enabled: true, allow: new(true), wantRefresh: true},
		{name: "throttled", minConfigVersion: configVersion, enabled: true, allow: new(false)},
		{name: "throttled in shadow mode", minConfigVersion: configVersion, enabled: true, shadowMode: true, allow: new(false), wantRefresh: true},
		{name: "limiter off", minConfigVersion: configVersion, wantRefresh: true},
		{name: "not a restore refresh", minConfigVersion: 0, enabled: true, wantRefresh: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			controller := shard.NewMockController(ctrl)
			registry := namespace.NewMockRegistry(ctrl)
			engine := historyi.NewMockEngine(ctrl)
			limiter := quotas.NewMockRequestRateLimiter(ctrl)
			shardContext := shard.NewTestContext(
				ctrl,
				&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
				tests.NewDynamicConfig(),
			)
			shardContext.SetEngineForTesting(engine)
			config := tests.NewDynamicConfig()
			config.TaskSchedulerEnableRateLimiter = dynamicconfig.GetBoolPropertyFn(tc.enabled)
			config.TaskSchedulerEnableRateLimiterShadowMode = dynamicconfig.GetBoolPropertyFn(tc.shadowMode)
			h := Handler{
				config:               config,
				metricsHandler:       metrics.NoopMetricsHandler,
				logger:               log.NewNoopLogger(),
				controller:           controller,
				namespaceRegistry:    registry,
				schedulerRateLimiter: limiter,
			}

			execution := &commonpb.WorkflowExecution{WorkflowId: "wf-id", RunId: "run-id"}
			controller.EXPECT().GetShardByNamespaceWorkflow(nsID, "wf-id").Return(shardContext, nil)
			registry.EXPECT().GetNamespaceByID(nsID).Return(namespaceAtVersion(t, nsID, enumspb.NAMESPACE_STATE_REGISTERED, configVersion), nil).AnyTimes()
			if tc.allow != nil {
				limiter.EXPECT().Allow(gomock.Any(), gomock.Any()).DoAndReturn(func(_ time.Time, request quotas.Request) bool {
					// The lowest priority, one token, charged to the namespace.
					require.Equal(t, headers.CallerTypePreemptable, request.CallerType)
					require.Equal(t, 1, request.Token)
					require.Equal(t, "ns", request.Caller)
					return *tc.allow
				})
			}
			if tc.wantRefresh {
				engine.EXPECT().RefreshWorkflowTasks(gomock.Any(), nsID, execution, chasm.WorkflowArchetypeID).Return(nil)
			}

			_, err := h.RefreshWorkflowTasks(context.Background(), &historyservice.RefreshWorkflowTasksRequest{
				NamespaceId: nsID.String(),
				ArchetypeId: chasm.WorkflowArchetypeID,
				Request: &adminservice.RefreshWorkflowTasksRequest{
					NamespaceId:               nsID.String(),
					Execution:                 execution,
					MinNamespaceConfigVersion: tc.minConfigVersion,
				},
			})
			if tc.wantRefresh {
				require.NoError(t, err)
				return
			}
			var resourceExhausted *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resourceExhausted)
			require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhausted.Scope)
		})
	}
}

func namespaceAtVersion(t *testing.T, id namespace.ID, state enumspb.NamespaceState, configVersion int64) *namespace.Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: id.String(), Name: "ns", State: state},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		ConfigVersion: configVersion,
	}
	ns, err := namespace.FromPersistentState(detail, namespace.NewDefaultReplicationResolverFactory()(detail))
	require.NoError(t, err)
	return ns
}
