package history

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestDescribeHistoryHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	controller := shard.NewMockController(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	h := Handler{
		config: &configs.Config{
			NumberOfShards: 10,
		},
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewNoopLogger(),
		controller:        controller,
		namespaceRegistry: namespaceRegistry,
		hostInfoProvider:  hostInfoProvider,
	}

	mockShard1 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(1)).Return(mockShard1, serviceerror.NewShardOwnershipLost("", ""))

	_, err := h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 1,
	})
	assert.Error(t, err)
	var sol *serviceerror.ShardOwnershipLost
	assert.True(t, errors.As(err, &sol))

	mockShard2 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 2,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(2)).Return(mockShard2, nil)
	controller.EXPECT().ShardIDs().Return([]int32{2})
	namespaceRegistry.EXPECT().GetRegistrySize().Return(int64(0), int64(0))
	hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("0.0.0.0"))
	_, err = h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 2,
	})
	assert.NoError(t, err)
}

func TestStartNexusOperation_SystemNexusEndpointPayloadMetadataFlag(t *testing.T) {
	registry := nexus.NewServiceRegistry()
	registry.MustRegister(chasmtests.NewTestServiceNexusService())
	nexusHandler, err := registry.NewHandler()
	require.NoError(t, err)

	h := Handler{
		logger:       log.NewNoopLogger(),
		nexusHandler: nexusHandler,
	}

	testCases := []struct {
		name       string
		operation  string
		expectFlag bool
	}{
		{name: "response with no nested payload", operation: "TestOperation", expectFlag: false},
		{name: "response with nested payload", operation: "TestOperationWithPayload", expectFlag: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := h.StartNexusOperation(context.Background(), &historyservice.StartNexusOperationRequest{
				Request: &nexuspb.StartOperationRequest{
					Service:   "TestService",
					Operation: tc.operation,
					RequestId: "test-request-id",
					Payload:   payload.EncodeString("Temporal"),
				},
			})
			require.NoError(t, err)

			result := resp.GetResponse().GetSyncSuccess().GetPayload()
			require.NotNil(t, result)

			value, ok := result.GetMetadata()[commonnexus.SystemEndpointPayloadMetadataKey]
			if tc.expectFlag {
				require.True(t, ok, "expected %s metadata flag to be set", commonnexus.SystemEndpointPayloadMetadataKey)
				require.Equal(t, "true", string(value))
			} else {
				require.False(t, ok, "expected %s metadata flag to be absent", commonnexus.SystemEndpointPayloadMetadataKey)
			}
		})
	}
}
