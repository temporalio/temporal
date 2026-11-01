package replication

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestSelectStreamImplementation(t *testing.T) {
	laneRule := func(remoteCluster string, shardIDs []int32, shardPercent int) dynamicconfig.ReplicationStreamImplementationRule {
		return dynamicconfig.ReplicationStreamImplementationRule{
			RemoteCluster:  remoteCluster,
			Implementation: streamImplementationLane,
			ShardIDs:       shardIDs,
			ShardPercent:   shardPercent,
		}
	}
	testCases := []struct {
		name          string
		rules         []dynamicconfig.ReplicationStreamImplementationRule
		shardID       int32
		expected      string
		expectInvalid bool
	}{
		{name: "NoRules", shardID: 1, expected: streamImplementationLegacy},
		{name: "ShardIDMatches", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", []int32{3, 7}, 0)}, shardID: 7, expected: streamImplementationLane},
		{name: "ShardIDDoesNotMatch", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", []int32{3}, 0)}, shardID: 7, expected: streamImplementationLegacy},
		{name: "OtherRemoteCluster", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("other", []int32{7}, 0)}, shardID: 7, expected: streamImplementationLegacy},
		{name: "ShardPercentIncludesShard", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 10)}, shardID: 10, expected: streamImplementationLane},
		{name: "ShardPercentExcludesShard", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 10)}, shardID: 11, expected: streamImplementationLegacy},
		{name: "ShardPercentRepeatsEvery100Shards", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 10)}, shardID: 110, expected: streamImplementationLane},
		{name: "ShardPercent100", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 100)}, shardID: 512, expected: streamImplementationLane},
		{
			name: "ExplicitLegacy",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				{RemoteCluster: "remote", Implementation: streamImplementationLegacy, ShardPercent: 100},
			},
			shardID:  7,
			expected: streamImplementationLegacy,
		},
		{name: "EmptyRemoteCluster", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("", []int32{7}, 0)}, shardID: 7, expected: streamImplementationLegacy, expectInvalid: true},
		{
			name: "DuplicateRemoteCluster",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				laneRule("remote", []int32{7}, 0),
				laneRule("remote", []int32{8}, 0),
			},
			shardID:       7,
			expected:      streamImplementationLegacy,
			expectInvalid: true,
		},
		{
			name: "UnknownImplementation",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				{RemoteCluster: "remote", Implementation: "v2", ShardIDs: []int32{7}},
			},
			shardID:       7,
			expected:      streamImplementationLegacy,
			expectInvalid: true,
		},
		{name: "NoShardSelection", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 0)}, shardID: 7, expected: streamImplementationLegacy, expectInvalid: true},
		{name: "BothShardSelections", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", []int32{7}, 10)}, shardID: 7, expected: streamImplementationLegacy, expectInvalid: true},
		{name: "ShardPercentAbove100", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 101)}, shardID: 7, expected: streamImplementationLegacy, expectInvalid: true},
		{name: "NegativeShardPercent", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, -1)}, shardID: 7, expected: streamImplementationLegacy, expectInvalid: true},
		{name: "InvalidShardID", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", []int32{0, 7}, 0)}, shardID: 7, expected: streamImplementationLegacy, expectInvalid: true},
		{
			name: "InvalidRuleForOtherClusterSelectsLegacy",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				laneRule("remote", []int32{7}, 0),
				laneRule("other", nil, 0),
			},
			shardID:       7,
			expected:      streamImplementationLegacy,
			expectInvalid: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			implementation, err := selectStreamImplementation(tc.rules, "remote", tc.shardID)
			require.Equal(t, tc.expected, implementation)
			if tc.expectInvalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSelectedStreamIsValid(t *testing.T) {
	testCases := []struct {
		name        string
		selected    string
		streamValid bool
		expected    bool
	}{
		{name: "SameImplementation", selected: streamImplementationLegacy, streamValid: true, expected: true},
		{name: "StreamInvalid", selected: streamImplementationLegacy, streamValid: false, expected: false},
		{name: "ImplementationChanged", selected: streamImplementationLane, streamValid: true, expected: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			sender := NewMockManagedStreamSender(ctrl)
			sender.EXPECT().IsValid().Return(tc.streamValid).AnyTimes()
			receiver := NewMockManagedStreamReceiver(ctrl)
			receiver.EXPECT().IsValid().Return(tc.streamValid).AnyTimes()
			selectImplementation := func() string { return tc.selected }

			selectedSender := &selectedStreamSender{
				ManagedStreamSender:  sender,
				implementation:       streamImplementationLegacy,
				selectImplementation: selectImplementation,
			}
			selectedReceiver := &selectedStreamReceiver{
				ManagedStreamReceiver: receiver,
				implementation:        streamImplementationLegacy,
				selectImplementation:  selectImplementation,
			}
			require.Equal(t, tc.expected, selectedSender.IsValid())
			require.Equal(t, tc.expected, selectedReceiver.IsValid())
		})
	}
}

func TestStreamSenderFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	dc := dynamicconfig.NewMemoryClient()
	config := configs.NewConfig(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), 1)
	factory := NewStreamSenderFactory()
	newSender := func() *selectedStreamSender {
		sender := factory.NewStreamSender(
			nil,
			shardContext,
			nil,
			nil,
			nil,
			"remote",
			1,
			NewClusterShardKey(2, 1),
			NewClusterShardKey(1, 7),
			config,
		)
		return sender.(*selectedStreamSender)
	}

	legacySender := newSender()
	require.IsType(t, &StreamSenderImpl{}, legacySender.ManagedStreamSender)

	dc.OverrideSetting(dynamicconfig.ReplicationStreamImplementation, dynamicconfig.ReplicationStreamImplementationSettings{
		Sender: []dynamicconfig.ReplicationStreamImplementationRule{
			{RemoteCluster: "remote", Implementation: streamImplementationLane, ShardIDs: []int32{7}},
		},
	})
	require.Equal(t, streamImplementationLane, legacySender.selectImplementation())

	laneSender := newSender()
	require.IsType(t, &laneStreamSender{}, laneSender.ManagedStreamSender)
	require.Equal(t, streamImplementationLane, laneSender.implementation)
}

func TestStreamReceiverFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, int64(2)).Return("remote").AnyTimes()
	dc := dynamicconfig.NewMemoryClient()
	processToolBox := ProcessToolBox{
		Config:          configs.NewConfig(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), 1),
		ClusterMetadata: clusterMetadata,
		MetricsHandler:  metrics.NoopMetricsHandler,
		Logger:          log.NewNoopLogger(),
	}
	factory := NewStreamReceiverFactory()
	newReceiver := func() *selectedStreamReceiver {
		receiver := factory.NewStreamReceiver(processToolBox, nil, NewClusterShardKey(1, 7), NewClusterShardKey(2, 1))
		return receiver.(*selectedStreamReceiver)
	}

	legacyReceiver := newReceiver()
	require.IsType(t, &StreamReceiverImpl{}, legacyReceiver.ManagedStreamReceiver)

	dc.OverrideSetting(dynamicconfig.ReplicationStreamImplementation, dynamicconfig.ReplicationStreamImplementationSettings{
		Receiver: []dynamicconfig.ReplicationStreamImplementationRule{
			{RemoteCluster: "remote", Implementation: streamImplementationLane, ShardIDs: []int32{7}},
		},
	})
	require.Equal(t, streamImplementationLane, legacyReceiver.selectImplementation())

	laneReceiver := newReceiver()
	require.IsType(t, &laneStreamReceiver{}, laneReceiver.ManagedStreamReceiver)
	require.Equal(t, streamImplementationLane, laneReceiver.implementation)
}

// Prometheus drops recordings of one metric with different tag names, so senders and
// receivers must record replication_stream_implementation_selected with the same tag names.
func TestStreamFactoriesRecordSameImplementationTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)
	dc := dynamicconfig.NewMemoryClient()
	config := configs.NewConfig(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), 1)

	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	shardContext.EXPECT().GetMetricsHandler().Return(captureHandler).AnyTimes()
	NewStreamSenderFactory().NewStreamSender(nil, shardContext, nil, nil, nil, "remote", 1, NewClusterShardKey(2, 1), NewClusterShardKey(1, 7), config)

	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, int64(2)).Return("remote").AnyTimes()
	NewStreamReceiverFactory().NewStreamReceiver(ProcessToolBox{
		Config:          config,
		ClusterMetadata: clusterMetadata,
		MetricsHandler:  captureHandler,
		Logger:          log.NewNoopLogger(),
	}, nil, NewClusterShardKey(1, 7), NewClusterShardKey(2, 1))

	recordings := capture.Snapshot()[metrics.ReplicationStreamImplementationSelected.Name()]
	require.Len(t, recordings, 2)
	require.Equal(t, slices.Sorted(maps.Keys(recordings[0].Tags)), slices.Sorted(maps.Keys(recordings[1].Tags)))
}
