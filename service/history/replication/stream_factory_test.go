package replication

import (
	"errors"
	"maps"
	"math"
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
		{name: "ShardPercentIncludesShard", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 10)}, shardID: 52, expected: streamImplementationLane},
		{name: "ShardPercentExcludesShard", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 10)}, shardID: 53, expected: streamImplementationLegacy},
		{name: "ShardPercentDoesNotRepeatEvery100Shards", rules: []dynamicconfig.ReplicationStreamImplementationRule{laneRule("remote", nil, 10)}, shardID: 110, expected: streamImplementationLegacy},
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
			name: "InvalidRuleForOtherClusterDoesNotChangeSelection",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				laneRule("remote", []int32{7}, 0),
				laneRule("other", nil, 0),
			},
			shardID:  7,
			expected: streamImplementationLane,
		},
		{
			name: "DuplicateRulesForOtherClusterDoNotChangeSelection",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				laneRule("other", []int32{7}, 0),
				laneRule("other", []int32{8}, 0),
				laneRule("remote", []int32{7}, 0),
			},
			shardID:  7,
			expected: streamImplementationLane,
		},
		{
			name: "EmptyRemoteClusterIsReportedWithoutChangingSelection",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				laneRule("", nil, 0),
				laneRule("remote", []int32{7}, 0),
			},
			shardID:       7,
			expected:      streamImplementationLane,
			expectInvalid: true,
		},
		{
			name:          "DecodeErrorSelectsLegacy",
			rules:         []dynamicconfig.ReplicationStreamImplementationRule{{RemoteCluster: "remote", DecodeError: errors.New("bad type")}},
			shardID:       7,
			expected:      streamImplementationLegacy,
			expectInvalid: true,
		},
		{
			name: "DecodeErrorForOtherClusterDoesNotChangeSelection",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				laneRule("remote", []int32{7}, 0),
				{RemoteCluster: "other", DecodeError: errors.New("bad type")},
			},
			shardID:  7,
			expected: streamImplementationLane,
		},
		{
			name: "DecodeErrorWithoutRemoteClusterIsReportedWithoutChangingSelection",
			rules: []dynamicconfig.ReplicationStreamImplementationRule{
				{DecodeError: errors.New("bad type")},
				laneRule("remote", []int32{7}, 0),
			},
			shardID:       7,
			expected:      streamImplementationLane,
			expectInvalid: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			implementation, err := selectStreamImplementation(tc.rules, "remote", tc.shardID, 512)
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
		name           string
		implementation string
		selected       string
		selectionErr   error
		streamValid    bool
		expected       bool
	}{
		{name: "SameImplementation", implementation: streamImplementationLegacy, selected: streamImplementationLegacy, streamValid: true, expected: true},
		{name: "StreamInvalid", implementation: streamImplementationLegacy, selected: streamImplementationLegacy, streamValid: false, expected: false},
		{name: "ImplementationChanged", implementation: streamImplementationLegacy, selected: streamImplementationLane, streamValid: true, expected: false},
		{name: "InvalidRuleRollsLaneBack", implementation: streamImplementationLane, selected: streamImplementationLegacy, selectionErr: errors.New("invalid rule"), streamValid: true, expected: false},
		{name: "InvalidRuleKeepsLegacy", implementation: streamImplementationLegacy, selected: streamImplementationLegacy, selectionErr: errors.New("invalid rule"), streamValid: true, expected: true},
		{name: "IgnoredEntryKeepsLane", implementation: streamImplementationLane, selected: streamImplementationLane, selectionErr: errors.New("missing remote cluster"), streamValid: true, expected: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			sender := NewMockManagedStreamSender(ctrl)
			sender.EXPECT().IsValid().Return(tc.streamValid).AnyTimes()
			receiver := NewMockManagedStreamReceiver(ctrl)
			receiver.EXPECT().IsValid().Return(tc.streamValid).AnyTimes()
			newSelector := func() *streamImplementationSelector {
				return &streamImplementationSelector{
					selectImplementation: func() (string, error) { return tc.selected, tc.selectionErr },
					logger:               log.NewNoopLogger(),
				}
			}

			selectedSender := &selectedStreamSender{
				ManagedStreamSender: sender,
				implementation:      tc.implementation,
				selector:            newSelector(),
			}
			selectedReceiver := &selectedStreamReceiver{
				ManagedStreamReceiver: receiver,
				implementation:        tc.implementation,
				selector:              newSelector(),
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
	shardContext.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()
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
	implementation, err := legacySender.selector.selectImplementation()
	require.NoError(t, err)
	require.Equal(t, streamImplementationLane, implementation)

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
		ThrottledLogger: log.NewNoopLogger(),
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
	implementation, err := legacyReceiver.selector.selectImplementation()
	require.NoError(t, err)
	require.Equal(t, streamImplementationLane, implementation)

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
	shardContext.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()
	shardContext.EXPECT().GetMetricsHandler().Return(captureHandler).AnyTimes()
	NewStreamSenderFactory().NewStreamSender(nil, shardContext, nil, nil, nil, "remote", 1, NewClusterShardKey(2, 1), NewClusterShardKey(1, 7), config)

	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, int64(2)).Return("remote").AnyTimes()
	NewStreamReceiverFactory().NewStreamReceiver(ProcessToolBox{
		Config:          config,
		ClusterMetadata: clusterMetadata,
		MetricsHandler:  captureHandler,
		Logger:          log.NewNoopLogger(),
		ThrottledLogger: log.NewNoopLogger(),
	}, nil, NewClusterShardKey(1, 7), NewClusterShardKey(2, 1))

	recordings := capture.Snapshot()[metrics.ReplicationStreamImplementationSelected.Name()]
	require.Len(t, recordings, 2)
	require.Equal(t, slices.Sorted(maps.Keys(recordings[0].Tags)), slices.Sorted(maps.Keys(recordings[1].Tags)))
}

func TestStreamImplementationPercentage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		shards   int32
		percent  int
		selected int
	}{
		{"SmallClusterOnePercent", 4, 1, 1},
		{"SmallClusterTenPercent", 4, 10, 1},
		{"ExactQuarter", 4, 25, 1},
		{"RoundedUpQuarter", 4, 26, 2},
		{"ThirtyTwoShards", 32, 10, 4},
		{"OneHundredShards", 100, 10, 10},
		{"FiveHundredTwelveShards", 512, 10, 52},
		{"AllShards", 512, 100, 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rule := dynamicconfig.ReplicationStreamImplementationRule{ShardPercent: tc.percent}
			selected := 0
			for id := int32(1); id <= tc.shards; id++ {
				if streamImplementationRuleMatchesShard(rule, id, tc.shards) {
					selected++
				}
			}
			require.Equal(t, tc.selected, selected)
		})
	}
}

func TestStreamImplementationPercentageOnlyAddsShards(t *testing.T) {
	t.Parallel()
	const shards = 512
	previous := make([]bool, shards+1)
	for percent := 1; percent <= 100; percent++ {
		rule := dynamicconfig.ReplicationStreamImplementationRule{ShardPercent: percent}
		for id := int32(1); id <= shards; id++ {
			selected := streamImplementationRuleMatchesShard(rule, id, shards)
			if previous[id] {
				require.True(t, selected, "shard %d must remain selected at %d percent", id, percent)
			}
			previous[id] = selected
		}
	}
	require.True(t, streamImplementationRuleMatchesShard(dynamicconfig.ReplicationStreamImplementationRule{ShardPercent: 100}, math.MaxInt32, math.MaxInt32))
	require.False(t, streamImplementationRuleMatchesShard(dynamicconfig.ReplicationStreamImplementationRule{ShardPercent: 99}, math.MaxInt32, math.MaxInt32))
}

func TestStreamFactoriesScopeInvalidRules(t *testing.T) {
	t.Parallel()
	rule := func(remote, implementation string) dynamicconfig.ReplicationStreamImplementationRule {
		return dynamicconfig.ReplicationStreamImplementationRule{RemoteCluster: remote, Implementation: implementation, ShardIDs: []int32{7}}
	}
	for _, role := range []string{"sender", "receiver"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name           string
				rules          []dynamicconfig.ReplicationStreamImplementationRule
				implementation string
				diagnostic     bool
			}{
				{"InvalidOtherCluster", []dynamicconfig.ReplicationStreamImplementationRule{rule("remote", "lane"), rule("other", "typo")}, "lane", false},
				{"EmptyCluster", []dynamicconfig.ReplicationStreamImplementationRule{rule("", "lane"), rule("remote", "lane")}, "lane", true},
				{"InvalidMatchingCluster", []dynamicconfig.ReplicationStreamImplementationRule{rule("remote", "typo")}, "legacy", true},
				{"LocalShardCount", []dynamicconfig.ReplicationStreamImplementationRule{{RemoteCluster: "remote", Implementation: "lane", ShardPercent: 10}}, "legacy", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					ctrl := gomock.NewController(t)
					// Errors must use the service's shared throttled logger.
					throttled := log.NewMockLogger(ctrl)
					if tc.diagnostic {
						throttled.EXPECT().Error("Invalid ReplicationStreamImplementation rule", gomock.Any(), gomock.Any(), gomock.Any())
					}
					dc := dynamicconfig.NewMemoryClient()
					dc.OverrideSetting(dynamicconfig.ReplicationStreamImplementation, dynamicconfig.ReplicationStreamImplementationSettings{Sender: tc.rules, Receiver: tc.rules})
					config := configs.NewConfig(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), 32)
					var selected string
					var selectImplementation func() (string, error)
					if role == "sender" {
						shard := historyi.NewMockShardContext(ctrl)
						shard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
						shard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
						shard.EXPECT().GetThrottledLogger().Return(throttled)
						sender := NewStreamSenderFactory().NewStreamSender(nil, shard, nil, nil, nil, "remote", 1, NewClusterShardKey(2, 1), NewClusterShardKey(1, 7), config).(*selectedStreamSender)
						selected, selectImplementation = sender.implementation, sender.selector.selectImplementation
						if tc.implementation == "lane" {
							require.IsType(t, &laneStreamSender{}, sender.ManagedStreamSender)
						} else {
							require.IsType(t, &StreamSenderImpl{}, sender.ManagedStreamSender)
						}
					} else {
						metadata := cluster.NewMockMetadata(ctrl)
						metadata.EXPECT().ClusterNameForFailoverVersion(true, int64(2)).Return("remote").AnyTimes()
						toolbox := ProcessToolBox{Config: config, ClusterMetadata: metadata, MetricsHandler: metrics.NoopMetricsHandler, Logger: log.NewNoopLogger(), ThrottledLogger: throttled}
						receiver := NewStreamReceiverFactory().NewStreamReceiver(toolbox, nil, NewClusterShardKey(1, 7), NewClusterShardKey(2, 1)).(*selectedStreamReceiver)
						selected, selectImplementation = receiver.implementation, receiver.selector.selectImplementation
						if tc.implementation == "lane" {
							require.IsType(t, &laneStreamReceiver{}, receiver.ManagedStreamReceiver)
						} else {
							require.IsType(t, &StreamReceiverImpl{}, receiver.ManagedStreamReceiver)
						}
					}
					require.Equal(t, tc.implementation, selected)
					// The retained selector must reread config after construction.
					dc.OverrideSetting(dynamicconfig.ReplicationStreamImplementation, dynamicconfig.ReplicationStreamImplementationSettings{
						Sender:   []dynamicconfig.ReplicationStreamImplementationRule{rule("remote", "typo")},
						Receiver: []dynamicconfig.ReplicationStreamImplementationRule{rule("remote", "typo")},
					})
					selected, err := selectImplementation()
					require.Equal(t, streamImplementationLegacy, selected)
					require.ErrorContains(t, err, "unknown Implementation")
				})
			}
		})
	}
}

// An edit made after a stream was created must be logged even when it leaves the selection
// unchanged: IsValid then keeps the stream, so the factory does not run again.
func TestSelectedStreamLogsLiveConfigErrors(t *testing.T) {
	t.Parallel()
	// A quoted ShardPercent fails to decode, which selects the default, legacy, for this remote.
	badlyTypedRule := map[string]any{"RemoteCluster": "remote", "Implementation": "lane", "ShardPercent": "10"}
	for _, role := range []string{"sender", "receiver"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			throttled := log.NewMockLogger(ctrl)
			dc := dynamicconfig.NewMemoryClient()
			setRules := func(rules ...any) {
				dc.OverrideSetting(dynamicconfig.ReplicationStreamImplementation, map[string]any{"Sender": rules, "Receiver": rules})
			}
			config := configs.NewConfig(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), 32)
			var isValid func() bool
			if role == "sender" {
				shard := historyi.NewMockShardContext(ctrl)
				shard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
				shard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
				shard.EXPECT().GetThrottledLogger().Return(throttled)
				sender := NewStreamSenderFactory().NewStreamSender(nil, shard, nil, nil, nil, "remote", 1, NewClusterShardKey(2, 1), NewClusterShardKey(1, 7), config).(*selectedStreamSender)
				require.Equal(t, streamImplementationLegacy, sender.implementation)
				// Stand in for a started stream.
				started := NewMockManagedStreamSender(ctrl)
				started.EXPECT().IsValid().Return(true).AnyTimes()
				sender.ManagedStreamSender = started
				isValid = sender.IsValid
			} else {
				metadata := cluster.NewMockMetadata(ctrl)
				metadata.EXPECT().ClusterNameForFailoverVersion(true, int64(2)).Return("remote").AnyTimes()
				toolbox := ProcessToolBox{Config: config, ClusterMetadata: metadata, MetricsHandler: metrics.NoopMetricsHandler, Logger: log.NewNoopLogger(), ThrottledLogger: throttled}
				receiver := NewStreamReceiverFactory().NewStreamReceiver(toolbox, nil, NewClusterShardKey(1, 7), NewClusterShardKey(2, 1)).(*selectedStreamReceiver)
				require.Equal(t, streamImplementationLegacy, receiver.implementation)
				started := NewMockManagedStreamReceiver(ctrl)
				started.EXPECT().IsValid().Return(true).AnyTimes()
				receiver.ManagedStreamReceiver = started
				isValid = receiver.IsValid
			}

			throttled.EXPECT().Error("Invalid ReplicationStreamImplementation rule", gomock.Any(), gomock.Any(), gomock.Any())
			setRules(badlyTypedRule)
			require.True(t, isValid(), "a legacy stream stays legacy")
			// The mock fails if the unchanged error is logged again.
			require.True(t, isValid())

			setRules()
			require.True(t, isValid())

			// The same error after a fix is a new edit, so it is logged again.
			throttled.EXPECT().Error("Invalid ReplicationStreamImplementation rule", gomock.Any(), gomock.Any(), gomock.Any())
			setRules(badlyTypedRule)
			require.True(t, isValid())
		})
	}
}

func TestSelectStreamImplementationWithBadlyTypedRule(t *testing.T) {
	t.Parallel()
	dc := dynamicconfig.NewMemoryClient()
	// Raw values, as a dynamic config file provides them, go through the setting's converter.
	dc.OverrideSetting(dynamicconfig.ReplicationStreamImplementation, map[string]any{
		"Sender": []any{
			map[string]any{"RemoteCluster": "remote", "Implementation": "lane", "ShardPercent": 100},
			map[string]any{"RemoteCluster": "other", "Implementation": "lane", "ShardPercent": "10"},
		},
	})
	config := configs.NewConfig(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), 4)
	rules := config.ReplicationStreamImplementation().Sender

	implementation, err := selectStreamImplementation(rules, "remote", 1, config.NumberOfShards)
	require.NoError(t, err)
	require.Equal(t, streamImplementationLane, implementation)

	implementation, err = selectStreamImplementation(rules, "other", 1, config.NumberOfShards)
	require.ErrorContains(t, err, "cannot decode rule")
	require.Equal(t, streamImplementationLegacy, implementation)
}
