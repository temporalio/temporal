//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_factory_mock.go

package replication

import (
	"fmt"
	"slices"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
)

const (
	streamImplementationLegacy = "legacy"
	streamImplementationLane   = "lane"

	// Operation tags of replication_stream_implementation_selected. Senders and receivers use
	// the same tag names, because Prometheus drops recordings of one metric with different tag
	// names.
	streamSenderOperation   = "ReplicationStreamSender"
	streamReceiverOperation = "ReplicationStreamReceiver"
)

type (
	// ManagedStreamSender is a StreamSender that can be started and waited on.
	ManagedStreamSender interface {
		StreamSender
		Start()
		Wait()
	}

	// ManagedStreamReceiver is a StreamReceiver that can be started.
	ManagedStreamReceiver interface {
		StreamReceiver
		Start()
	}

	// StreamSenderFactory creates the sender side of a replication stream.
	StreamSenderFactory interface {
		NewStreamSender(
			server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
			shardContext historyi.ShardContext,
			historyEngine historyi.Engine,
			ssRateLimiter ServerSchedulerRateLimiter,
			taskConverter SourceTaskConverter,
			clientClusterName string,
			clientClusterShardCount int32,
			clientShardKey ClusterShardKey,
			serverShardKey ClusterShardKey,
			config *configs.Config,
		) ManagedStreamSender
	}

	// StreamReceiverFactory creates the receiver side of a replication stream.
	StreamReceiverFactory interface {
		NewStreamReceiver(
			processToolBox ProcessToolBox,
			taskConverter ExecutableTaskConverter,
			clientShardKey ClusterShardKey,
			serverShardKey ClusterShardKey,
		) ManagedStreamReceiver
	}

	streamSenderFactory   struct{}
	streamReceiverFactory struct {
		laneFeedback LaneFeedbackProvider
	}

	// selectedStreamSender is invalid once the implementation selected for its stream changes.
	// The stream receiver monitor then stops it, and the stream reconnects with the newly
	// selected implementation.
	selectedStreamSender struct {
		ManagedStreamSender
		implementation       string
		selectImplementation func() string
	}

	// selectedStreamReceiver is invalid once the implementation selected for its stream
	// changes. The stream receiver monitor then replaces it with the newly selected
	// implementation.
	selectedStreamReceiver struct {
		ManagedStreamReceiver
		implementation       string
		selectImplementation func() string
	}
)

// NewStreamSenderFactory returns a factory that creates the sender implementation selected by
// the ReplicationStreamImplementation dynamic config.
func NewStreamSenderFactory() StreamSenderFactory {
	return streamSenderFactory{}
}

// NewStreamReceiverFactory returns a factory that creates the receiver implementation selected
// by the ReplicationStreamImplementation dynamic config. Lane receivers report laneFeedback to
// their senders.
func NewStreamReceiverFactory(laneFeedback LaneFeedbackProvider) StreamReceiverFactory {
	return streamReceiverFactory{laneFeedback: laneFeedback}
}

func (streamSenderFactory) NewStreamSender(
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext historyi.ShardContext,
	historyEngine historyi.Engine,
	ssRateLimiter ServerSchedulerRateLimiter,
	taskConverter SourceTaskConverter,
	clientClusterName string,
	clientClusterShardCount int32,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
	config *configs.Config,
) ManagedStreamSender {
	selectImplementation := func() (string, error) {
		return selectStreamImplementation(
			config.ReplicationStreamImplementation().Sender,
			clientClusterName,
			serverShardKey.ShardID,
		)
	}
	implementation, err := selectImplementation()
	if err != nil {
		shardContext.GetLogger().Error("Invalid ReplicationStreamImplementation config. Using the legacy stream sender.", tag.Error(err))
	}
	metrics.ReplicationStreamImplementationSelected.With(shardContext.GetMetricsHandler()).Record(
		1,
		metrics.OperationTag(streamSenderOperation),
		metrics.ReplicationStreamImplementationTag(implementation),
	)

	var sender ManagedStreamSender
	if implementation == streamImplementationLane {
		sender = newLaneStreamSender(
			server,
			shardContext,
			historyEngine,
			ssRateLimiter,
			taskConverter,
			clientClusterName,
			clientClusterShardCount,
			clientShardKey,
			serverShardKey,
			config,
		)
	} else {
		sender = NewStreamSender(
			server,
			shardContext,
			historyEngine,
			ssRateLimiter,
			taskConverter,
			clientClusterName,
			clientClusterShardCount,
			clientShardKey,
			serverShardKey,
			config,
		)
	}
	return &selectedStreamSender{
		ManagedStreamSender: sender,
		implementation:      implementation,
		selectImplementation: func() string {
			implementation, _ := selectImplementation()
			return implementation
		},
	}
}

func (f streamReceiverFactory) NewStreamReceiver(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) ManagedStreamReceiver {
	remoteClusterName := processToolBox.ClusterMetadata.ClusterNameForFailoverVersion(true, int64(serverShardKey.ClusterID))
	selectImplementation := func() (string, error) {
		return selectStreamImplementation(
			processToolBox.Config.ReplicationStreamImplementation().Receiver,
			remoteClusterName,
			clientShardKey.ShardID,
		)
	}
	implementation, err := selectImplementation()
	if err != nil {
		processToolBox.Logger.Error("Invalid ReplicationStreamImplementation config. Using the legacy stream receiver.", tag.Error(err))
	}
	metrics.ReplicationStreamImplementationSelected.With(processToolBox.MetricsHandler).Record(
		1,
		metrics.OperationTag(streamReceiverOperation),
		metrics.ReplicationStreamImplementationTag(implementation),
	)

	var receiver ManagedStreamReceiver
	if implementation == streamImplementationLane {
		receiver = newLaneStreamReceiver(processToolBox, taskConverter, clientShardKey, serverShardKey, f.laneFeedback)
	} else {
		receiver = NewStreamReceiver(processToolBox, taskConverter, clientShardKey, serverShardKey)
	}
	return &selectedStreamReceiver{
		ManagedStreamReceiver: receiver,
		implementation:        implementation,
		selectImplementation: func() string {
			implementation, _ := selectImplementation()
			return implementation
		},
	}
}

func (s *selectedStreamSender) IsValid() bool {
	return s.selectImplementation() == s.implementation && s.ManagedStreamSender.IsValid()
}

func (r *selectedStreamReceiver) IsValid() bool {
	return r.selectImplementation() == r.implementation && r.ManagedStreamReceiver.IsValid()
}

// selectStreamImplementation returns the implementation of the stream with remoteCluster on the
// local shard shardID. It returns legacy when no rule matches. If any rule is invalid, it returns
// legacy and an error, so an invalid config never selects the lane implementation.
func selectStreamImplementation(
	rules []dynamicconfig.ReplicationStreamImplementationRule,
	remoteCluster string,
	shardID int32,
) (string, error) {
	if err := validateStreamImplementationRules(rules); err != nil {
		return streamImplementationLegacy, err
	}
	for _, rule := range rules {
		if rule.RemoteCluster == remoteCluster && streamImplementationRuleMatchesShard(rule, shardID) {
			return rule.Implementation, nil
		}
	}
	return streamImplementationLegacy, nil
}

func validateStreamImplementationRules(rules []dynamicconfig.ReplicationStreamImplementationRule) error {
	remoteClusters := make(map[string]struct{}, len(rules))
	for i, rule := range rules {
		if rule.RemoteCluster == "" {
			return fmt.Errorf("rule %d: RemoteCluster is empty", i)
		}
		if _, ok := remoteClusters[rule.RemoteCluster]; ok {
			return fmt.Errorf("rule %d: more than one rule for RemoteCluster %q", i, rule.RemoteCluster)
		}
		remoteClusters[rule.RemoteCluster] = struct{}{}

		if rule.Implementation != streamImplementationLegacy && rule.Implementation != streamImplementationLane {
			return fmt.Errorf("rule %d: unknown Implementation %q", i, rule.Implementation)
		}
		if (len(rule.ShardIDs) > 0) == (rule.ShardPercent != 0) {
			return fmt.Errorf("rule %d: set exactly one of ShardIDs and ShardPercent", i)
		}
		if rule.ShardPercent < 0 || rule.ShardPercent > 100 {
			return fmt.Errorf("rule %d: ShardPercent %d is outside 1-100", i, rule.ShardPercent)
		}
		for _, shardID := range rule.ShardIDs {
			if shardID < 1 {
				return fmt.Errorf("rule %d: invalid shard ID %d", i, shardID)
			}
		}
	}
	return nil
}

func streamImplementationRuleMatchesShard(rule dynamicconfig.ReplicationStreamImplementationRule, shardID int32) bool {
	if len(rule.ShardIDs) > 0 {
		return slices.Contains(rule.ShardIDs, shardID)
	}
	// Shard IDs start at 1. The percentage applies within each block of 100 shard IDs.
	return int((shardID-1)%100) < rule.ShardPercent
}
