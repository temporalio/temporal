//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_factory_mock.go

package replication

import (
	"errors"
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

	streamSenderFactory struct {
		lanePolicyFactory LaneStreamPolicyFactory
	}
	streamReceiverFactory struct {
		laneFeedback LaneFeedbackProvider
	}

	// selectedStreamSender is invalid once the implementation selected for its stream changes.
	// The stream receiver monitor then stops it, and the stream reconnects with the newly
	// selected implementation.
	selectedStreamSender struct {
		ManagedStreamSender
		implementation       string
		selectImplementation func() (string, error)
	}

	// selectedStreamReceiver is invalid once the implementation selected for its stream
	// changes. The stream receiver monitor then replaces it with the newly selected
	// implementation.
	selectedStreamReceiver struct {
		ManagedStreamReceiver
		implementation       string
		selectImplementation func() (string, error)
	}
)

// NewStreamSenderFactory returns a factory that creates the sender implementation selected by
// the ReplicationStreamImplementation dynamic config. Lane senders get each stream's lane
// policy from lanePolicyFactory.
func NewStreamSenderFactory(lanePolicyFactory LaneStreamPolicyFactory) StreamSenderFactory {
	return streamSenderFactory{lanePolicyFactory: lanePolicyFactory}
}

// NewStreamReceiverFactory returns a factory that creates the receiver implementation selected
// by the ReplicationStreamImplementation dynamic config. Lane receivers report laneFeedback to
// their senders.
func NewStreamReceiverFactory(laneFeedback LaneFeedbackProvider) StreamReceiverFactory {
	return streamReceiverFactory{laneFeedback: laneFeedback}
}

func (f streamSenderFactory) NewStreamSender(
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
	// Read live config both at construction and when IsValid checks for a mode change.
	selectImplementation := func() (string, error) {
		return selectStreamImplementation(
			config.ReplicationStreamImplementation().Sender,
			clientClusterName,
			serverShardKey.ShardID,
			config.NumberOfShards,
		)
	}
	implementation, err := selectImplementation()
	if err != nil {
		shardContext.GetThrottledLogger().Error("Invalid ReplicationStreamImplementation rule",
			tag.ClusterName(clientClusterName), tag.NewStringTag("implementation", implementation), tag.Error(err))
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
			f.lanePolicyFactory,
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
		ManagedStreamSender:  sender,
		implementation:       implementation,
		selectImplementation: selectImplementation,
	}
}

func (f streamReceiverFactory) NewStreamReceiver(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) ManagedStreamReceiver {
	remoteClusterName := processToolBox.ClusterMetadata.ClusterNameForFailoverVersion(true, int64(serverShardKey.ClusterID))
	// Read live config both at construction and when IsValid checks for a mode change.
	selectImplementation := func() (string, error) {
		return selectStreamImplementation(
			processToolBox.Config.ReplicationStreamImplementation().Receiver,
			remoteClusterName,
			clientShardKey.ShardID,
			processToolBox.Config.NumberOfShards,
		)
	}
	implementation, err := selectImplementation()
	if err != nil {
		processToolBox.ThrottledLogger.Error("Invalid ReplicationStreamImplementation rule",
			tag.ClusterName(remoteClusterName), tag.NewStringTag("implementation", implementation), tag.Error(err))
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
		selectImplementation:  selectImplementation,
	}
}

func (s *selectedStreamSender) IsValid() bool {
	// The selector returns a usable implementation even on error: invalid matching rules
	// select legacy, and entries without a remote cluster are ignored. Construction logs errors.
	implementation, _ := s.selectImplementation()
	return implementation == s.implementation && s.ManagedStreamSender.IsValid()
}

func (r *selectedStreamReceiver) IsValid() bool {
	// Apply the selector's fallback result, as selectedStreamSender.IsValid does.
	implementation, _ := r.selectImplementation()
	return implementation == r.implementation && r.ManagedStreamReceiver.IsValid()
}

// selectStreamImplementation returns the implementation of the stream with remoteCluster on the
// local shard shardID. Invalid or duplicate rules for this remote select legacy. Rules for other
// remotes do not affect this stream. Entries without a remote are ignored and reported as errors,
// without changing the selected implementation.
func selectStreamImplementation(
	rules []dynamicconfig.ReplicationStreamImplementationRule,
	remoteCluster string,
	shardID int32,
	shardCount int32,
) (string, error) {
	matchingRule := -1
	var configErr error
	for i, rule := range rules {
		if rule.RemoteCluster == "" {
			configErr = errors.Join(configErr, fmt.Errorf("rule %d: RemoteCluster is empty", i))
			continue
		}
		if rule.RemoteCluster != remoteCluster {
			continue
		}
		if matchingRule >= 0 {
			return streamImplementationLegacy, errors.Join(configErr,
				fmt.Errorf("rule %d: more than one rule for RemoteCluster %q", i, rule.RemoteCluster))
		}
		matchingRule = i
	}
	if matchingRule < 0 {
		return streamImplementationLegacy, configErr
	}
	rule := rules[matchingRule]
	if err := validateStreamImplementationRule(rule); err != nil {
		return streamImplementationLegacy, errors.Join(configErr, fmt.Errorf("rule %d: %w", matchingRule, err))
	}
	if streamImplementationRuleMatchesShard(rule, shardID, shardCount) {
		return rule.Implementation, configErr
	}
	return streamImplementationLegacy, configErr
}

func validateStreamImplementationRule(rule dynamicconfig.ReplicationStreamImplementationRule) error {
	if rule.Implementation != streamImplementationLegacy && rule.Implementation != streamImplementationLane {
		return fmt.Errorf("unknown Implementation %q", rule.Implementation)
	}
	if (len(rule.ShardIDs) > 0) == (rule.ShardPercent != 0) {
		return errors.New("set exactly one of ShardIDs and ShardPercent")
	}
	if rule.ShardPercent < 0 || rule.ShardPercent > 100 {
		return fmt.Errorf("ShardPercent %d is outside 1-100", rule.ShardPercent)
	}
	for _, shardID := range rule.ShardIDs {
		if shardID < 1 {
			return fmt.Errorf("invalid shard ID %d", shardID)
		}
	}
	return nil
}

func streamImplementationRuleMatchesShard(rule dynamicconfig.ReplicationStreamImplementationRule, shardID, shardCount int32) bool {
	if len(rule.ShardIDs) > 0 {
		return slices.Contains(rule.ShardIDs, shardID)
	}
	// Select the first ceil(shardCount * percent / 100) shards. Raising the percentage
	// only adds shards. Use int64 so large shard counts cannot overflow the products.
	return int64(shardID-1)*100 < int64(rule.ShardPercent)*int64(shardCount)
}
