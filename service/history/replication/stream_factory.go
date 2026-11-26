//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_factory_mock.go

package replication

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
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
		implementation string
		selector       *streamImplementationSelector
	}

	// selectedStreamReceiver is invalid once the implementation selected for its stream
	// changes. The stream receiver monitor then replaces it with the newly selected
	// implementation.
	selectedStreamReceiver struct {
		ManagedStreamReceiver
		implementation string
		selector       *streamImplementationSelector
	}

	// streamImplementationSelector reads a stream's implementation from live config, both at
	// construction and when IsValid checks for a mode change. It logs each new config error,
	// so an edit made after the stream was created is reported even when it leaves the
	// selection unchanged.
	streamImplementationSelector struct {
		selectImplementation func() (string, error)
		logger               log.Logger
		remoteCluster        string

		mu      sync.Mutex
		lastErr string
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
	selector := &streamImplementationSelector{
		selectImplementation: func() (string, error) {
			return selectStreamImplementation(
				config.ReplicationStreamImplementation().Sender,
				clientClusterName,
				serverShardKey.ShardID,
				config.NumberOfShards,
			)
		},
		logger:        shardContext.GetThrottledLogger(),
		remoteCluster: clientClusterName,
	}
	implementation := selector.selected()
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
		ManagedStreamSender: sender,
		implementation:      implementation,
		selector:            selector,
	}
}

func (f streamReceiverFactory) NewStreamReceiver(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) ManagedStreamReceiver {
	remoteClusterName := processToolBox.ClusterMetadata.ClusterNameForFailoverVersion(true, int64(serverShardKey.ClusterID))
	selector := &streamImplementationSelector{
		selectImplementation: func() (string, error) {
			return selectStreamImplementation(
				processToolBox.Config.ReplicationStreamImplementation().Receiver,
				remoteClusterName,
				clientShardKey.ShardID,
				processToolBox.Config.NumberOfShards,
			)
		},
		logger:        processToolBox.ThrottledLogger,
		remoteCluster: remoteClusterName,
	}
	implementation := selector.selected()
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
		selector:              selector,
	}
}

func (s *selectedStreamSender) IsValid() bool {
	// For an invalid rule the selector returns the default, legacy, so a lane stream restarts
	// as legacy and a legacy stream is unchanged. An ignored entry leaves the selection
	// unchanged. Either way the selector logs the error.
	return s.selector.selected() == s.implementation && s.ManagedStreamSender.IsValid()
}

func (r *selectedStreamReceiver) IsValid() bool {
	// As in selectedStreamSender.IsValid: for an invalid rule the selector returns the
	// default, legacy, and logs the error.
	return r.selector.selected() == r.implementation && r.ManagedStreamReceiver.IsValid()
}

// selected returns the stream's implementation from live config. It is usable even when the
// config has an error: an invalid rule selects the default, legacy. It logs an error only when
// it differs from the last one it logged, so an invalid config is reported once per stream,
// not on every IsValid check.
func (s *streamImplementationSelector) selected() string {
	implementation, err := s.selectImplementation()
	var errMessage string
	if err != nil {
		errMessage = err.Error()
	}
	s.mu.Lock()
	isNewErr := errMessage != s.lastErr
	s.lastErr = errMessage
	s.mu.Unlock()
	if err != nil && isNewErr {
		s.logger.Error("Invalid ReplicationStreamImplementation rule",
			tag.ClusterName(s.remoteCluster), tag.NewStringTag("implementation", implementation), tag.Error(err))
	}
	return implementation
}

// selectStreamImplementation returns the implementation of the stream with remoteCluster on the
// local shard shardID. It always returns an implementation the caller can use, even with an error:
//   - No rule for this remote, or a rule that does not include the shard: the default, legacy.
//   - An invalid or duplicate rule for this remote, including one that failed to decode: the
//     default, legacy, and an error.
//   - An entry without a remote: ignored. The selection is unchanged, and the error reports it.
//
// Rules for other remotes do not affect this stream.
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
			err := fmt.Errorf("rule %d: RemoteCluster is empty", i)
			if rule.DecodeError != nil {
				err = fmt.Errorf("rule %d: cannot decode a RemoteCluster: %w", i, rule.DecodeError)
			}
			configErr = errors.Join(configErr, err)
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
	if rule.DecodeError != nil {
		return fmt.Errorf("cannot decode rule: %w", rule.DecodeError)
	}
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
