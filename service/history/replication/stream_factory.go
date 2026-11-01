//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_factory_mock.go

package replication

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
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

	legacyStreamSenderFactory   struct{}
	legacyStreamReceiverFactory struct{}
)

// NewLegacyStreamSenderFactory returns a factory that creates StreamSenderImpl.
func NewLegacyStreamSenderFactory() StreamSenderFactory {
	return legacyStreamSenderFactory{}
}

// NewLegacyStreamReceiverFactory returns a factory that creates StreamReceiverImpl.
func NewLegacyStreamReceiverFactory() StreamReceiverFactory {
	return legacyStreamReceiverFactory{}
}

func (legacyStreamSenderFactory) NewStreamSender(
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
	return NewStreamSender(
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

func (legacyStreamReceiverFactory) NewStreamReceiver(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) ManagedStreamReceiver {
	return NewStreamReceiver(
		processToolBox,
		taskConverter,
		clientShardKey,
		serverShardKey,
	)
}
