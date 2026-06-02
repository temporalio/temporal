package stream

import (
	"go.uber.org/fx"

	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
)

// Register registers the stream Library with the chasm Registry.
func Register(registry *chasm.Registry, library *Library) error {
	return registry.Register(library)
}

type historyShardResolver struct {
	numShards int32
}

func (r historyShardResolver) Shard(namespaceID, streamID string) int32 {
	return common.WorkflowIDToHistoryShard(namespaceID, streamID, r.numShards)
}

func NewHistoryShardResolver(persistenceConfig config.Persistence) ShardResolver {
	return historyShardResolver{numShards: persistenceConfig.NumHistoryShards}
}

// Module is the fx module for the native-streams chasm library.
var Module = fx.Module(
	"chasm.lib.stream",
	fx.Provide(NewHistoryShardResolver),
	fx.Provide(NewHandler),
	fx.Provide(NewSweepExpiredTaskHandler),
	fx.Provide(NewAbortCleanupTaskHandler),
	fx.Provide(NewCloseCleanupTaskHandler),
	fx.Provide(NewOwnerWorkflowCloseTaskHandler),
	fx.Provide(NewPublisherDedupSweepTaskHandler),
	fx.Provide(NewDeliveryTaskHandler),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)

// FrontendModule exposes StreamService on the public frontend and forwards
// requests to the history-owned stream handler through the generated layered
// client.
var FrontendModule = fx.Module(
	"chasm.lib.stream.frontend",
	fx.Provide(streampb.NewStreamServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
	fx.Invoke(RegisterFrontendService),
)
