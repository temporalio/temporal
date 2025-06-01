//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination engine_factory_mock.go

package shard

import historyi "go.temporal.io/server/service/history/interfaces"

type (
	// EngineFactory is used to create an instance of sharded history engine
	EngineFactory interface {
		CreateEngine(context historyi.ShardContext) historyi.Engine
	}
)
