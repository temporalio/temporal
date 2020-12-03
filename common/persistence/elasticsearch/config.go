package elasticsearch

import (
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	// Config contains all configs for indexer
	Config struct {
		IndexerConcurrency       dynamicconfig.IntPropertyFn
		ESProcessorNumOfWorkers  dynamicconfig.IntPropertyFn
		ESProcessorBulkActions   dynamicconfig.IntPropertyFn // max number of requests in bulk
		ESProcessorBulkSize      dynamicconfig.IntPropertyFn // max total size of bytes in bulk
		ESProcessorFlushInterval dynamicconfig.DurationPropertyFn
		ValidSearchAttributes    dynamicconfig.MapPropertyFn
	}
)
