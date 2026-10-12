package elasticsearch

import (
	"go.temporal.io/server/common/dynamicconfig"
)

const (
	PersistenceName = "elasticsearch"
)

type (
	ProcessorConfig struct {
		IndexerConcurrency dynamicconfig.IntPropertyFn
		// TODO: remove ESProcessor prefix
		ESProcessorNumOfWorkers  dynamicconfig.IntPropertyFn
		ESProcessorBulkActions   dynamicconfig.IntPropertyFn // max number of requests in bulk
		ESProcessorBulkSize      dynamicconfig.IntPropertyFn // max total size of bytes in bulk
		ESProcessorFlushInterval dynamicconfig.DurationPropertyFn

		ESProcessorAckTimeout dynamicconfig.DurationPropertyFn
	}
)
