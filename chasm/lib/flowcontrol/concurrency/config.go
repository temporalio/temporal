package concurrency

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/stream_batcher"
)

var defaultServerBatcherOptions = stream_batcher.BatcherOptions{
	MaxItems:      100,
	MinDelay:      5 * time.Millisecond,
	MaxDelay:      10 * time.Millisecond,
	IdleTime:      time.Minute,
	ClearInterval: time.Hour,
}
var defaultClientBatcherOptions = stream_batcher.BatcherOptions{
	MaxItems:      50,
	MinDelay:      5 * time.Millisecond,
	MaxDelay:      10 * time.Millisecond,
	IdleTime:      time.Minute,
	ClearInterval: time.Hour,
}

var ServerBatcherOptions = dynamicconfig.NewGlobalTypedSetting(
	"flowcontrol.concurrency.serverBatcher",
	defaultServerBatcherOptions,
	`Batcher options for concurrency limiter requests on server. Requires server restart.`,
)
var MatchingClientBatcherOptions = dynamicconfig.NewGlobalTypedSetting(
	"flowcontrol.concurrency.matchingClientBatcher",
	defaultClientBatcherOptions,
	`Batcher options for concurrency limiter requests from matching. Requires server restart.`,
)
var HistoryClientBatcherOptions = dynamicconfig.NewGlobalTypedSetting(
	"flowcontrol.concurrency.historyClientBatcher",
	defaultClientBatcherOptions,
	`Batcher options for concurrency limiter releases from history transfer queue/CHASM activities. Requires server restart.`,
)
