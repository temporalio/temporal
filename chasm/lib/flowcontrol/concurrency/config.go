package concurrency

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/streambatcher"
)

var defaultServerBatcherOptions = streambatcher.BatcherOptions{
	MaxItems:      100,
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

type WaitLongPollOptions struct {
	// Timeout is the maximum time a single Wait call will block on the server.
	Timeout time.Duration
	// Buffer is the minimum time left before the caller's deadline when the long poll returns.
	Buffer time.Duration
}

var defaultWaitLongPollOptions = WaitLongPollOptions{
	Timeout: time.Minute,
	Buffer:  time.Second,
}

var WaitLongPoll = dynamicconfig.NewGlobalTypedSetting(
	"flowcontrol.concurrency.waitLongPoll",
	defaultWaitLongPollOptions,
	`Long-poll timeout and buffer for concurrency limiter Wait calls. Since reservation expiry is
noticed lazily, Timeout also bounds how long a waiter may take to notice a slot freed by an
expired reservation.`,
)

const defaultReserveTimeout = 30 * time.Second

var ReserveTimeout = dynamicconfig.NewGlobalDurationSetting(
	"flowcontrol.concurrency.reserveTimeout",
	defaultReserveTimeout,
	`How long a concurrency limiter slot reservation lasts before it expires if not committed.`,
)

type StagedWakeOptions struct {
	// Interval is the time between wake stages.
	Interval time.Duration
	// MaxStage is the stage at which all remaining waiters are woken. The number of tokens
	// woken doubles at each stage before that. Capped to 10.
	MaxStage int32
}

var defaultStagedWakeOptions = StagedWakeOptions{
	Interval: time.Second,
	MaxStage: 8,
}

var StagedWake = dynamicconfig.NewGlobalTypedSetting(
	"flowcontrol.concurrency.stagedWake",
	defaultStagedWakeOptions,
	`Options for waking concurrency limiter waiters in stages when slots become available.`,
)
