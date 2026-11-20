package concurrency

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
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
