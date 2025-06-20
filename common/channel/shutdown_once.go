package channel

import (
	"sync/atomic"
)

const (
	shutdownOnceStatusOpen   int32 = 0
	shutdownOnceStatusClosed int32 = 1
)

type (
	ShutdownOnce interface {
		// Shutdown broadcast shutdown signal
		Shutdown()
		// IsShutdown return true if already shutdown
		IsShutdown() bool
		// Channel for shutdown notification
		Channel() <-chan struct{}
	}

	ShutdownOnceImpl struct {
		status  int32
		channel chan struct{}
	}
)

func NewShutdownOnce() *ShutdownOnceImpl {
	return &ShutdownOnceImpl{
		status:  shutdownOnceStatusOpen,
		channel: make(chan struct{}),
	}
}

func (c *ShutdownOnceImpl) Shutdown() {
	if atomic.CompareAndSwapInt32(
		&c.status,
		shutdownOnceStatusOpen,
		shutdownOnceStatusClosed,
	) {
		close(c.channel)
	}
}

func (c *ShutdownOnceImpl) IsShutdown() bool {
	return atomic.LoadInt32(&c.status) == shutdownOnceStatusClosed
}

func (c *ShutdownOnceImpl) Channel() <-chan struct{} {
	return c.channel
}
