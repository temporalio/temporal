package membership

import (
	"sync/atomic"
	"time"

	"github.com/uber/ringpop-go/discovery/statichosts"

	"github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/swim"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

type (
	// RingPop is a simple wrapper
	RingPop struct {
		status int32
		*ringpop.Ringpop
		logger          log.Logger
		maxJoinDuration time.Duration
	}
)

// NewRingPop create a new ring pop wrapper
func NewRingPop(
	ringPop *ringpop.Ringpop,
	maxJoinDuration time.Duration,
	logger log.Logger,
) *RingPop {
	return &RingPop{
		status:          common.DaemonStatusInitialized,
		Ringpop:         ringPop,
		maxJoinDuration: maxJoinDuration,
		logger:          logger,
	}
}

// Start start ring pop
func (r *RingPop) Start(bootstrapHostPorts []string) {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	bootParams := &swim.BootstrapOptions{
		MaxJoinDuration:  r.maxJoinDuration,
		DiscoverProvider: statichosts.New(bootstrapHostPorts...),
	}

	_, err := r.Ringpop.Bootstrap(bootParams)
	if err != nil {
		r.logger.Fatal("unable to bootstrap ringpop", tag.Error(err))
	}
}

// Stop stop ring pop
func (r *RingPop) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.Destroy()
}
