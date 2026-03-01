package sim_runtime

import (
	"fmt"
	"time"

	"go.temporal.io/server/tools/gomad/util/verify"
)

const (
	mainGoroutineID = 0
	stuckTimeout    = 1 * time.Second
)

type (
	goroutineId    uint64
	goroutineState int
	goroutine      struct {
		id             goroutineId
		fn             func()
		state          goroutineState
		syncBlock      *syncBlock
		internal       bool
		syncCh         chan struct{}
		suspendedCh    chan struct{}
		sourceLocation string
	}
	suspendReason int
)

const (
	ready goroutineState = iota
	running
	suspended
	done
)

const (
	suspendCls suspendReason = iota
	suspendTimer
	suspendRcv
	suspendSnd
)

func NewGoroutine(fn func(), internal bool) {
	CurrentSimulator().scheduler.add(&goroutine{
		id:             goroutineId(nextId("go")),
		fn:             fn,
		internal:       internal,
		syncCh:         make(chan struct{}),
		suspendedCh:    make(chan struct{}),
		sourceLocation: sourceLocation(2),
	})
}

func (g *goroutine) run() {
	switch g.state {
	case ready:
		// goroutine hasn't been started yet, let's spawn it
		Dbg("🚀", "launch", LocTag(g.sourceLocation))
		g.state = running
		go g.spawn()
	case suspended:
		// goroutine was suspended, let's resume it
		Dbg("▶️", "resume")
		g.state = running
		<-g.suspendedCh // this unblocks the goroutine
	default:
		// scheduler should prevent this from ever happening
		panic(fmt.Sprintf("cannot resume goroutine #%d since it is %v", g.id, g.describeState()))
	}
}

func (g *goroutine) spawn() {
	// once this goroutine completes ...
	defer func() { g.done() }()

	// execute the actual code inside the goroutine
	g.fn()
}

func (g *goroutine) suspended(b *syncBlock, tags ...Tag) {
	verify.T(g.state == running,
		"trying to suspend goroutine #%v that was not running: (%v)", g.id, g.describeState())

	if CurrentSimulator().debug {
		switch {
		case b.requireSyncMatch:
			switch b.op {
			case cls:
				Dbg("🛑📭❌", "cls block", tags...)
			case rcv:
				Dbg("🛑📭➡️️", "rcv block", tags...)
			case snd:
				Dbg("🛑📭⬅️️", "snd block", tags...)
			case slc:
				Dbg("🛑🌀", "slc block", tags...)
			}
		case b.delay != 0:
			Dbg("⏸️⏳", "suspend", append(tags, AnyTag("t", b.delay))...)
		default:
			// TODO: don't always suspend right away here, instead accumulate syncBlocks and continue
			Dbg("⏸️", "suspend")
		}
	}

	g.state = suspended
	g.syncBlock = b
	g.syncBlock.g = g

	// TODO
	g.syncCh <- struct{}{}
	g.suspendedCh <- struct{}{}
}

func (g *goroutine) done() {
	verify.T(g.state == running,
		"trying to complete goroutine #%v that was not running: (%v)", g.id, g.describeState())

	Dbg("✔️", "done")

	// update state to tell scheduler to clean up this goroutine
	g.state = done

	// close channel to unblock scheduler that waits for this
	close(g.suspendedCh)
	close(g.syncCh)
}

func (g *goroutine) waitUntilStopped() {
	<-g.syncCh
}

func (g *goroutine) describeState() string {
	switch g.state {
	case ready:
		return "ready"
	case running:
		return "running"
	case suspended:
		return "suspended"
	case done:
		return "done"
	default:
		return "unknown"
	}
}

func (r suspendReason) string() string {
	switch r {
	case suspendCls:
		return "cls"
	case suspendTimer:
		return "timer"
	case suspendRcv:
		return "rcv"
	case suspendSnd:
		return "snd"
	default:
		return "unknown"
	}
}
