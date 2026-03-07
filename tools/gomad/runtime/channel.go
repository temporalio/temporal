package sim_runtime

import (
	"fmt"
	"reflect"
	"runtime"
	"unsafe"

	"go.temporal.io/server/tools/gomad/util/verify"
)

var _ Channel = &ChanState[any]{}

type (
	ChannelId        uint64
	ChanState[T any] struct {
		id          ChannelId
		buf         []T // backing buffer for buffered channels (replaces native channel)
		cap         int // channel capacity (from make(chan T, cap))
		inflight    *T
		hasInflight bool // needed to disambiguate `inflight` holding the message `nil`
		closed      bool
	}
	Channel interface {
		Snd(msg any)
		write(msg any)
		RcvOk() (msg any, ok bool)
		read() (msg any, ok bool)
		requiresSyncMatch(op SyncOp) bool
	}
)

func GetOrCreateChan[T any](ch chan T) *ChanState[T] {
	if ch == nil {
		return nil
	}

	// If no simulation is active (e.g. called from a package init() before Start()),
	// return nil. Callers must fall back to native channel operations in this case.
	sim := tryAnySimulator()
	if sim == nil {
		return nil
	}

	key := chanKey(ch)

	sim.scheduler.channelsMu.Lock()
	if simCh, ok := sim.scheduler.channels[key]; ok {
		sim.scheduler.channelsMu.Unlock()
		return simCh.(*ChanState[T])
	}
	sim.scheduler.channelsMu.Unlock()

	id := ChannelId(nextId(sim, "chan"))
	Dbg("📭🆕", "open", ChanTag(id), AnyTag("src", sourceLocation(2)))
	bufCap := cap(ch)
	simCh := &ChanState[T]{
		id:  id,
		buf: make([]T, 0, bufCap),
		cap: bufCap,
	}

	// Absorb any pre-existing state from the native channel (e.g. values buffered or
	// closed before the simulation started). This lets package-level channels that were
	// closed in an init() function appear correctly closed inside the simulation.
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				simCh.closed = true
			} else {
				simCh.buf = append(simCh.buf, v)
			}
		default:
			goto drained
		}
		if simCh.closed {
			break
		}
	}
drained:
	sim.scheduler.channelsMu.Lock()
	// Re-check in case another goroutine created the entry while we were initializing.
	if existing, ok := sim.scheduler.channels[key]; ok {
		sim.scheduler.channelsMu.Unlock()
		return existing.(*ChanState[T])
	}
	sim.scheduler.channels[key] = simCh
	sim.scheduler.channelsMu.Unlock()

	// Register a finalizer to remove the map entry when ch's underlying hchan is
	// collected by the GC. We must NOT store ch in ChanState for this to fire:
	// any strong reference to ch (or hchan) in ChanState would prevent collection.
	//
	// SetFinalizer (not AddCleanup) is used intentionally: SetFinalizer guarantees
	// the GC does not recycle the hchan's memory before the finalizer runs. This
	// prevents a new channel allocated at the same address from being matched to a
	// stale, already-closed ChanState before the map entry is removed.
	// AddCleanup does NOT provide this guarantee — it allows memory recycling before
	// the cleanup fires, which creates a race that causes "closing already closed
	// channel" panics.
	hchan := (*byte)(unsafe.Pointer(reflect.ValueOf(ch).Pointer()))
	runtime.SetFinalizer(hchan, func(*byte) {
		sim.scheduler.channelsMu.Lock()
		delete(sim.scheduler.channels, key)
		sim.scheduler.channelsMu.Unlock()
	})

	return simCh
}

func (c *ChanState[T]) RcvOk() (msg any, ok bool) {
	// If called from outside the cooperative scheduler, delegate to a simulated goroutine.
	if tryCurrentGoroutine() == nil {
		s := tryAnySimulator()
		type result struct {
			msg any
			ok  bool
		}
		nativeDone := make(chan result, 1)
		g := &goroutine{
			id:          goroutineId(nextId(s, "go")),
			sim:         s,
			fn:          func() { m, o := c.RcvOk(); nativeDone <- result{m, o} },
			syncCh:      make(chan struct{}),
			suspendedCh: make(chan struct{}),
		}
		s.scheduler.addFromNative(g)
		r := <-nativeDone
		return r.msg, r.ok
	}

	// first, attempt to fulfill the operation without requiring a sync

	if c.canRcvBuffered() {
		// no need to wait for sync match, buffered message available
		msg, ok = c.readFromBuffer()

		// TODO:
		suspend(&syncBlock{
			pt:               c,
			op:               rcv,
			requireSyncMatch: false,
		}, ChanTag(c.id))

		return
	}

	if c.closed {
		// no need to wait for sync match, already closed
		msg, ok = c.readFromClosed()
		return
	}

	// well, need to wait for sync match - *block*!
	suspend(&syncBlock{
		pt:               c,
		op:               rcv,
		requireSyncMatch: true,
		onSync:           func() { msg, ok = c.read() },
	}, ChanTag(c.id))
	return
}

func (c *ChanState[T]) read() (msg any, ok bool) {
	if c.hasInflight {
		return c.readInFlight()
	} else if c.canRcvBuffered() {
		return c.readFromBuffer()
	} else if c.closed {
		return c.readFromClosed()
	}
	panic(fmt.Sprintf("internal error: failed to rcv from channel #%v", c.id))
}

func (c *ChanState[T]) readInFlight() (msg any, ok bool) {
	verify.T(c.hasInflight, "channel #%v must have in-flight message", c.id)
	Dbg("📭📖", "rcv", ChanTag(c.id), c.bufTag(), CurLocTag())
	msg = *c.inflight
	c.inflight = nil
	c.hasInflight = false
	return msg, true
}

func (c *ChanState[T]) readFromBuffer() (msg any, ok bool) {
	verify.T(c.canRcvBuffered(), "channel #%v must have buffered messages", c.id)
	Dbg("📭📖📚", "rcv buf", ChanTag(c.id), c.bufTag(), CurLocTag())
	msg = c.buf[0]
	c.buf = c.buf[1:]
	return msg, true
}

func (c *ChanState[T]) readFromClosed() (msg any, ok bool) {
	verify.T(c.closed, "channel #%v must be closed", c.id)
	Dbg("📭📖0️⃣", "rcv zero", ChanTag(c.id), c.bufTag(), CurLocTag())
	var zeroVal T
	return zeroVal, false
}

func (c *ChanState[T]) Snd(msg any) {
	// Check for closed channel before delegation so the panic propagates
	// synchronously to the caller even from non-cooperative goroutines.
	verify.T(!c.closed, "sending to closed channel #%v", c.id)

	// If called from outside the cooperative scheduler, always delegate to a simulated
	// goroutine. Multiple non-cooperative goroutines (e.g. FX lifecycle goroutines) may
	// call Snd concurrently; serializing through the scheduler prevents data races on
	// ChanState even for buffered channels that currently have available capacity.
	if tryCurrentGoroutine() == nil {
		s := tryAnySimulator()
		nativeDone := make(chan struct{}, 1)
		g := &goroutine{
			id:          goroutineId(nextId(s, "go")),
			sim:         s,
			fn:          func() { c.Snd(msg); nativeDone <- struct{}{} },
			syncCh:      make(chan struct{}),
			suspendedCh: make(chan struct{}),
		}
		s.scheduler.addFromNative(g)
		<-nativeDone
		return
	}

	// first, attempt to fulfill the operation without requiring a sync

	if c.closed {
		// no need to wait for sync match since this will crash immediately!
		c.write(msg)
	}

	if c.canSndBuffered() {
		// no need to wait for sync match, buffer is available
		c.write(msg)

		// TODO:
		suspend(&syncBlock{
			pt:               c,
			op:               snd,
			requireSyncMatch: false,
		}, ChanTag(c.id))

		return
	}

	// well, need to wait for sync match - *block*!
	suspend(&syncBlock{
		pt:               c,
		op:               snd,
		requireSyncMatch: true,
		onSync:           func() { c.write(msg) },
	}, ChanTag(c.id))
}

func (c *ChanState[T]) write(msg any) {
	verify.T(!c.closed, "sending to closed channel #%v", c.id)

	var typedMsg T
	if msg != nil { // casting `nil` would panic
		typedMsg = msg.(T)
	}

	if c.canSndBuffered() {
		c.writeToBuffer(typedMsg)
	} else {
		c.writeToInFlight(typedMsg)
	}
}

func (c *ChanState[T]) writeToBuffer(msg T) {
	Dbg("📭📝📚", "snd buf", ChanTag(c.id), c.bufTag(), CurLocTag())
	verify.T(c.canSndBuffered(), "channel #%v buffer cannot be written to", c.id)
	c.buf = append(c.buf, msg)
}

func (c *ChanState[T]) writeToInFlight(msg T) {
	Dbg("📭📝", "snd", ChanTag(c.id), c.bufTag(), CurLocTag())
	verify.T(!c.hasInflight, "there is already an in-flight message in channel #%v", c.id)
	c.inflight = &msg
	c.hasInflight = true
}

func (c *ChanState[T]) Cls() {
	verify.T(!c.closed, "closing already closed channel #%v", c.id)

	// If called from outside the cooperative scheduler, delegate to a simulated goroutine
	// so the close runs at the correct simulated time and properly wakes blocked receivers.
	if tryCurrentGoroutine() == nil {
		s := tryAnySimulator()
		nativeDone := make(chan struct{}, 1)
		g := &goroutine{
			id:          goroutineId(nextId(s, "go")),
			sim:         s,
			fn:          func() { c.Cls(); nativeDone <- struct{}{} },
			syncCh:      make(chan struct{}),
			suspendedCh: make(chan struct{}),
		}
		s.scheduler.addFromNative(g)
		<-nativeDone
		return
	}

	c.closed = true
	// Note: the native channel is not closed here; buffer state and close
	// semantics are managed entirely through ChanState.buf and ChanState.closed.

	// *block* to sync all channel receivers
	suspend(&syncBlock{
		pt:               c,
		op:               cls,
		requireSyncMatch: false,
	}, ChanTag(c.id))
}

func (c *ChanState[T]) requiresSyncMatch(op SyncOp) bool {
	if c.closed {
		// a closed channel never requires a sync match
		return false
	}

	switch op {
	case snd:
		return !c.canSndBuffered()
	case rcv:
		return !c.canRcvBuffered()
	default:
		panic(fmt.Sprintf("unsupported sync operation: %v", op))
	}
}

func (c *ChanState[T]) canSndBuffered() bool {
	return c.cap > 0 && len(c.buf) < c.cap
}

func (c *ChanState[T]) canRcvBuffered() bool {
	return len(c.buf) > 0
}

// IsClosed reports whether the channel has been closed.
// Safe to call from a cooperative goroutine without suspending.
func (c *ChanState[T]) IsClosed() bool {
	return c.closed
}

func (c *ChanState[T]) bufferedChannel() bool {
	return c.cap > 0
}

func (c *ChanState[T]) bufTag() Tag {
	if c.cap > 0 {
		return AnyTag("buf", fmt.Sprintf("%d/%d", len(c.buf), c.cap))
	}
	return emptyTag
}

func ChanId[T any](ch chan T) ChannelId {
	return GetOrCreateChan(*(&ch)).id
}

// chanKey returns the identity of a channel as a uintptr map key.
// uintptr is intentionally not traced by the GC, allowing the underlying
// hchan to be collected when all chan T references are gone.
func chanKey(ch any) uintptr {
	return reflect.ValueOf(ch).Pointer()
}
