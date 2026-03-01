package sim_runtime

import (
	"fmt"
	"reflect"
	"unsafe"

	"go.temporal.io/server/tools/gomad/util/verify"
)

var _ Channel = &ChanState[any]{}

type (
	ChannelId        uint64
	ChanState[T any] struct {
		id          ChannelId
		nativeCh    chan T
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

	ptr := chanPtr(ch)
	if simCh, ok := CurrentSimulator().scheduler.channels[ptr]; ok {
		c := simCh.(*ChanState[T])
		c.nativeCh = ch
		return c
	}

	id := ChannelId(nextId("chan"))
	Dbg("📭🆕", "open", ChanTag(id), AnyTag("src", sourceLocation(2)))
	simCh := &ChanState[T]{id: id}
	simCh.nativeCh = ch
	CurrentSimulator().scheduler.channels[ptr] = simCh
	return simCh
}

func (c *ChanState[T]) RcvOk() (msg any, ok bool) {
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
	Dbg("📭📖", "rcv", ChanTag(c.id), BufTag(c.nativeCh), CurLocTag())
	msg = *c.inflight
	c.inflight = nil
	c.hasInflight = false
	return msg, true
}

func (c *ChanState[T]) readFromBuffer() (msg any, ok bool) {
	verify.T(c.canRcvBuffered(), "channel #%v must have buffered messages", c.id)
	Dbg("📭📖📚", "rcv buf", ChanTag(c.id), BufTag(c.nativeCh), CurLocTag())
	return <-c.nativeCh, true
}

func (c *ChanState[T]) readFromClosed() (msg any, ok bool) {
	verify.T(c.closed, "channel #%v must be closed", c.id)
	Dbg("📭📖0️⃣", "rcv zero", ChanTag(c.id), BufTag(c.nativeCh), CurLocTag())
	var zeroVal T
	return zeroVal, false
}

func (c *ChanState[T]) Snd(msg any) {
	verify.T(!c.closed, "sending to closed channel #%v", c.id)

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
	Dbg("📭📝📚", "snd buf", ChanTag(c.id), BufTag(c.nativeCh), CurLocTag())
	verify.T(c.canSndBuffered(), "channel #%v buffer cannot be written to", c.id)
	c.nativeCh <- msg
}

func (c *ChanState[T]) writeToInFlight(msg T) {
	Dbg("📭📝", "snd", ChanTag(c.id), BufTag(c.nativeCh), CurLocTag())
	verify.T(!c.hasInflight, "there is already an in-flight message in channel #%v", c.id)
	c.inflight = &msg
	c.hasInflight = true
}

func (c *ChanState[T]) Cls() {
	verify.T(!c.closed, "closing already closed channel #%v", c.id)

	c.closed = true
	close(c.nativeCh)

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
	isBuffered := c.bufferedChannel()
	isNotFull := len(c.nativeCh) < cap(c.nativeCh)
	return isBuffered && isNotFull
}

func (c *ChanState[T]) canRcvBuffered() bool {
	isBuffered := c.bufferedChannel()
	hasMessage := len(c.nativeCh) > 0
	return isBuffered && hasMessage
}

func (c *ChanState[T]) bufferedChannel() bool {
	return cap(c.nativeCh) > 0
}

func ChanId[T any](ch chan T) ChannelId {
	return GetOrCreateChan(*(&ch)).id
}

func chanPtr(ch any) unsafe.Pointer {
	return unsafe.Pointer(reflect.ValueOf(ch).Pointer())
}
