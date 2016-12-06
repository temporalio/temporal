package coroutine

import (
	"runtime"
	"sync"

	"golang.org/x/net/context"
)

type valueCallbackPair struct {
	value    interface{}
	callback func()
}

type channelImpl struct {
	size         int                 // Channel buffer size. 0 for non buffered.
	buffer       []interface{}       // buffered messages
	blockedSends []valueCallbackPair // puts waiting when buffer is full.
	blockedRecvs []func(interface{}) // receives waiting when no messages are available.
	closed       bool                // true if channel is closed
}

// Single case statement of the Select
type selectCase struct {
	channel   Channel       // Channel of this case.
	recvFunc  *RecvCaseFunc // function to call when channel has a message. nil for send case.
	sendFunc  *SendCaseFunc // function to call when channel accepted a message. nil for receive case.
	sendValue *interface{}  // value to send to the channel. Used only for send case.
}

// Implements Selector interface
type selectorImpl struct {
	cases       []selectCase     // cases that this select is comprised from
	defaultFunc *DefaultCaseFunc // default case
}

// ContextProvider is an interface that a Context implementation passed as a parameter to Channel and Selector
// methods must implement.
// The result of GetContext() call must be the context passed to the coroutine function which is contextImpl.
// It is needed to be able to pass contexts that extend coroutine.Context to these methods.
// See WorkflowContext implementation for an example.
type ContextProvider interface {
	GetContext() Context
}

type contextImpl struct {
	context.Context
	dispatcher   *dispatcherImpl // dispatcher this context belongs to
	aboutToBlock chan bool       // Used to notify dispatcher that coroutine that owns this context is about to block
	unblock      chan bool       // Used to notify coroutine that it should continue executing. When returned value is false goroutine must call runtime.Goexit.
	keptBlocked  bool            // true indicates that coroutine didn't make any progress since the last yield unblocking
	closed       bool            // indicates that owning coroutine has finished execution
}

type dispatcherImpl struct {
	sequence   int
	coroutines []*contextImpl
	executing  bool       // currently running ExecuteUntilAllBlocked. Used to avoid recursive calls to it.
	mutex      sync.Mutex // Used to synchronize executing
}

// Assert that structs do indeed implement the interfaces
var _ Channel = (*channelImpl)(nil)
var _ Context = (*contextImpl)(nil)
var _ Selector = (*selectorImpl)(nil)
var _ Dispatcher = (*dispatcherImpl)(nil)

func (c *channelImpl) Recv(ctx Context) (v interface{}, more bool) {
	ctxImpl := ctx.(ContextProvider).GetContext().(*contextImpl)
	hasResult := false
	var result interface{}
	for {
		if hasResult {
			ctxImpl.unblocked()
			return result, true
		}
		if len(c.buffer) > 0 {
			r := c.buffer[0]
			c.buffer = c.buffer[1:]
			ctxImpl.unblocked()
			return r, true
		}
		if c.closed {
			return nil, false
		}
		if len(c.blockedSends) > 0 {
			b := c.blockedSends[0]
			c.blockedSends = c.blockedSends[1:]
			b.callback()
			ctxImpl.unblocked()
			return b.value, true
		}
		c.blockedRecvs = append(c.blockedRecvs, func(v interface{}) {
			result = v
			hasResult = true
		})
		ctxImpl.yield()
	}
}

func (c *channelImpl) RecvAsync() (v interface{}, ok bool, more bool) {
	if len(c.buffer) > 0 {
		r := c.buffer[0]
		c.buffer = c.buffer[1:]
		return r, true, true
	}
	if c.closed {
		return nil, false, false
	}
	if len(c.blockedSends) > 0 {
		b := c.blockedSends[0]
		c.blockedSends = c.blockedSends[1:]
		b.callback()
		return b.value, true, true
	}
	return nil, false, true
}

func (c *channelImpl) Send(ctx Context, v interface{}) {
	ctxImpl := ctx.(ContextProvider).GetContext().(*contextImpl)
	valueConsumed := false
	for {
		// Check for closed in the loop as close can be called when send is blocked
		if c.closed {
			panic("Closed channel")
		}
		if valueConsumed {
			ctxImpl.unblocked()
			return
		}
		if len(c.buffer) < c.size {
			c.buffer = append(c.buffer, v)
			ctxImpl.unblocked()
			return
		}
		if len(c.blockedRecvs) > 0 {
			blockedGet := c.blockedRecvs[0]
			c.blockedRecvs = c.blockedRecvs[1:]
			blockedGet(v)
			ctxImpl.unblocked()
			return
		}
		c.blockedSends = append(c.blockedSends,
			valueCallbackPair{value: v, callback: func() { valueConsumed = true }})
		ctxImpl.yield()
	}
}

func (c *channelImpl) SendAsync(v interface{}) (ok bool) {
	if c.closed {
		panic("Closed channel")
	}
	if len(c.buffer) < c.size {
		c.buffer = append(c.buffer, v)
		return true
	}
	if len(c.blockedRecvs) > 0 {
		blockedGet := c.blockedRecvs[0]
		c.blockedRecvs = c.blockedRecvs[1:]
		blockedGet(v)
		return true
	}
	return false
}

func (c *channelImpl) Close() {
	c.closed = true
	// All blocked sends are going to panic
	for i := 0; i < len(c.blockedSends); i++ {
		b := c.blockedSends[i]
		b.callback()
	}
}

// initialYield called at the beginning of the coroutine execution
func (ctx *contextImpl) initialYield() {
	active := <-ctx.unblock
	if !active {
		runtime.Goexit()
	}
}

// yield indicates that coroutine cannot make progress and should sleep
// this call blocks
func (ctx *contextImpl) yield() {
	ctx.aboutToBlock <- true
	active := <-ctx.unblock
	if !active {
		runtime.Goexit()
	}
	ctx.keptBlocked = true
}

// unblocked is called by coroutine to indicate that since the last time yield was unblocked channel or select
// where unblocked versus calling yield again after checking their condition
func (ctx *contextImpl) unblocked() {
	ctx.keptBlocked = false
}

func (ctx *contextImpl) call() {
	ctx.unblock <- true
	<-ctx.aboutToBlock
}

func (ctx *contextImpl) close() {
	ctx.closed = true
	ctx.aboutToBlock <- true
}

func (ctx *contextImpl) exit() {
	if !ctx.closed {
		ctx.unblock <- false
	}
}

// GetContext from flow.ContextProvider interface
func (ctx *contextImpl) GetContext() Context {
	return ctx
}

func (ctx *contextImpl) NewCoroutine(f Func) {
	ctx.dispatcher.newCoroutine(f)
}

func (ctx *contextImpl) NewSelector() Selector {
	return &selectorImpl{}
}

func (ctx *contextImpl) NewChannel() Channel {
	return &channelImpl{}
}

func (ctx *contextImpl) NewBufferedChannel(size int) Channel {
	return &channelImpl{size: size}
}

func (d *dispatcherImpl) newCoroutine(f Func) {
	ctx := d.newContext()
	go func(ctx *contextImpl) {
		defer ctx.close()
		ctx.initialYield()
		f(ctx)
	}(ctx)
}

func (d *dispatcherImpl) newContext() *contextImpl {
	c := &contextImpl{
		Context:      context.Background(),
		dispatcher:   d,
		aboutToBlock: make(chan bool, 1),
		unblock:      make(chan bool),
	}
	d.sequence++
	d.coroutines = append(d.coroutines, c)
	return c
}

func (d *dispatcherImpl) ExecuteUntilAllBlocked() {
	d.mutex.Lock()
	if d.executing {
		panic("call to ExecuteUntilAllBlocked (possibly from a coroutine) while it is already running")
	}
	d.executing = true
	d.mutex.Unlock()
	defer func() { d.executing = false }()
	allBlocked := false
	// Keep executing until at least one goroutine made some progress
	for !allBlocked {
		// Give every coroutine chance to execute removing closed ones
		allBlocked = true
		lastSequence := d.sequence
		for i := 0; i < len(d.coroutines); i++ {
			c := d.coroutines[i]
			if !c.closed {
				// TODO: Support handling of panic in a coroutine by dispatcher.
				// TODO: Dump all outstanding coroutines if one of them panics
				c.call()
			}
			// c.call() can close the context so check again
			if c.closed {
				// remove the closed one from the slice
				d.coroutines = append(d.coroutines[:i],
					d.coroutines[i+1:]...)
				i--
				allBlocked = false
			} else {
				allBlocked = allBlocked && (c.keptBlocked || c.closed)
			}
		}
		// Set allBlocked to false if new coroutines where created
		allBlocked = allBlocked && lastSequence == d.sequence
		if len(d.coroutines) == 0 {
			break
		}
	}
}

func (d *dispatcherImpl) IsDone() bool {
	return len(d.coroutines) == 0
}

func (d *dispatcherImpl) Close() {
	for i := 0; i < len(d.coroutines); i++ {
		c := d.coroutines[i]
		if !c.closed {
			c.exit()
		}
	}
}

func (s *selectorImpl) AddRecv(c Channel, f RecvCaseFunc) Selector {
	s.cases = append(s.cases, selectCase{channel: c, recvFunc: &f})
	return s
}

func (s *selectorImpl) AddSend(c Channel, v interface{}, f SendCaseFunc) Selector {
	s.cases = append(s.cases, selectCase{channel: c, sendFunc: &f, sendValue: &v})
	return s
}

func (s *selectorImpl) AddDefault(f DefaultCaseFunc) {
	s.defaultFunc = &f
}

func (s *selectorImpl) Select(ctx Context) {
	ctxImpl := ctx.(ContextProvider).GetContext().(*contextImpl)
	for {
		for _, pair := range s.cases {
			if pair.recvFunc != nil {
				v, ok, more := pair.channel.RecvAsync()
				if ok || !more {
					f := *pair.recvFunc
					f(v, more)
					ctxImpl.unblocked()
					return
				}
			} else {
				ok := pair.channel.SendAsync(*pair.sendValue)
				if ok {
					f := *pair.sendFunc
					f()
					ctxImpl.unblocked()
					return
				}
			}
		}
		if s.defaultFunc != nil {
			f := *s.defaultFunc
			f()
			ctxImpl.unblocked()
			return
		}
		ctxImpl.yield()
	}
}
