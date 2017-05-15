// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package actor

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber-go/timer/twheel"
)

type (
	// Action identifies distinct call/cast actions handled by actor
	Action int

	// CastFunc is fire-and-forget action (w/o reply)
	CastFunc func(req interface{})
	// CallFunc is request/reply action
	CallFunc func(req interface{}) (interface{}, error)
	// AsyncCallFunc is async request/reply action
	AsyncCallFunc func(req interface{}, resp *Response)
	// StopFunc is a function called when actor is terminating
	StopFunc func()
	// CancelFunc cancels a scheduled call
	CancelFunc func() (canceled bool)
)

const (
	setup int32 = iota
	running
	stopped
)

// Option is Actor's option
type Option func(*opts)

type opts struct {
	logger   bark.Logger
	trace    bool
	queueCap int
	descrs   map[Action]string
}

// WithLogger provides optional logger for tracing actor's execution
func WithLogger(logger bark.Logger) Option {
	return func(opts *opts) {
		opts.logger = logger
	}
}

// WithTracing enables or disables tracing of actor's execution
func WithTracing(enabled bool) Option {
	return func(opts *opts) {
		opts.trace = enabled
	}
}

// WithActionDescriptions sets optional readable descriptions of actor actions
func WithActionDescriptions(descrs map[Action]string) Option {
	return func(opts *opts) {
		opts.descrs = descrs
	}
}

// WithQueueCapacity overrides actor's action queue capacity
func WithQueueCapacity(capacity int) Option {
	return func(opts *opts) {
		opts.queueCap = capacity
	}
}

type request struct {
	id  Action
	req interface{}

	// cast-specific part
	cast CastFunc

	// call-specific part
	call AsyncCallFunc
	resp *Response
}

// Response is black-box response representation that allows to asynchronously reply to call
type Response struct {
	ch     chan struct{}
	result interface{}
	err    error
}

// Reply unblocks call request and returns provided pair of result and error (if any).
// Reply should be called exactly once per each call. Response struct is reused.
func (r *Response) Reply(result interface{}, err error) {
	r.result = result
	r.err = err
	r.ch <- struct{}{}
}

// Actor is generic execution behavior that serializes all actions and provides both fire-and-forget
// and request-reply handling.
type Actor struct {
	name  string
	casts []CastFunc
	calls []AsyncCallFunc
	stop  StopFunc

	state    int32
	requests chan request
	stopChan chan struct{}
	doneChan chan struct{}

	logger bark.Logger
	trace  bool
	descrs map[Action]string

	respPool sync.Pool
}

// New creates new named instance of Actor
func New(name string, options ...Option) *Actor {
	opts := &opts{queueCap: 100}
	for _, option := range options {
		option(opts)
	}
	var logger bark.Logger
	if opts.logger == nil {
		l := logrus.New()
		logger = bark.NewLoggerFromLogrus(l)
	} else {
		logger = opts.logger
	}
	logger = logger.WithField("name", name)
	return &Actor{
		name:     name,
		requests: make(chan request, opts.queueCap),
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		logger:   opts.logger,
		trace:    opts.trace,
		descrs:   opts.descrs,
		respPool: sync.Pool{
			New: func() interface{} {
				return &Response{ch: make(chan struct{})}
			},
		},
	}
}

// RegisterCall registers call (request-reply) action for specified action ID
func (a *Actor) RegisterCall(id Action, call CallFunc) {
	// best effort check for registering action in non-setup state
	if atomic.LoadInt32(&a.state) != setup {
		panic(fmt.Sprintf("Actor %v: registering call #%v in running state", a.name, a.action(id)))
	}
	idx := int(id)
	for idx >= len(a.calls) {
		a.calls = append(a.calls, nil)
	}
	a.calls[idx] = func(req interface{}, resp *Response) {
		res, err := call(req)
		resp.Reply(res, err)
		if a.trace {
			a.logger.Debug("Actor call ended: id=%v, res=%v, err=%v",
				a.action(id), res, err)
		}
	}
}

// RegisterAsyncCall registers asynchronous call (request-reply) action for specified action ID
func (a *Actor) RegisterAsyncCall(id Action, call AsyncCallFunc) {
	// best effort check for registering action in non-setup state
	if atomic.LoadInt32(&a.state) != setup {
		panic(fmt.Sprintf("Actor %v: registering async call #%v in running state",
			a.name, a.action(id)))
	}
	idx := int(id)
	for idx >= len(a.calls) {
		a.calls = append(a.calls, nil)
	}
	a.calls[idx] = func(req interface{}, resp *Response) {
		call(req, resp)
		if a.trace {
			a.logger.Debug("Actor async call ended. id=%v", a.action(id))
		}
	}
}

// RegisterCast registers cast (fire-and-forget) action for specified action ID
func (a *Actor) RegisterCast(id Action, cast CastFunc) {
	// best effort check for registering action in non-setup state
	if atomic.LoadInt32(&a.state) != setup {
		panic(fmt.Sprintf("Actor %v: registering call #%v in running state", a.name, a.action(id)))
	}
	idx := int(id)
	for idx >= len(a.casts) {
		a.casts = append(a.casts, nil)
	}
	a.casts[idx] = cast
}

// RegisterStop registers stop function executed before actor is stopped
func (a *Actor) RegisterStop(stop StopFunc) {
	// best effort check for registering action in non-setup state
	if atomic.LoadInt32(&a.state) != setup {
		panic(fmt.Sprintf("Actor %v: registering stop action in running state", a.name))
	}
	a.stop = stop
}

// Start starts actor and starts processing actions
func (a *Actor) Start() {
	if atomic.CompareAndSwapInt32(&a.state, setup, running) {
		if a.trace {
			a.logger.Debug("Actor starts")
		}
		go a.process()
	}
}

// Stop initiates actor shutdown.
// Stop returns channel that is closed when shutdown is completed.
func (a *Actor) Stop() <-chan struct{} {
	if a.trace {
		a.logger.Debug("Actor stop requested")
	}
	switch atomic.SwapInt32(&a.state, stopped) {
	case setup:
		close(a.doneChan)
	case running:
		close(a.stopChan)
	}
	return a.Done()
}

// Done returns channel, that will be closed when actor is shutdown.
func (a *Actor) Done() <-chan struct{} {
	return a.doneChan
}

// Call invokes call action (synchronously) identified by provided ID, passes req as argument and
// returns whatever was returned from call action. Call assumes that call action will never return
// error, but if it does, actor panics.
func (a *Actor) Call(id Action, req interface{}) interface{} {
	res, err := a.CallErr(id, req)
	if err != nil {
		panic(fmt.Sprintf("Actor %v: action #%v returned error: %v", a.name, a.action(id), err))
	}
	return res
}

// CallErr invokes call action (synchronously) identified by provided ID, passes req as argument
// and returns whatever result is returned from call action, as well as any error, if any.
func (a *Actor) CallErr(id Action, req interface{}) (interface{}, error) {
	idx := int(id)
	if idx < 0 || idx >= len(a.calls) {
		panic(fmt.Sprintf("Actor %v: invalid call #%v provided", a.name, a.action(id)))
	}
	call := a.calls[idx]
	if call == nil {
		panic(fmt.Sprintf("Actor %v: no call action #%v is defined", a.name, a.action(id)))
	}

	resp := a.respPool.Get().(*Response)
	a.requests <- request{
		id:   id,
		req:  req,
		call: call,
		resp: resp,
	}
	<-resp.ch
	res := resp.result
	err := resp.err
	resp.result, resp.err = nil, nil
	a.respPool.Put(resp)
	return res, err
}

// Cast invokes registered cast action (asynchronously) identified by provided ID and passes
// req as argument.
func (a *Actor) Cast(id Action, req interface{}) {
	idx := int(id)
	if idx < 0 || idx >= len(a.casts) {
		panic(fmt.Sprintf("Actor %v: invalid cast ID %v provided", a.name, a.action(id)))
	}
	cast := a.casts[id]
	if cast == nil {
		panic(fmt.Sprintf("Actor %v: no cast action #%v is defined", a.name, a.action(id)))
	}

	a.requests <- request{
		id:   id,
		req:  req,
		cast: cast,
	}
}

type scheduledCall struct {
	id  Action
	req interface{}
}

func (a *Actor) callScheduled(arg interface{}) {
	s := arg.(*scheduledCall)
	a.Cast(s.id, s.req)
}

func emptyFunc() bool { return false }

var timeoutWheel = twheel.NewTimeoutWheel()

// ScheduleCast schedules a cast action with a specified delay.
// Returns a function which can be called to cancel the scheduled action.
// Cancelling is not atomic so it doesn't guarantee that the scheduled action is not going to execute.
func (a *Actor) ScheduleCast(id Action, req interface{}, d time.Duration) CancelFunc {
	timeout, err := timeoutWheel.Schedule(d, a.callScheduled,
		&scheduledCall{id: id, req: req})
	if err != nil {
		a.logger.Debug("Schedule called on stopped actor")
		return emptyFunc
	}
	return timeout.Stop
}

func (a *Actor) process() {
	for {
		select {
		case req := <-a.requests:
			a.processAction(req)
		case <-a.stopChan:
			a.terminate()
			return
		}
	}
}

func (a *Actor) processAction(req request) {
	defer func() {
		if err := recover(); err != nil {
			if req.call != nil {
				panic(fmt.Sprintf("Actor %v: paniced during call #%v, args=%#v: %v",
					a.name, a.action(req.id), req.req, err))
			} else if req.cast != nil {
				panic(fmt.Sprintf("Actor %v: paniced during cast #%v: %v",
					a.name, a.action(req.id), err))
			} else {
				panic(fmt.Sprintf("Actor %v: paniced during stop: %v", a.name, err))
			}
		}
	}()

	if cast := req.cast; cast != nil {
		if a.trace {
			a.logger.Debug("Actor cast start. id=%v, arg=%v", a.action(req.id), req.req)
		}

		cast(req.req)

		if a.trace {
			a.logger.Debug("Actor cast ended. id=%v", a.action(req.id))
		}
	} else {
		if a.trace {
			a.logger.Debug("Actor call start. id=%v, arg=%v",
				a.action(req.id), req.req)
		}

		req.call(req.req, req.resp)
	}
}

func (a *Actor) terminate() {
	if a.stop != nil {
		if a.trace {
			a.logger.Debug("Actor is stopping")
		}
		a.stop()
	}

	if a.trace {
		a.logger.Debug("Actor stopped")
	}
	close(a.doneChan)
}

func (a *Actor) action(id Action) string {
	if descr, ok := a.descrs[id]; ok {
		return descr
	}
	return strconv.Itoa(int(id))
}
