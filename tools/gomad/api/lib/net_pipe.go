// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lib

import (
	"io"
	"net"
	"os"
	"sync"
	"time"

	SIMAPI "go.temporal.io/server/tools/gomad/api/lang"
)

// pipeDeadline is an abstraction for handling timeouts.
type pipeDeadline struct {
	mu     Mutex // Guards timer and cancel
	timer  *Timer
	cancel chan struct{}
} // Must be non-nil

func makePipeDeadline() pipeDeadline {
	SIMAPI.FuncStart()
	return pipeDeadline{cancel: make(chan struct{})}
}

// set sets the point in time when the deadline will time out.
// A timeout event is signaled by closing the channel returned by waiter.
// Once a timeout has occurred, the deadline can be refreshed by specifying a
// t value in the future.
//
// A zero value for t prevents timeout.
func (d *pipeDeadline) set(t time.Time) {
	SIMAPI.FuncStart()
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil && !d.timer.Stop() {
		SIMAPI.ChanRcv(d.cancel) // Wait for the timer callback to finish and close cancel
	}
	d.timer = nil

	// Time is zero, then there is no deadline.
	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}

	// Time in the future, setup a timer to cancel in the future.
	if dur := Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		d.timer = AfterFunc(dur, func() {
			SIMAPI.ChanClose(d.cancel)
		})
		return
	}

	// Time in the past, so close immediately.
	if !closed {
		SIMAPI.ChanClose(d.cancel)
	}
}

// wait returns a channel that is closed when the deadline is exceeded.
func (d *pipeDeadline) wait() chan struct{} {
	SIMAPI.FuncStart()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	SIMAPI.FuncStart()
	switch selector := SIMAPI.Select(0, SIMAPI.RcvChan(c), nil, nil); selector.Case {
	case 0:
		return true
	default:

		return false
	}

}

type pipeAddr struct{}

func (pipeAddr) Network() string { SIMAPI.FuncStart(); return "pipe" }
func (pipeAddr) String() string  { SIMAPI.FuncStart(); return "pipe" }

type pipe struct {
	wrMu Mutex // Serialize Write operations

	// Used by local Read to interact with remote Write.
	// Successful receive on rdRx is always followed by send on rdTx.
	rdRx <-chan []byte
	rdTx chan<- int

	// Used by local Write to interact with remote Read.
	// Successful send on wrTx is always followed by receive on wrRx.
	wrTx chan<- []byte
	wrRx <-chan int

	once       Once // Protects closing localDone
	localDone  chan struct{}
	remoteDone <-chan struct{}

	readDeadline  pipeDeadline
	writeDeadline pipeDeadline
}

// Pipe creates a synchronous, in-memory, full duplex
// network connection; both ends implement the [Conn] interface.
// Reads on one end are matched with writes on the other,
// copying data directly between the two; there is no internal
// buffering.
func Pipe() (net.Conn, net.Conn) {
	SIMAPI.FuncStart()
	cb1 := make(chan []byte)
	cb2 := make(chan []byte)
	cn1 := make(chan int)
	cn2 := make(chan int)
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	p1 := &pipe{rdRx: cb1, rdTx: cn1,
		wrTx: cb2, wrRx: cn2,
		localDone: done1, remoteDone: done2,
		readDeadline:  makePipeDeadline(),
		writeDeadline: makePipeDeadline()}

	p2 := &pipe{rdRx: cb2, rdTx: cn2,
		wrTx: cb1, wrRx: cn1,
		localDone: done2, remoteDone: done1,
		readDeadline:  makePipeDeadline(),
		writeDeadline: makePipeDeadline()}

	return p1, p2
}

func (*pipe) LocalAddr() net.Addr  { SIMAPI.FuncStart(); return pipeAddr{} }
func (*pipe) RemoteAddr() net.Addr { SIMAPI.FuncStart(); return pipeAddr{} }

func (p *pipe) Read(b []byte) (int, error) {
	SIMAPI.FuncStart()
	n, err := p.read(b)
	if err != nil && err != io.EOF && err != io.ErrClosedPipe {
		err = &net.OpError{Op: "read", Net: "pipe", Err: err}
	}
	return n, err
}

func (p *pipe) read(b []byte) (n int, err error) {
	SIMAPI.FuncStart()
	switch {
	case isClosedChan(p.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(p.remoteDone):
		return 0, io.EOF
	case isClosedChan(p.readDeadline.wait()):
		return 0, os.ErrDeadlineExceeded
	}
	switch selector := SIMAPI.Select(0, SIMAPI.RcvChan(p.rdRx), nil, 0, SIMAPI.RcvChan(p.localDone), nil, 0, SIMAPI.RcvChan(p.remoteDone), nil, 0, SIMAPI.RcvChan(p.readDeadline.wait()), nil); selector.Case {
	case 0:
		var bw []byte
		selector.Assign(&bw)
		nr := copy(b, bw)
		SIMAPI.ChanSend(p.rdTx, nr)
		return nr, nil
	case 1:

		return 0, io.ErrClosedPipe
	case 2:

		return 0, io.EOF
	case 3:

		return 0, os.ErrDeadlineExceeded
	default:
		panic("unreachable")
	}

}

func (p *pipe) Write(b []byte) (int, error) {
	SIMAPI.FuncStart()
	n, err := p.write(b)
	if err != nil && err != io.ErrClosedPipe {
		err = &net.OpError{Op: "write", Net: "pipe", Err: err}
	}
	return n, err
}

func (p *pipe) write(b []byte) (n int, err error) {
	SIMAPI.FuncStart()
	switch {
	case isClosedChan(p.localDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(p.remoteDone):
		return 0, io.ErrClosedPipe
	case isClosedChan(p.writeDeadline.wait()):
		return 0, os.ErrDeadlineExceeded
	}

	p.wrMu.Lock() // Ensure entirety of b is written together
	defer p.wrMu.Unlock()
	for once := true; once || len(b) > 0; once = false {
		switch selector := SIMAPI.Select(1, SIMAPI.SndChan(p.wrTx), func() any {
			return b
		}, 0, SIMAPI.RcvChan(p.localDone), nil, 0, SIMAPI.RcvChan(p.remoteDone), nil, 0, SIMAPI.RcvChan(p.writeDeadline.wait()), nil); selector.Case {
		case 0:
			nw := SIMAPI.ChanRcv(p.wrRx)
			b = b[nw:]
			n += nw
		case 1:

			return n, io.ErrClosedPipe
		case 2:

			return n, io.ErrClosedPipe
		case 3:

			return n, os.ErrDeadlineExceeded
		default:
			panic("unreachable")
		}

	}
	return n, nil
}

func (p *pipe) SetDeadline(t time.Time) error {
	SIMAPI.FuncStart()
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.readDeadline.set(t)
	p.writeDeadline.set(t)
	return nil
}

func (p *pipe) SetReadDeadline(t time.Time) error {
	SIMAPI.FuncStart()
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.readDeadline.set(t)
	return nil
}

func (p *pipe) SetWriteDeadline(t time.Time) error {
	SIMAPI.FuncStart()
	if isClosedChan(p.localDone) || isClosedChan(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.writeDeadline.set(t)
	return nil
}

func (p *pipe) Close() error {
	SIMAPI.FuncStart()
	p.once.Do(func() { SIMAPI.ChanClose(p.localDone) })
	return nil
}

type _ = sync.Mutex
type _ = sync.Once

var _ = time.AfterFunc

type _ = time.Timer

var _ = time.Until
