// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package nettest

import (
	"errors"
	"net"
)

type (
	// Pipe is an in-memory socket. It provides similar functionality to net.Listen and net.Dial via the Pipe.Accept
	// and Pipe.Connect methods, respectively. It is useful for faster and more deterministic testing of netcode.
	Pipe struct {
		clients chan connCb
	}

	// connResult captures a value which is either a connection or an error.
	connResult struct {
		conn net.Conn
		err  error
	}

	// connCb is a callback that a connection result will be sent to.
	connCb chan connResult
)

// ErrCanceled indicates that an operation was canceled by the client (e.g. via ctx.Done()).
var ErrCanceled = errors.New("pipe operation canceled by client")

// NewPipe returns a Pipe.
func NewPipe() *Pipe {
	return &Pipe{
		clients: make(chan connCb),
	}
}

// Accept returns a connection to the server for a client.
func (p *Pipe) Accept(cancel <-chan struct{}) (net.Conn, error) {
	select {
	case <-cancel:
		return nil, ErrCanceled
	case client := <-p.clients:
		clientConn, serverConn := net.Pipe()
		client <- connResult{conn: clientConn}

		return serverConn, nil
	}
}

// Connect returns a connection to a client for the server.
func (p *Pipe) Connect(cancel <-chan struct{}) (net.Conn, error) {
	clientConnCb := make(connCb)
	select {
	case p.clients <- clientConnCb:
		r := <-clientConnCb
		return r.conn, r.err
	case <-cancel:
		return nil, ErrCanceled
	}
}
