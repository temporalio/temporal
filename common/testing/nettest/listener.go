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

import "net"

// NewListener returns a net.Listener which uses the given Pipe to simulate a network connection.
func NewListener(pipe *Pipe) *PipeListener {
	return &PipeListener{
		Pipe: pipe,
		done: make(chan struct{}),
	}
}

// PipeListener is a net.Listener which uses a Pipe to simulate a network connection.
type PipeListener struct {
	*Pipe
	// We cancel calls to Accept using the done channel so that tests don't hang if they're broken.
	done chan struct{}
}

var _ net.Listener = (*PipeListener)(nil)

func (t *PipeListener) Accept() (net.Conn, error) {
	return t.Pipe.Accept(t.done)
}

func (t *PipeListener) Close() error {
	close(t.done)
	return nil
}

func (t *PipeListener) Addr() net.Addr {
	return &net.TCPAddr{}
}
