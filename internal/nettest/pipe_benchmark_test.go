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

package nettest_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"

	"go.temporal.io/server/internal/nettest"
)

type (
	socket interface {
		Accept() (net.Conn, error)
		Connect() (net.Conn, error)
		Close() error
	}
	networkSocket struct {
		*net.Dialer
		net.Listener
	}
	pipeSocket struct {
		*nettest.Pipe
	}

	socketBenchmark struct {
		socket socket
	}

	socketFactory interface {
		createSocket() (socket, error)
	}
	tcpSocketFactory  struct{}
	unixSocketFactory struct{}
	pipeSocketFactory struct{}

	socketFactoryBenchmark struct {
		name          string
		socketFactory socketFactory
	}
)

const (
	numMessages = 1000
	msg         = "Hello, Peer!"
)

// BenchmarkPipe benchmarks the performance of several socket implementations against Pipe.
// The test runs several iterations for each type of socket. Each iteration binds a listener and creates one server
// and one client. The server runs an echo service, and the client sends numMessages packets containing msg as the
// payload, asserting that the response is equal to the request.The output contains the number of iterations completed
// in the first column and the time per iteration, in nanoseconds, in the second column. Results with more iterations
// and a lesser ns/op value indicate a faster implementation.
func BenchmarkPipe(b *testing.B) {
	for _, tc := range []socketFactoryBenchmark{
		{
			name:          "pipe",
			socketFactory: pipeSocketFactory{},
		},
		{
			name:          "unix",
			socketFactory: unixSocketFactory{},
		},
		{
			name:          "tcp",
			socketFactory: tcpSocketFactory{},
		},
	} {

		b.Run(tc.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tc.benchmarkEchoService(b)
				}
			})
		})
	}
}

func (tc socketFactoryBenchmark) benchmarkEchoService(b *testing.B) {
	b.Helper()

	l, err := tc.socketFactory.createSocket()
	if err != nil {
		b.Error(err)
		return
	}

	defer func() {
		if err := l.Close(); err != nil {
			b.Error(err)
		}
	}()

	bi := socketBenchmark{socket: l}

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(2)

	go func() {
		defer wg.Done()
		bi.runEchoServer(b)
	}()
	go func() {
		defer wg.Done()
		bi.runClient(b)
	}()
}

func (bi socketBenchmark) runEchoServer(b *testing.B) {
	b.Helper()

	l := bi.socket

	c, err := l.Accept()
	if err != nil {
		b.Error(err)
		return
	}

	defer func() {
		if err := c.Close(); err != nil {
			b.Error(err)
		}
	}()

	buf := make([]byte, len(msg))

	for {
		n, err := c.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			b.Error(err)

			return
		}

		if _, err := c.Write(buf[:n]); err != nil {
			b.Error(err)
			return
		}
	}
}

func (bi socketBenchmark) runClient(b *testing.B) {
	b.Helper()

	l := bi.socket

	c, err := l.Connect()
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		if err := c.Close(); err != nil {
			b.Error(err)
		}
	}()

	buf := make([]byte, len(msg))

	for i := 0; i < numMessages; i++ {
		_, err = c.Write([]byte(msg))
		if err != nil {
			b.Fatal(err)
		}

		n, err := c.Read(buf)
		if err != nil {
			b.Fatal(err)
		}

		if !bytes.Equal(buf[:n], []byte(msg)) {
			b.Errorf("buffer: %s", buf[:n])
		}
	}
}

func (p pipeSocket) Accept() (net.Conn, error) {
	return p.Pipe.Accept(nil)
}

func (p pipeSocket) Connect() (net.Conn, error) {
	return p.Pipe.Connect(nil)
}

func (p pipeSocket) Close() error {
	return nil
}

func (s networkSocket) Connect() (net.Conn, error) {
	return net.Dial(s.Listener.Addr().Network(), s.Listener.Addr().String())
}

func (f pipeSocketFactory) createSocket() (socket, error) {
	return pipeSocket{nettest.NewPipe()}, nil
}

func (f unixSocketFactory) createSocket() (socket, error) {
	file, err := os.CreateTemp("", "*.sock")
	if err != nil {
		return nil, err
	}

	if err := file.Close(); err != nil {
		return nil, err
	}

	if err := os.Remove(file.Name()); err != nil {
		return nil, err
	}

	l, err := net.Listen("unix", file.Name())
	if err != nil {
		return nil, err
	}

	return networkSocket{Dialer: &net.Dialer{}, Listener: l}, nil
}

func (f tcpSocketFactory) createSocket() (socket, error) {
	l, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return nil, err
	}

	return networkSocket{Dialer: &net.Dialer{}, Listener: l}, nil
}
